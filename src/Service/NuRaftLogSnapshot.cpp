#include <unordered_set>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <Service/NuRaftCommon.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <sys/uio.h>
#include <Poco/File.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <IO/WriteHelpers.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_FSYNC;
extern const int CANNOT_CLOSE_FILE;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int CORRUPTED_DATA;
extern const int UNKNOWN_FORMAT_VERSION;
}

using nuraft::cs_new;

//const int KeeperSnapshotStore::SNAPSHOT_THREAD_NUM = 4;

const UInt32 KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE;

int openFileForWrite(const std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    errno = 0;
    int snap_fd = ::open(obj_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (snap_fd < 0)
    {
        LOG_ERROR(log, "Created new snapshot object {} failed, fd {}, error:{}", obj_path, snap_fd, strerror(errno));
        return -1;
    }
    return snap_fd;
}

bool isFileHeader(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'H', 'e', 'a', 'd'};
    };
    return magic == magic_num;
}

bool isFileTail(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'T', 'a', 'i', 'l'};
    };
    return magic == magic_num;
}

int openFileAndWriteHeader(std::string & obj_path, const SnapshotVersion version)
{
    int snap_fd = openFileForWrite(obj_path);
    if (snap_fd != -1) /// write header
    {
        union
        {
            uint64_t magic_num;
            uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'H', 'e', 'a', 'd'};
        };

        auto version_uint8 = static_cast<uint8_t>(version);
        if (write(snap_fd, &magic_num, 8) != 8)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write head magic to file descriptor");

        if (write(snap_fd, &version_uint8, 1) != 1)
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write version to file descriptor");
    }
    return snap_fd;
}

size_t writeTailAndClose(int snap_fd, UInt32 checksum)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'T', 'a', 'i', 'l'};
    };

    if (write(snap_fd, &magic_num, 8) != 8)
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write tail magic to file descriptor");

    if (write(snap_fd, &checksum, 4) != 4)
        throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write file checksum to file descriptor");

    if (::fsync(snap_fd) == -1)
    {
        throw Exception(ErrorCodes::CANNOT_FSYNC, "Cannot fsync");
    }
    if (::close(snap_fd) == -1)
    {
        throw Exception(ErrorCodes::CANNOT_CLOSE_FILE, "Cannot close file");
    }
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "write checksum {}", checksum);
    return sizeof(UInt32);
}

int openFileForRead(std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    errno = 0;
    int snap_fd = ::open(obj_path.c_str(), O_RDWR);
    if (snap_fd < 0)
    {
        LOG_ERROR(log, "Open snapshot object {} failed, fd {}, error:{}", obj_path, snap_fd, strerror(errno));
        return -1;
    }
    return snap_fd;
}

std::pair<size_t, UInt32> saveBatch(int & snap_fd, ptr<SnapshotBatchPB> & batch_pb, const std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));

    std::string str_buf;
    batch_pb->SerializeToString(&str_buf);

    SnapshotBatchHeader header;
    header.data_length = str_buf.size();
    header.data_crc = DB::getCRC32(str_buf.c_str(), str_buf.size());

    struct iovec vec[2];
    vec[0].iov_base = &header;
    vec[0].iov_len = SnapshotBatchHeader::HEADER_SIZE;
    vec[1].iov_base = const_cast<void *>(reinterpret_cast<const void *>(str_buf.c_str()));
    vec[1].iov_len = header.data_length;

    errno = 0;
    //pwritev(seg_fd, vec, 2, file_size);
    ssize_t ret = writev(snap_fd, vec, 2);
    if (ret < 0 || ret != static_cast<ssize_t>(vec[0].iov_len + vec[1].iov_len))
    {
        LOG_ERROR(log, "Write {}, real size {}, error:{}", ret, vec[0].iov_len + vec[1].iov_len, strerror(errno));
        return {-1, 0};
    }
    LOG_DEBUG(log, "Save object batch, path {}, length {}, crc {}", obj_path, header.data_length, header.data_crc);
    return { SnapshotBatchHeader::HEADER_SIZE + header.data_length, header.data_crc };
}

UInt32 updateCheckSum(UInt32 checksum, UInt32 data_crc)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    union
    {
        UInt64 data;
        UInt32 crc[2];
    };
    crc[0] = checksum;
    crc[1] = data_crc;
    UInt32 new_checksum = DB::getCRC32(reinterpret_cast<const char *>(&data), 8);
    LOG_INFO(log, "new_checksum {}, checksum {}, header.data_crc {}.", new_checksum, checksum, data_crc);
    return new_checksum;
}

std::pair<size_t, UInt32> saveBatchAndUpdateCheckSum(int & snap_fd, ptr<SnapshotBatchPB> & batch_pb, const std::string & obj_path, UInt32 checksum)
{
    auto [save_size, data_crc] = saveBatch(snap_fd, batch_pb, obj_path);
    return { save_size, updateCheckSum(checksum, data_crc) };
}

static String toString(const Coordination::ACLs & acls)
{
    WriteBufferFromOwnString ret;
    String left_bracket = "[ ";
    String comma = ", ";
    String right_bracket = " ]";
    ret.write(left_bracket.c_str(), left_bracket.length());
    for (auto & acl : acls)
    {
        auto permissions = std::to_string(acl.permissions);
        ret.write(permissions.c_str(), permissions.length());
        ret.write(comma.c_str(), comma.length());
        ret.write(acl.scheme.c_str(), acl.scheme.length());
        ret.write(comma.c_str(), comma.length());
        ret.write(acl.id.c_str(), acl.id.length());
        ret.write(comma.c_str(), comma.length());
    }
    ret.write(right_bracket.c_str(), right_bracket.length());
    return ret.str();
}

//return end object index
void createObjectContainer(
    SvsKeeperStorage::Container::InnerMap & innerMap,
    UInt32 obj_idx,
    UInt16 object_count,
    UInt32 save_batch_size,
    std::map<ulong, std::string> & objects_path,
    const SvsKeeperStorage & storage,
    const SnapshotVersion version)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    UInt32 max_node_size = (innerMap.size() - 1) / object_count + 1;
    auto container_it = innerMap.getMap().begin();

    ptr<SnapshotBatchPB> batch_pb;
    UInt32 node_index = 1;
    int snap_fd = -1;
    size_t file_size = 0;
    std::string obj_path;
    LOG_INFO(log, "Begin create snapshot object for container, max node size {}, node index {}", max_node_size, node_index);
    UInt32 checksum = 0;
    while (node_index <= innerMap.size())
    {
        if (max_node_size == 1 || node_index % max_node_size == 1)
        {
            if (snap_fd > 0)
            {
                file_size += writeTailAndClose(snap_fd, checksum);
            }
            checksum = 0;
            obj_path = objects_path[obj_idx];
            snap_fd = openFileAndWriteHeader(obj_path, CURRENT_SNAPSHOT_VERSION);
            if (snap_fd > 0)
            {
                LOG_INFO(log, "Create snapshot object, path {}, obj_idx {}, node index {}", obj_path, obj_idx, node_index);
                obj_idx++;
            }
            else
            {
                return;
            }
        }

        //first node
        if (node_index % save_batch_size == 1 || node_index % max_node_size == 1 || node_index == innerMap.size() + 1)
        {
            batch_pb = cs_new<SnapshotBatchPB>();
            batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_DATA);
            LOG_DEBUG(log, "New node batch, index {}, snap type {}", node_index, batch_pb->batch_type());
        }

        SnapshotItemPB * data = batch_pb->add_data();
        WriteBufferFromNuraftBuffer out;
        ptr<KeeperNode> node = container_it->second;
        Coordination::write(container_it->first, out);
        Coordination::write(node->data, out);

        if (version >= V1)
        {
            writeBinary(node->acl_id, out);
            LOG_TRACE(log, "createObjectContainer path {}, acl_id {}", container_it->first, node->acl_id);
        }
        else if (version == V0)
        {
            const auto & acls = storage.acl_map.convertNumber(node->acl_id);
            Coordination::write(acls, out);
        }

        Coordination::write(node->is_ephemeral, out);
        Coordination::write(node->is_sequental, out);
        Coordination::write(node->stat, out);

        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index % max_node_size == 0 || node_index == innerMap.size())
        {
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(snap_fd, batch_pb, obj_path, checksum);
            checksum = new_checksum;
            if (save_size > 0)
            {
                file_size += save_size;
            }
            else
            {
                return;
            }
        }

        container_it++;
        node_index++;
    }

    if (snap_fd > 0)
    {
        file_size += writeTailAndClose(snap_fd, checksum);
    }

    LOG_INFO(
        log,
        "Finish create snapshot object for container, max node size {}, node index {}, file size {}",
        max_node_size,
        node_index,
        file_size);
}

void createObjectEphemeral(
    SvsKeeperStorage::Ephemerals & ephemerals,
    UInt32 obj_idx,
    UInt16 object_count,
    UInt32 save_batch_size,
    std::map<ulong, std::string> & objects_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    UInt32 max_node_size = (ephemerals.size() - 1) / object_count + 1;
    auto ephemeral_it = ephemerals.begin();

    ptr<SnapshotBatchPB> batch_pb;
    std::string obj_path;
    UInt32 node_index = 1;
    int snap_fd = -1;
    size_t file_size = 0;
    LOG_INFO(log, "Begin create snapshot ephemeral object, max node size {}, node index {}", max_node_size, node_index);
    UInt32 checksum = 0;
    while (node_index <= ephemerals.size())
    {
        if (max_node_size == 1 || node_index % max_node_size == 1)
        {
            if (snap_fd > 0)
            {
                ::close(snap_fd);
            }
            obj_path = objects_path[obj_idx];
            snap_fd = openFileAndWriteHeader(obj_path, CURRENT_SNAPSHOT_VERSION);
            if (snap_fd > 0)
            {
                LOG_INFO(
                    log, "Create snapshot ephemeral object success, path {}, obj_idx {}, node index {}", obj_path, obj_idx, node_index);
                obj_idx++;
            }
            else
            {
                return;
            }
        }

        //first node
        if (node_index % save_batch_size == 1 || node_index % max_node_size == 1 || node_index == ephemerals.size() + 1)
        {
            batch_pb = cs_new<SnapshotBatchPB>();
            batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_DATA_EPHEMERAL);
            LOG_DEBUG(log, "New node batch, index {}, snap type {}", node_index, batch_pb->batch_type());
        }

        SnapshotItemPB * data = batch_pb->add_data();
        WriteBufferFromNuraftBuffer out;
        Coordination::write(ephemeral_it->first, out);
        Coordination::write(ephemeral_it->second.size(), out);
        for (const auto & path : ephemeral_it->second)
        {
            Coordination::write(path, out);
        }

        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index % max_node_size == 0 || node_index == ephemerals.size())
        {
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(snap_fd, batch_pb, obj_path, checksum);
            checksum = new_checksum;
            if (save_size > 0)
            {
                file_size += save_size;
            }
            else
            {
                break;
            }
        }
        ephemeral_it++;
        node_index++;
    }

    if (snap_fd > 0)
    {
        file_size += writeTailAndClose(snap_fd, checksum);
    }

    LOG_INFO(
        log,
        "Finish create snapshot ephemeral object, max node size {}, node index {}, file size {}",
        max_node_size,
        node_index,
        file_size);
}

//Create sessions
void createSessions(SvsKeeperStorage & storage, UInt32 save_batch_size, const SnapshotVersion version, std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    ptr<SnapshotBatchPB> batch_pb;
    int snap_fd = openFileAndWriteHeader(obj_path, CURRENT_SNAPSHOT_VERSION);
    if (snap_fd > 0)
    {
        LOG_INFO(log, "Create sessions object, path {}", obj_path);
    }
    else
    {
        LOG_WARNING(log, "Create sessions object failed, path {}, fd {}", obj_path, snap_fd);
        return;
    }

    auto session_it = storage.session_and_timeout.begin();
    UInt32 node_index = 1;
    size_t file_size = 0;

    UInt32 checksum = 0;

    while (node_index <= storage.session_and_timeout.size())
    {
        //first node
        if (node_index % save_batch_size == 1)
        {
            batch_pb = cs_new<SnapshotBatchPB>();
            batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_SESSION);
            LOG_DEBUG(log, "New node batch, index {}, snap type {}", node_index, batch_pb->batch_type());
        }

        SnapshotItemPB * data_pb = batch_pb->add_data();
        WriteBufferFromNuraftBuffer out;
        Coordination::write(session_it->first, out); //SessionID
        Coordination::write(session_it->second, out); //Timeout_ms

        LOG_TRACE(log, "SessionID {}, Timeout_ms {}", session_it->first, session_it->second);

        if (version >= V1)
        {
            SvsKeeperStorage::AuthIDs ids;
            if (storage.session_and_auth.count(session_it->first))
                ids = storage.session_and_auth.at(session_it->first);

            writeBinary(ids.size(), out);
            for (const auto & [scheme, id] : ids)
            {
                writeBinary(scheme, out);
                writeBinary(id, out);
            }
        }

        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data_pb->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index == storage.session_and_timeout.size())
        {
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(snap_fd, batch_pb, obj_path, checksum);
            checksum = new_checksum;
            if (save_size > 0)
            {
                file_size += save_size;
            }
            else
            {
                break;
            }
        }
        session_it++;
        node_index++;
    }

    LOG_INFO(
        log, "Finish create sessions object, object path {}, sessions count {}, file size {}", obj_path, storage.session_and_timeout.size(), file_size);

    if (snap_fd > 0)
    {
        file_size += writeTailAndClose(snap_fd, checksum);
    }

    LOG_INFO(
        log, "Finish create sessions object, object path {}, sessions count {}, file size {}", obj_path, storage.session_and_timeout.size(), file_size);
}

//Save map<string, string> or map<string, uint64>
template <typename T>
void createMap(T & snap_map, UInt32 save_batch_size, std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    ptr<SnapshotBatchPB> batch_pb;
    int snap_fd = openFileAndWriteHeader(obj_path, CURRENT_SNAPSHOT_VERSION);
    if (snap_fd > 0)
    {
        LOG_INFO(log, "Create string map object, path {}", obj_path);
    }
    else
    {
        LOG_WARNING(log, "Create string map object failed, path {}, fd {}", obj_path, snap_fd);
        return;
    }

    auto map_it = snap_map.begin();
    UInt32 node_index = 1;
    size_t file_size = 0;
    UInt32 checksum = 0;
    while (node_index <= snap_map.size())
    {
        //first node
        if (node_index % save_batch_size == 1)
        {
            batch_pb = cs_new<SnapshotBatchPB>();
            if constexpr (std::is_same_v<T, KeeperSnapshotStore::StringMap>)
            {
                batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_STRINGMAP);
            }
            else if constexpr (std::is_same_v<T, KeeperSnapshotStore::IntMap>)
            {
                batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_UINTMAP);
            }
            LOG_DEBUG(log, "New node batch, index {}, snap type {}", node_index, batch_pb->batch_type());
        }

        SnapshotItemPB * data_pb = batch_pb->add_data();
        WriteBufferFromNuraftBuffer out;
        Coordination::write(map_it->first, out); //Key
        Coordination::write(map_it->second, out); //Value
        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data_pb->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index == snap_map.size())
        {
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(snap_fd, batch_pb, obj_path, checksum);
            checksum = new_checksum;
            if (save_size > 0)
            {
                file_size += save_size;
            }
            else
            {
                break;
            }
        }
        map_it++;
        node_index++;
    }

    LOG_INFO(
        log, "Finish create string map object, object path {}, map count {}, file size {}", obj_path, snap_map.size(), file_size);

    if (snap_fd > 0)
    {
        file_size += writeTailAndClose(snap_fd, checksum);
    }
}

void createAclMaps(SvsKeeperStorage & storage, UInt32 save_batch_size, std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    ptr<SnapshotBatchPB> batch_pb;
    int snap_fd = openFileAndWriteHeader(obj_path, CURRENT_SNAPSHOT_VERSION);
    if (snap_fd > 0)
    {
        LOG_INFO(log, "Create acl map object, path {}", obj_path);
    }
    else
    {
        LOG_WARNING(log, "Create acl map object failed, path {}, fd {}", obj_path, snap_fd);
        return;
    }

    const auto & acl_map = storage.acl_map.getMapping();
    auto acl_it = acl_map.begin();
    UInt32 node_index = 1;
    size_t file_size = 0;
    UInt32 checksum = 0;

    while (node_index <= acl_map.size())
    {
        //first node
        if (node_index % save_batch_size == 1)
        {
            batch_pb = cs_new<SnapshotBatchPB>();
            batch_pb->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_ACLMAP);
            LOG_DEBUG(log, "New node batch, index {}, snap type {}", node_index, batch_pb->batch_type());
        }

        SnapshotItemPB * data_pb = batch_pb->add_data();
        WriteBufferFromNuraftBuffer out;

        /// Serialize ACLs MAP
//        writeBinary(acl_map.size(), out);
        const auto & [acl_id, acls] = *acl_it;
        LOG_TRACE(log, "createAclMaps acl_id {}, node_acls {}", acl_id, toString(acls));
        writeBinary(acl_id, out);
        writeBinary(acls.size(), out);
        for (const auto & acl : acls)
        {
            writeBinary(acl.permissions, out);
            writeBinary(acl.scheme, out);
            writeBinary(acl.id, out);
        }

        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data_pb->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index == acl_map.size())
        {
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(snap_fd, batch_pb, obj_path, checksum);
            checksum = new_checksum;
            if (save_size > 0)
            {
                file_size += save_size;
            }
            else
            {
                break;
            }

        }
        acl_it++;
        node_index++;
    }

    LOG_INFO(
        log, "Finish create acl map object, object path {}, acl count {}, file size {}", obj_path, acl_map.size(), file_size);

    if (snap_fd > 0)
    {
        file_size += writeTailAndClose(snap_fd, checksum);
    }
}

void KeeperSnapshotStore::getObjectPath(ulong object_id, std::string & obj_path)
{
    char path_buf[1024];
    snprintf(path_buf, 1024, SNAPSHOT_FILE_NAME, curr_time.c_str(), last_log_index, object_id);
    obj_path = path_buf;
    obj_path = snap_dir + "/" + obj_path;
}

void KeeperSnapshotStore::getFileTime(const std::string & file_name, std::string & time)
{
    auto it1 = file_name.find("_");
    auto it2 = file_name.find("_", it1 + 1);
    time = file_name.substr(it1 + 1, it2 - it1);
}

size_t KeeperSnapshotStore::createObjects(SvsKeeperStorage & storage)
{
    if (snap_meta->size() == 0)
    {
        return 0;
    }
    if (Directory::createDir(snap_dir) != 0)
    {
        LOG_ERROR(log, "Fail to create snapshot directory {}", snap_dir);
        return 0;
    }
    //BackendTimer::getCurrentTime(curr_time);

    size_t block_max_size = 0;

    for (UInt32 i = 0; i < storage.container.getBlockNum(); i++)
    {
        block_max_size = std::max(block_max_size, storage.container.getMap(i).size());
        LOG_DEBUG(log, "Block index {} size {} , block max size {}", i, storage.container.getMap(i).size(), block_max_size);
    }

    size_t container_object_count = (block_max_size - 1) / max_object_node_size + 1;

    size_t ephemeral_object_count = 0;
    if (storage.ephemerals.size() > 0)
    {
        ephemeral_object_count = (storage.ephemerals.size() - 1) / max_object_node_size + 1;
    }

    //Normal node objects、Ephemeral node objects、Sessions、Others(int_map)、ACL_MAP、TODO cluster_config
    size_t obj_size = storage.container.getBlockNum() * container_object_count + ephemeral_object_count + 1 + 1 + 1;

    LOG_DEBUG(
        log,
        "Block num {}, block max size {} , container_object_count {}, ephemeral_object_count {}",
        storage.container.getBlockNum(),
        block_max_size,
        container_object_count,
        ephemeral_object_count);

    std::string obj_path;
    std::map<ulong, std::string> objects;
    //[1, obj_size]
    for (size_t i = 1; i <= obj_size; i++)
    {
        getObjectPath(i, obj_path);
        objects_path.emplace(i, obj_path);
        objects.emplace(i, obj_path);
        LOG_DEBUG(log, "Object index {}, path {}", i, obj_path);
    }

    UInt32 batch_size = save_batch_size;

    ThreadPool container_thread_pool(SNAPSHOT_THREAD_NUM + 1);
    for (UInt32 thread_idx = 0; thread_idx < SNAPSHOT_THREAD_NUM; thread_idx++)
    {
        container_thread_pool.trySchedule([thread_idx, &storage, &objects, container_object_count, batch_size, snp_version = version] {
            Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.ContainerThread"));
            int obj_idx = 0;
            for (UInt32 i = 0; i < storage.container.getBlockNum(); i++)
            {
                if (i % SNAPSHOT_THREAD_NUM == thread_idx)
                {
                    obj_idx = i * container_object_count + 1;
                    LOG_INFO(
                        thread_log,
                        "Create object container, thread_idx {}, block_num {}, obj_index {}, obj count {}, batch_size {}, total obj count "
                        "{}",
                        thread_idx,
                        i,
                        obj_idx,
                        container_object_count,
                        batch_size,
                        objects.size());
                    createObjectContainer(storage.container.getMap(i), obj_idx, container_object_count, batch_size, objects, storage, snp_version);
                }
            }
        });
    }

    ThreadPool ephemeral_thread_pool(1);
    if (storage.ephemerals.size() > 0)
    {
        int obj_idx = storage.container.getBlockNum() * container_object_count + 1;
        ephemeral_thread_pool.trySchedule([&storage, &objects, obj_idx, ephemeral_object_count, batch_size] {
            Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.EphemeralThread"));
            LOG_DEBUG(
                thread_log,
                "Create object ephemeral, obj index {}, obj count {}, batch_size {}, total obj count {}",
                obj_idx,
                ephemeral_object_count,
                batch_size,
                objects.size());
            createObjectEphemeral(storage.ephemerals, obj_idx, ephemeral_object_count, batch_size, objects);
        });
    }
    container_thread_pool.wait();
    ephemeral_thread_pool.wait();

    //Save sessions
    createSessions(storage, save_batch_size, version, objects[obj_size - 2]);

    IntMap int_map;
    int_map["ZXID"] = storage.zxid;
    int_map["SESSIONID"] = storage.session_id_counter;

    //Save uint map
    createMap(int_map, save_batch_size, objects[obj_size - 1]);

    if (version >= V1)
    {
        createAclMaps(storage, save_batch_size, objects[obj_size]);
    }

    return obj_size;
}

void KeeperSnapshotStore::init(std::string create_time = "")
{
    if (create_time.empty())
    {
        BackendTimer::getCurrentTime(curr_time);
    }
    else
    {
        curr_time = create_time;
    }
    curr_time_t = BackendTimer::parseTime(curr_time);
}

bool KeeperSnapshotStore::loadHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head)
{
    head.reset();
    errno = 0;
    if (readUInt32(fs, head.data_length) != 0)
    {
        if (!fs->eof())
        {
            LOG_ERROR(log, "Cant read header data_length from snapshot file, error:{}.", strerror(errno));
        }
        return false;
    }

    if (readUInt32(fs, head.data_crc) != 0)
    {
        if (!fs->eof())
        {
            LOG_ERROR(log, "Cant read header data_crc from snapshot file, error:{}.", strerror(errno));
        }
        return false;
    }
    //LOG_INFO(log, "Offset {}, header data length {}, data crc {}", offset, head->data_length, head->data_crc);
    return true;
}

bool KeeperSnapshotStore::parseOneObject(std::string obj_path, SvsKeeperStorage & storage)
{
    ptr<std::fstream> snap_fs = cs_new<std::fstream>();
    snap_fs->open(obj_path, std::ios::in | std::ios::binary);
    if (snap_fs->fail())
    {
        LOG_ERROR(log, "Open snapshot object {} for read failed, error:{}", obj_path, strerror(errno));
        return false;
    }
    snap_fs->seekg(0, snap_fs->end);
    size_t file_size = snap_fs->tellg();
    snap_fs->seekg(0, snap_fs->beg);

    LOG_INFO(log, "Open snapshot object {} for read,file size {}", obj_path, file_size);

    size_t read_size = 0;
    SnapshotBatchHeader header;
    UInt32 checksum = 0;
    SnapshotVersion version_ = CURRENT_SNAPSHOT_VERSION;
    while (!snap_fs->eof())
    {
        size_t cur_read_size = read_size;
        UInt64 magic;
        readUInt64(snap_fs, magic);
        read_size += 8;
        if (isFileHeader(magic))
        {
            LOG_INFO(log, "obj_path {}, read file header", obj_path);
            char * buf = reinterpret_cast<char *>(&version_);
            snap_fs->read(buf, sizeof(uint8_t));
            read_size += 1;
        }
        else if (isFileTail(magic))
        {
            UInt32 file_checksum;
            char * buf = reinterpret_cast<char *>(&file_checksum);
            snap_fs->read(buf, sizeof(UInt32));
            read_size += 4;
            LOG_INFO(log, "obj_path {}, file_checksum {}, checksum {}.", obj_path, file_checksum, checksum);
            if (file_checksum != checksum)
                throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "snapshot {} checksum doesn't match", obj_path);
            break;
        }
        else
        {
            LOG_INFO(log, "obj_path {}, didn't read the header and tail of the file", obj_path);
            snap_fs->seekg(cur_read_size);
            read_size = cur_read_size;
        }
        if (!loadHeader(snap_fs, header))
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "snapshot {} load header error", obj_path);
        }

        checksum = updateCheckSum(checksum, header.data_crc);
        char * body_buf = new char[header.data_length];
        read_size += (SnapshotBatchHeader::HEADER_SIZE + header.data_length);
        errno = 0;
        if (!snap_fs->read(body_buf, header.data_length))
        {
            LOG_ERROR(log, "Cant read snapshot object file {}, error:{}.", obj_path, strerror(errno));
            delete[] body_buf;
            return false;
        }

        //LOG_INFO(
        //    log, "Load data length {}, crc {}, end point {}, is eof {}", header.data_length, header.data_crc, read_size, snap_fs->eof());

        if (!verifyCRC32(body_buf, header.data_length, header.data_crc))
        {
            LOG_ERROR(log, "Found corrupted data, file {}", obj_path);
            delete[] body_buf;
            return false;
        }
        SnapshotBatchPB batch_pb;
        batch_pb.ParseFromString(std::string(body_buf, header.data_length));
        switch (batch_pb.batch_type())
        {
            case SnapshotTypePB::SNAPSHOT_TYPE_DATA: {
                LOG_INFO(log, "Load batch size {}, end point {}", batch_pb.data_size(), read_size);
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(data.size() + 1);
                    //buf->put(data.data(), data.size());
                    buf->put(data);
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    ptr<KeeperNode> node = cs_new<KeeperNode>();
                    std::string key;
                    try
                    {
                        Coordination::read(key, in);
                        Coordination::read(node->data, in);
                        if (version_ >= SnapshotVersion::V1)
                        {
                            readBinary(node->acl_id, in);
                        }
                        else if (version_ == SnapshotVersion::V0)
                        {
                            /// Deserialize ACL
                            Coordination::ACLs acls;
                            Coordination::read(acls, in);
                            node->acl_id = storage.acl_map.convertACLs(acls);
                            LOG_TRACE(log, "parseOneObject path {}, acl_id {}, node_acls {}", key, node->acl_id, toString(acls));
                        }

                        /// Some strange ACLID during deserialization from ZooKeeper
                        if (node->acl_id == std::numeric_limits<uint64_t>::max())
                            node->acl_id = 0;

                        storage.acl_map.addUsage(node->acl_id);
                        LOG_TRACE(log, "parseOneObject path {}, acl_id {}", key, node->acl_id);

                        Coordination::read(node->is_ephemeral, in);
                        Coordination::read(node->is_sequental, in);
                        Coordination::read(node->stat, in);
                        LOG_TRACE(log, "Read key {}", key);
                        storage.container.emplace(key, std::move(node));
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Cant read snapshot data {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            key,
                            e.displayText());
                        break;
                    }
                }
            }
            break;
            case SnapshotTypePB::SNAPSHOT_TYPE_DATA_EPHEMERAL: {
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(data.size() + 1);
                    buf->put(data);
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    Int64 session_id;
                    size_t path_size;
                    try
                    {
                        Coordination::read(session_id, in);
                        Coordination::read(path_size, in);
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Cant read type_ephemeral snapshot {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            e.displayText());
                        break;
                    }

                    std::unordered_set<std::string> set;
                    for (size_t i = 0; i < path_size; i++)
                    {
                        std::string path;
                        try
                        {
                            Coordination::read(path, in);
                        }
                        catch (Coordination::Exception & e)
                        {
                            LOG_WARNING(
                                log,
                                "Cant read ephemeral path from snapshot {}, session id {}, path index {}, excepiton {}",
                                obj_path,
                                session_id,
                                data_idx,
                                e.displayText());
                            break;
                        }
                        LOG_TRACE(log, "Read session id {}, ephemerals {}", session_id, path);
                        set.emplace(path);
                    }
                    storage.ephemerals[session_id] = set;
                }
            }
            break;
            case SnapshotTypePB::SNAPSHOT_TYPE_CONFIG:
                break;
            case SnapshotTypePB::SNAPSHOT_TYPE_SERVER:
                break;
            case SnapshotTypePB::SNAPSHOT_TYPE_SESSION: {
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(data.size() + 1);
                    buf->put(data);
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    int64_t session_id;
                    int64_t timeout;
                    try
                    {
                        Coordination::read(session_id, in);
                        Coordination::read(timeout, in);
                        if (version_ >= SnapshotVersion::V1)
                        {
                            size_t session_auths_size;
                            readBinary(session_auths_size, in);

                            SvsKeeperStorage::AuthIDs ids;
                            size_t session_auth_counter = 0;
                            while (session_auth_counter < session_auths_size)
                            {
                                String scheme, id;
                                readBinary(scheme, in);
                                readBinary(id, in);
                                ids.emplace_back(SvsKeeperStorage::AuthID{scheme, id});

                                session_auth_counter++;
                            }
                            if (!ids.empty())
                                storage.session_and_auth[session_id] = ids;
                        }
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Cant read type_ephemeral snapshot {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            e.displayText());
                        break;
                    }
                    LOG_TRACE(log, "Read session id {}, timeout {}", session_id, timeout);
                    storage.addSessionID(session_id, timeout);
                }
            }
            break;
            case SnapshotTypePB::SNAPSHOT_TYPE_ACLMAP:
                if (version_ >= SnapshotVersion::V1)
                {
                    for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                    {
                        const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                        const std::string & data = item_pb.data();
                        ptr<buffer> buf = buffer::alloc(data.size() + 1);
                        buf->put(data);
                        buf->pos(0);
                        ReadBufferFromNuraftBuffer in(buf);

                        uint64_t acl_id;
                        readBinary(acl_id, in);

                        size_t acls_size;
                        readBinary(acls_size, in);
                        Coordination::ACLs acls;
                        for (size_t i = 0; i < acls_size; ++i)
                        {
                            Coordination::ACL acl;
                            readBinary(acl.permissions, in);
                            readBinary(acl.scheme, in);
                            readBinary(acl.id, in);
                            acls.push_back(acl);
                        }
                        LOG_TRACE(log, "parseOneObject acl_id {}, node_acls {}", acl_id, toString(acls));
                        storage.acl_map.addMapping(acl_id, acls);
                    }
                }
                break;
            case SnapshotTypePB::SNAPSHOT_TYPE_UINTMAP: {
                IntMap int_map;
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(data.size() + 1);
                    buf->put(data);
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    std::string key;
                    int64_t value;
                    try
                    {
                        Coordination::read(key, in);
                        Coordination::read(value, in);
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Cant read uint map snapshot {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            e.displayText());
                        break;
                    }
                    int_map[key] = value;
                }
                if (int_map.find("ZXID") != int_map.end())
                {
                    storage.zxid = int_map["ZXID"];
                }
                if (int_map.find("SESSIONID") != int_map.end())
                {
                    storage.session_id_counter = int_map["SESSIONID"];
                }
            }
            break;
            default:
                break;
        }
        delete[] body_buf;
    }
    return true;
}

void KeeperSnapshotStore::parseObject(SvsKeeperStorage & storage)
{
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > CURRENT_SNAPSHOT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    ThreadPool object_thread_pool(SNAPSHOT_THREAD_NUM);
    for (UInt32 thread_idx = 0; thread_idx < SNAPSHOT_THREAD_NUM; thread_idx++)
    {
        object_thread_pool.trySchedule([this, thread_idx, &storage] {
            Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.parseObjectThread"));
            UInt32 obj_idx = 0;
            for (auto it = this->objects_path.begin(); it != this->objects_path.end(); it++)
            {
                if (obj_idx % SNAPSHOT_THREAD_NUM == thread_idx)
                {
                    LOG_INFO(
                        thread_log,
                        "Parse object, thread_idx {}, obj_index {}, path {}, obj size {}",
                        thread_idx,
                        it->first,
                        it->second,
                        this->objects_path.size());
                    try
                    {
                        this->parseOneObject(it->second, storage);
                    }
                    catch(Exception & e)
                    {
                        LOG_ERROR(log, "parseOneObject error {}, {}", it->second, getExceptionMessage(e, true));
                    }
                }
                obj_idx++;
            }
        });
    }
    object_thread_pool.wait();

    storage.buildPathChildren();

    auto node = storage.container.get("/");
    if (node != nullptr)
    {
        LOG_INFO(log, "Root path children count {}", node->children.size());
    }
    else
    {
        LOG_INFO(log, "Cant find root path");
    }
}

bool KeeperSnapshotStore::existObject(ulong obj_id)
{
    return (objects_path.find(obj_id) != objects_path.end());
}

void KeeperSnapshotStore::loadObject(ulong obj_id, ptr<buffer> & buffer)
{
    if (!existObject(obj_id))
    {
        LOG_WARNING(log, "Not exist object {}", obj_id);
        return;
    }

    std::string obj_path;
    getObjectPath(obj_id, obj_path);

    int snap_fd = openFileForRead(obj_path);
    if (snap_fd < 0)
    {
        return;
    }

    ::lseek(snap_fd, 0, SEEK_SET);
    size_t file_size = ::lseek(snap_fd, 0, SEEK_END);
    ::lseek(snap_fd, 0, SEEK_SET);

    auto snap_version = static_cast<uint8_t>(version);

    buffer = buffer::alloc(file_size + sizeof(uint8_t));
    buffer->put(snap_version);

    size_t offset = 0;
    char read_buf[IO_BUFFER_SIZE];
    while (offset < file_size)
    {
        int buf_size = IO_BUFFER_SIZE;
        if (offset + IO_BUFFER_SIZE >= file_size)
        {
            buf_size = file_size - offset;
        }
        errno = 0;
        ssize_t ret = pread(snap_fd, read_buf, buf_size, offset);
        if (ret < 0)
        {
            LOG_ERROR(
                log,
                "Read object failed, path {}, offset {}, length {}, ret {}, erron {}, error:{}",
                obj_path,
                offset,
                buf_size,
                ret,
                errno,
                strerror(errno));
            break;
        }
        buffer->put_raw(reinterpret_cast<nuraft::byte *>(read_buf), buf_size);
        offset += buf_size;
    }

    if (snap_fd > 0)
    {
        ::close(snap_fd);
    }

    LOG_INFO(log, "Load object obj_id {}, file_size {}.", obj_id, file_size);
}

void KeeperSnapshotStore::saveObject(ulong obj_id, buffer & buffer)
{
    if (Directory::createDir(snap_dir) != 0)
    {
        LOG_ERROR(log, "Fail to create snapshot directory {}", snap_dir);
        return;
    }

    std::string obj_path;
    getObjectPath(obj_id, obj_path);

    int snap_fd = openFileForWrite(obj_path);
    if (snap_fd < 0)
    {
        return;
    }

    buffer.pos(0);
    size_t offset = 0;
    while (offset < buffer.size())
    {
        int buf_size;
        if (offset + IO_BUFFER_SIZE < buffer.size())
        {
            buf_size = IO_BUFFER_SIZE;
        }
        else
        {
            buf_size = buffer.size() - offset;
        }
        errno = 0;
        ssize_t ret = pwrite(snap_fd, buffer.get_raw(buf_size), buf_size, offset);
        if (ret < 0)
        {
            LOG_ERROR(
                log,
                "Write object failed, path {}, offset {}, length {}, ret {}, erron {}, error:{}",
                obj_path,
                offset,
                buf_size,
                ret,
                errno,
                strerror(errno));
            break;
        }
        offset += buf_size;
    }

    if (snap_fd > 0)
    {
        ::close(snap_fd);
    }

    objects_path[obj_id] = obj_path;
    LOG_INFO(log, "Save object path {}, file size {}, obj_id {}.", obj_path, buffer.size(), obj_id);
}

void KeeperSnapshotStore::addObjectPath(ulong obj_id, std::string & path)
{
    objects_path[obj_id] = path;
}

size_t KeeperSnapshotManager::createSnapshot(snapshot & meta, SvsKeeperStorage & storage)
{
    size_t store_size = storage.container.size() + storage.ephemerals.size();
    meta.set_size(store_size);
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
    snap_store->init();
    LOG_INFO(
        log,
        "Create snapshot last_log_term {}, last_log_idx {}, size {}, SM container size {}, SM ephemeral size {}",
        meta.get_last_log_term(),
        meta.get_last_log_idx(),
        meta.size(),
        storage.container.size(),
        storage.ephemerals.size());
    size_t obj_size = snap_store->createObjects(storage);
    snapshots[meta.get_last_log_idx()] = snap_store;
    return obj_size;
}

bool KeeperSnapshotManager::receiveSnapshot(snapshot & meta)
{
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
    snap_store->init();
    snapshots[meta.get_last_log_idx()] = snap_store;
    return true;
}

bool KeeperSnapshotManager::existSnapshot(const snapshot & meta)
{
    return snapshots.find(meta.get_last_log_idx()) != snapshots.end();
}

bool KeeperSnapshotManager::existSnapshotObject(const snapshot & meta, ulong obj_id)
{
    auto it = snapshots.find(meta.get_last_log_idx());
    if (it == snapshots.end())
    {
        LOG_INFO(log, "Not exists snapshot last_log_idx {}", meta.get_last_log_idx());
        return false;
    }
    ptr<KeeperSnapshotStore> store = it->second;
    bool exist = store->existObject(obj_id);
    LOG_INFO(log, "Find object {} by last_log_idx {} and object id {}", exist, meta.get_last_log_idx(), obj_id);
    return exist;
}

bool KeeperSnapshotManager::loadSnapshotObject(const snapshot & meta, ulong obj_id, ptr<buffer> & buffer)
{
    auto it = snapshots.find(meta.get_last_log_idx());
    if (it == snapshots.end())
    {
        LOG_WARNING(log, "Cant find snapshot, last log index {}", meta.get_last_log_idx());
        return false;
    }
    ptr<KeeperSnapshotStore> store = it->second;
    store->loadObject(obj_id, buffer);
    return true;
}

bool KeeperSnapshotManager::saveSnapshotObject(snapshot & meta, ulong obj_id, buffer & buffer)
{
    auto it = snapshots.find(meta.get_last_log_idx());
    ptr<KeeperSnapshotStore> store;
    if (it == snapshots.end())
    {
        meta.set_size(0);
        store = cs_new<KeeperSnapshotStore>(snap_dir, meta);
        store->init();
        snapshots[meta.get_last_log_idx()] = store;
    }
    else
    {
        store = it->second;
    }
    store->saveObject(obj_id, buffer);
    return true;
}

bool KeeperSnapshotManager::parseSnapshot(const snapshot & meta, SvsKeeperStorage & storage)
{
    auto it = snapshots.find(meta.get_last_log_idx());
    if (it == snapshots.end())
    {
        LOG_WARNING(log, "Cant find snapshot, last log index {}", meta.get_last_log_idx());
        return false;
    }
    ptr<KeeperSnapshotStore> store = it->second;
    store->parseObject(storage);
    LOG_INFO(
        log,
        "Finish parse snapshot, StateMachine container size {}, ephemeral size {}",
        storage.container.size(),
        storage.ephemerals.size());
    return true;
}

size_t KeeperSnapshotManager::loadSnapshotMetas()
{
    Poco::File file_dir(snap_dir);
    if (!file_dir.exists())
        return 0;

    std::vector<std::string> file_vec;
    file_dir.list(file_vec);
    char time_str[128];
    unsigned long log_last_index;
    unsigned long object_id;

    for (auto file : file_vec)
    {
        if (file.find("snapshot_") == file.npos)
        {
            LOG_INFO(log, "Skip no snapshot file {}", file);
            continue;
        }
        sscanf(file.c_str(), "snapshot_%[^_]_%lu_%lu", time_str, &log_last_index, &object_id);

        if (snapshots.find(log_last_index) == snapshots.end())
        {
            ptr<nuraft::cluster_config> config = cs_new<nuraft::cluster_config>(log_last_index, log_last_index - 1);
            nuraft::snapshot meta(log_last_index, 1, config);
            ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
            snap_store->init(time_str);
            snapshots[meta.get_last_log_idx()] = snap_store;
            LOG_INFO(log, "load filename {}, time {}, index {}, object id {}", file, time_str, log_last_index, object_id);
        }
        std::string full_path = snap_dir + "/" + file;
        snapshots[log_last_index]->addObjectPath(object_id, full_path);
    }
    LOG_INFO(log, "Load snapshot metas {} from snapshot directory {}", snapshots.size(), snap_dir);
    return snapshots.size();
}

ptr<snapshot> KeeperSnapshotManager::lastSnapshot()
{
    LOG_INFO(log, "Get last snapshot, snapshot size {}", snapshots.size());
    auto entry = snapshots.rbegin();
    if (entry == snapshots.rend())
        return nullptr;
    return entry->second->getSnapshot();
}

time_t KeeperSnapshotManager::getLastCreateTime()
{
    auto entry = snapshots.rbegin();
    if (entry == snapshots.rend())
        return 0L;
    return entry->second->getCreateTimeT();
}

size_t KeeperSnapshotManager::removeSnapshots()
{
    Int64 remove_count = static_cast<Int64>(snapshots.size()) - static_cast<Int64>(keep_max_snapshot_count);
    char time_str[128];
    unsigned long log_last_index;
    unsigned long object_id;
    while (remove_count > 0)
    {
        auto it = snapshots.begin();
        ulong remove_log_index = it->first;
        Poco::File dir_obj(snap_dir);
        if (dir_obj.exists())
        {
            std::vector<std::string> files;
            dir_obj.list(files);
            for (auto file : files)
            {
                if (file.find("snapshot_") == file.npos)
                {
                    LOG_INFO(log, "Skip no snapshot file {}", file);
                    continue;
                }
                sscanf(file.c_str(), "snapshot_%[^_]_%lu_%lu", time_str, &log_last_index, &object_id);
                if (remove_log_index == log_last_index)
                {
                    LOG_INFO(
                        log,
                        "remove_count {}, snapshot size {}, remove log index {}, file {}",
                        remove_count,
                        snapshots.size(),
                        remove_log_index,
                        file);
                    Poco::File(snap_dir + "/" + file).remove();
                    if (snapshots.find(remove_log_index) != snapshots.end())
                    {
                        snapshots.erase(it);
                    }
                }
            }
        }
        remove_count--;
    }
    return snapshots.size();
}
}

#ifdef __clang__
#    pragma clang diagnostic pop
#endif
