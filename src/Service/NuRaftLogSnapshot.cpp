#include <algorithm>
#include <fcntl.h>
#include <filesystem>
#include <stdio.h>
#include <stdlib.h>
#include <sys/uio.h>
#include <unistd.h>

#include <Poco/File.h>
#include <Poco/NumberFormatter.h>

#include <Common/Exception.h>
#include <Common/IO/WriteHelpers.h>
#include <Common/Stopwatch.h>

#include <Service/KeeperUtils.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperIO.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif


namespace RK
{
namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using nuraft::cs_new;
using Poco::NumberFormatter;

const UInt32 KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE;

const String MAGIC_SNAPSHOT_TAIL = "SnapTail";
const String MAGIC_SNAPSHOT_HEAD = "SnapHead";

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

/// snapshot object file header
bool isFileHeader(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'H', 'e', 'a', 'd'};
    };
    return magic == magic_num;
}

/// snapshot object file tail
bool isFileTail(UInt64 magic)
{
    union
    {
        uint64_t magic_num;
        uint8_t magic_array[8] = {'S', 'n', 'a', 'p', 'T', 'a', 'i', 'l'};
    };
    return magic == magic_num;
}

std::shared_ptr<WriteBufferFromFile> openFileAndWriteHeader(const String & path, const SnapshotVersion version)
{
    auto out = std::make_shared<WriteBufferFromFile>(path);
    out->write(MAGIC_SNAPSHOT_HEAD.data(), MAGIC_SNAPSHOT_HEAD.size());
    writeIntBinary(static_cast<uint8_t>(version), *out);
    return out;
}

void writeTailAndClose(std::shared_ptr<WriteBufferFromFile> & out, UInt32 checksum)
{
    out->write(MAGIC_SNAPSHOT_TAIL.data(), MAGIC_SNAPSHOT_TAIL.size());
    writeIntBinary(checksum, *out);
    out->close();
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

/// save batch data in snapshot object
std::pair<size_t, UInt32> saveBatch(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchPB> & batch)
{
    if (!batch)
        batch = cs_new<SnapshotBatchPB>();

    std::string str_buf;
    batch->SerializeToString(&str_buf);

    SnapshotBatchHeader header;
    header.data_length = str_buf.size();
    header.data_crc = RK::getCRC32(str_buf.c_str(), str_buf.size());

    writeIntBinary(header.data_length, *out);
    writeIntBinary(header.data_crc, *out);

    out->write(str_buf.c_str(), header.data_length);
    out->next();

    return {SnapshotBatchHeader::HEADER_SIZE + header.data_length, header.data_crc};
}

UInt32 updateCheckSum(UInt32 checksum, UInt32 data_crc)
{
    union
    {
        UInt64 data;
        UInt32 crc[2];
    };
    crc[0] = checksum;
    crc[1] = data_crc;
    return RK::getCRC32(reinterpret_cast<const char *>(&data), 8);
}

std::pair<size_t, UInt32>
saveBatchAndUpdateCheckSum(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchPB> & batch, UInt32 checksum)
{
    auto [save_size, data_crc] = saveBatch(out, batch);
    /// rebuild batch
    batch = cs_new<SnapshotBatchPB>();
    return {save_size, updateCheckSum(checksum, data_crc)};
}

void serializeAcls(ACLMap & acls, String path, UInt32 save_batch_size, SnapshotVersion version)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));

    const auto & acl_map = acls.getMapping();
    LOG_INFO(log, "Begin create snapshot acl object, acl size {}, path {}", acl_map.size(), path);

    auto out = openFileAndWriteHeader(path, version);
    ptr<SnapshotBatchPB> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (const auto & acl_it : acl_map)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
                checksum = new_checksum;
            }
            batch = cs_new<SnapshotBatchPB>();
            batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_ACLMAP);
        }

        /// append to batch
        SnapshotItemPB * entry = batch->add_data();
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(acl_it.first, buf);
        Coordination::write(acl_it.second, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last acl batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
}

[[maybe_unused]] size_t serializeEphemerals(KeeperStore::Ephemerals & ephemerals, std::mutex & mutex, String path, UInt32 save_batch_size)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot ephemeral object, node size {}, path {}", ephemerals.size(), path);

    ptr<SnapshotBatchPB> batch;

    std::lock_guard lock(mutex);

    if (ephemerals.empty())
    {
        LOG_INFO(log, "Create snapshot ephemeral nodes size is 0");
        return 0;
    }

    auto out = cs_new<WriteBufferFromFile>(path);
    uint64_t index = 0;
    for (auto & ephemeral_it : ephemerals)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                saveBatch(out, batch);
            }
            batch = cs_new<SnapshotBatchPB>();
            batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_DATA_EPHEMERAL);
        }

        /// append to batch
        SnapshotItemPB * entry = batch->add_data();
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(ephemeral_it.first, buf);
        Coordination::write(ephemeral_it.second.size(), buf);

        for (const auto & node_path : ephemeral_it.second)
        {
            Coordination::write(node_path, buf);
        }

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    saveBatch(out, batch);
    out->close();
    return 1;
}

/**
 * Serialize sessions and return the next_session_id before serialize
 */
int64_t serializeSessions(KeeperStore & store, UInt32 save_batch_size, const SnapshotVersion version, std::string & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));

    auto out = openFileAndWriteHeader(path, version);


    LOG_INFO(log, "Begin create snapshot session object, session size {}, path {}", store.session_and_timeout.size(), path);

    std::lock_guard lock(store.session_mutex);
    std::lock_guard acl_lock(store.auth_mutex);

    int64_t next_session_id = store.session_id_counter;
    ptr<SnapshotBatchPB> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (auto & session_it : store.session_and_timeout)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
                checksum = new_checksum;
            }
            batch = cs_new<SnapshotBatchPB>();
            batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_SESSION);
        }

        /// append to batch
        SnapshotItemPB * entry = batch->add_data();
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(session_it.first, buf); //NewSession
        Coordination::write(session_it.second, buf); //Timeout_ms

        Coordination::AuthIDs ids;
        if (store.session_and_auth.count(session_it.first))
            ids = store.session_and_auth.at(session_it.first);
        Coordination::write(ids, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
    checksum = new_checksum;
    writeTailAndClose(out, checksum);

    return next_session_id;
}

/**
 * Save map<string, string> or map<string, uint64>
 */
template <typename T>
void serializeMap(T & snap_map, UInt32 save_batch_size, SnapshotVersion version, std::string & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot map object, map size {}, path {}", snap_map.size(), path);

    auto out = openFileAndWriteHeader(path, version);
    ptr<SnapshotBatchPB> batch;

    uint64_t index = 0;
    UInt32 checksum = 0;

    for (auto & it : snap_map)
    {
        /// flush and rebuild batch
        if (index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
                checksum = new_checksum;
            }

            batch = cs_new<SnapshotBatchPB>();
            if constexpr (std::is_same_v<T, KeeperSnapshotStore::StringMap>)
                batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_STRINGMAP);
            else if constexpr (std::is_same_v<T, KeeperSnapshotStore::IntMap>)
                batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_UINTMAP);
            else
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Only support string and int map.");
        }

        /// append to batch
        SnapshotItemPB * entry = batch->add_data();
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(it.first, buf);
        Coordination::write(it.second, buf);

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    auto [_, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
    checksum = new_checksum;
    writeTailAndClose(out, checksum);
}

void KeeperSnapshotStore::getObjectPath(ulong object_id, std::string & obj_path)
{
    char path_buf[1024];
    snprintf(path_buf, 1024, SNAPSHOT_FILE_NAME, curr_time.c_str(), last_log_index, object_id);
    obj_path = path_buf;
    obj_path = snap_dir + "/" + obj_path;
}

size_t KeeperSnapshotStore::getObjectIdx(const std::string & file_name)
{
    auto it1 = file_name.find_last_of('_');
    return std::stoi(file_name.substr(it1 + 1, file_name.size() - it1));
}

size_t KeeperSnapshotStore::serializeDataTree(KeeperStore & storage)
{
    std::shared_ptr<WriteBufferFromFile> out;
    ptr<SnapshotBatchPB> batch;

    uint64_t processed = 0;
    uint32_t checksum = 0;

    serializeNode(out, batch, storage, "/", processed, checksum);
    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
    LOG_INFO(log, "Creating snapshot processed data size {}, current zxid {}", processed, storage.zxid);

    return getObjectIdx(out->getFileName());
}

void KeeperSnapshotStore::serializeNode(
    ptr<WriteBufferFromFile> & out,
    ptr<SnapshotBatchPB> & batch,
    KeeperStore & store,
    const String & path,
    uint64_t & processed,
    uint32_t & checksum)
{
    auto node = store.container.get(path);

    /// In case of node is deleted
    if (!node)
        return;

    std::shared_ptr<KeeperNode> node_copy;
    {
        std::shared_lock lock(node->mutex);
        node_copy = node->clone();
    }

    if (processed % max_object_node_size == 0)
    {
        /// time to create new snapshot object
        uint64_t obj_id = processed / max_object_node_size;

        if (obj_id != 0)
        {
            /// flush last batch data
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
            checksum = new_checksum;

            /// close current object file
            writeTailAndClose(out, checksum);
            /// reset checksum
            checksum = 0;
        }
        String new_obj_path;
        /// for there are 4 objects before data objects
        getObjectPath(obj_id + 4, new_obj_path);

        LOG_INFO(log, "Create new snapshot object {}, path {}", obj_id + 4, new_obj_path);
        out = openFileAndWriteHeader(new_obj_path, version);
    }

    /// flush and rebuild batch
    if (processed % save_batch_size == 0)
    {
        /// skip flush the first batch
        if (processed != 0)
        {
            /// flush data in batch to file
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSum(out, batch, checksum);
            checksum = new_checksum;
        }
        else
        {
            if (!batch)
                batch = cs_new<SnapshotBatchPB>();
        }
    }

    LOG_TRACE(log, "Append node path {}", path);
    appendNodeToBatch(batch, path, node_copy);
    processed++;

    String path_with_slash = path;
    if (path != "/")
        path_with_slash += '/';

    for (const auto & child : node->children)
        serializeNode(out, batch, store, path_with_slash + child, processed, checksum);
}

void KeeperSnapshotStore::appendNodeToBatch(ptr<SnapshotBatchPB> batch, const String & path, std::shared_ptr<KeeperNode> node)
{
    SnapshotItemPB * entry = batch->add_data();
    WriteBufferFromNuraftBuffer buf;

    Coordination::write(path, buf);
    Coordination::write(node->data, buf);
    Coordination::write(node->acl_id, buf);
    Coordination::write(node->is_ephemeral, buf);
    Coordination::write(node->is_sequential, buf);
    Coordination::write(node->stat, buf);

    ptr<buffer> data = buf.getBuffer();
    data->pos(0);
    entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));
}

size_t KeeperSnapshotStore::createObjects(KeeperStore & store, int64_t next_zxid, int64_t next_session_id)
{
    if (snap_meta->size() == 0)
    {
        return 0;
    }
    if (Directory::createDir(snap_dir) != 0)
    {
        LOG_ERROR(log, "Fail to create snapshot directory {}", snap_dir);
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "Fail to create snapshot directory {}", snap_dir);
    }

    size_t data_object_count = store.container.size() / max_object_node_size;
    if (store.container.size() % max_object_node_size)
    {
        data_object_count += 1;
    }

    //uint map、Sessions、acls、Normal node objects
    size_t total_obj_count = data_object_count + 3;

    LOG_INFO(
        log,
        "Creating snapshot with approximately data_object_count {}, total_obj_count {}, next zxid {}, next session id {}",
        data_object_count,
        total_obj_count,
        next_zxid,
        next_session_id);

    /// 1. Save uint map before nodes
    IntMap int_map;
    /// Next transaction id
    int_map["ZXID"] = next_zxid;
    /// Next session id
    int_map["SESSIONID"] = next_session_id;

    String map_path;
    getObjectPath(1, map_path);
    serializeMap(int_map, save_batch_size, version, map_path);

    /// 2. Save sessions
    String session_path;
    /// object index should start from 1
    getObjectPath(2, session_path);
    int64_t serialized_next_session_id = serializeSessions(store, save_batch_size, version, session_path);
    LOG_INFO(
        log,
        "Creating snapshot nex_session_id {}, serialized_next_session_id {}",
        toHexString(next_session_id),
        toHexString(serialized_next_session_id));

    /// 3. Save acls
    String acl_path;
    /// object index should start from 1
    getObjectPath(3, acl_path);
    serializeAcls(store.acl_map, acl_path, save_batch_size, version);

    /// 4. Save data tree
    size_t last_id = serializeDataTree(store);

    total_obj_count = last_id;
    LOG_INFO(log, "Creating snapshot real data_object_count {}, total_obj_count {}", total_obj_count - 3, total_obj_count);

    /// add all path to objects_path
    for (size_t i = 1; i < total_obj_count + 1; i++)
    {
        String path;
        getObjectPath(i, path);
        addObjectPath(i, path);
    }

    return total_obj_count;
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

bool KeeperSnapshotStore::loadBatchHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head)
{
    head.reset();
    errno = 0;
    if (readUInt32(fs, head.data_length) != 0)
    {
        if (!fs->eof())
        {
            LOG_ERROR(log, "Can't read header data_length from snapshot file, error:{}.", strerror(errno));
        }
        return false;
    }

    if (readUInt32(fs, head.data_crc) != 0)
    {
        if (!fs->eof())
        {
            LOG_ERROR(log, "Can't read header data_crc from snapshot file, error:{}.", strerror(errno));
        }
        return false;
    }

    return true;
}

bool KeeperSnapshotStore::parseObject(std::string obj_path, KeeperStore & store)
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
    SnapshotVersion version_ = SnapshotVersion::None;
    while (!snap_fs->eof())
    {
        size_t cur_read_size = read_size;
        UInt64 magic;
        readUInt64(snap_fs, magic);
        read_size += 8;
        if (isFileHeader(magic))
        {
            char * buf = reinterpret_cast<char *>(&version_);
            snap_fs->read(buf, sizeof(uint8_t));
            read_size += 1;
            LOG_INFO(log, "obj_path {}, read file header, version {}", obj_path, uint8_t(version_));
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
            if (version_ == SnapshotVersion::None)
            {
                version_ = SnapshotVersion::V0;
                LOG_INFO(log, "obj_path {}, didn't read version, set to V0", obj_path);
            }

            LOG_INFO(log, "obj_path {}, didn't read the header and tail of the file", obj_path);
            snap_fs->seekg(cur_read_size);
            read_size = cur_read_size;
        }
        if (!loadBatchHeader(snap_fs, header))
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA, "snapshot {} load header error", obj_path);
        }

        checksum = updateCheckSum(checksum, header.data_crc);
        char * body_buf = new char[header.data_length];
        read_size += (SnapshotBatchHeader::HEADER_SIZE + header.data_length);

        if (!snap_fs->read(body_buf, header.data_length))
        {
            LOG_ERROR(
                log, "Can't read snapshot object file {} size {}, only {} could be read", obj_path, header.data_length, snap_fs->gcount());
            delete[] body_buf;
            return false;
        }

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
                            Coordination::read(node->acl_id, in);
                        }
                        else if (version_ == SnapshotVersion::V0)
                        {
                            /// Deserialize ACL
                            Coordination::ACLs acls;
                            Coordination::read(acls, in);
                            node->acl_id = store.acl_map.convertACLs(acls);
                            LOG_TRACE(log, "parseObject path {}, acl_id {}", key, node->acl_id);
                        }

                        /// Some strange ACLID during deserialization from ZooKeeper
                        if (node->acl_id == std::numeric_limits<uint64_t>::max())
                            node->acl_id = 0;

                        store.acl_map.addUsage(node->acl_id);
                        LOG_TRACE(log, "parseObject path {}, acl_id {}", key, node->acl_id);

                        Coordination::read(node->is_ephemeral, in);
                        Coordination::read(node->is_sequential, in);
                        Coordination::read(node->stat, in);
                        auto ephemeral_owner = node->stat.ephemeralOwner;
                        LOG_TRACE(log, "Load snapshot read key {}, node stat {}", key, node->stat.toString());
                        store.container.emplace(key, std::move(node));

                        if (ephemeral_owner != 0)
                        {
                            /// TODO LOG_INFO -> LOG_TRACE
                            LOG_INFO(log, "Load snapshot find ephemeral node {} - {}", ephemeral_owner, key);
                            std::lock_guard l(store.ephemerals_mutex);
                            auto & ephemeral_nodes = store.ephemerals[ephemeral_owner];
                            ephemeral_nodes.emplace(key);
                        }
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Can't read snapshot data {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            key,
                            e.displayText());
                        break;
                    }
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
                            Coordination::AuthIDs ids;
                            Coordination::read(ids, in);
                            {
                                if (!ids.empty())
                                {
                                    std::lock_guard lock(store.auth_mutex);
                                    store.session_and_auth[session_id] = ids;
                                }
                            }
                        }
                    }
                    catch (Coordination::Exception & e)
                    {
                        LOG_WARNING(
                            log,
                            "Can't read type_ephemeral snapshot {}, data index {}, key {}, excepiton {}",
                            obj_path,
                            data_idx,
                            e.displayText());
                        break;
                    }
                    LOG_TRACE(log, "Read session id {}, timeout {}", session_id, timeout);
                    store.addSessionID(session_id, timeout);
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
                        Coordination::ACLs acls;

                        Coordination::read(acl_id, in);
                        Coordination::read(acls, in);

                        LOG_TRACE(log, "parseObject acl_id {}", acl_id);
                        store.acl_map.addMapping(acl_id, acls);
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
                            "Can't read uint map snapshot {}, data index {}, key {}, exception {}",
                            obj_path,
                            data_idx,
                            e.displayText());
                        break;
                    }
                    int_map[key] = value;
                }
                if (int_map.find("ZXID") != int_map.end())
                {
                    store.zxid = int_map["ZXID"];
                }
                if (int_map.find("SESSIONID") != int_map.end())
                {
                    store.session_id_counter = int_map["SESSIONID"];
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

void KeeperSnapshotStore::loadLatestSnapshot(KeeperStore & store)
{
    SnapshotVersion current_version = static_cast<SnapshotVersion>(version);
    if (current_version > version)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version);

    ThreadPool object_thread_pool(SNAPSHOT_THREAD_NUM);
    for (UInt32 thread_idx = 0; thread_idx < SNAPSHOT_THREAD_NUM; thread_idx++)
    {
        object_thread_pool.trySchedule([this, thread_idx, &store] {
            Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.parseObjectThread"));
            UInt32 obj_idx = 0;
            for (auto it = this->objects_path.begin(); it != this->objects_path.end(); it++)
            {
                if (obj_idx % SNAPSHOT_THREAD_NUM == thread_idx)
                {
                    LOG_INFO(
                        thread_log,
                        "Parse snapshot object, thread_idx {}, obj_index {}, path {}, obj size {}",
                        thread_idx,
                        it->first,
                        it->second,
                        this->objects_path.size());
                    try
                    {
                        this->parseObject(it->second, store);
                    }
                    catch (Exception & e)
                    {
                        LOG_ERROR(log, "parseObject error {}, {}", it->second, getExceptionMessage(e, true));
                    }
                }
                obj_idx++;
            }
        });
    }
    object_thread_pool.wait();

    /// Build node tree relationship
    store.buildPathChildren();

    LOG_INFO(
        log,
        "Load snapshot done: nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
        store.getNodesCount(),
        store.getTotalEphemeralNodesCount(),
        store.getSessionCount(),
        store.getSessionIDCounter(),
        store.getZxid());
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

    std::string obj_path = objects_path.at(obj_id);

    int snap_fd = openFileForRead(obj_path);
    if (snap_fd < 0)
    {
        return;
    }

    ::lseek(snap_fd, 0, SEEK_SET);
    size_t file_size = ::lseek(snap_fd, 0, SEEK_END);
    ::lseek(snap_fd, 0, SEEK_SET);

    buffer = buffer::alloc(file_size);
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

size_t KeeperSnapshotManager::createSnapshot(snapshot & meta, KeeperStore & store, int64_t next_zxid, int64_t next_session_id)
{
    size_t store_size = store.container.size() + store.ephemerals.size();
    meta.set_size(store_size);
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
    snap_store->init();
    LOG_INFO(
        log,
        "Create snapshot last_log_term {}, last_log_idx {}, size {}, nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
        meta.get_last_log_term(),
        meta.get_last_log_idx(),
        meta.size(),
        store.getNodesCount(),
        store.getTotalEphemeralNodesCount(),
        store.getSessionCount(),
        store.getSessionIDCounter(),
        store.getZxid());
    size_t obj_size = snap_store->createObjects(store, next_zxid, next_session_id);
    snapshots[meta.get_last_log_idx()] = snap_store;
    return obj_size;
}

bool KeeperSnapshotManager::receiveSnapshotMeta(snapshot & meta)
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
        LOG_WARNING(log, "Can't find snapshot, last log index {}", meta.get_last_log_idx());
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

bool KeeperSnapshotManager::parseSnapshot(const snapshot & meta, KeeperStore & storage)
{
    auto it = snapshots.find(meta.get_last_log_idx());
    if (it == snapshots.end())
    {
        LOG_WARNING(log, "Can't find snapshot, last log index {}", meta.get_last_log_idx());
        return false;
    }
    ptr<KeeperSnapshotStore> store = it->second;
    store->loadLatestSnapshot(storage);
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

    for (const auto & file : file_vec)
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
    return entry->second->getSnapshotMeta();
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
            for (const auto & file : files)
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
