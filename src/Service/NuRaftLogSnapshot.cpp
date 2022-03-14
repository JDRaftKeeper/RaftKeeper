#include <algorithm>
#include <filesystem>
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
#include <Common/Stopwatch.h>
#include <IO/WriteHelpers.h>

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif


#ifndef RAFT_SERVICE_TEST
//#define RAFT_SERVICE_TEST
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using nuraft::cs_new;
namespace fs = std::filesystem;

const UInt32 KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE;

int openFileForWrite(std::string & obj_path)
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

size_t saveBatch(std::shared_ptr<WriteBufferFromFile> & out, ptr<SnapshotBatchPB> & batch)
{
    if (!batch)
        return 0;

    std::string str_buf;
    batch->SerializeToString(&str_buf);

    SnapshotBatchHeader header;
    header.data_length = str_buf.size();
    header.data_crc = DB::getCRC32(str_buf.c_str(), str_buf.size());

    writeIntBinary(header.data_length, *out);
    writeIntBinary(header.data_crc, *out);

    out->write(str_buf.c_str(), header.data_length);
    out->next();

    return SnapshotBatchHeader::HEADER_SIZE + header.data_length;
}

size_t saveBatch(int & snap_fd, ptr<SnapshotBatchPB> & batch_pb, std::string obj_path)
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
        return -1;
    }
    LOG_DEBUG(log, "Save object batch, path {}, length {}, crc {}", obj_path, header.data_length, header.data_crc);
    return SnapshotBatchHeader::HEADER_SIZE + header.data_length;
}

size_t serializeEphemerals(
    SvsKeeperStorage::Ephemerals & ephemerals,
    std::mutex & mutex,
    String path,
    UInt32 save_batch_size)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot ephemeral object, node size {}, path {}", ephemerals.size(), path);

    ptr<SnapshotBatchPB> batch;

    std::lock_guard lock(mutex);

    if(ephemerals.empty())
    {
        LOG_INFO(log, "Create snapshot ephemeral nodes size is 0");
        return 0;
    }

    auto out = cs_new<WriteBufferFromFile>(path);
    uint64_t index = 0;
    for (auto & ephemeral_it : ephemerals)
    {
        /// flush and rebuild batch
        if(index % save_batch_size == 0)
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

/** Serialize sessions and return the next_session_id before serialize
 */
int64_t serializeSessions(
    SvsKeeperStorage & storage, UInt32 save_batch_size, std::string & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));

    ptr<SnapshotBatchPB> batch;
    auto out = cs_new<WriteBufferFromFile>(path);

    std::lock_guard lock(storage.session_mutex);
    LOG_INFO(log, "Begin create snapshot session object, session size {}, path {}", storage.session_and_timeout.size(), path);

    int64_t next_session_id = storage.session_id_counter;
    uint64_t index = 0;

    for (auto & session_it : storage.session_and_timeout)
    {
        /// flush and rebuild batch
        if(index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                saveBatch(out, batch);
            }
            batch = cs_new<SnapshotBatchPB>();
            batch->set_batch_type(SnapshotTypePB::SNAPSHOT_TYPE_SESSION);
        }

        /// append to batch
        SnapshotItemPB * entry = batch->add_data();
        WriteBufferFromNuraftBuffer buf;
        Coordination::write(session_it.first, buf); //SessionID
        Coordination::write(session_it.second, buf); //Timeout_ms

        ptr<buffer> data = buf.getBuffer();
        data->pos(0);
        entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));

        index++;
    }

    /// flush the last batch
    saveBatch(out, batch);
    out->close();

    return next_session_id;
}

/**Save map<string, string> or map<string, uint64>
 */
template <typename T>
void serializeMap(T & snap_map, UInt32 save_batch_size, std::string & path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    LOG_INFO(log, "Begin create snapshot map object, map size {}, path {}", snap_map.size(), path);

    ptr<SnapshotBatchPB> batch;
    auto out = cs_new<WriteBufferFromFile>(path);

    uint64_t index = 0;
    for (auto & it : snap_map)
    {
        /// flush and rebuild batch
        if(index % save_batch_size == 0)
        {
            /// skip flush the first batch
            if (index != 0)
            {
                /// write data in batch to file
                saveBatch(out, batch);
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
    saveBatch(out, batch);
    out->close();
}

void KeeperSnapshotStore::getObjectPath(ulong object_id, std::string & obj_path)
{
    char path_buf[1024];
    snprintf(path_buf, 1024, SNAPSHOT_FILE_NAME, curr_time.c_str(), log_last_index, object_id);
    obj_path = path_buf;
    obj_path = snap_dir + "/" + obj_path;
}

void KeeperSnapshotStore::getFileTime(const std::string & file_name, std::string & time)
{
    auto it1 = file_name.find('_');
    auto it2 = file_name.find('_', it1 + 1);
    time = file_name.substr(it1 + 1, it2 - it1);
}

size_t KeeperSnapshotStore::getObjectIdx(const std::string & file_name)
{
    auto it1 = file_name.find_last_of('_');
    return std::stoi(file_name.substr(it1 + 1, file_name.size() - it1));
}

size_t KeeperSnapshotStore::serializeDataTree(SvsKeeperStorage & storage)
{
    std::shared_ptr<WriteBufferFromFile> out;
    ptr<SnapshotBatchPB> batch;

    uint64_t processed = 0;
    serializeNode(out, batch, storage, "/", processed);
    saveBatch(out, batch);

    out->close();
    LOG_INFO(log, "Creating snapshot processed data size {}, current zxid {}", processed, storage.zxid);

    return getObjectIdx(out->getFileName());
}

void KeeperSnapshotStore::serializeNode(
    std::shared_ptr<WriteBufferFromFile> & out,
    ptr<SnapshotBatchPB> & batch,
    SvsKeeperStorage & storage,
    const String & path,
    uint64_t & processed)
{
    auto node = storage.container.get(path);

    /// In case of node is deleted
    if (!node)
        return;

    std::shared_ptr<KeeperNode> node_copy;
    {
        std::shared_lock(node->mutex);
        node_copy = node->clone();
    }

    if (processed % max_object_node_size == 0)
    {
        /// time to create new snapshot object
        uint64_t obj_id = processed / max_object_node_size;

        if (obj_id != 0)
        {
            /// close current object file
            out->close();
        }
        String new_obj_path;
        /// for there are 3 objects before data bojects
        getObjectPath(obj_id + 3, new_obj_path);

        LOG_INFO(log, "Create new snapshot object {}, path {}", obj_id + 3, new_obj_path);
        out = cs_new<WriteBufferFromFile>(new_obj_path);
    }

    /// flush and rebuild batch
    if(processed % save_batch_size == 0)
    {
        /// skip flush the first batch
        if (processed != 0)
        {
            /// write data in accumulator to file
            saveBatch(out, batch);
        }
        batch = cs_new<SnapshotBatchPB>();
    }

    appendNodeToBatch(batch, path, node_copy);
    processed++;

    String path_with_slash = path;
    if (path != "/")
        path_with_slash += '/';

    for (const auto & child : node->children)
        serializeNode(out, batch, storage, path_with_slash + child, processed);
}

void KeeperSnapshotStore::appendNodeToBatch(ptr<SnapshotBatchPB> batch, const String & path, std::shared_ptr<KeeperNode> node)
{
# ifdef RAFT_SERVICE_TEST
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
#endif
    SnapshotItemPB * entry = batch->add_data();
    WriteBufferFromNuraftBuffer buf;
    Coordination::write(path, buf);
    Coordination::write(node->data, buf);
    Coordination::write(node->acls, buf);
    Coordination::write(node->is_ephemeral, buf);
    Coordination::write(node->is_sequental, buf);
    Coordination::write(node->stat, buf);

    ptr<buffer> data = buf.getBuffer();
    data->pos(0);
    entry->set_data(std::string(reinterpret_cast<char *>(data->data_begin()), data->size()));
}

size_t KeeperSnapshotStore::createObjects(SvsKeeperStorage & storage, int64_t next_zxid, int64_t next_session_id)
{
    if (Directory::createDir(snap_dir) != 0)
    {
        LOG_ERROR(log, "Fail to create snapshot directory {}", snap_dir);
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "Fail to create snapshot directory {}", snap_dir);
    }

    size_t data_object_count = storage.container.size() / max_object_node_size;
    if (storage.container.size() % max_object_node_size)
    {
        data_object_count += 1;
    }

    //uint map、Sessions、Ephemeral nodes、Normal node objects
    size_t total_obj_count = data_object_count + 1 + 1;

    LOG_INFO(log, "Creating snapshot with approximately data_object_count {}, total_obj_count {}, next zxid {}, next session id {}",
             data_object_count, total_obj_count, next_zxid, next_session_id);

# ifdef RAFT_SERVICE_TEST
    LOG_INFO(log, "RAFT_SERVICE_TEST ON");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
#endif

    /// Save uint map before nodes
    IntMap int_map;
    /// Next transaction id
    int_map["ZXID"] = next_zxid;
    /// Next session id
    int_map["SESSIONID"] = next_session_id;

    /// Save sessions
    String session_path;
    /// object index should start from 1
    getObjectPath(1, session_path);
    int64_t serialized_next_session_id = serializeSessions(storage, save_batch_size, session_path);
    LOG_INFO(log,
             "Creating snapshot nex_session_id {}, serialized_next_session_id {}",
             getHexUIntLowercase(next_session_id),
             getHexUIntLowercase(serialized_next_session_id));

    String map_path;
    getObjectPath(2, map_path);
    serializeMap(int_map, save_batch_size, map_path);

    /// Save data tree
    size_t last_id = serializeDataTree(storage);

    total_obj_count = last_id;
    LOG_INFO(log, "Creating snapshot real data_object_count {}, total_obj_count {}", total_obj_count - 2, total_obj_count);

    /// add all path to objects_path
    for (size_t i=1; i<total_obj_count+1; i++)
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
    while (!snap_fs->eof())
    {
        if (!loadHeader(snap_fs, header))
        {
            return false;
        }
        char * body_buf = new char[header.data_length];
        read_size += sizeof(SnapshotBatchHeader) + header.data_length;
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
                        Coordination::read(node->acls, in);
                        Coordination::read(node->is_ephemeral, in);
                        Coordination::read(node->is_sequental, in);
                        Coordination::read(node->stat, in);
                        auto ephemeral_owner = node->stat.ephemeralOwner;
                        LOG_TRACE(log, "Load snapshot read key {}", key);
                        storage.container.emplace(key, std::move(node));

                        if (ephemeral_owner != 0)
                        {
                            LOG_TRACE(log, "Load snapshot find ephemeral node {} - {}", ephemeral_owner, key);
                            std::lock_guard l(storage.ephemerals_mutex);
                            auto & ephemeral_nodes = storage.ephemerals[ephemeral_owner];
                            ephemeral_nodes.emplace(key);
                        }
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
            case SnapshotTypePB::SNAPSHOT_TYPE_STRINGMAP:
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
                    this->parseOneObject(it->second, storage);
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

size_t KeeperSnapshotManager::createSnapshot(snapshot & meta, SvsKeeperStorage & storage, int64_t next_zxid, int64_t next_session_id)
{
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
    Stopwatch timer;
    size_t obj_size = snap_store->createObjects(storage, next_zxid, next_session_id);
    snapshots[meta.get_last_log_idx()] = snap_store;
    meta.set_size(obj_size);
    LOG_INFO(log, "Create snapshot done time cost {} ms, object size {}", timer.elapsedMilliseconds(), obj_size);
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
