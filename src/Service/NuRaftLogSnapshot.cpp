#include <algorithm>
#include <fcntl.h>
#include <filesystem>
#include <stdio.h>
#include <unistd.h>

#include <Poco/DateTime.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/File.h>
#include <Poco/NumberFormatter.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <Service/KeeperUtils.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/ReadBufferFromNuRaftBuffer.h>
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
    extern const int CORRUPTED_SNAPSHOT;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int SNAPSHOT_OBJECT_NOT_EXISTS;
    extern const int SNAPSHOT_NOT_EXISTS;
}

using nuraft::cs_new;
using Poco::NumberFormatter;

void KeeperSnapshotStore::getObjectPath(ulong object_id, String & obj_path)
{
    char path_buf[1024];
    snprintf(path_buf, 1024, SNAPSHOT_FILE_NAME, curr_time.c_str(), last_log_index, object_id);
    obj_path = path_buf;
    obj_path = snap_dir + "/" + obj_path;
}

size_t KeeperSnapshotStore::getObjectIdx(const String & file_name)
{
    auto it = file_name.find_last_of('_');
    return std::stoi(file_name.substr(it + 1, file_name.size() - it));
}

size_t KeeperSnapshotStore::serializeDataTreeV2(KeeperStore & storage)
{
    std::shared_ptr<WriteBufferFromFile> out;
    ptr<SnapshotBatchBody> batch;

    uint64_t processed = 0;
    uint32_t checksum = 0;

    serializeNodeV2(out, batch, storage, "/", processed, checksum);
    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
    LOG_INFO(log, "Creating snapshot processed data size {}, current zxid {}", processed, storage.zxid);

    return getObjectIdx(out->getFileName());
}

void KeeperSnapshotStore::serializeNodeV2(
    ptr<WriteBufferFromFile> & out,
    ptr<SnapshotBatchBody> & batch,
    KeeperStore & store,
    const String & path,
    uint64_t & processed,
    uint32_t & checksum)
{
    auto node = store.container.get(path);

    /// In case of node is deleted
    if (!node)
        return;

    std::shared_ptr<KeeperNode> node_copy = node->clone();

    if (processed % max_object_node_size == 0)
    {
        /// time to create new snapshot object
        uint64_t obj_id = processed / max_object_node_size;

        if (obj_id != 0)
        {
            /// flush last batch data
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
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
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum);
            checksum = new_checksum;
        }
        else
        {
            if (!batch)
                batch = cs_new<SnapshotBatchBody>();
        }
    }

    LOG_TRACE(log, "Append node path {}", path);
    appendNodeToBatchV2(batch, path, node_copy, version);
    processed++;

    String path_with_slash = path;
    if (path != "/")
        path_with_slash += '/';

    for (const auto & child : node->children)
        serializeNodeV2(out, batch, store, path_with_slash + child, processed, checksum);
}

void KeeperSnapshotStore::appendNodeToBatchV2(
    ptr<SnapshotBatchBody> batch, const String & path, std::shared_ptr<KeeperNode> node, SnapshotVersion version)
{
    WriteBufferFromNuraftBuffer buf;

    Coordination::write(path, buf);
    Coordination::write(node->data, buf);
    if (version == SnapshotVersion::V0)
    {
        /// Just ignore acls for snapshot V0 /// TODO delete
        Coordination::ACLs acls;
        Coordination::write(acls, buf);
    }
    else
        Coordination::write(node->acl_id, buf);
    Coordination::write(node->is_ephemeral, buf);
    Coordination::write(node->is_sequential, buf);
    Coordination::write(node->stat, buf);

    ptr<buffer> data = buf.getBuffer();
    data->pos(0);
    batch->add(String(reinterpret_cast<char *>(data->data_begin()), data->size()));
}

size_t KeeperSnapshotStore::createObjects(KeeperStore & store, int64_t next_zxid, int64_t next_session_id)
{
    return createObjectsV2(store, next_zxid, next_session_id);
}

size_t KeeperSnapshotStore::createObjectsV2(KeeperStore & store, int64_t next_zxid, int64_t next_session_id)
{
    if (snap_meta->size() == 0)
    {
        return 0;
    }

    Poco::File(snap_dir).createDirectories();

    size_t data_object_count = store.container.size() / max_object_node_size;
    if (store.container.size() % max_object_node_size)
    {
        data_object_count += 1;
    }

    //uint map、Sessions、acls、Normal node objects
    size_t total_obj_count = data_object_count + 3;

    LOG_INFO(
        log,
        "Creating snapshot v3 with approximately data_object_count {}, total_obj_count {}, next zxid {}, next session id {}",
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
    serializeMapV2(int_map, save_batch_size, version, map_path);

    /// 2. Save sessions
    String session_path;
    /// object index should start from 1
    getObjectPath(2, session_path);
    int64_t serialized_next_session_id = serializeSessionsV2(store, save_batch_size, version, session_path);
    LOG_INFO(
        log,
        "Creating snapshot nex_session_id {}, serialized_next_session_id {}",
        toHexString(next_session_id),
        toHexString(serialized_next_session_id));

    /// 3. Save acls
    String acl_path;
    /// object index should start from 1
    getObjectPath(3, acl_path);
    serializeAclsV2(store.acl_map, acl_path, save_batch_size, version);

    /// 4. Save data tree
    size_t last_id = serializeDataTreeV2(store);

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

void KeeperSnapshotStore::init(String create_time = "")
{
    if (create_time.empty())
    {
        Poco::DateTime now;
        curr_time = Poco::DateTimeFormatter::format(now, "%Y%m%d%H%M%S");
    }
    else
    {
        curr_time = create_time;
    }
}

void KeeperSnapshotStore::parseBatchHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head)
{
    head.reset();
    errno = 0;
    if (readUInt32(fs, head.data_length) != 0)
    {
        if (!fs->eof())
            throwFromErrno("Can't read header data_length from snapshot file", ErrorCodes::CORRUPTED_SNAPSHOT);
    }

    if (readUInt32(fs, head.data_crc) != 0)
    {
        if (!fs->eof())
            throwFromErrno("Can't read header data_crc from snapshot file", ErrorCodes::CORRUPTED_SNAPSHOT);
    }
}

void KeeperSnapshotStore::parseObject(KeeperStore & store, String obj_path, BucketEdges & buckets_edges, BucketNodes & bucket_nodes)
{
    ptr<std::fstream> snap_fs = cs_new<std::fstream>();
    snap_fs->open(obj_path, std::ios::in | std::ios::binary);

    if (snap_fs->fail())
        throwFromErrno("Open snapshot object " + obj_path + " for read failed", ErrorCodes::CORRUPTED_SNAPSHOT);

    snap_fs->seekg(0, snap_fs->end);
    size_t file_size = snap_fs->tellg();
    snap_fs->seekg(0, snap_fs->beg);

    LOG_INFO(log, "Open snapshot object {} for read, file size {}", obj_path, file_size);

    size_t read_size = 0;
    SnapshotBatchHeader header;
    UInt32 checksum = 0;
    SnapshotVersion version_from_obj = SnapshotVersion::None;

    while (!snap_fs->eof())
    {
        size_t cur_read_size = read_size;
        UInt64 magic;

        // If raft snapshot version is v0, we get eof when read magic.
        // Just log it, and break;
        if (readUInt64(snap_fs, magic) != 0)
        {
            if (snap_fs->eof() && version_from_obj == SnapshotVersion::V0)
            {
                LOG_DEBUG(log, "obj_path {}, read file tail, version {}", obj_path, uint8_t(version_from_obj));
                break;
            }
            throw Exception(
                ErrorCodes::CORRUPTED_SNAPSHOT, "snapshot {} load magic error, version {}", obj_path, toString(version_from_obj));
        }

        read_size += 8;
        if (isSnapshotFileHeader(magic))
        {
            char * buf = reinterpret_cast<char *>(&version_from_obj);
            snap_fs->read(buf, sizeof(uint8_t));
            read_size += 1;
            LOG_DEBUG(log, "Got snapshot file header with version {}", toString(version_from_obj));
            if (version_from_obj > CURRENT_SNAPSHOT_VERSION)
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version_from_obj);
        }
        else if (isSnapshotFileTail(magic))
        {
            UInt32 file_checksum;
            char * buf = reinterpret_cast<char *>(&file_checksum);
            snap_fs->read(buf, sizeof(UInt32));
            read_size += 4;
            LOG_DEBUG(log, "obj_path {}, file_checksum {}, checksum {}.", obj_path, file_checksum, checksum);
            if (file_checksum != checksum)
                throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "snapshot {} checksum doesn't match", obj_path);
            break;
        }
        else
        {
            if (version_from_obj == SnapshotVersion::None)
            {
                version_from_obj = SnapshotVersion::V0;
                LOG_INFO(log, "snapshot has no version, set to V0", obj_path);
            }

            LOG_DEBUG(log, "obj_path {}, didn't read the header and tail of the file", obj_path);
            snap_fs->seekg(cur_read_size);
            read_size = cur_read_size;
        }

        parseBatchHeader(snap_fs, header);

        checksum = updateCheckSum(checksum, header.data_crc);
        String body_string(header.data_length, '0');
        char * body_buf = body_string.data();
        read_size += (SnapshotBatchHeader::HEADER_SIZE + header.data_length);

        if (!snap_fs->read(body_buf, header.data_length))
        {
            throwFromErrno(
                "Can't read snapshot object file " + obj_path + ", batch size " + std::to_string(header.data_length) + ", only "
                    + std::to_string(snap_fs->gcount()) + " could be read",
                ErrorCodes::CORRUPTED_SNAPSHOT);
        }

        if (!verifyCRC32(body_buf, header.data_length, header.data_crc))
        {
            throwFromErrno("Can't read snapshot object file " + obj_path + ", batch crc not match.", ErrorCodes::CORRUPTED_SNAPSHOT);
        }

        parseBatchBodyV2(store, body_string, buckets_edges, bucket_nodes, version_from_obj);
    }
}

void KeeperSnapshotStore::parseBatchBodyV2(KeeperStore & store, const String & body_string, BucketEdges & buckets_edges, BucketNodes & bucket_nodes, SnapshotVersion version_)
{
    ptr<SnapshotBatchBody> batch;
    batch = SnapshotBatchBody::parse(body_string);
    switch (batch->type)
    {
        case SnapshotBatchType::SNAPSHOT_TYPE_DATA:
            LOG_DEBUG(log, "Parsing batch data from snapshot, data count {}", batch->size());
            parseBatchDataV2(store, *batch, buckets_edges, bucket_nodes, version_);
            break;
        case SnapshotBatchType::SNAPSHOT_TYPE_SESSION: {
            LOG_DEBUG(log, "Parsing batch session from snapshot, session count {}", batch->size());
            parseBatchSessionV2(store, *batch, version_);
        }
        break;
        case SnapshotBatchType::SNAPSHOT_TYPE_ACLMAP:
            LOG_DEBUG(log, "Parsing batch acl from snapshot, acl count {}", batch->size());
            parseBatchAclMapV2(store, *batch, version_);
            break;
        case SnapshotBatchType::SNAPSHOT_TYPE_UINTMAP:
            LOG_DEBUG(log, "Parsing batch int_map from snapshot, element count {}", batch->size());
            parseBatchIntMapV2(store, *batch, version_);
            LOG_DEBUG(log, "Parsed zxid {}, session_id_counter {}", store.zxid, store.session_id_counter);
            break;
        case SnapshotBatchType::SNAPSHOT_TYPE_CONFIG:
        case SnapshotBatchType::SNAPSHOT_TYPE_SERVER:
            break;
        default:
            break;
    }
}

void KeeperSnapshotStore::loadLatestSnapshot(KeeperStore & store)
{
    auto objects_cnt = objects_path.size();
    ThreadPool thread_pool(SNAPSHOT_THREAD_NUM);

    all_objects_edges = std::vector<BucketEdges>(objects_cnt);
    all_objects_nodes = std::vector<BucketNodes>(objects_cnt);

    LOG_INFO(log, "Parsing snapshot objects from disk");
    Stopwatch watch;

    for (UInt32 thread_id = 0; thread_id < SNAPSHOT_THREAD_NUM; thread_id++)
    {
        thread_pool.trySchedule(
            [this, thread_id, &store]
            {
                Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.parseObjectThread#" + std::to_string(thread_id)));
                UInt32 obj_idx = 0;
                for (auto it = this->objects_path.begin(); it != this->objects_path.end(); it++)
                {
                    if (obj_idx % SNAPSHOT_THREAD_NUM == thread_id)
                    {
                        LOG_INFO(thread_log, "Parsing snapshot object {}", it->second);
                        parseObject(store, it->second, all_objects_edges[obj_idx], all_objects_nodes[obj_idx]);
                    }
                    obj_idx++;
                }
            });
    }

    thread_pool.wait();
    LOG_INFO(log, "Parsing snapshot objects costs {}ms", watch.elapsedMilliseconds());

    LOG_INFO(log, "Building data tree from snapshot objects");
    watch.restart();

    /// Build data tree relationship in parallel
    for (UInt32 thread_id = 0; thread_id < SNAPSHOT_THREAD_NUM; thread_id++)
    {
        thread_pool.trySchedule(
            [this, thread_id, &store]
            {
                Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.buildDataTreeThread#" + std::to_string(thread_id)));
                for (UInt32 bucket_id = 0; bucket_id < store.container.getBucketNum(); bucket_id++)
                {
                    if (bucket_id % SNAPSHOT_THREAD_NUM == thread_id)
                    {
                        LOG_INFO(thread_log, "Filling bucket {} in data tree", bucket_id);
                        store.fillDataTreeBucket(all_objects_nodes, bucket_id);
                        LOG_INFO(thread_log, "Building children set for data tree bucket {}", bucket_id);
                        store.buildBucketChildren(all_objects_edges, bucket_id);
                    }
                }
            });
    }

    thread_pool.wait();
    LOG_INFO(log, "Building data tree costs {}ms", watch.elapsedMilliseconds());

    LOG_INFO(
        log,
        "Loading snapshot done: nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
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
        throw Exception(ErrorCodes::SNAPSHOT_OBJECT_NOT_EXISTS, "Snapshot object {} does not exist", obj_id);

    String obj_path = objects_path.at(obj_id);

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
    Poco::File(snap_dir).createDirectories();

    String obj_path;
    getObjectPath(obj_id, obj_path);

    int snap_fd = openFileForWrite(obj_path);

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

void KeeperSnapshotStore::addObjectPath(ulong obj_id, String & path)
{
    objects_path[obj_id] = path;
}

size_t KeeperSnapshotManager::createSnapshot(
    snapshot & meta, KeeperStore & store, int64_t next_zxid, int64_t next_session_id, SnapshotVersion version)
{
    size_t store_size = store.container.size();
    meta.set_size(store_size);
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size, SAVE_BATCH_SIZE, version);
    snap_store->init();
    LOG_INFO(
        log,
        "Create snapshot last_log_term {}, last_log_idx {}, size {}, nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, "
        "zxid {}",
        meta.get_last_log_term(),
        meta.get_last_log_idx(),
        meta.size(),
        store.getNodesCount(),
        store.getTotalEphemeralNodesCount(),
        store.getSessionCount(),
        next_session_id,
        next_zxid);
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
        throw Exception(
            ErrorCodes::SNAPSHOT_NOT_EXISTS,
            "Error when loading snapshot object {}, for snapshot {} does not exist",
            obj_id,
            meta.get_last_log_idx());


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
        throw Exception(ErrorCodes::SNAPSHOT_NOT_EXISTS, "Error when parsing snapshot {}, for it does not exist", meta.get_last_log_idx());
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

    std::vector<String> file_vec;
    file_dir.list(file_vec);
    char time_str[128];

    uint64_t log_last_index;
    uint64_t object_id;

    for (const auto & file : file_vec)
    {
        if (file.find("snapshot_") == file.npos)
        {
            LOG_INFO(log, "Skip non-snapshot file {}", file);
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
            LOG_INFO(log, "Load filename {}, time {}, index {}, object id {}", file, time_str, log_last_index, object_id);
        }
        String full_path = snap_dir + "/" + file;
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
            std::vector<String> files;
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
