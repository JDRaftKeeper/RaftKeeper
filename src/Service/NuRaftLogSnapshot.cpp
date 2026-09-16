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
#include <fmt/format.h>

#include <Service/Crc32.h>
#include <Service/KeeperUtils.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/ReadBufferFromNuRaftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <Service/ZstdLogCodec.h>
#include <ZooKeeper/ZooKeeperIO.h>


namespace RK
{
namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CORRUPTED_SNAPSHOT;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int SNAPSHOT_OBJECT_NOT_EXISTS;
    extern const int SNAPSHOT_NOT_EXISTS;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int INVALID_SNAPSHOT_FILE_NAME;
    extern const int SNAPSHOT_OBJECT_INCOMPLETE;
}

using nuraft::cs_new;

void KeeperSnapshotStore::getObjectPath(ulong object_id, String & obj_path) const
{
    SnapObject s_obj(curr_time.c_str(), last_log_term, last_log_index, object_id);
    obj_path = snap_dir + "/" + s_obj.getObjectName();
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
    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
    LOG_INFO(log, "Creating snapshot processed data size {}, current zxid {}", processed, storage.getZxid());

    return getObjectIdx(out->getFileName());
}

size_t KeeperSnapshotStore::serializeDataTreeAsync(SnapTask & snap_task) const
{
    std::shared_ptr<WriteBufferFromFile> out;
    ptr<SnapshotBatchBody> batch;

    auto checksum = serializeNodeAsync(out, batch, *snap_task.buckets_nodes);
    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
    checksum = new_checksum;

    writeTailAndClose(out, checksum);
    LOG_INFO(log, "Creating snapshot processed data size {}, current zxid {}", snap_task.nodes_count, snap_task.next_zxid);

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
    auto node = store.getNode(path);

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
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
            checksum = new_checksum;

            /// close current object file
            writeTailAndClose(out, checksum);
            /// reset checksum
            checksum = 0;
        }
        String new_obj_path;
        /// Data objects follow the metadata objects.
        getObjectPath(obj_id + format.metadataObjects() + 1, new_obj_path);

        LOG_INFO(log, "Creating new snapshot object {}, path {}", obj_id + format.metadataObjects() + 1, new_obj_path);
        out = openFileAndWriteHeader(new_obj_path, format);
    }

    /// flush and rebuild batch
    if (processed % save_batch_size == 0)
    {
        /// skip flush the first batch
        if (processed != 0)
        {
            /// flush data in batch to file
            auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
            checksum = new_checksum;
        }
        else
        {
            if (!batch)
                batch = cs_new<SnapshotBatchBody>();
        }
    }

    LOG_TRACE(log, "Append node path {}", path);
    appendNodeToBatchV2(batch, path, node_copy, format.version);
    processed++;

    String path_with_slash = path;
    if (path != "/")
        path_with_slash += '/';

    for (const auto & child : node->children)
        serializeNodeV2(out, batch, store, path_with_slash + child, processed, checksum);
}

uint32_t KeeperSnapshotStore::serializeNodeAsync(
    ptr<WriteBufferFromFile> & out,
    ptr<SnapshotBatchBody> & batch,
    BucketNodes & bucket_nodes) const
{
    uint64_t processed = 0;
    uint32_t checksum = 0;
    for (auto && bucket : bucket_nodes)
    {
        for (auto && [path, node] : bucket)
        {
            if (processed % max_object_node_size == 0)
            {
                /// time to create new snapshot object
                uint64_t obj_id = processed / max_object_node_size;

                if (obj_id != 0)
                {
                    /// flush last batch data
                    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
                    checksum = new_checksum;

                    /// close current object file
                    writeTailAndClose(out, checksum);
                    /// reset checksum
                    checksum = 0;
                }
                String new_obj_path;
                /// Data objects follow the metadata objects.
                getObjectPath(obj_id + format.metadataObjects() + 1, new_obj_path);

                LOG_INFO(log, "Creating new snapshot object {}, path {}", obj_id + format.metadataObjects() + 1, new_obj_path);
                out = openFileAndWriteHeader(new_obj_path, format);
            }

            /// flush and rebuild batch
            if (processed % save_batch_size == 0)
            {
                /// skip flush the first batch
                if (processed != 0)
                {
                    /// flush data in batch to file
                    auto [save_size, new_checksum] = saveBatchAndUpdateCheckSumV2(out, batch, checksum, format);
                    checksum = new_checksum;
                }
                else
                {
                    if (!batch)
                        batch = cs_new<SnapshotBatchBody>();
                }
            }

            LOG_TRACE(log, "Append node path {}", path);
            appendNodeToBatchV2(batch, path, node, format.version);
            processed++;
        }
    }
    return checksum;
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

size_t KeeperSnapshotStore::createObjectsAsync(SnapTask & snap_task)
{
    return createObjectsAsyncImpl(snap_task);
}


size_t KeeperSnapshotStore::createObjectsV2(KeeperStore & store, int64_t next_zxid, int64_t next_session_id)
{
    if (snap_meta->size() == 0)
    {
        return 0;
    }

    Poco::File(snap_dir).createDirectories();

    size_t data_object_count = store.getNodesCount() / max_object_node_size;
    if (store.getNodesCount() % max_object_node_size)
    {
        data_object_count += 1;
    }

    //uint map、Sessions、acls、Normal node objects
    size_t total_obj_count = data_object_count + format.metadataObjects();

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
    /// Object count
    int_map["OBJECTCOUNT"] = total_obj_count;

    auto session_and_timeout = store.getSessionAndTimeOut();
    auto session_and_auth = store.getSessionAndAuth();
    serializeMetadata(int_map, session_and_timeout, session_and_auth, store.getACLMap().getMapping());

    /// 4. Save data tree
    size_t last_id = serializeDataTreeV2(store);

    total_obj_count = last_id;
    LOG_INFO(
        log,
        "Creating snapshot real data_object_count {}, total_obj_count {}",
        total_obj_count - format.metadataObjects(),
        total_obj_count);

    /// add all path to objects_path
    for (size_t i = 1; i < total_obj_count + 1; i++)
    {
        String path;
        getObjectPath(i, path);
        addObjectPath(i, path);
    }

    return total_obj_count;
}


size_t KeeperSnapshotStore::createObjectsAsyncImpl(SnapTask & snap_task)
{
    if (snap_meta->size() == 0)
    {
        return 0;
    }

    Poco::File(snap_dir).createDirectories();

    size_t data_object_count = (snap_task.nodes_count + max_object_node_size -1) / max_object_node_size;

    //uint map、Sessions、acls、Normal node objects
    size_t total_obj_count = data_object_count + format.metadataObjects();

    LOG_INFO(
        log,
        "Creating async snapshot with approximately data_object_count {}, total_obj_count {}, next zxid {}, next session id {}",
        data_object_count,
        total_obj_count,
        snap_task.next_zxid,
        snap_task.next_session_id);

    /// 1. Save uint map before nodes
    IntMap int_map;
    /// Next transaction id
    int_map["ZXID"] = snap_task.next_zxid;
    /// Next session id
    int_map["SESSIONID"] = snap_task.next_session_id;
    /// Object count
    int_map["OBJECTCOUNT"] = total_obj_count;

    serializeMetadata(int_map, snap_task.session_and_timeout, snap_task.session_and_auth, snap_task.acl_map);

    /// 4. Save data tree
    size_t last_id = serializeDataTreeAsync(snap_task);

    total_obj_count = last_id;
    LOG_INFO(
        log,
        "Creating snapshot real data_object_count {}, total_obj_count {}",
        total_obj_count - format.metadataObjects(),
        total_obj_count);

    /// add all path to objects_path
    for (size_t i = 1; i < total_obj_count + 1; i++)
    {
        String path;
        getObjectPath(i, path);
        addObjectPath(i, path);
    }

    return total_obj_count;
}

void KeeperSnapshotStore::serializeMetadata(
    IntMap & counters, SessionAndTimeout & sessions, SessionAndAuth & auth, const NumToACLMap & acls) const
{
    String path;
    getObjectPath(1, path);
    if (format.version >= SnapshotVersion::V4)
    {
        serializeSnapshotMetadata(counters, sessions, auth, acls, save_batch_size, format, path);
    }
    else
    {
        serializeMapV2(counters, save_batch_size, format, path);
        getObjectPath(2, path);
        serializeSessionsV2(sessions, auth, save_batch_size, format, path);
        getObjectPath(3, path);
        serializeAclsV2(acls, path, save_batch_size, format);
    }
}

void KeeperSnapshotStore::init(const String & create_time)
{
    if (create_time.empty())
    {
        Poco::LocalDateTime now;
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
    SnapshotVersion version_from_obj = SnapshotVersion::UNKNOWN;
    SnapshotFormat object_format;

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
            char format_bytes[8];
            if (!snap_fs->read(format_bytes, 1))
                throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Truncated snapshot version in {}", obj_path);
            auto version_byte = static_cast<uint8_t>(format_bytes[0]);
            if (version_byte > static_cast<uint8_t>(MAX_SNAPSHOT_VERSION))
                throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported snapshot version {}", version_byte);
            size_t format_size = version_byte >= static_cast<uint8_t>(SnapshotVersion::V4) ? 8 : 1;
            if (format_size > 1 && !snap_fs->read(format_bytes + 1, format_size - 1))
                throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Truncated snapshot format header in {}", obj_path);
            ReadBufferFromMemory format_buffer(format_bytes, format_size);
            object_format = readSnapshotFormat(format_buffer);
            version_from_obj = object_format.version;
            read_size += format_size;
            LOG_DEBUG(log, "Got snapshot file header with version {}", toString(version_from_obj));
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
            if (version_from_obj == SnapshotVersion::UNKNOWN)
            {
                version_from_obj = SnapshotVersion::V0;
                object_format = SnapshotFormat(SnapshotVersion::V0);
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

        if (object_format.codec == SnapshotCodec::Zstd)
        {
            try
            {
                auto decompressed = ZstdLogCodec::decompress(body_string.data(), body_string.size());
                body_string.assign(reinterpret_cast<const char *>(decompressed->data_begin()), decompressed->size());
            }
            catch (Exception & e)
            {
                e.addMessage("Can't decompress snapshot object " + obj_path);
                throw;
            }
        }

        parseBatchBodyV2(store, body_string, buckets_edges, bucket_nodes, version_from_obj);
    }
}

void KeeperSnapshotStore::parseBatchBodyV2(
    KeeperStore & store,
    const String & body_string,
    BucketEdges & buckets_edges,
    BucketNodes & bucket_nodes,
    SnapshotVersion version_)
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
        case SnapshotBatchType::SNAPSHOT_TYPE_UINTMAP: {
            LOG_DEBUG(log, "Parsing batch int_map from snapshot, element count {}", batch->size());
            auto counters = parseBatchIntMapV2(store, loaded_objects_count, *batch, version_);
            if (counters.contains("ZXID"))
                loaded_zxid = true;
            if (counters.contains("SESSIONID"))
                loaded_session_id = true;
            LOG_DEBUG(log, "Parsed zxid {}, session_id_counter {}", store.getZxid(), store.getSessionIDCounter());
            break;
        }
        case SnapshotBatchType::SNAPSHOT_TYPE_CONFIG:
        case SnapshotBatchType::SNAPSHOT_TYPE_SERVER:
            break;
        default:
            break;
    }
}

void KeeperSnapshotStore::loadLatestSnapshot(KeeperStore & store, bool require_complete_state)
{
    size_t objects_cnt = objects_path.size();
    loaded_objects_count.reset();
    loaded_zxid = false;
    loaded_session_id = false;

    // The object IDs are consecutive starting from 1,
    // so the first number must be 1, and the last number must be the total count.
    if (objects_path.empty() || objects_path.begin()->first != 1 || objects_path.rbegin()->first != objects_cnt)
    {
        throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE,
        "Loading snapshot objects error, expecting {} objects, got {}",
        objects_cnt, objects_path.size());
    }

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

    if (require_complete_state && !loaded_objects_count)
        throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE, "Snapshot metadata does not contain OBJECTCOUNT");

    if (loaded_objects_count && *loaded_objects_count != objects_path.size())
    {
        throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE,
            "Loading snapshot objects error, expecting {} objects, got {} objects",
            *loaded_objects_count, objects_path.size());
    }

    if (require_complete_state)
    {
        if (!loaded_zxid)
            throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE, "Snapshot metadata does not contain ZXID");
        if (!loaded_session_id)
            throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE, "Snapshot metadata does not contain SESSIONID");

        /// Inspect parsed records before the initialized store can supply a default root.
        bool has_root = false;
        for (const auto & object_nodes : all_objects_nodes)
            for (const auto & [path, node] : object_nodes[store.getBucketIndex("/")])
                if (path == "/")
                    has_root = true;
        if (!has_root)
            throw Exception(ErrorCodes::SNAPSHOT_OBJECT_INCOMPLETE, "Snapshot does not contain root node /");
    }

    LOG_INFO(log, "Building data tree from snapshot objects");
    watch.restart();

    /// Build data tree relationship in parallel
    for (UInt32 thread_id = 0; thread_id < SNAPSHOT_THREAD_NUM; thread_id++)
    {
        thread_pool.trySchedule(
            [this, thread_id, &store]
            {
                Poco::Logger * thread_log = &(Poco::Logger::get("KeeperSnapshotStore.buildDataTreeThread#" + std::to_string(thread_id)));
                for (UInt32 bucket_id = 0; bucket_id < store.getDataTreeBucketNum(); bucket_id++)
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

    all_objects_edges.clear();
    all_objects_nodes.clear();

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
        ssize_t ret = pread(snap_fd, read_buf, buf_size, offset);

        if (ret < 0)
            throwFromErrno(
                ErrorCodes::CORRUPTED_SNAPSHOT, "Fail to read snapshot file {}, offset {}, length {}", obj_path, offset, buf_size);

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
            throwFromErrno(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Fail to write a snapshot file {}", obj_path);
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

size_t KeeperSnapshotManager::createSnapshotAsync(SnapTask & snap_task, SnapshotFormat format)
{
    auto && meta = snap_task.s;
    meta->set_size(snap_task.nodes_count);
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, *meta, object_node_size, SAVE_BATCH_SIZE, format);
    snap_store->init();
    LOG_INFO(
        log,
        "Creating snapshot with last_log_term {}, last_log_idx {}, size {}, nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, "
        "zxid {}",
        meta->get_last_log_term(),
        meta->get_last_log_idx(),
        meta->size(),
        snap_task.nodes_count,
        snap_task.ephemeral_nodes_count,
        snap_task.session_count,
        snap_task.next_session_id,
        snap_task.next_zxid);
    size_t obj_size = snap_store->createObjectsAsync(snap_task);
    snapshots[getSnapshotStoreMapKey(*meta)] = snap_store;
    return obj_size;
}

size_t KeeperSnapshotManager::createSnapshot(
    snapshot & meta, KeeperStore & store, int64_t next_zxid, int64_t next_session_id, SnapshotFormat format)
{
    size_t store_size = store.getNodesCount();
    meta.set_size(store_size);
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size, SAVE_BATCH_SIZE, format);
    snap_store->init();
    LOG_INFO(
        log,
        "Creating snapshot with last_log_term {}, last_log_idx {}, size {}, nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, "
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
    snapshots[getSnapshotStoreMapKey(meta)] = snap_store;
    return obj_size;
}

bool KeeperSnapshotManager::receiveSnapshotMeta(snapshot & meta)
{
    ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
    snap_store->init();
    snapshots[getSnapshotStoreMapKey(meta)] = snap_store;
    return true;
}

bool KeeperSnapshotManager::existSnapshot(const snapshot & meta) const
{
    return snapshots.find(getSnapshotStoreMapKey(meta)) != snapshots.end();
}

bool KeeperSnapshotManager::existSnapshotObject(const snapshot & meta, ulong obj_id) const
{
    auto it = snapshots.find(getSnapshotStoreMapKey(meta));
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
    auto it = snapshots.find(getSnapshotStoreMapKey(meta));

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
    auto it = snapshots.find(getSnapshotStoreMapKey(meta));
    ptr<KeeperSnapshotStore> store;
    if (it == snapshots.end())
    {
        meta.set_size(0);
        store = cs_new<KeeperSnapshotStore>(snap_dir, meta);
        store->init();
        snapshots[getSnapshotStoreMapKey(meta)] = store;
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
    auto it = snapshots.find(getSnapshotStoreMapKey(meta));
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
    snapshots.clear();

    Poco::File file_dir(snap_dir);

    if (!file_dir.exists())
        return 0;

    std::vector<String> files;
    file_dir.list(files);

    for (const auto & file : files)
    {
        if (!file.starts_with("snapshot_"))
        {
            LOG_WARNING(log, "Skip non-snapshot file {}", file);
            continue;
        }

        SnapObject s_obj;
        if (!s_obj.parseInfoFromObjectName(file))
            throw Exception(ErrorCodes::INVALID_SNAPSHOT_FILE_NAME, "Invalid snapshot object file name {}", file);

        auto key = getSnapshotStoreMapKey(s_obj);

        if (!snapshots.contains(key))
        {
            ptr<nuraft::cluster_config> config = cs_new<nuraft::cluster_config>(s_obj.log_last_index, s_obj.log_last_index - 1);
            nuraft::snapshot meta(s_obj.log_last_index, s_obj.log_last_term, config);
            ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
            snap_store->init(s_obj.create_time);
            snapshots[key] = snap_store;
        }

        String full_path = snap_dir + "/" + file;
        LOG_INFO(log, "Load filename {}, term {}, index {}, object id {}", file, s_obj.log_last_term, s_obj.log_last_index, s_obj.object_id);
        snapshots[key]->addObjectPath(s_obj.object_id, full_path);
    }

    LOG_INFO(log, "Loaded {} snapshot metas from snapshot directory {}", snapshots.size(), snap_dir);
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

    LOG_INFO(log, "There are {} snapshots, we will try to move {} of them", snapshots.size(), remove_count);

    while (remove_count > 0)
    {
        auto it = snapshots.begin();
        uint128_t remove_term_log_index = it->first;
        auto [log_term, log_index] = getTermLogFromSnapshotStoreMapKey(remove_term_log_index);
        LOG_INFO(log, "Remove snapshot with term {} log index {}", log_term, log_index);

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

                SnapObject s_obj;
                if (!s_obj.parseInfoFromObjectName(file))
                    continue;

                auto key = getSnapshotStoreMapKey(s_obj);
                if (remove_term_log_index == key)
                {
                    LOG_INFO(
                        log,
                        "remove_count {}, snapshot size {}, remove term with term {} log index {}, file {}",
                        remove_count,
                        snapshots.size(),
                        log_term,
                        log_index,
                        file);
                    Poco::File(snap_dir + "/" + file).remove();
                    if (snapshots.find(key) != snapshots.end())
                    {
                        snapshots.erase(it);
                    }
                }
            }
        }
        remove_count--;
    }

    if (snapshots.size() > keep_max_snapshot_count)
        LOG_ERROR(log, "Snapshots size() is still large than keep_max_snapshot_count {}, it's a bug",
                  snapshots.size(), keep_max_snapshot_count);

    return snapshots.size();
}

}
