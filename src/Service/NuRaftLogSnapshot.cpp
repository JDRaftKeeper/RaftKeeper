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

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif

namespace DB
{
using nuraft::cs_new;

//const int KeeperSnapshotStore::SNAPSHOT_THREAD_NUM = 4;

int openFileForWrite(std::string & obj_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    errno = 0;
    int snap_fd = ::open(obj_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
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

//return end object index
void createObjectContainer(
    SvsKeeperStorage::Container::InnerMap & innerMap,
    UInt32 obj_idx,
    UInt16 object_count,
    UInt32 save_batch_size,
    std::map<ulong, std::string> & objects_path)
{
    Poco::Logger * log = &(Poco::Logger::get("KeeperSnapshotStore"));
    UInt32 max_node_size = (innerMap.size() - 1) / object_count + 1;
    auto container_it = innerMap.getMap().begin();

    ptr<SnapshotBatchPB> batch_pb;
    UInt32 node_index = 1;
    int snap_fd = -1;
    size_t file_size = 0;
    std::string obj_path;
    LOG_INFO(log, "Create snapshot object for container, max node size {}, node index {}", max_node_size, node_index);
    while (node_index <= innerMap.size())
    {
        if (max_node_size == 1 || node_index % max_node_size == 1)
        {
            if (snap_fd > 0)
            {
                ::close(snap_fd);
            }
            obj_path = objects_path[obj_idx];
            snap_fd = openFileForWrite(obj_path);
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
        Coordination::write(node->acls, out);
        Coordination::write(node->is_ephemeral, out);
        Coordination::write(node->is_sequental, out);
        Coordination::write(node->stat, out);
        Coordination::write(node->seq_num, out);

        ptr<buffer> buf = out.getBuffer();
        buf->pos(0);
        data->set_data(std::string(reinterpret_cast<char *>(buf->data_begin()), buf->size()));

        //last node, save to file
        if (node_index % save_batch_size == 0 || node_index % max_node_size == 0 || node_index == innerMap.size())
        {
            auto ret = saveBatch(snap_fd, batch_pb, obj_path);
            if (ret > 0)
            {
                file_size += ret;
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
        ::close(snap_fd);
    }
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
    while (node_index <= ephemerals.size())
    {
        if (node_index % max_node_size == 1)
        {
            if (snap_fd > 0)
            {
                ::close(snap_fd);
            }
            obj_path = objects_path[obj_idx];
            snap_fd = openFileForWrite(obj_path);
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
            auto ret = saveBatch(snap_fd, batch_pb, obj_path);
            if (ret > 0)
            {
                file_size += ret;
            }
            else
            {
                return;
            }
        }
        ephemeral_it++;
        node_index++;
    }

    if (snap_fd > 0)
    {
        ::close(snap_fd);
    }
}

void KeeperSnapshotStore::getObjectPath(ulong object_id, std::string & obj_path)
{
    char path_buf[1024];
    snprintf(path_buf, 1024, SNAPSHOT_FILE_NAME, curr_time.c_str(), log_last_index, object_id);
    obj_path = path_buf;
    obj_path = snap_dir + "/" + obj_path;
}

void KeeperSnapshotStore::getFileTime(const std::string file_name, std::string & time)
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

    size_t obj_size = storage.container.getBlockNum() * container_object_count + ephemeral_object_count;

    LOG_DEBUG(
        log,
        "Block num {}, block max size {} , container_object_count {}, ephemeral_object_count {}",
        storage.container.getBlockNum(),
        block_max_size,
        container_object_count,
        ephemeral_object_count);

    std::string obj_path;
    std::map<ulong, std::string> objects;
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
        container_thread_pool.trySchedule([thread_idx, &storage, &objects, container_object_count, batch_size] {
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
                    createObjectContainer(storage.container.getMap(i), obj_idx, container_object_count, batch_size, objects);
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
                //LOG_INFO(log, "Load batch size {}, end point {}", batch_pb.data_size(), read_size);
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(sizeof(uint32_t) + data.size());
                    //buf->put(data.data(), data.size());
                    buf->put(data);
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    ptr<KeeperNode> node = cs_new<KeeperNode>();
                    std::string key;
                    Coordination::read(key, in);
                    //LOG_INFO(log, "Read key {}", key);
                    Coordination::read(node->data, in);
                    Coordination::read(node->acls, in);
                    Coordination::read(node->is_ephemeral, in);
                    Coordination::read(node->is_sequental, in);
                    Coordination::read(node->stat, in);
                    Coordination::read(node->seq_num, in);
                    storage.container.emplace(key, std::move(node));
                }
            }
            break;
            case SnapshotTypePB::SNAPSHOT_TYPE_DATA_EPHEMERAL: {
                for (int data_idx = 0; data_idx < batch_pb.data_size(); data_idx++)
                {
                    const SnapshotItemPB & item_pb = batch_pb.data(data_idx);
                    const std::string & data = item_pb.data();
                    ptr<buffer> buf = buffer::alloc(sizeof(uint32_t) + data.size());
                    buf->put(data.data(), data.size());
                    buf->pos(0);
                    ReadBufferFromNuraftBuffer in(buf);
                    Int64 session_id;
                    size_t path_size;
                    Coordination::read(session_id, in);
                    Coordination::read(path_size, in);

                    std::unordered_set<std::string> set;
                    for (size_t i = 0; i < path_size; i++)
                    {
                        std::string path;
                        Coordination::read(path, in);
                        set.emplace(path);
                    }
                    storage.ephemerals[session_id] = set;
                }
            }
            break;
            case SnapshotTypePB::SNAPSHOT_TYPE_CONFIG:
            case SnapshotTypePB::SNAPSHOT_TYPE_SERVER:
                //TODO:Add other type logic
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
    ulong log_last_index;
    ulong object_id;

    for (auto it = file_vec.begin(); it != file_vec.end(); it++)
    {
        sscanf((*it).c_str(), "snapshot_%[^_]_%lu_%lu", time_str, &log_last_index, &object_id);
        if (snapshots.find(log_last_index) == snapshots.end())
        {
            ptr<nuraft::cluster_config> config = cs_new<nuraft::cluster_config>(log_last_index, log_last_index - 1);
            nuraft::snapshot meta(log_last_index, 1, config);
            ptr<KeeperSnapshotStore> snap_store = cs_new<KeeperSnapshotStore>(snap_dir, meta, object_node_size);
            snap_store->init(time_str);
            snapshots[meta.get_last_log_idx()] = snap_store;
            LOG_INFO(log, "load filename {}, time {}, index {}, object id {}", (*it), time_str, log_last_index, object_id);
        }
        std::string full_path = snap_dir + "/" + *it;
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
    size_t remove_count = snapshots.size() - keep_max_snapshot_count;
    char time_str[128];
    ulong log_last_index;
    ulong object_id;
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
