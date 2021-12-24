#pragma once

#include <map>
#include <string>
#include <Service/LogEntry.h>
#include <Service/NuRaftCommon.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/proto/Log.pb.h>
#include <libnuraft/nuraft.hxx>
#include <Common/ZooKeeper/IKeeper.h>


namespace DB
{
using nuraft::snapshot;
using nuraft::ulong;
using StringVec = std::vector<std::string>;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ACL map, and last_log_term for file name
};

static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V1;

struct SnapshotBatchHeader
{
    // The length of the batch data (uncompressed)
    UInt32 data_length;
    // The CRC32C of the batch data.
    // If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;
    void reset()
    {
        data_length = 0;
        data_crc = 0;
    }
    static const size_t HEADER_SIZE = 8;
};

//Snapshot stored in disk, one snapshot object corresponds one file
//SnapshotHeader + (SnapshotBatchHeader+LogEntryBody)[...]
class KeeperSnapshotStore
{
public:
    KeeperSnapshotStore(
        const std::string & snap_dir_,
        snapshot & meta,
        UInt32 max_object_node_size_ = MAX_OBJECT_NODE_SIZE,
        UInt32 save_batch_size_ = SAVE_BATCH_SIZE)
        : snap_dir(snap_dir_)
        , max_object_node_size(max_object_node_size_)
        , save_batch_size(save_batch_size_)
        , log(&(Poco::Logger::get("KeeperSnapshotStore")))
    {
        //snap_header.entry_size = meta.size();
        log_last_index = meta.get_last_log_idx();
        ptr<buffer> snap_buf = meta.serialize();
        snap_meta = snapshot::deserialize(*(snap_buf.get()));
        if (max_object_node_size == 0)
        {
            LOG_WARNING(log, "max_object_node_size > 0");
            max_object_node_size = MAX_OBJECT_NODE_SIZE;
        }
    }
    ~KeeperSnapshotStore() { }

    //create snapshot object, return the size of objects
    size_t createObjects(SvsKeeperStorage & storage);
    // init snapshot store for receive snapshot object
    void init(std::string create_time);
    void parseObject(SvsKeeperStorage & storage);

    void loadObject(ulong obj_id, ptr<buffer> & buffer);
    bool existObject(ulong obj_id);
    void saveObject(ulong obj_id, buffer & buffer);

    void addObjectPath(ulong obj_id, std::string & path);

    ptr<snapshot> getSnapshot() { return snap_meta; }

    time_t & getCreateTimeT() { return curr_time_t; }

    static void getFileTime(const std::string file_name, std::string & time);

public:
#ifdef __APPLE__
    //snapshot_createtime_lastlogindex_objectid
    static constexpr char SNAPSHOT_FILE_NAME[] = "snapshot_%s_%llu_%llu";
#else
    //snapshot_createtime_lastlogindex_objectid
    static constexpr char SNAPSHOT_FILE_NAME[] = "snapshot_%s_%lu_%lu";
#endif

    using StringMap = std::unordered_map<std::string, std::string>;
    using IntMap = std::unordered_map<std::string, int64_t>;

    //0.3KB / Node * 100M Count =  300MB
    static const UInt32 MAX_OBJECT_NODE_SIZE = 1000000;
    // 100M Count / 10K = 10K
    static const UInt32 SAVE_BATCH_SIZE = 10000;
    static const int SNAPSHOT_THREAD_NUM = 8;
    static const int IO_BUFFER_SIZE = 16384; //16K

    SnapshotVersion version = CURRENT_SNAPSHOT_VERSION;

private:
    void getObjectPath(ulong object_id, std::string & path);
    bool parseOneObject(std::string obj_path, SvsKeeperStorage & storage);
    bool loadHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head);

private:
    std::string snap_dir;
    UInt32 max_object_node_size;
    UInt32 save_batch_size;
    Poco::Logger * log;
    //SnapshotHeader snap_header;
    ptr<snapshot> snap_meta;
    UInt64 log_last_index;
    std::map<ulong, std::string> objects_path;
    std::string curr_time;
    time_t curr_time_t;
    std::shared_ptr<ThreadPool> snapshot_thread;
};

using KeeperSnapshotStoreMap = std::map<uint64_t, ptr<KeeperSnapshotStore>>;

//Manage some snapshot object
class KeeperSnapshotManager
{
public:
    KeeperSnapshotManager(const std::string & snap_dir_, UInt32 keep_max_snapshot_count_, UInt32 object_node_size_)
        : snap_dir(snap_dir_)
        , keep_max_snapshot_count(keep_max_snapshot_count_)
        , object_node_size(object_node_size_)
        , log(&(Poco::Logger::get("KeeperSnapshotManager")))
    {
    }
    ~KeeperSnapshotManager() { }
    size_t createSnapshot(snapshot & meta, SvsKeeperStorage & storage, const SnapshotVersion version = SnapshotVersion::V1);
    bool receiveSnapshot(snapshot & meta);
    bool existSnapshot(const snapshot & meta);
    bool existSnapshotObject(const snapshot & meta, ulong obj_id);
    bool loadSnapshotObject(const snapshot & meta, ulong obj_id, ptr<buffer> & buffer);
    bool saveSnapshotObject(snapshot & meta, ulong obj_id, buffer & buffer);
    bool parseSnapshot(const snapshot & meta, SvsKeeperStorage & storage, const SnapshotVersion version = SnapshotVersion::V1);
    ptr<snapshot> lastSnapshot();
    time_t getLastCreateTime();
    size_t loadSnapshotMetas();
    size_t removeSnapshots();
    
private:
    std::string snap_dir;
#ifdef __clang__
    [[maybe_unused]] UInt32 keep_max_snapshot_count;
    [[maybe_unused]] std::atomic<uint64_t> last_committed_idx;
#else
    UInt32 keep_max_snapshot_count;
    std::atomic<uint64_t> last_committed_idx;
#endif
    UInt32 object_node_size;

    Poco::Logger * log;
    //std::mutex snap_mutex;
    KeeperSnapshotStoreMap snapshots;
    std::string last_create_time_str;
};

}
