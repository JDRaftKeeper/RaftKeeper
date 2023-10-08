#pragma once

#include <map>
#include <string>

#include <Common/IO/WriteBufferFromFile.h>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperStore.h>
#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#include <Service/proto/Log.pb.h>
#include <ZooKeeper/IKeeper.h>


namespace RK
{
using nuraft::snapshot;
using nuraft::ulong;

enum SnapshotVersion : uint8_t
{
    V0 = 0,
    V1 = 1, /// with ACL map, and last_log_term for file name
    None = 255,
};

static constexpr auto CURRENT_SNAPSHOT_VERSION = SnapshotVersion::V1;

struct SnapshotBatchHeader
{
    /// The length of the batch data (uncompressed)
    UInt32 data_length;
    /// The CRC32C of the batch data.
    /// If compression is enabled, this is the checksum of the compressed data.
    UInt32 data_crc;
    void reset()
    {
        data_length = 0;
        data_crc = 0;
    }
    static const size_t HEADER_SIZE = 8;
};

/**
 * Operate a snapshot, when the current snapshot is down, we should renew a store.
 *
 * A complete snapshot contains some object, every object is a file:
 *      snapshot_20230317070531_4351621_1
 *      snapshot_20230317070531_4351621_2
 *      snapshot_20230317070531_4351621_3
 *      snapshot_20230317070531_4351621_4
 * Object file name format see SNAPSHOT_FILE_NAME_V1.
 *
 * Snapshot object format:
 *      SnapshotHeader + (SnapshotBatch)[...] + SnapshotTail
 */
class KeeperSnapshotStore
{
public:
    using StringMap = std::unordered_map<std::string, std::string>;
    using IntMap = std::unordered_map<std::string, int64_t>;

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
        last_log_index = meta.get_last_log_idx();
        last_log_term = meta.get_last_log_term();

        ptr<buffer> snap_buf = meta.serialize();
        snap_meta = snapshot::deserialize(*(snap_buf.get()));

        if (max_object_node_size == 0)
        {
            LOG_WARNING(log, "max_object_node_size > 0");
            max_object_node_size = MAX_OBJECT_NODE_SIZE;
        }
    }

    ~KeeperSnapshotStore() = default;

    /// Create snapshot object, return the size of objects
    size_t createObjects(KeeperStore & store, int64_t next_zxid = 0, int64_t next_session_id = 0);

    /// initialize a snapshot store
    void init(std::string create_time);

    /// parse the latest snapshot
    void loadLatestSnapshot(KeeperStore & store);

    /// load on object of the latest snapshot
    void loadObject(ulong obj_id, ptr<buffer> & buffer);

    /// whether an object id exist
    bool existObject(ulong obj_id);

    /// save an object
    void saveObject(ulong obj_id, buffer & buffer);

    void addObjectPath(ulong obj_id, std::string & path);

    /// get snapshot metadata
    ptr<snapshot> getSnapshotMeta() { return snap_meta; }

    /// get snapshot create time
    time_t & getCreateTimeT() { return curr_time_t; }

    /// parse object id from file name
    static size_t getObjectIdx(const std::string & file_name);

#ifdef __APPLE__
    /// create_time, last_log_index, object_id
    static constexpr char SNAPSHOT_FILE_NAME[] = "snapshot_%s_%llu_%llu";
    /// create_time, last_log_index, last_log_term, object_id
    static constexpr char SNAPSHOT_FILE_NAME_V1[] = "snapshot_%s_%llu_%llu_%llu";
#else
    /// create_time, last_log_index, object_id
    static constexpr char SNAPSHOT_FILE_NAME[] = "snapshot_%s_%lu_%lu";
    /// create_time, last_log_index, last_log_term, object_id
    static constexpr char SNAPSHOT_FILE_NAME_V1[] = "snapshot_%s_%lu_%lu_%lu";
#endif

    static const String MAGIC_SNAPSHOT_TAIL;
    static const String MAGIC_SNAPSHOT_HEAD;

    /// 0.3KB / Node * 100M Count =  300MB
    static const UInt32 MAX_OBJECT_NODE_SIZE = 1000000;
    /// 100M Count / 10K = 10K
    static const UInt32 SAVE_BATCH_SIZE = 10000;

    static const int SNAPSHOT_THREAD_NUM = 8;
    static const int IO_BUFFER_SIZE = 16384; /// 16K

    SnapshotVersion version = CURRENT_SNAPSHOT_VERSION;

private:
    /// get path of an object
    void getObjectPath(ulong object_id, std::string & path);

    /// parse object
    bool parseObject(std::string obj_path, KeeperStore & store);

    /// load batch header in an object
    /// TODO use inter nal buffer
    bool loadBatchHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head);

    /// serialize whole data tree
    size_t serializeDataTree(KeeperStore & storage);

    /**
     * Serialize data tree by deep traversal.
     */
    void serializeNode(
        ptr<WriteBufferFromFile> & out,
        ptr<SnapshotBatchPB> & batch,
        KeeperStore & store,
        const String & path,
        uint64_t & processed,
        uint32_t & checksum);

    /// append node to batch
    inline static void appendNodeToBatch(ptr<SnapshotBatchPB> batch, const String & path, std::shared_ptr<KeeperNode> node);

    /// snapshot directory, note than the directory may contain more than one snapshot.
    std::string snap_dir;

    /// an object can contain how many items
    UInt32 max_object_node_size;

    /// a batch can contain how many items
    UInt32 save_batch_size;

    Poco::Logger * log;

    /// metadata of a snapshot
    ptr<snapshot> snap_meta;

    /// last log index in the snapshot
    UInt64 last_log_index;

    /// lost log index term in the snapshot
    UInt64 last_log_term;

    std::map<ulong, std::string> objects_path;

    std::string curr_time;
    /// snapshot create time, determined when create a new snapshot.
    time_t curr_time_t;

    /// used to create snapshot asynchronously,
    /// but now creating snapshot is synchronous
    std::shared_ptr<ThreadPool> snapshot_thread;
};

using KeeperSnapshotStoreMap = std::map<uint64_t, ptr<KeeperSnapshotStore>>;

/**
 * Snapshots manager who may create, remove snapshot.
 */
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

    ~KeeperSnapshotManager() = default;

    size_t createSnapshot(snapshot & meta, KeeperStore & storage, int64_t next_zxid = 0, int64_t next_session_id = 0);

    /// save snapshot meta, invoked when we receive an snapshot from leader.
    bool receiveSnapshotMeta(snapshot & meta);

    /// save snapshot object, invoked when we receive an snapshot from leader.
    bool saveSnapshotObject(snapshot & meta, ulong obj_id, buffer & buffer);

    /// whether snapshot exists
    bool existSnapshot(const snapshot & meta);

    /// whether snapshot object exists
    bool existSnapshotObject(const snapshot & meta, ulong obj_id);

    /// load snapshot object, invoked when leader should send snapshot to others.
    bool loadSnapshotObject(const snapshot & meta, ulong obj_id, ptr<buffer> & buffer);

    /// parse snapshot object, invoked when follower apply received snapshot to state machine.
    bool parseSnapshot(const snapshot & meta, KeeperStore & storage);

    /// latest snapshot meta
    ptr<snapshot> lastSnapshot();

    /// when initializing, load snapshots meta
    size_t loadSnapshotMetas();

    time_t getLastCreateTime();

    /// remove outdated snapshots, after invoked at mast keep_max_snapshot_count remains.
    size_t removeSnapshots();

private:

    /// snapshot directory
    std::string snap_dir;

    /// max snapshot count to remain
    UInt32 keep_max_snapshot_count;

    /// item limit of an object
    UInt32 object_node_size;
    Poco::Logger * log;

    KeeperSnapshotStoreMap snapshots;
    std::string last_create_time_str;
};

}
