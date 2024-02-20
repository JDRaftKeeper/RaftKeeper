#pragma once

#include <Service/SnapshotCommon.h>


namespace RK
{
using nuraft::snapshot;
using nuraft::ulong;

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
    KeeperSnapshotStore(
        const String & snap_dir_,
        snapshot & meta,
        UInt32 max_object_node_size_ = MAX_OBJECT_NODE_SIZE,
        UInt32 save_batch_size_ = SAVE_BATCH_SIZE,
        SnapshotVersion version_ = CURRENT_SNAPSHOT_VERSION)
        : version(version_)
        , snap_dir(snap_dir_)
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
    void init(String create_time);

    /// Load the latest snapshot object.
    void loadLatestSnapshot(KeeperStore & store);

    /// load on object of the latest snapshot
    void loadObject(ulong obj_id, ptr<buffer> & buffer);

    /// whether an object id exist
    bool existObject(ulong obj_id);

    /// save an object
    void saveObject(ulong obj_id, buffer & buffer);

    void addObjectPath(ulong obj_id, String & path);

    /// get snapshot metadata
    ptr<snapshot> getSnapshotMeta() { return snap_meta; }

    /// parse object id from file name
    static size_t getObjectIdx(const String & file_name);

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

    static constexpr int SNAPSHOT_THREAD_NUM = 8;
    static constexpr int IO_BUFFER_SIZE = 16384; /// 16K

    SnapshotVersion version;

private:
    /// For snapshot version v1 /// TODO delete
    size_t createObjectsV1(KeeperStore & store, int64_t next_zxid = 0, int64_t next_session_id = 0);
    /// For snapshot version v3
    size_t createObjectsV2(KeeperStore & store, int64_t next_zxid = 0, int64_t next_session_id = 0);

    /// get path of an object
    void getObjectPath(ulong object_id, String & path);

    /// Parse an snapshot object. We should take the version from snapshot in general.
    void parseObject(KeeperStore & store, String obj_path);

    /// Parse batch header in an object
    /// TODO use internal buffer
    void parseBatchHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head);

    /// Parse a batch /// TODO delete
    void parseBatchBody(KeeperStore & store, char * batch_buf, size_t length, SnapshotVersion version_);
    /// Parse a batch
    void parseBatchBodyV2(KeeperStore & store, char * buf, size_t length, SnapshotVersion version_);

    /// Serialize whole data tree /// TODO delete
    size_t serializeDataTree(KeeperStore & storage);

    /// For snapshot version v2
    size_t serializeDataTreeV2(KeeperStore & storage);

    /// Serialize data tree by deep traversal. /// TODO delete
    void serializeNode(
        ptr<WriteBufferFromFile> & out,
        ptr<SnapshotBatchPB> & batch,
        KeeperStore & store,
        const String & path,
        uint64_t & processed,
        uint32_t & checksum);

    /// For snapshot version v3
    void serializeNodeV2(
        ptr<WriteBufferFromFile> & out,
        ptr<SnapshotBatchBody> & batch,
        KeeperStore & store,
        const String & path,
        uint64_t & processed,
        uint32_t & checksum);

    /// Append node to batch /// TODO delete
    inline static void
    appendNodeToBatch(ptr<SnapshotBatchPB> batch, const String & path, std::shared_ptr<KeeperNode> node, SnapshotVersion version);
    /// For snapshot version v2
    inline static void
    appendNodeToBatchV2(ptr<SnapshotBatchBody> batch, const String & path, std::shared_ptr<KeeperNode> node, SnapshotVersion version);

    /// Snapshot directory, note than the directory may contain more than one snapshot.
    String snap_dir;

    /// How many items an object can contain
    UInt32 max_object_node_size;

    /// How many items a batch can contain
    UInt32 save_batch_size;

    Poco::Logger * log;

    /// metadata of a snapshot
    ptr<snapshot> snap_meta;

    /// Last log index in the snapshot
    UInt64 last_log_index;

    /// Lost log index term in the snapshot
    UInt64 last_log_term;

    std::map<ulong, String> objects_path;

    /// Appended to snapshot file name
    String curr_time;

    /// Used to create snapshot asynchronously,
    /// but now creating snapshot is synchronous
    std::shared_ptr<ThreadPool> snapshot_thread;
};

using KeeperSnapshotStoreMap = std::map<uint64_t, ptr<KeeperSnapshotStore>>;

/**
 * Snapshots manager who may create, remove snapshots.
 */
class KeeperSnapshotManager
{
public:
    KeeperSnapshotManager(const String & snap_dir_, UInt32 keep_max_snapshot_count_, UInt32 object_node_size_)
        : snap_dir(snap_dir_)
        , keep_max_snapshot_count(keep_max_snapshot_count_)
        , object_node_size(object_node_size_)
        , log(&(Poco::Logger::get("KeeperSnapshotManager")))
    {
    }

    ~KeeperSnapshotManager() = default;

    size_t createSnapshot(
        snapshot & meta,
        KeeperStore & store,
        int64_t next_zxid = 0,
        int64_t next_session_id = 0,
        SnapshotVersion version = CURRENT_SNAPSHOT_VERSION);

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

    /// remove outdated snapshots, after invoked at mast keep_max_snapshot_count remains.
    size_t removeSnapshots();

private:
    /// snapshot directory
    String snap_dir;

    /// max snapshot count to remain
    UInt32 keep_max_snapshot_count;

    /// item limit of an object
    UInt32 object_node_size;
    Poco::Logger * log;

    KeeperSnapshotStoreMap snapshots;
    String last_create_time_str;
};

}
