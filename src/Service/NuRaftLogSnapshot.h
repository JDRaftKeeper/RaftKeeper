#pragma once

#include <Service/SnapshotCommon.h>
#include <Service/Metrics.h>
#include <Common/Stopwatch.h>
#include <charconv>


namespace RK
{
using nuraft::snapshot;
using nuraft::ulong;

/// Asynchronously snapshot creating task.
struct SnapTask
{
    ptr<snapshot> s;
    int64_t next_zxid;
    int64_t next_session_id;
    int64_t nodes_count;
    int64_t ephemeral_nodes_count;
    int64_t session_count;
    SessionAndTimeout session_and_timeout;
    std::unordered_map<uint64_t, Coordination::ACLs> acl_map;
    KeeperStore::SessionAndAuth session_and_auth;
    std::shared_ptr<KeeperStore::BucketNodes> buckets_nodes;
    nuraft::async_result<bool>::handler_type when_done;

    SnapTask(const ptr<snapshot> & s_, KeeperStore & store, nuraft::async_result<bool>::handler_type & when_done_)
        : s(s_), next_zxid(store.getZxid()), next_session_id(store.getSessionIDCounter()), when_done(when_done_)
    {
        auto * log = &Poco::Logger::get("SnapTask");
        session_and_timeout = store.getSessionAndTimeOut();
        session_count = session_and_timeout.size();
        session_and_auth = store.getSessionAndAuth();

        acl_map = store.getACLMap().getMapping();
        Stopwatch watch;
        buckets_nodes = store.dumpDataTree();
        LOG_INFO(log, "Dumping data tree costs {}ms", watch.elapsedMilliseconds());
        Metrics::getMetrics().snap_blocking_time_ms->add(watch.elapsedMilliseconds());

        for (auto && bucket : *buckets_nodes)
        {
            LOG_DEBUG(log, "Get bucket size {}", bucket.size());
        }

        nodes_count = store.getNodesCount();
        ephemeral_nodes_count = store.getTotalEphemeralNodesCount();
    }
};

struct SnapObject
{
    /// create_time, last_log_index, object_id
    static constexpr char SNAPSHOT_FILE_NAME[] = "snapshot_{}_{}_{}";

    /// create_time, last_log_term, last_log_index, object_id
    static constexpr char SNAPSHOT_FILE_NAME_V1[] = "snapshot_{}_{}_{}_{}";

    String create_time;
    UInt64 log_last_term;
    UInt64 log_last_index;
    UInt64 object_id;

    explicit SnapObject(const String & _create_time = "", UInt64 _log_last_term = 1, UInt64 _log_last_index = 1, UInt64 _object_id = 1)
        :create_time(_create_time), log_last_term(_log_last_term), log_last_index(_log_last_index), object_id(_object_id)
    {
    }

    String getObjectName()
    {
        return fmt::format(SNAPSHOT_FILE_NAME_V1, create_time, log_last_term, log_last_index, object_id);
    }

    bool parseInfoFromObjectName(const String & object_name)
    {
        auto tryReadUInt64Text = [] (const String & str, UInt64 & num)
        {
            auto [_, ec] = std::from_chars(str.data(), str.data() + str.size(), num);
            return ec == std::errc();
        };

        Strings tokens;
        splitInto<'_'>(tokens, object_name);

        if (tokens.size() == 4)
        {
            if (!tryReadUInt64Text(tokens[2], log_last_index))
                return false;

            if (!tryReadUInt64Text(tokens[3], object_id))
                return false;
        }
        else if (tokens.size() == 5)
        {
            if (!tryReadUInt64Text(tokens[2], log_last_term))
                return false;

            if (!tryReadUInt64Text(tokens[3], log_last_index))
                return false;

            if (!tryReadUInt64Text(tokens[4], object_id))
                return false;
        }
        else
        {
            return false;
        }

        create_time = std::move(tokens[1]);

        return true;
    }
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

    /// Create async snapshot object by snap_task, return the size of objects
    size_t createObjectsAsync(SnapTask & snap_task);

    /// initialize a snapshot store
    void init(const String & create_time  = "");

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

    static constexpr int SNAPSHOT_THREAD_NUM = 8;
    static constexpr int IO_BUFFER_SIZE = 16384; /// 16K

    SnapshotVersion version;

    std::map<ulong, String> getObjectPaths() const { return objects_path; }

private:
    /// For snapshot version v2
    size_t createObjectsV2(KeeperStore & store, int64_t next_zxid = 0, int64_t next_session_id = 0);

    /// For create snapshot async
    size_t createObjectsAsyncImpl(SnapTask & snap_task);

    /// get path of an object
    void getObjectPath(ulong object_id, String & path) const;

    /// Parse an snapshot object. We should take the version from snapshot in general.
    void parseObject(KeeperStore & store, String obj_path, BucketEdges &, BucketNodes &);

    /// Parse batch header in an object
    /// TODO use internal buffer
    void parseBatchHeader(ptr<std::fstream> fs, SnapshotBatchHeader & head);

    /// Parse a batch
    void parseBatchBodyV2(KeeperStore & store, const String &, BucketEdges &, BucketNodes &, SnapshotVersion version_);

    /// For snapshot version v2
    size_t serializeDataTreeV2(KeeperStore & storage);

    /// For async snapshot
    size_t serializeDataTreeAsync(SnapTask & snap_task) const;

    /// For snapshot version v2
    void serializeNodeV2(
        ptr<WriteBufferFromFile> & out,
        ptr<SnapshotBatchBody> & batch,
        KeeperStore & store,
        const String & path,
        uint64_t & processed,
        uint32_t & checksum);

    /// For async snapshot
    uint32_t serializeNodeAsync(
        ptr<WriteBufferFromFile> & out,
        ptr<SnapshotBatchBody> & batch,
        BucketNodes & bucket_nodes) const;

    /// Append node to batch version v2
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

    /// Loaded snapshot object count which is read from object1
    /// Added from RaftKeeper v2.2.0
    std::optional<UInt32> loaded_objects_count;

    std::vector<BucketEdges> all_objects_edges;
    std::vector<BucketNodes> all_objects_nodes;

    /// Appended to snapshot file name
    String curr_time;

    /// Used to create snapshot asynchronously,
    /// but now creating snapshot is synchronous
    std::shared_ptr<ThreadPool> snapshot_thread;
};

// In Raft, each log entry can be uniquely identified by the combination of its Log Index and Term.
inline uint128_t getSnapshotStoreMapKeyImpl(UInt64 log_term, UInt64 log_idx)
{
    return static_cast<uint128_t>(log_term) << 64 | log_idx;
}

inline uint128_t getSnapshotStoreMapKey(const snapshot & meta)
{
    return getSnapshotStoreMapKeyImpl(meta.get_last_log_term(), meta.get_last_log_idx());
}

inline uint128_t getSnapshotStoreMapKey(const SnapObject & obj)
{
    return getSnapshotStoreMapKeyImpl(obj.log_last_term, obj.log_last_index);
}

inline std::pair<uint64_t, uint64_t> getTermLogFromSnapshotStoreMapKey(uint128_t & key)
{
    return {static_cast<uint64_t>(key >> 64), static_cast<uint64_t>(key & 0xFFFFFFFFFFFFFFFF)};
}

// Map key is term << 64 | log
using KeeperSnapshotStoreMap = std::map<uint128_t, ptr<KeeperSnapshotStore>>;

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

    size_t createSnapshotAsync(
        SnapTask & snap_task,
        SnapshotVersion version = CURRENT_SNAPSHOT_VERSION);

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
    bool existSnapshot(const snapshot & meta) const;

    /// whether snapshot object exists
    bool existSnapshotObject(const snapshot & meta, ulong obj_id) const;

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

    const KeeperSnapshotStoreMap & getSnapshots() const { return snapshots; }

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
