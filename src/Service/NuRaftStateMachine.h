#pragma once

#include <atomic>
#include <cassert>
#include <mutex>
#include <string.h>
#include <time.h>
#include <unordered_map>

#include <common/types.h>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperStore.h>
#include <Service/LastCommittedIndexManager.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/Settings.h>
#include <Service/ThreadSafeQueue.h>


namespace RK
{
using nuraft::async_result;
using nuraft::buffer;
using nuraft::cs_new;

using KeeperResponsesQueue = ThreadSafeQueue<ResponseForSession>;

class RequestProcessor;

class NuRaftStateMachine : public nuraft::state_machine
{
public:
    NuRaftStateMachine(
        KeeperResponsesQueue & responses_queue_,
        const RaftSettingsPtr & raft_settings_,
        String & snap_dir,
        String & log_dir,
        UInt32 internal,
        UInt32 keep_max_snapshot_count,
        std::mutex & new_session_id_callback_mutex_,
        std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback_,
        ptr<nuraft::log_store> log_store_ = nullptr,
        String super_digest = "",
        UInt32 object_node_size = MAX_OBJECT_NODE_SIZE,
        std::shared_ptr<RequestProcessor> request_processor_ = nullptr);

    ~NuRaftStateMachine() override = default;

    /// do nothing
    ptr<buffer> pre_commit(const ulong log_idx, buffer & data) override; // NOLINT(readability-avoid-const-params-in-decls)

    /// do nothing
    void rollback(const ulong log_idx, buffer & data) override; // NOLINT(readability-avoid-const-params-in-decls)

    /**
     * Commit the given Raft log.
     *
     * NOTE:
     *   Given memory buffer is owned by caller, so that
     *   commit implementation should clone it if user wants to
     *   use the memory even after the commit call returns.
     *
     *   Here provide a default implementation for facilitating the
     *   situation when application does not care its implementation.
     *
     * @param log_idx Raft log number to commit.
     * @param data Payload of the Raft log.
     * @return Result value of state machine.
     */
    ptr<buffer> commit(const ulong log_idx, buffer & data) override; // NOLINT(readability-avoid-const-params-in-decls)

    /// Just for unit test
    ptr<buffer> commit(const ulong log_idx, buffer & data, bool ignore_response); // NOLINT(readability-avoid-const-params-in-decls)

    /**
     * Decide to create snapshot or not.
     * Once the pre-defined condition is satisfied, Raft core will invoke
     * this function to ask if it needs to create a new snapshot.
     * If user-defined state machine does not want to create snapshot
     * at this time, this function will return `false`.
     *
     * @return `true` if wants to create snapshot.
     *         `false` if does not want to create snapshot.
     */
    bool chk_create_snapshot() override;

    /**
     * Create a snapshot corresponding to the given info.
     *
     * @param s Snapshot info to create.
     * @param when_done Callback function that will be called after
     *                  snapshot creation is done.
     */
    void create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done) override;

    /// sync create snapshot
    void create_snapshot(snapshot & s, int64_t next_zxid = 0, int64_t next_session_id = 0);

    /// async create snapshot
    void create_snapshot_async(SnapTask &);

    /**
     * (Deprecated)
     * Read the given snapshot chunk.
     * This API is for snapshot sender (i.e., leader).
     *
     * @param s Snapshot instance to read.
     * @param offset Byte offset of given chunk.
     * @param[out] data Buffer where the read chunk will be stored.
     * @return Amount of bytes read.
     *         0 if failed.
     */
    int read_snapshot_data(snapshot & s, const ulong offset, buffer & data) override; // NOLINT(readability-avoid-const-params-in-decls)

    /**
     * (Deprecated)
     * Save the given snapshot chunk to local snapshot.
     * This API is for snapshot receiver (i.e., follower).
     *
     * Since snapshot itself may be quite big, save_snapshot_data()
     * will be invoked multiple times for the same snapshot `s`. This
     * function should decode the {offset, data} and re-construct the
     * snapshot. After all savings are done, apply_snapshot() will be
     * called at the end.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param s Snapshot instance to save.
     * @param offset Byte offset of given chunk.
     * @param data Payload of given chunk.
     */
    void save_snapshot_data(snapshot & s, const ulong offset, buffer & data) override; // NOLINT(readability-avoid-const-params-in-decls)

    /**
     * Read the given snapshot object.
     * This API is for snapshot sender (i.e., leader).
     *
     * Same as above, this is an optional API for users who want to
     * use logical snapshot.
     *
     * @param s Snapshot instance to read.
     * @param[in,out] user_snp_ctx
     *     User-defined instance that needs to be passed through
     *     the entire snapshot read. It can be a pointer to
     *     state machine specific iterators, or whatever.
     *     On the first `read_logical_snp_obj` call, it will be
     *     set to `null`, and this API may return a new pointer if necessary.
     *     Returned pointer will be passed to next `read_logical_snp_obj`
     *     call.
     * @param obj_id Object ID to read.
     * @param[out] data_out Buffer where the read object will be stored.
     * @param[out] is_last_obj Set `true` if this is the last object.
     * @return Negative number if failed.
     */
    int read_logical_snp_obj(snapshot & s, void *& user_snp_ctx, ulong obj_id, ptr<buffer> & data_out, bool & is_last_obj) override;

    /**
     * Save the given snapshot object to local snapshot.
     * This API is for snapshot receiver (i.e., follower).
     *
     * This is an optional API for users who want to use logical
     * snapshot. Instead of splitting a snapshot into multiple
     * physical chunks, this API uses logical objects corresponding
     * to a unique object ID. Users are responsible for defining
     * what object is: it can be a key-value pair, a set of
     * key-value pairs, or whatever.
     *
     * Same as `commit()`, memory buffer is owned by caller.
     *
     * @param s Snapshot instance to save.
     * @param obj_id [in,out]
     *     Object ID.
     *     As a result of this API call, the next object ID
     *     that receiver wants to get should be set to
     *     this parameter.
     * @param data Payload of given object.
     * @param is_first_obj `true` if this is the first object.
     * @param is_last_obj `true` if this is the last object.
     */
    void save_logical_snp_obj(snapshot & s, ulong & obj_id, buffer & data, bool is_first_obj, bool is_last_obj) override;

    /// whether snapshot object exists.
    bool existSnapshotObject(snapshot & s, ulong obj_id);

    /**
     * Apply received snapshot to state machine. Note that you should reset the state machine first.
     *
     * @param s Snapshot instance to apply.
     * @return `true` on success.
     */
    bool apply_snapshot(snapshot & s) override;
    bool applySnapshotImpl(snapshot & s);

    /**
     * Replay logs to state machine. Invoked when startup.
     */
    void replayLogs(ptr<nuraft::log_store> log_store_, uint64_t from, uint64_t to);

    /**
     * Free user-defined instance that is allocated by
     * `read_logical_snp_obj`.
     * This is an optional API for users who want to use logical snapshot.
     *
     * @param user_snp_ctx User-defined instance to free.
     */
    void free_user_snp_ctx(void *& user_snp_ctx) override;

    /**
     * Get the latest snapshot instance.
     *
     * This API will be invoked at the initialization of Raft server,
     * so that the last last snapshot should be durable for server restart,
     * if you want to avoid unnecessary catch-up.
     *
     * @return Pointer to the latest snapshot.
     */
    ptr<snapshot> last_snapshot() override;

    ulong last_commit_index() override { return last_committed_idx; }

    /// get persisted last committed index
    ulong getLastCommittedIndex()
    {
        return committed_log_manager->get();
    }

    /// whether znode exist
    bool exists(const String & path);

    /// get an znode
    KeeperNode & getNode(const String & path);

    KeeperStore & getStore() { return store; }

    /// process read request
    [[maybe_unused]] void processReadRequest(const RequestForSession & request_for_session);

    /// get expired session
    std::vector<int64_t> getDeadSessions();

    /// for 4lw commands
    int64_t getLastProcessedZxid() const;

    /// node count
    uint64_t getNodesCount() const;

    /// how many watches registered
    uint64_t getTotalWatchesCount() const;

    /// how many paths watched
    uint64_t getWatchedPathsCount() const;

    /// how many sessions register watch
    uint64_t getSessionsWithWatchesCount() const;

    /// dump watches
    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    uint64_t getSessionWithEphemeralNodesCount() const;
    uint64_t getTotalEphemeralNodesCount() const;

    /// get approximate data size
    /// TODO need a more accurate value
    uint64_t getApproximateDataSize() const;

    /// Whether contains a session, note that leader contains all sessions in cluster.
    /// and follower only contains local session.
    bool containsSession(int64_t session_id) const;

    /// whether a snapshot creating is in progress.
    bool getSnapshoting() const
    {
        return in_snapshot;
    }

    void shutdown();

    /// deserialize a RequestForSession
    static RequestForSession parseRequest(nuraft::buffer & data);
    /// serialize a RequestForSession
    static ptr<buffer> serializeRequest(RequestForSession & request);

private:
    /// Clear the whole state machine.
    /// Used when apply_snapshot.
    void reset();

    ptr<RequestForSession> createRequestSession(ptr<log_entry> & entry);

    /// Asynchronously snapshot creating thread.
    /// Now it is not used.
    void snapThread();

    /// Only contains session_id
    static bool isNewSessionRequest(nuraft::buffer & data);

    /// Contains session_id and timeout
    static bool isUpdateSessionRequest(nuraft::buffer & data);

    Poco::Logger * log;
    /// raft related settings
    RaftSettingsPtr raft_settings;

    /// data storage
    KeeperStore store;

    /// all response goes here
    KeeperResponsesQueue & responses_queue;

    std::shared_ptr<RequestProcessor> request_processor;

    /// Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;

    /// Keep the last committed index
    ptr<LastCommittedIndexManager> committed_log_manager;

    std::mutex snapshot_mutex;
    String snapshot_dir;

    ptr<KeeperSnapshotManager> snap_mgr;

    /// The minimal interval to create snapshot
    int32_t snapshot_creating_interval;

    std::atomic<int64_t> last_snapshot_time;

    /// When get a not exist node, return blank.
    KeeperNode default_node;

    /// whether a snapshot creating is in progress.
    std::atomic_bool in_snapshot = false;
    std::atomic_bool snap_task_ready{false};
    std::atomic_uint64_t snap_start_time;

    ThreadFromGlobalPool snap_thread;

    std::shared_ptr<SnapTask> snap_task;
    std::atomic<bool> shutdown_called{false};

    std::mutex & new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback;
};

}
