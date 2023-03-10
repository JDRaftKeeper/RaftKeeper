#pragma once

#include <atomic>
#include <cassert>
#include <mutex>
#include <unordered_map>
#include <string.h>
#include <time.h>
#include <Service/KeeperStore.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/RaftTaskManager.h>
#include <Service/Settings.h>
#include <Service/ThreadSafeQueue.h>
#include <libnuraft/nuraft.hxx>
#include <common/types.h>


namespace RK
{
using nuraft::async_result;
using nuraft::buffer;
using nuraft::cs_new;

using KeeperResponsesQueue = ThreadSafeQueue<KeeperStore::ResponseForSession>;

class RequestProcessor;

class NuRaftStateMachine : public nuraft::state_machine
{
public:
    NuRaftStateMachine(
        KeeperResponsesQueue & responses_queue_,
        const RaftSettingsPtr & raft_settings_,
        std::string & snap_dir,
        UInt32 snap_begin_second,
        UInt32 snap_end_second,
        UInt32 internal,
        UInt32 keep_max_snapshot_count,
        std::mutex & new_session_id_callback_mutex_,
        std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback_,
        ptr<nuraft::log_store> log_store_ = nullptr,
        std::string super_digest = "",
        UInt32 object_node_size = KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE,
        std::shared_ptr<RequestProcessor> request_processor_ = nullptr);

    ~NuRaftStateMachine() override = default;

    ptr<buffer> pre_commit(const ulong log_idx, buffer & data) override;
    void rollback(const ulong log_idx, buffer & data) override;
    ptr<buffer> commit(const ulong log_idx, buffer & data) override;

    /// @ignore_response whether push response into queue
    /// Just for unit test
    ptr<buffer> commit(const ulong log_idx, buffer & data, bool ignore_response);


    bool chk_create_snapshot() override;
    //for unit test
    bool chk_create_snapshot(time_t curr_time);
    void create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done) override;
    //sync create snapshot
    void create_snapshot(snapshot & s, int64_t next_zxid = 0, int64_t next_session_id = 0);

    //raw_binary(deprecated)
    int read_snapshot_data(snapshot & s, const ulong offset, buffer & data) override;
    void save_snapshot_data(snapshot & s, const ulong offset, buffer & data) override;

    // logical_object
    int read_logical_snp_obj(snapshot & s, void *& user_snp_ctx, ulong obj_id, ptr<buffer> & data_out, bool & is_last_obj) override;
    void save_logical_snp_obj(snapshot & s, ulong & obj_id, buffer & data, bool is_first_obj, bool is_last_obj) override;
    bool exist_snapshot_object(snapshot & s, ulong obj_id);

    bool apply_snapshot(snapshot & s) override;

    void free_user_snp_ctx(void *& user_snp_ctx) override;

    ptr<snapshot> last_snapshot() override;

    ulong last_commit_index() override { return last_committed_idx; }

    //Get persistence last committed index
    ulong getLastCommittedIndex()
    {
        ulong index;
        task_manager->getLastCommitted(index);
        return index;
    }

    bool exists(const std::string & path);

    KeeperNode & getNode(const std::string & path);

    KeeperStore & getStore() { return store; }

    void processReadRequest(const KeeperStore::RequestForSession & request_for_session);

    std::vector<int64_t> getDeadSessions();

    /// Introspection functions for 4lw commands
    int64_t getLastProcessedZxid() const;

    uint64_t getNodesCount() const;
    uint64_t getTotalWatchesCount() const;
    uint64_t getWatchedPathsCount() const;
    uint64_t getSessionsWithWatchesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

    uint64_t getSessionWithEphemeralNodesCount() const;
    uint64_t getTotalEphemeralNodesCount() const;
    uint64_t getApproximateDataSize() const;
    bool containsSession(int64_t session_id) const;

    uint64_t getSnapshotCount() const
    {
        return snap_count;
    }

    uint64_t getSnapshotTimeMs() const
    {
        return snap_time_ms;
    }

    bool getSnapshoting() const
    {
        return in_snapshot;
    }

    void shutdown();

    static KeeperStore::RequestForSession parseRequest(nuraft::buffer & data);
    static ptr<buffer> serializeRequest(KeeperStore::RequestForSession & request);

private:
    ptr<KeeperStore::RequestForSession> createRequestSession(ptr<log_entry> & entry);
    void snapThread();

    /// Only contains session_id
    static bool isNewSessionRequest(nuraft::buffer & data);
    /// Contains session_id and timeout
    static bool isUpdateSessionRequest(nuraft::buffer & data);

    Poco::Logger * log;
    RaftSettingsPtr raft_settings;

    //NodeMap node_map;
    KeeperStore store;
    KeeperResponsesQueue & responses_queue;

    std::shared_ptr<RequestProcessor> request_processor;

    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;
    //Backend async task manager
    ptr<RaftTaskManager> task_manager;
    // Mutex for `snapshots`.
    std::mutex snapshot_mutex;
    std::string snapshot_dir;
    BackendTimer timer;
    ptr<KeeperSnapshotManager> snap_mgr;
    KeeperNode default_node;

    std::atomic_int64_t snap_count{0};
    std::atomic_int64_t snap_time_ms{0};
    std::atomic_bool in_snapshot = false;

    ThreadFromGlobalPool snap_thread;

    struct SnapTask
    {
        ptr<snapshot> s;
        int64_t next_zxid;
        int64_t next_session_id;
        async_result<bool>::handler_type when_done;
        SnapTask(const ptr<snapshot> & s_, int64_t next_zxid_, int64_t next_session_id_, async_result<bool>::handler_type & when_done_)
        : s(s_), next_zxid(next_zxid_), next_session_id(next_session_id_), when_done(when_done_)
        {
        }
    };
    std::shared_ptr<SnapTask> snap_task;

    std::atomic<bool> shutdown_called{false};

    std::mutex & new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback;
};

};
