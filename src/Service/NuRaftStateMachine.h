#pragma once

#include <atomic>
#include <cassert>
#include <mutex>
#include <unordered_map>
#include <string.h>
#include <time.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/SvsKeeperSettings.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/SvsKeeperThreadSafeQueue.h>
#include <libnuraft/nuraft.hxx>
#include <common/types.h>


namespace DB
{
using nuraft::async_result;
using nuraft::buffer;
using nuraft::cs_new;
using nuraft::ptr;

using SvsKeeperResponsesQueue = ConcurrentBoundedQueue<SvsKeeperStorage::ResponseForSession>;

class NuRaftStateMachine : public nuraft::state_machine
{
public:
    NuRaftStateMachine(
        SvsKeeperResponsesQueue & responses_queue_,
        const SvsKeeperSettingsPtr & coordination_settings_,
        std::string & snap_dir,
        UInt32 snap_begin_second,
        UInt32 snap_end_second,
        UInt32 internal,
        UInt32 keep_max_snapshot_count,
        ptr<nuraft::log_store> logstore = nullptr,
        UInt32 object_node_size = KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE);

    ~NuRaftStateMachine() override { }

    ptr<buffer> pre_commit(const ulong log_idx, buffer & data) override;
    void rollback(const ulong log_idx, buffer & data) override;
    ptr<buffer> commit(const ulong log_idx, buffer & data) override;

    bool chk_create_snapshot() override;
    //for unit test
    bool chk_create_snapshot(time_t curr_time);
    void create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done) override;
    //sync create snapshot
    void create_snapshot(snapshot & s);

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

    bool exists(const std::string & path);

    KeeperNode & getNode(const std::string & path);

    //NodeMap & getNodeMap() { return node_map; }
    SvsKeeperStorage & getStorage() { return storage; }

    SvsKeeperStorage::ResponsesForSessions singleProcessReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session);

    void processReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session);

    std::unordered_set<int64_t> getDeadSessions();

    UInt64 getNodeNum() { return storage.getNodeNum(); }
    UInt64 getWatchNodeNum() { return storage.getWatchNodeNum(); }
    UInt64 getEphemeralNodeNum() { return storage.getEphemeralNodeNum(); }

    UInt64 getNodeSizeMB() { return storage.getNodeSizeMB(); }

    UInt64 getZxid() { return storage.zxid.load(); }

    /// no need to lock
    UInt64 getSessionNum() { return storage.getSessionNum(); }

    SessionAndWatcherPtr getWatchInfo() { return storage.cloneWatchInfo(); }

    EphemeralsPtr getEphemeralInfo() { return storage.cloneEphemeralInfo(); }

    void shutdownStorage();

    static SvsKeeperStorage::RequestForSession parseRequest(nuraft::buffer & data);
    static ptr<buffer> serializeRequest(SvsKeeperStorage::RequestForSession & request);

private:
    ptr<SvsKeeperStorage::RequestForSession> createRequestSession(ptr<log_entry> & entry);
    static bool isNewSessionRequest(nuraft::buffer & data);

    Poco::Logger * log;
    SvsKeeperSettingsPtr coordination_settings;

    //NodeMap node_map;
    SvsKeeperStorage storage;
    SvsKeeperResponsesQueue & responses_queue;

    // Last committed Raft log number.
    std::atomic<uint64_t> last_committed_idx;
    // Mutex for `snapshots`.
    std::mutex snapshot_mutex;
    std::string snapshot_dir;
    BackendTimer timer;
    ptr<KeeperSnapshotManager> snap_mgr;
    KeeperNode default_node;
};

};
