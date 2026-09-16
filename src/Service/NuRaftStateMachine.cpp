#include <atomic>
#include <mutex>
#include <string>

#include <Common/setThreadName.h>

#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/RequestProcessor.h>
#include <Service/ThreadSafeQueue.h>
#include <ZooKeeper/ZooKeeperIO.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int STALE_LOG;
    extern const int GAP_BETWEEN_SNAPSHOT_AND_LOG;
}

struct ReplayLogBatch
{
    ulong batch_start_index = 0;
    ulong batch_end_index = 0;
    ptr<std::vector<LogEntryWithVersion>> log_entries;
    ptr<std::vector<ptr<RequestForSession>>> requests;
};

NuRaftStateMachine::NuRaftStateMachine(
    KeeperResponsesQueue & responses_queue_,
    const RaftSettingsPtr & raft_settings_,
    String & snap_dir,
    String & log_dir,
    UInt32 internal,
    UInt32 keep_max_snapshot_count,
    std::mutex & new_session_id_callback_mutex_,
    std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback_,
    ptr<log_store> log_store_,
    String super_digest,
    UInt32 object_node_size,
    std::shared_ptr<RequestProcessor> request_processor_)
    : raft_settings(raft_settings_)
    , store(raft_settings->dead_session_check_period_ms, super_digest)
    , responses_queue(responses_queue_)
    , request_processor(request_processor_)
    , raft_log_store(log_store_)
    , last_committed_idx(0)
    , snapshot_creating_interval(static_cast<uint64_t>(internal) * 1000000)
    , last_snapshot_time(getCurrentTimeMicroseconds())
    , new_session_id_callback_mutex(new_session_id_callback_mutex_)
    , new_session_id_callback(new_session_id_callback_)
    , log(&(Poco::Logger::get("KeeperStateMachine")))
{
    LOG_INFO(log, "Begin to initialize state machine");

    snapshot_dir = snap_dir;
    snap_mgr = cs_new<KeeperSnapshotManager>(snapshot_dir, keep_max_snapshot_count, object_node_size);

    /// Load snapshot meta from disk
    auto snapshots_count = snap_mgr->loadSnapshotMetas();

    /// Create committed log index manager early so we can validate snapshots against
    /// the log store before accepting one. This avoids falling back to a snapshot
    /// whose log range has already been compacted away.
    committed_log_manager = cs_new<LastCommittedIndexManager>(log_dir);
    uint64_t previous_last_commit_id = committed_log_manager->get();

    if (snapshots_count > 0)
    {
        LOG_INFO(log, "Found {} snapshots from disk, trying from newest to oldest", snapshots_count);

        bool loaded = false;
        const auto & snapshots = snap_mgr->getSnapshots();
        for (auto it = snapshots.rbegin(); it != snapshots.rend(); ++it)
        {
            auto current_meta = it->second->getSnapshotMeta();
            try
            {
                LOG_INFO(
                    log,
                    "Trying to load snapshot with last_log_term {}, last_log_idx {}",
                    current_meta->get_last_log_term(),
                    current_meta->get_last_log_idx());
                if (applySnapshotImpl(*current_meta))
                {
                    /// Verify the log store can bridge from this snapshot to the last committed index.
                    /// If logs between snapshot and last committed have been compacted, this snapshot
                    /// is too old to be useful. Older snapshots have smaller last_log_idx, so the gap
                    /// can only widen — abort the fallback loop instead of trying older snapshots.
                    if (previous_last_commit_id > last_committed_idx && log_store_)
                    {
                        ulong log_start = log_store_->start_index();
                        if (last_committed_idx + 1 < log_start)
                        {
                            LOG_WARNING(
                                log,
                                "Snapshot at last_log_idx {} requires log replay from {} but log store starts at {}. "
                                "Older snapshots would only widen the gap, aborting fallback.",
                                last_committed_idx.load(),
                                last_committed_idx + 1,
                                log_start);
                            store.reset();
                            last_committed_idx = 0;
                            break;
                        }
                    }

                    loaded = true;
                    LOG_INFO(
                        log,
                        "Successfully loaded snapshot with last_log_idx {}",
                        current_meta->get_last_log_idx());
                    break;
                }
            }
            catch (const Exception & e)
            {
                LOG_WARNING(
                    log,
                    "Failed to load snapshot at last_log_idx {}: {}. Trying older snapshot.",
                    current_meta->get_last_log_idx(),
                    e.displayText());
                store.reset();
                last_committed_idx = 0;
            }
        }

        if (!loaded)
            LOG_WARNING(log, "All {} snapshots failed to load, proceeding with empty state", snapshots_count);
    }
    else
    {
        LOG_INFO(log, "No snapshots found on disk");
    }

    /// Last committed idx of the previous startup, we should apply log to here.
    if (previous_last_commit_id == 0)
    {
        LOG_INFO(log, "No previous last commit idx found, skip replaying logs.");
    }
    else if (previous_last_commit_id <= last_committed_idx)
    {
        LOG_WARNING(
            log,
            "Previous last commit idx {} is less than the last committed idx {} from snapshot, skip replaying logs.",
            previous_last_commit_id,
            last_committed_idx.load());
    }
    else
    {
        LOG_INFO(log, "Replaying logs from {} to {}", last_committed_idx + 1, previous_last_commit_id);
        replayLogs(log_store_, last_committed_idx + 1, previous_last_commit_id);
    }

    /// If the node is empty and join cluster, the log index is less than the last index of the snapshot, so compact is required.
    if (log_store_ && log_store_->next_slot() <= last_committed_idx)
        log_store_->compact(last_committed_idx);

    LOG_INFO(
        log,
        "Replaying logs done: nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
        store.getNodesCount(),
        store.getTotalEphemeralNodesCount(),
        store.getSessionCount(),
        store.getSessionIDCounter(),
        store.getZxid());

    store.initializeSystemNodes();
    bg_snap_thread = ThreadFromGlobalPool([this] { snapThread(); });
}

void NuRaftStateMachine::snapThread()
{
    LOG_INFO(log, "Starting background creating snapshot thread.");
    setThreadName("snapThread");

    while (!shutdown_called)
    {
        if (snap_task_ready)
        {
            auto current_task = std::move(snap_task);
            snap_task_ready = false;

            LOG_INFO(
                log,
                "Create snapshot last_log_term {}, last_log_idx {}",
                current_task->s->get_last_log_term(),
                current_task->s->get_last_log_idx());

            create_snapshot_async(*current_task);
            ptr<std::exception> except(nullptr);
            bool ret = true;

            current_task->when_done(ret, except);

            Metrics::getMetrics().snap_count->add(1);
            Metrics::getMetrics().snap_time_ms->add(getCurrentTimeMilliseconds() - snap_start_time);

            last_snapshot_time = getCurrentTimeMicroseconds();
            in_snapshot = false;

            LOG_INFO(log, "Snapshot created time cost {} ms", getCurrentTimeMilliseconds() - snap_start_time);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

ptr<buffer> NuRaftStateMachine::pre_commit(const ulong, buffer &)
{
    return nullptr;
}

/// Do nothing, as doesn't do anything on pre-commit.
void NuRaftStateMachine::rollback(const ulong log_idx, buffer & data)
{
    LOG_TRACE(log, "pre commit, log index {}, data size {}", log_idx, data.size());
}

ptr<buffer> NuRaftStateMachine::commit(const ulong log_idx, buffer & data, bool ignore_response)
{
    auto request_for_session = deserializeKeeperRequest(data);
    LOG_TRACE(log, "Commit log {}, request {}", log_idx, request_for_session->toSimpleString());

    if (request_processor)
        request_processor->commit(*request_for_session);
    else
        store.processRequest(responses_queue, *request_for_session, {}, true, ignore_response);

    last_committed_idx = log_idx;
    committed_log_manager->push(last_committed_idx);

    return nullptr;
}

ptr<buffer> NuRaftStateMachine::commit(const ulong log_idx, buffer & data)
{
    return commit(log_idx, data, false);
}

void NuRaftStateMachine::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;
    LOG_INFO(log, "Shutting down state machine");

    store.finalize();
    committed_log_manager->shutDown();
    bg_snap_thread.join();
    LOG_INFO(log, "State machine shut down done!");
}

bool NuRaftStateMachine::chk_create_snapshot()
{
    Poco::Timestamp now;
    return !in_snapshot && now > last_snapshot_time + snapshot_creating_interval;
}

void NuRaftStateMachine::create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done)
{
    size_t wait_times = 0;
    while (request_processor && request_processor->commitQueueSize() != 0)
    {
        /// wait commit queue empty
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (++wait_times % 1000 == 0)
        {
            LOG_WARNING(log, "Wait commit queue to empty");
        }
    }

    in_snapshot = true;
    snap_start_time = getCurrentTimeMilliseconds();

    LOG_INFO(log, "Creating snapshot last_log_term {}, last_log_idx {}", s.get_last_log_term(), s.get_last_log_idx());

    if (!raft_settings->async_snapshot)
    {
        create_snapshot(s, store.getZxid(), store.getSessionIDCounter());
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);

        Metrics::getMetrics().snap_count->add(1);
        Metrics::getMetrics().snap_time_ms->add(getCurrentTimeMilliseconds() - snap_start_time);

        last_snapshot_time = getCurrentTimeMicroseconds();
        in_snapshot = false;

        LOG_INFO(log, "Created snapshot, time cost {} ms", getCurrentTimeMilliseconds() - snap_start_time);
    }
    else
    {
        /// Need make a copy of s
        ptr<buffer> snp_buf = s.serialize();
        auto snap_copy = snapshot::deserialize(*snp_buf);
        snap_task = std::make_shared<SnapTask>(snap_copy, store, when_done);
        snap_task_ready = true;

        LOG_INFO(log, "Scheduling asynchronous creating snapshot task, time cost {} ms", getCurrentTimeMilliseconds() - snap_start_time);
    }
}

void NuRaftStateMachine::create_snapshot(snapshot & s, int64_t next_zxid, int64_t next_session_id)
{
    std::lock_guard lock(snapshot_mutex);
    snap_mgr->createSnapshot(s, store, next_zxid, next_session_id, raft_settings->getSnapshotFormat());
    snap_mgr->removeSnapshots();
    compactLogStore();
}

void NuRaftStateMachine::create_snapshot_async(SnapTask & s)
{
    std::lock_guard lock(snapshot_mutex);
    snap_mgr->createSnapshotAsync(s, raft_settings->getSnapshotFormat());
    snap_mgr->removeSnapshots();
    compactLogStore();
}

void NuRaftStateMachine::save_snapshot_data(snapshot &, const ulong, buffer &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method is deprecated");
}

int NuRaftStateMachine::read_snapshot_data(snapshot &, const ulong, buffer &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "method is deprecated");
}

int NuRaftStateMachine::read_logical_snp_obj(snapshot & s, void *& user_snp_ctx, ulong obj_id, ptr<buffer> & data_out, bool & is_last_obj)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    if (!snap_mgr->existSnapshot(s))
    {
        data_out = nullptr;
        is_last_obj = true;
        LOG_INFO(log, "Can't find snapshot by last_log_idx {}, object id {}", s.get_last_log_idx(), obj_id);
        return 0;
    }

    if (obj_id == 0)
    {
        // Object ID == 0: first object
        data_out = buffer::alloc(sizeof(UInt32));
        nuraft::buffer_serializer bs(data_out);
        bs.put_i32(0);
        is_last_obj = false;
        LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", s.get_last_log_idx(), obj_id, false);
        return 0;
    }

    // Object ID > 0: second object, put actual value.
    snap_mgr->loadSnapshotObject(s, obj_id, data_out);
    is_last_obj = !(snap_mgr->existSnapshotObject(s, obj_id + 1));

    LOG_INFO(log, "Read snapshot object, last_log_idx {}, object id {}, is_last {}", s.get_last_log_idx(), obj_id, is_last_obj);
    user_snp_ctx = nullptr;

    return 0;
}

void NuRaftStateMachine::save_logical_snp_obj(snapshot & s, ulong & obj_id, buffer & data, bool is_first_obj, bool is_last_obj)
{
    if (obj_id == 0)
    {
        // Object ID == 0: it contains dummy value, create snapshot context.
        snap_mgr->receiveSnapshotMeta(s);
    }
    else
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        // Object ID > 0: actual snapshot value, save to local disk
        snap_mgr->saveSnapshotObject(s, obj_id, data);
    }
    LOG_INFO(log, "Save logical snapshot , object id {}, is_first_obj {}, is_last_obj {}", obj_id, is_first_obj, is_last_obj);
    obj_id++;
}

bool NuRaftStateMachine::existSnapshotObject(snapshot & s, ulong obj_id) const
{
    return snap_mgr->existSnapshotObject(s, obj_id);
}

bool NuRaftStateMachine::apply_snapshot(snapshot & s)
{
    /// The invoker is from NuRaft, we should reset the state machine
    LOG_INFO(log, "Reset state machine.");
    reset();

    return applySnapshotImpl(s);
}

bool NuRaftStateMachine::applySnapshotImpl(snapshot & s)
{
    LOG_INFO(log, "Applying snapshot term {}, last log index {}, size {}", s.get_last_log_term(), s.get_last_log_idx(), s.size());
    std::lock_guard lock(snapshot_mutex);
    bool succeed = snap_mgr->parseSnapshot(s, store);
    if (succeed)
    {
        last_committed_idx = s.get_last_log_idx();
        LOG_INFO(log, "Applied snapshot, now the last log index is {}", last_committed_idx.load());
    }
    return succeed;
}

void NuRaftStateMachine::replayLogs(ptr<log_store> log_store_, uint64_t from, uint64_t to)
{
    if (!log_store_)
    {
        LOG_WARNING(log, "There is no log_store, skip to replay logs.");
        return;
    }

    ulong first_index_in_store = log_store_->start_index();
    ulong last_index_in_store = log_store_->next_slot() - 1;

    if (last_index_in_store == 0)
    {
        LOG_WARNING(log, "Log store is empty, skip to replay logs.");
        return;
    }

    if (from > last_index_in_store)
        throw Exception(
            ErrorCodes::STALE_LOG,
            "Logs in log store is stale. Last log index in log store is {} and snapshot last log index is {}. If the snapshot is copied "
            "from another server, please clean the logs to start the server.",
            last_index_in_store,
            from);

    if (from < first_index_in_store)
        throw Exception(ErrorCodes::GAP_BETWEEN_SNAPSHOT_AND_LOG, "There is log gap between snapshot and log store, {} / {}", from, first_index_in_store);

    if (to > last_index_in_store)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Last committed index is large than last log index in logstore, {} / {}", to, last_index_in_store);

    if (to < last_index_in_store)
        LOG_WARNING(log, "The last log index in log store is {} which is larger than 'to' {}, which maybe caused by not all logs are committed in last running or the latest committed index is not persisted.", last_index_in_store, to);

    /// [ batch_start_index, batch_end_index )
    std::atomic<ulong> batch_start_index = from;
    std::atomic<ulong> batch_end_index = 0;

    ThreadSafeQueue<ReplayLogBatch> log_queue;

    /// Loading and applying asynchronously
    auto load_thread = ThreadFromGlobalPool(
        [to, &log_queue, &batch_start_index, &batch_end_index, &log_store_]
        {
            Poco::Logger * thread_log = &(Poco::Logger::get("LoadLogThread"));
            while (batch_start_index < to)
            {
                while (log_queue.size() > 10)
                {
                    LOG_DEBUG(thread_log, "Sleep 100ms to wait for applying log");
                    usleep(100000);
                }

                /// 0.3 * 10000 = 3M
                batch_end_index = batch_start_index + 10000;
                if (batch_end_index > to + 1)
                    batch_end_index = to + 1;

                LOG_INFO(thread_log, "Begin to load batch [{} , {})", batch_start_index.load(), batch_end_index.load());

                ReplayLogBatch batch;
                batch.log_entries
                    = dynamic_cast<NuRaftFileLogStore *>(log_store_.get())->log_entries_version_ext(batch_start_index, batch_end_index, 0);

                batch.batch_start_index = batch_start_index;
                batch.batch_end_index = batch_end_index;
                batch.requests = cs_new<std::vector<ptr<RequestForSession>>>();

                for (auto & entry_with_version : *batch.log_entries)
                {
                    if (entry_with_version.entry->get_val_type() != nuraft::log_val_type::app_log)
                    {
                        LOG_DEBUG(thread_log, "Found non app nuraft log(type {}), ignore it", toString(entry_with_version.entry->get_val_type()));
                        batch.requests->push_back(nullptr);
                    }
                    else
                    {
                        /// user requests
                        auto request = deserializeKeeperRequest(entry_with_version.entry->get_buf());
                        batch.requests->push_back(request);
                    }
                }

                LOG_INFO(thread_log, "Finish to load batch [{}, {})", batch_start_index.load(), batch_end_index.load());
                log_queue.push(batch);
                batch_start_index.store(batch_end_index);
            }
        });

    /// Apply loaded logs
    while (!log_queue.empty() || batch_start_index < to)
    {
        while (log_queue.empty() && batch_start_index != to)
        {
            LOG_DEBUG(log, "Sleep 100ms to wait for log loading");
            usleep(100000);
        }

        ReplayLogBatch batch;
        log_queue.peek(batch);

        if (batch.log_entries == nullptr)
        {
            LOG_DEBUG(log, "log vector is null");
            break;
        }

        for (size_t i = 0; i < batch.log_entries->size(); ++i)
        {
            ulong log_index = batch.batch_start_index + i;
            auto & entry_with_version = (*batch.log_entries)[i];

            if (entry_with_version.entry->get_val_type() != nuraft::log_val_type::app_log)
                continue;

            auto & request = (*batch.requests)[i];
            LOG_TRACE(log, "Replaying log {}, request {}", log_index, request->toString());

            store.processRequest(responses_queue, *request, {}, true, true);

            if (!isNewSessionRequest(request->request->getOpNum()) && request->session_id > store.getSessionIDCounter())
            {
                /// We may receive an error session id from client, and we just ignore it.
                LOG_WARNING(
                    log,
                    "Storage's session_id_counter {} must bigger than the session id {} of log.",
                    toHexString(store.getSessionIDCounter()),
                    toHexString(request->session_id));
            }
        }

        log_queue.pop();
        last_committed_idx = batch.batch_end_index - 1;

        LOG_INFO(log, "Replayed log batch [{}, {})", batch.batch_start_index, batch.batch_end_index);
    }

    load_thread.join();

    LOG_INFO(
        log,
        "Replay done, node count {}, session count {}, ephemeral nodes {}, watch count {}",
        getNodesCount(),
        store.getSessionCount(),
        getTotalEphemeralNodesCount(),
        getTotalWatchesCount());
}

void NuRaftStateMachine::free_user_snp_ctx(void *& user_snp_ctx)
{
    /// In this example, `read_logical_snp_obj` doesn't create
    /// `user_snp_ctx`. Nothing to do in this function.
    if (user_snp_ctx != nullptr)
    {
        free(user_snp_ctx);
        user_snp_ctx = nullptr;
    }
}

ptr<snapshot> NuRaftStateMachine::last_snapshot()
{
    std::lock_guard lock(snapshot_mutex);
    return snap_mgr->lastSnapshot();
}

bool NuRaftStateMachine::exists(const String & path)
{
    return (store.getNode(path) != nullptr);
}

KeeperNode & NuRaftStateMachine::getNode(const String & path)
{
    auto node = store.getNode(path);
    if (node != nullptr)
    {
        return *node.get();
    }
    return default_node;
}

void NuRaftStateMachine::reset()
{
    {
        std::lock_guard lock(snapshot_mutex);
        in_snapshot = false;
    }
    store.reset();
    last_committed_idx = 0;
    {
        std::lock_guard lock(new_session_id_callback_mutex);
        new_session_id_callback.clear();
    }
}

void NuRaftStateMachine::compactLogStore()
{
    if (!raft_log_store)
        return;

    const auto & snapshots = snap_mgr->getSnapshots();
    if (snapshots.empty())
    {
        LOG_DEBUG(log, "No snapshots to retain, skipping log compaction");
        return;
    }

    /// The oldest retained snapshot defines the safe compaction boundary:
    /// we can delete all logs up to (but not including) its first index
    /// because every kept snapshot must have a complete log trail behind it.
    auto oldest_snapshot = snapshots.begin()->second->getSnapshotMeta();
    UInt64 oldest_idx = oldest_snapshot->get_last_log_idx();

    /// Don't compact if the boundary is at or before index 1 (nothing to remove).
    if (oldest_idx <= 1)
    {
        LOG_DEBUG(log, "Oldest snapshot last_log_idx is {}, nothing to compact", oldest_idx);
        return;
    }

    /// compact(last_log_index) removes log entries up to and including last_log_index.
    /// We compact up to oldest_idx - 1 so that logs from oldest_idx onward are preserved,
    /// ensuring the oldest snapshot can still be replayed.
    UInt64 compact_upto = oldest_idx - 1;

    UInt64 log_start = raft_log_store->start_index();
    if (compact_upto < log_start)
    {
        LOG_DEBUG(
            log,
            "Logs already compacted past {} (log store starts at {}), nothing to compact",
            compact_upto,
            log_start);
        return;
    }

    LOG_INFO(
        log,
        "Compacting log store up to {} (oldest retained snapshot at {})",
        compact_upto,
        oldest_idx);
    raft_log_store->compact(compact_upto);
}


std::vector<int64_t> NuRaftStateMachine::getDeadSessions() const
{
    return store.getDeadSessions();
}

int64_t NuRaftStateMachine::getLastProcessedZxid() const
{
    return store.getZxid();
}

uint64_t NuRaftStateMachine::getNodesCount() const
{
    return store.getNodesCount();
}

uint64_t NuRaftStateMachine::getTotalWatchesCount() const
{
    return store.getTotalWatchesCount();
}

uint64_t NuRaftStateMachine::getWatchedPathsCount() const
{
    return store.getWatchedPathsCount();
}

uint64_t NuRaftStateMachine::getSessionsWithWatchesCount() const
{
    return store.getSessionsWithWatchesCount();
}

uint64_t NuRaftStateMachine::getTotalEphemeralNodesCount() const
{
    return store.getTotalEphemeralNodesCount();
}

uint64_t NuRaftStateMachine::getSessionWithEphemeralNodesCount() const
{
    return store.getSessionWithEphemeralNodesCount();
}

void NuRaftStateMachine::dumpWatches(WriteBufferFromOwnString & buf) const
{
    store.dumpWatches(buf);
}

void NuRaftStateMachine::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    store.dumpWatchesByPath(buf);
}

void NuRaftStateMachine::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    store.dumpSessionsAndEphemerals(buf);
}

uint64_t NuRaftStateMachine::getApproximateDataSize() const
{
    return store.getApproximateDataSize();
}

bool NuRaftStateMachine::containsSession(int64_t session_id) const
{
    return store.containsSession(session_id);
}

}
