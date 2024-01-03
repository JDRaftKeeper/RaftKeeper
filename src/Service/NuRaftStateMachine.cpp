#include <atomic>
#include <mutex>
#include <string>

#include <Poco/File.h>

#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>

#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/RequestProcessor.h>
#include <Service/ThreadSafeQueue.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperIO.h>


#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif


using namespace nuraft;

namespace RK
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

struct ReplayLogBatch
{
    ulong batch_start_index = 0;
    ulong batch_end_index = 0;
    ptr<std::vector<VersionLogEntry>> log_vec;
    ptr<std::vector<ptr<RequestForSession>>> request_vec;
};

NuRaftStateMachine::NuRaftStateMachine(
    KeeperResponsesQueue & responses_queue_,
    const RaftSettingsPtr & raft_settings_,
    std::string & snap_dir,
    UInt32 internal,
    UInt32 keep_max_snapshot_count,
    std::mutex & new_session_id_callback_mutex_,
    std::unordered_map<int64_t, ptr<std::condition_variable>> & new_session_id_callback_,
    ptr<log_store> log_store_,
    std::string super_digest,
    UInt32 object_node_size,
    std::shared_ptr<RequestProcessor> request_processor_)
    : raft_settings(raft_settings_)
    , store(raft_settings->dead_session_check_period_ms, super_digest)
    , responses_queue(responses_queue_)
    , request_processor(request_processor_)
    , last_committed_idx(0)
    , new_session_id_callback_mutex(new_session_id_callback_mutex_)
    , new_session_id_callback(new_session_id_callback_)
{
    log = &(Poco::Logger::get("KeeperStateMachine"));

    LOG_INFO(log, "Begin to initialize state machine");

    snapshot_dir = snap_dir;
    timer.interval = internal;
    snap_mgr = cs_new<KeeperSnapshotManager>(snapshot_dir, keep_max_snapshot_count, object_node_size);

    /// Load snapshot meta from disk
    LOG_INFO(log, "Loading snapshot, found {} snapshots from disk, load the latest one", snap_mgr->loadSnapshotMetas());
    auto last_snapshot = snap_mgr->lastSnapshot();
    if (last_snapshot != nullptr)
        applySnapshotImpl(*last_snapshot);

    task_manager = cs_new<RaftTaskManager>(snapshot_dir);
    uint64_t previous_last_commit_id = 0; /// Last committed idx of the previous startup, we should apply log to here.
    task_manager->getLastCommitted(previous_last_commit_id);

    LOG_INFO(log, "Loading logs from {} to {}", last_committed_idx + 1, previous_last_commit_id);
    replayLogs(log_store_, last_committed_idx + 1, previous_last_commit_id);

    /// If the node is empty and join cluster, the log index is less than the last index of the snapshot, so compact is required.
    if (log_store_ && log_store_->next_slot() <= last_committed_idx)
        log_store_->compact(last_committed_idx);

    LOG_INFO(log, "Starting background creating snapshot thread.");
    snap_thread = ThreadFromGlobalPool([this] { snapThread(); });
}

ptr<RequestForSession> NuRaftStateMachine::createRequestSession(ptr<log_entry> & entry)
{
    if (entry->get_val_type() != nuraft::log_val_type::app_log)
        return nullptr;

    ReadBufferFromNuraftBuffer buffer(entry->get_buf());
    ptr<RequestForSession> request_for_session = cs_new<RequestForSession>();

    readIntBinary(request_for_session->session_id, buffer);
    if (buffer.eof())
    {
        LOG_DEBUG(log, "session time out {}", toHexString(request_for_session->session_id));
        return nullptr;
    }

    int32_t length;
    Coordination::read(length, buffer);
    if (length <= 0)
    {
        return nullptr;
    }

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    request_for_session->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session->request->xid = xid;
    request_for_session->request->readImpl(buffer);

    if (buffer.eof())
        request_for_session->create_time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    else
        Coordination::read(request_for_session->create_time, buffer);

    return request_for_session;
}

void NuRaftStateMachine::snapThread()
{
    setThreadName("snapThread");
    while (!shutdown_called)
    {
        if (snap_task)
        {
            Stopwatch stopwatch;
            in_snapshot = true;

            LOG_WARNING(
                log,
                "Create snapshot last_log_term {}, last_log_idx {}",
                snap_task->s->get_last_log_term(),
                snap_task->s->get_last_log_idx());

            create_snapshot(*snap_task->s, snap_task->next_zxid, snap_task->next_session_id);
            ptr<std::exception> except(nullptr);
            bool ret = true;

            snap_task->when_done(ret, except);
            snap_task = nullptr;

            stopwatch.stop();
            in_snapshot = false;

            snap_count.fetch_add(1);
            snap_time_ms.fetch_add(stopwatch.elapsedMilliseconds());

            LOG_INFO(log, "Create snapshot time cost {} ms", stopwatch.elapsedMilliseconds());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

RequestForSession NuRaftStateMachine::parseRequest(nuraft::buffer & data)
{
    ReadBufferFromNuraftBuffer buffer(data);
    RequestForSession request_for_session;
    /// TODO unify digital encoding mode
    readIntBinary(request_for_session.session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    //    bool is_internal;
    //    Coordination::read(is_internal, buffer);

    request_for_session.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session.request->xid = xid;
    request_for_session.request->readImpl(buffer);

    if (!buffer.eof())
        Coordination::read(request_for_session.create_time, buffer);
    else /// backward compatibility
        request_for_session.create_time
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    auto * log = &(Poco::Logger::get("NuRaftStateMachine"));
    LOG_TRACE(
        log,
        "Parsed request session id {}, length {}, xid {}, opnum {}",
        toHexString(request_for_session.session_id),
        length,
        xid,
        Coordination::toString(opnum));

    return request_for_session;
}

ptr<buffer> NuRaftStateMachine::serializeRequest(RequestForSession & session_request)
{
    WriteBufferFromNuraftBuffer out;
    /// TODO unify digital encoding mode, see parseRequest
    writeIntBinary(session_request.session_id, out);
    session_request.request->write(out);
    Coordination::write(session_request.create_time, out);
    return out.getBuffer();
}

ptr<buffer> NuRaftStateMachine::pre_commit(const ulong log_idx, buffer & data)
{
    LOG_TRACE(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
    return nullptr;
}

/// Do nothing, as this example doesn't do anything on pre-commit.
void NuRaftStateMachine::rollback(const ulong log_idx, buffer & data)
{
    LOG_TRACE(log, "pre commit, log index {}, data size {}", log_idx, data.size());
}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::commit(const ulong log_idx, nuraft::buffer & data, bool ignore_response)
{
    LOG_TRACE(log, "Begin commit log index {}", log_idx);

    if (isNewSessionRequest(data))
    {
        nuraft::buffer_serializer timeout_data(data);
        int64_t session_timeout_ms = timeout_data.get_i64();

        auto response = nuraft::buffer::alloc(sizeof(int64_t));
        int64_t session_id;

        nuraft::buffer_serializer bs(response);
        {
            std::unique_lock session_id_lock(new_session_id_callback_mutex);
            session_id = store.getSessionID(session_timeout_ms);
            bs.put_i64(session_id);

            LOG_DEBUG(log, "Commit session id {} with timeout {}", toHexString(session_id), session_timeout_ms);

            last_committed_idx = log_idx;
            task_manager->afterCommitted(last_committed_idx);

            if (new_session_id_callback.contains(session_id))
                new_session_id_callback.find(session_id)->second->notify_all();
            else
                LOG_DEBUG(
                    log,
                    "Not found callback for session id {}, maybe time out or before wait or not allocate from local",
                    toHexString(session_id));
        }

        return response;
    }
    else if (isUpdateSessionRequest(data))
    {
        nuraft::buffer_serializer data_serializer(data);
        int64_t session_id = data_serializer.get_i64();
        int64_t session_timeout_ms = data_serializer.get_i64();

        auto response = nuraft::buffer::alloc(1);
        nuraft::buffer_serializer bs(response);

        {
            std::unique_lock session_id_lock(new_session_id_callback_mutex);
            int8_t is_success = store.updateSessionTimeout(session_id, session_timeout_ms);
            bs.put_i8(is_success);

            LOG_DEBUG(log, "Update session id {} with timeout {}, response {}", toHexString(session_id), session_timeout_ms, is_success);
            last_committed_idx = log_idx;
            task_manager->afterCommitted(last_committed_idx);

            if (new_session_id_callback.contains(session_id))
                new_session_id_callback.find(session_id)->second->notify_all();
            else
                LOG_DEBUG(
                    log,
                    "Not found callback for session id {}, maybe time out or before wait or not allocate from local",
                    toHexString(session_id));
        }

        return response;
    }
    else
    {
        auto request_for_session = parseRequest(data);
        KeeperStore::ResponsesForSessions responses_for_sessions;

        LOG_TRACE(
            log,
            "Commit log index {}, session {}, xid {}, request {}",
            log_idx,
            toHexString(request_for_session.session_id),
            request_for_session.request->xid,
            request_for_session.request->toString());

        if (request_for_session.create_time > 0)
        {
            Int64 elapsed = Poco::Timestamp().epochMicroseconds() / 1000 - request_for_session.create_time;
            if (elapsed > 1000)
                LOG_WARNING(
                    log,
                    "Commit log {} request process time {}ms, session {} xid {} req type {}",
                    log_idx,
                    elapsed,
                    toHexString(request_for_session.session_id),
                    request_for_session.request->xid,
                    Coordination::toString(request_for_session.request->getOpNum()));
        }

        if (request_processor)
            request_processor->commit(request_for_session);
        else
            store.processRequest(responses_queue, request_for_session, {}, true, ignore_response);

        last_committed_idx = log_idx;
        task_manager->afterCommitted(last_committed_idx);

        return nullptr;
    }
}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::commit(const ulong log_idx, buffer & data)
{
    return commit(log_idx, data, false);
}

void NuRaftStateMachine::processReadRequest(const RequestForSession & request_for_session)
{
    store.processRequest(responses_queue, request_for_session);
}

std::vector<int64_t> NuRaftStateMachine::getDeadSessions()
{
    return store.getDeadSessions();
}

int64_t NuRaftStateMachine::getLastProcessedZxid() const
{
    return store.zxid.load();
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

void NuRaftStateMachine::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;
    LOG_INFO(log, "Shutting down state machine");

    store.finalize();
    task_manager->shutDown();
    snap_thread.join();
    LOG_INFO(log, "State machine shut down done!");
}

bool NuRaftStateMachine::chk_create_snapshot()
{
    return chk_create_snapshot(0L);
}

bool NuRaftStateMachine::chk_create_snapshot(time_t curr_time)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    time_t prev_time = snap_mgr->getLastCreateTime();
    return !in_snapshot && timer.isActionTime(prev_time, curr_time);
}

void NuRaftStateMachine::create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done)
{
    if (!raft_settings->async_snapshot)
    {
        size_t wait_times = 0;
        while (request_processor && request_processor->commitQueueSize() != 0)
        {
            /// wait commit queue empty
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            if (++wait_times % 1000 == 0)
            {
                LOG_WARNING(log, "Wait commit queue to empty 1s");
            }
        }

        Stopwatch stopwatch;
        in_snapshot = true;

        LOG_WARNING(log, "Create snapshot last_log_term {}, last_log_idx {}", s.get_last_log_term(), s.get_last_log_idx());

        create_snapshot(s, store.zxid, store.session_id_counter);
        ptr<std::exception> except(nullptr);
        bool ret = true;
        when_done(ret, except);

        stopwatch.stop();
        in_snapshot = false;

        snap_count.fetch_add(1);
        snap_time_ms.fetch_add(stopwatch.elapsedMilliseconds());

        LOG_INFO(log, "Create snapshot time cost {} ms", stopwatch.elapsedMilliseconds());
    }
    else
    {
        /// Need make a copy of s
        auto t1 = Poco::Timestamp().epochMicroseconds();
        ptr<buffer> snp_buf = s.serialize();
        auto t2 = Poco::Timestamp().epochMicroseconds();
        auto snap_copy = snapshot::deserialize(*snp_buf);
        auto t3 = Poco::Timestamp().epochMicroseconds();
        snap_task = std::make_shared<SnapTask>(snap_copy, store.zxid, store.session_id_counter, when_done);
        auto t4 = Poco::Timestamp().epochMicroseconds();
        LOG_INFO(log, "Async create snapshot time cost {}us, {}us, {}us", (t2 - t1), (t3 - t2), (t4 - t3));
    }
}

void NuRaftStateMachine::create_snapshot(snapshot & s, int64_t next_zxid, int64_t next_session_id)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    snap_mgr->createSnapshot(s, store, next_zxid, next_session_id);
    snap_mgr->removeSnapshots();
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
        buffer_serializer bs(data_out);
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

bool NuRaftStateMachine::existSnapshotObject(snapshot & s, ulong obj_id)
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
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    bool succeed = snap_mgr->parseSnapshot(s, store);
    if (succeed)
    {
        last_committed_idx = s.get_last_log_idx();
        LOG_INFO(log, "Applied snapshot, now the last log index is {}", last_committed_idx);
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

    ulong last_log_index = log_store_->next_slot() - 1;
    if (last_log_index == 0)
    {
        LOG_WARNING(log, "We have no log in local");
        return;
    }

    if (to == 0)
    {
        LOG_WARNING(log, "Try too replay log to 0, caused by there is no previous last_commit_idx, maybe this is the first start.");
        return;
    }

    if (to < last_log_index)
        last_log_index = to;

    LOG_INFO(log, "Replay logs from {} to {}", from, last_log_index);

    /// [ batch_start_index, batch_end_index )
    std::atomic<ulong> batch_start_index = from;
    std::atomic<ulong> batch_end_index = 0;

    ThreadSafeQueue<ReplayLogBatch> log_queue;

    /// Loading and applying asynchronously
    auto load_thread = ThreadFromGlobalPool(
        [this, last_log_index, &log_queue, &batch_start_index, &batch_end_index, &log_store_]
        {
            Poco::Logger * thread_log = &(Poco::Logger::get("LoadLogThread"));
            while (batch_start_index < last_log_index)
            {
                while (log_queue.size() > 10)
                {
                    LOG_DEBUG(thread_log, "Sleep 100ms to wait for applying log");
                    usleep(100000);
                }

                /// 0.3 * 10000 = 3M
                batch_end_index = batch_start_index + 10000;
                if (batch_end_index > last_log_index + 1)
                    batch_end_index = last_log_index + 1;

                LOG_INFO(thread_log, "Begin to load batch [{} , {})", batch_start_index, batch_end_index);

                ReplayLogBatch batch;
                batch.log_vec
                    = dynamic_cast<NuRaftFileLogStore *>(log_store_.get())->log_entries_version_ext(batch_start_index, batch_end_index, 0);

                batch.batch_start_index = batch_start_index;
                batch.batch_end_index = batch_end_index;
                batch.request_vec = cs_new<std::vector<ptr<RequestForSession>>>();

                for (auto entry : *(batch.log_vec))
                {
                    if (entry.entry->get_val_type() != nuraft::log_val_type::app_log)
                    {
                        LOG_DEBUG(thread_log, "Found non app log(type {}), ignore it", entry.entry->get_val_type());
                        batch.request_vec->push_back(nullptr);
                    }
                    else if (isNewSessionRequest(entry.entry->get_buf()))
                    {
                        batch.request_vec->push_back(nullptr);
                    }
                    else if (isUpdateSessionRequest(entry.entry->get_buf()))
                    {
                        batch.request_vec->push_back(nullptr);
                    }
                    else
                    {
                        /// user requests
                        ptr<RequestForSession> ptr_request = createRequestSession(entry.entry);
                        batch.request_vec->push_back(ptr_request);
                    }
                }

                LOG_INFO(thread_log, "Finish to load batch [{}, {})", batch_start_index, batch_end_index);
                log_queue.push(batch);
                batch_start_index.store(batch_end_index);
            }
        });

    /// Apply loaded logs
    while (!log_queue.empty() || batch_start_index < last_log_index)
    {
        while (log_queue.empty() && batch_start_index != last_log_index)
        {
            LOG_DEBUG(log, "Sleep 100ms to wait for log loading");
            usleep(100000);
        }

        ReplayLogBatch batch;
        log_queue.peek(batch);

        if (batch.log_vec == nullptr)
        {
            LOG_DEBUG(log, "log vector is null");
            break;
        }

        for (size_t i = 0; i < batch.log_vec->size(); ++i)
        {
            auto entry = (*batch.log_vec)[i];
            if (entry.entry->get_val_type() != nuraft::log_val_type::app_log)
                continue;

            if (isNewSessionRequest(entry.entry->get_buf()))
            {
                /// replay session
                int64_t session_timeout_ms = entry.entry->get_buf().get_ulong();
                int64_t session_id = store.getSessionID(session_timeout_ms);
                LOG_TRACE(log, "Replay log create session {} with timeout {} from log", toHexString(session_id), session_timeout_ms);
            }
            else if (isUpdateSessionRequest(entry.entry->get_buf()))
            {
                /// replay update session
                nuraft::buffer_serializer data_serializer(entry.entry->get_buf());
                int64_t session_id = data_serializer.get_i64();
                int64_t session_timeout_ms = data_serializer.get_i64();

                store.updateSessionTimeout(session_id, session_timeout_ms);
                LOG_TRACE(log, "Replay log update session {} with timeout {}", toHexString(session_id), session_timeout_ms);
            }
            else
            {
                /// replay user requests
                auto & request = (*batch.request_vec)[i];
                LOG_TRACE(
                    log, "Replay log request, session {}, request {}", toHexString(request->session_id), request->request->toString());
                store.processRequest(responses_queue, *request, {}, true, true);
                if (request->request->getOpNum() != Coordination::OpNum::NewSession && request->session_id > store.session_id_counter)
                {
                    LOG_WARNING(
                        log,
                        "Storage's session_id_counter {} must bigger than the session id {} of log.",
                        toHexString(store.session_id_counter),
                        toHexString(request->session_id));
                    store.session_id_counter = request->session_id;
                }
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
        store.session_and_timeout.size(),
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
    /// Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    LOG_INFO(log, "last_snapshot invoke");
    return snap_mgr->lastSnapshot();
}

bool NuRaftStateMachine::exists(const std::string & path)
{
    return (store.container.count(path) == 1);
}

KeeperNode & NuRaftStateMachine::getNode(const std::string & path)
{
    auto node_ptr = store.container.get(path);
    if (node_ptr != nullptr)
    {
        return *node_ptr.get();
    }
    return default_node;
}

bool NuRaftStateMachine::isNewSessionRequest(nuraft::buffer & data)
{
    return data.size() == sizeof(int64);
}

bool NuRaftStateMachine::isUpdateSessionRequest(nuraft::buffer & data)
{
    return data.size() == sizeof(int64) + sizeof(int64);
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

}

#ifdef __clang__
#    pragma clang diagnostic pop
#endif
