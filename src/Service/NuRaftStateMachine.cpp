#include <atomic>
#include <cassert>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <math.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <Service/proto/Log.pb.h>
#include <Poco/File.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Service/SvsKeeperProfileEvents.h>


#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#endif


using namespace nuraft;

namespace ServiceProfileEvents
{
extern const Event create_snapshot_count;
extern const Event load_snapshot_count;
extern const Event apply_received_snapshot_count;
}

namespace DB
{
struct ReplayLogBatch
{
    ulong batch_start_index = 0;
    ulong batch_end_index = 0;
    ptr<std::vector<ptr<log_entry>>> log_vec;
    ptr<std::vector<ptr<SvsKeeperStorage::RequestForSession>>> request_vec;
};

nuraft::ptr<nuraft::buffer> writeResponses(SvsKeeperStorage::ResponsesForSessions & responses)
{
    WriteBufferFromNuraftBuffer buffer;
    for (const auto & response_and_session : responses)
    {
        writeIntBinary(response_and_session.session_id, buffer);
        response_and_session.response->write(buffer);
    }
    return buffer.getBuffer();
}


NuRaftStateMachine::NuRaftStateMachine(
    SvsKeeperResponsesQueue & responses_queue_,
    const SvsKeeperSettingsPtr & coordination_settings_,
    std::string & snap_dir,
    UInt32 snap_begin_second,
    UInt32 snap_end_second,
    UInt32 internal,
    UInt32 keep_max_snapshot_count,
    ptr<log_store> logstore,
    UInt32 object_node_size)
    : coordination_settings(coordination_settings_)
    , storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds())
    , responses_queue(responses_queue_)
{
    log = &(Poco::Logger::get("KeeperStateMachine"));

    LOG_INFO(log, "begin init state machine, snapshot directory {}", snap_dir);

    snapshot_dir = snap_dir;
    timer.begin_second = snap_begin_second;
    timer.end_second = snap_end_second;
    timer.interval = internal;

    snap_mgr = cs_new<KeeperSnapshotManager>(snapshot_dir, keep_max_snapshot_count, object_node_size);
    //load snapshot meta from disk
    size_t meta_size = snap_mgr->loadSnapshotMetas();
    //get last snapshot
    auto last_snapshot = snap_mgr->lastSnapshot();
    if (last_snapshot != nullptr)
    {
        last_committed_idx = last_snapshot->get_last_log_idx();
        apply_snapshot(*(last_snapshot.get()));
        /// In order to meet the initial application of snapshot in the cluster. At this time, the log index is less than the last index of the snapshot, and compact is required.
        if (logstore->next_slot() <= last_committed_idx)
            logstore->compact(last_committed_idx);
    }
    else
    {
        last_committed_idx = 0;
    }

    LOG_INFO(log, "Replay snapshot meta size {}, last log index {} in snapshot", meta_size, last_committed_idx);

    //[ batch_start_index, batch_end_index )
    ulong batch_start_index = 0;
    ulong batch_end_index = 0;
    if (logstore != nullptr)
    {
        std::mutex load_mutex;
        std::condition_variable load_cond;
        ulong last_log_index = logstore->next_slot() - 1;
        batch_start_index = last_committed_idx + 1;
        std::queue<ReplayLogBatch> log_queue;

        LOG_INFO(log, "Begin replay log, first log index {} and last log index {} in log file", batch_start_index, last_log_index);

        UInt32 REPLAY_THREAD_NUM = 1;
        ThreadPool object_thread_pool(REPLAY_THREAD_NUM);
        for (UInt32 thread_idx = 0; thread_idx < REPLAY_THREAD_NUM; thread_idx++)
        {
            object_thread_pool.trySchedule(
                [this, thread_idx, last_log_index, &load_mutex, &log_queue, &batch_start_index, &batch_end_index, &logstore] {
                    Poco::Logger * thread_log = &(Poco::Logger::get("LoadLogThread"));
                    while (batch_start_index < last_log_index)
                    {
                        while (log_queue.size() >= 10)
                        {
                            LOG_DEBUG(thread_log, "Sleep 1s wait for replay log");
                            sleep(1);
                            //load_cond.wait();
                        }

                        //0.3 * 10000 = 3M
                        batch_end_index = batch_start_index + 10000;
                        if (batch_end_index > last_log_index + 1)
                            batch_end_index = last_log_index + 1;

                        LOG_INFO(
                            thread_log,
                            "Begin load batch log to state machine, thread {}, batch [ {} , {} )",
                            thread_idx,
                            batch_start_index,
                            batch_end_index);

                        ReplayLogBatch batch;
                        batch.log_vec = logstore->log_entries_ext(batch_start_index, batch_end_index, 0);
                        batch.batch_start_index = batch_start_index;
                        batch.batch_end_index = batch_end_index;
                        batch.request_vec = cs_new<std::vector<ptr<SvsKeeperStorage::RequestForSession>>>();

                        for (auto entry : *(batch.log_vec))
                        {
                            if(entry->get_val_type() != nuraft::log_val_type::app_log)
                                continue;

                            if(isNewSessionRequest(entry->get_buf()))
                            {
                                /// replay session
                                int64_t session_timeout_ms = entry->get_buf().get_ulong();
                                int64_t session_id = storage.getSessionID(session_timeout_ms);
                                LOG_DEBUG(log, "replay session_id {} with timeout {} from log", session_id, session_timeout_ms);
                            }
                            else
                            {
                                /// replay nodes
                                ptr<SvsKeeperStorage::RequestForSession> ptr_request = this->createRequestSession(entry);
                                if (ptr_request != nullptr)
                                {
                                    batch.request_vec->push_back(ptr_request);
                                }
                            }
                        }

                        {
                            std::lock_guard queue_lock(load_mutex);
                            log_queue.push(batch);
                        }
                        LOG_INFO(
                            thread_log,
                            "Finish load batch log to state machine, thread {}, batch [ {} , {} )",
                            thread_idx,
                            batch_start_index,
                            batch_end_index);
                        batch_start_index = batch_end_index;
                    }
                });
        }

        while (log_queue.size() > 0 || batch_start_index < last_log_index)
        {
            ReplayLogBatch batch;
            {
                while (log_queue.size() == 0 && batch_start_index != last_log_index)
                {
                    LOG_DEBUG(
                        log,
                        "Sleep 1s, log queue size {}, start index {}, last index {}",
                        log_queue.size(),
                        batch_start_index,
                        last_log_index);
                    sleep(1);
                }
                if (log_queue.size() > 0)
                {
                    std::lock_guard queue_lock(load_mutex);
                    batch = log_queue.front();
                }
            }
            if (batch.log_vec == nullptr)
            {
                LOG_DEBUG(log, "log vector is null");
                break;
            }
            for (auto request : *(batch.request_vec))
            {
                storage.processRequest(request->request, request->session_id);
                if (request->session_id > storage.session_id_counter)
                {
                    LOG_WARNING(log, "Storage's session_id_counter {} must more than the session id {} of log.", storage.session_id_counter, request->session_id);
                    storage.session_id_counter = request->session_id;
                }
            }
            log_queue.pop();
            last_committed_idx = batch.batch_end_index - 1;
            LOG_INFO(log, "Replay start index {}, commit index {}", batch.batch_start_index, last_committed_idx);
            batch.log_vec = nullptr;
            batch.request_vec = nullptr;
            batch.batch_start_index = 0;
            batch.batch_end_index = 0;
        }
        object_thread_pool.wait();
    }
    LOG_INFO(log, "Replay log index {} in log store", last_committed_idx);
}

ptr<SvsKeeperStorage::RequestForSession> NuRaftStateMachine::createRequestSession(ptr<log_entry> & entry)
{
    if (entry->get_val_type() != nuraft::log_val_type::app_log)
    {
        return nullptr;
    }

    ReadBufferFromNuraftBuffer buffer(entry->get_buf());

    ptr<SvsKeeperStorage::RequestForSession> request_for_session = cs_new<SvsKeeperStorage::RequestForSession>();

    readIntBinary(request_for_session->session_id, buffer);
    if (buffer.eof())
    {
        LOG_DEBUG(log, "session time out {}", request_for_session->session_id);
        return nullptr;
    }

    int32_t length;
    Coordination::read(length, buffer);
    if (length <= 0)
    {
        return nullptr;
    }
    //LOG_DEBUG(log, "length {}", length);

    int32_t xid;
    Coordination::read(xid, buffer);
    //LOG_DEBUG(log, "xid {}", xid);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);
    //LOG_DEBUG(log, "opnum {}", opnum);

    request_for_session->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session->request->xid = xid;
    request_for_session->request->readImpl(buffer);
    return request_for_session;
}

SvsKeeperStorage::RequestForSession NuRaftStateMachine::parseRequest(nuraft::buffer & data)
{
    //auto log = &(Poco::Logger::get("KeeperStateMachine"));

    ReadBufferFromNuraftBuffer buffer(data);
    SvsKeeperStorage::RequestForSession request_for_session;
    readIntBinary(request_for_session.session_id, buffer);
    //LOG_DEBUG(log, "session id {}", request_for_session.session_id);

    int32_t length;
    Coordination::read(length, buffer);
    //LOG_DEBUG(log, "length {}", length);

    int32_t xid;
    Coordination::read(xid, buffer);
    //LOG_DEBUG(log, "xid {}", xid);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);
    //LOG_DEBUG(log, "opnum {}", opnum);

    request_for_session.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session.request->xid = xid;
    request_for_session.request->readImpl(buffer);

    //LOG_DEBUG(log, "Parse session id {}, length {}, xid {}, opnum {}", request_for_session.session_id, length, xid, opnum);

    return request_for_session;
}

ptr<buffer> NuRaftStateMachine::serializeRequest(SvsKeeperStorage::RequestForSession & session_request)
{
    WriteBufferFromNuraftBuffer out;
    Coordination::write(session_request.session_id, out);
    session_request.request->write(out);
    //auto log = &(Poco::Logger::get("KeeperStateMachine"));
    //LOG_INFO(log, "Serialize size {}", out.getBuffer()->size());
    return out.getBuffer();
}

ptr<buffer> NuRaftStateMachine::pre_commit(const ulong log_idx, buffer & data)
{
    // Nothing to do with pre-commit in this example.
    LOG_DEBUG(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
    return nullptr;
}

void NuRaftStateMachine::rollback(const ulong log_idx, buffer & data)
{
    // Nothing to do with rollback,
    // as this example doesn't do anything on pre-commit.
    LOG_DEBUG(log, "pre commit, log indx {}, data size {}", log_idx, data.size());
}

nuraft::ptr<nuraft::buffer> NuRaftStateMachine::commit(const ulong log_idx, nuraft::buffer & data)
{
    //2^19 = 524,288
    if (log_idx << 45 == 0)
    {
        LOG_INFO(log, "Begin commit log index {}", log_idx);
    }

    if (isNewSessionRequest(data))
    {
        nuraft::buffer_serializer timeout_data(data);
        int64_t session_timeout_ms = timeout_data.get_i64();
        auto response = nuraft::buffer::alloc(sizeof(int64_t));
        int64_t session_id;
        nuraft::buffer_serializer bs(response);
        {
            session_id = storage.getSessionID(session_timeout_ms);
            bs.put_i64(session_id);
        }
        LOG_DEBUG(log, "Session ID response {} with timeout {}", session_id, session_timeout_ms);
        last_committed_idx = log_idx;
        return response;
    }
    else
    {
        auto request_for_session = parseRequest(data);
        SvsKeeperStorage::ResponsesForSessions responses_for_sessions;
        {
            LOG_DEBUG(
                log,
                "Commit log index {}, SessionID/XID #{}#{}",
                log_idx,
                request_for_session.session_id,
                request_for_session.request->xid);
            /// TODO atomic process request and add response to queue
            responses_for_sessions = storage.processRequest(request_for_session.request, request_for_session.session_id);
            for (auto & response_for_session : responses_for_sessions)
                responses_queue.push(response_for_session);
        }

        last_committed_idx = log_idx;
        return nullptr;
    }
}

void NuRaftStateMachine::processReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
    SvsKeeperStorage::ResponsesForSessions responses;
    {
        responses = storage.processRequest(request_for_session.request, request_for_session.session_id);
    }
    for (const auto & response : responses)
        responses_queue.push(response);
}

std::unordered_set<int64_t> NuRaftStateMachine::getDeadSessions()
{
    return storage.getDeadSessions();
}

void NuRaftStateMachine::shutdownStorage()
{
    storage.finalize();
}

bool compareTime(const std::string & s1, const std::string & s2)
{
    return (s1 > s2);
}

void getDateFromFile(const std::string file_name, std::string & date)
{
    std::size_t p1 = file_name.find("_");
    std::size_t p2 = file_name.rfind("_");
    date = file_name.substr(p1 + 1, p2);
}

bool NuRaftStateMachine::chk_create_snapshot()
{
    return chk_create_snapshot(0L);
}

bool NuRaftStateMachine::chk_create_snapshot(time_t curr_time)
{
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    time_t prev_time = snap_mgr->getLastCreateTime();
    return timer.isActionTime(prev_time, curr_time);
}

void NuRaftStateMachine::create_snapshot(snapshot & s, async_result<bool>::handler_type & when_done)
{
    create_snapshot(s);
    ptr<std::exception> except(nullptr);
    bool ret = true;
    when_done(ret, except);
}

void NuRaftStateMachine::create_snapshot(snapshot & s)
{
    LOG_WARNING(log, "Create snapshot last_log_term {}, last_log_idx {}", s.get_last_log_term(), s.get_last_log_idx());
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        snap_mgr->createSnapshot(s, storage);
        snap_mgr->removeSnapshots();
    }
    ServiceProfileEvents::increment(ServiceProfileEvents::create_snapshot_count, 1);
}

void NuRaftStateMachine::save_snapshot_data(snapshot & s, const ulong offset, buffer & data)
{
    LOG_INFO(
        log,
        "Save snapshot data, snapshot last term {}, last index {}, offset {}, data size {}",
        s.get_last_log_term(),
        s.get_last_log_idx(),
        offset,
        data.size());
}

int NuRaftStateMachine::read_snapshot_data(snapshot & s, const ulong offset, buffer & data)
{
    LOG_INFO(
        log,
        "read snapshot data, snapshot last term {}, last index {}, offset {}, data size {}",
        s.get_last_log_term(),
        s.get_last_log_idx(),
        offset,
        data.size());
    return 0;
}

int NuRaftStateMachine::read_logical_snp_obj(snapshot & s, void *& user_snp_ctx, ulong obj_id, ptr<buffer> & data_out, bool & is_last_obj)
{
    ServiceProfileEvents::increment(ServiceProfileEvents::create_snapshot_count, 1);
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    // Snapshot doesn't exist.
    if (!snap_mgr->existSnapshot(s))
    {
        //snap_mgr->createSnapshot(s, node_map);
        data_out = nullptr;
        is_last_obj = true;
        LOG_INFO(log, "Cant find snapshot by last_log_idx {}, object id {}", s.get_last_log_idx(), obj_id);
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
        snap_mgr->receiveSnapshot(s);
    }
    else
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex);
        // Object ID > 0: actual snapshot value, save to local disk
        snap_mgr->saveSnapshotObject(s, obj_id, data);
    }
    LOG_INFO(log, "Save logical snapshot , object id {}, is_first_obj {}, is_last_obj {}", obj_id, is_first_obj, is_last_obj);
    // Request next object.
    obj_id++;
}

bool NuRaftStateMachine::exist_snapshot_object(snapshot & s, ulong obj_id)
{
    return snap_mgr->existSnapshotObject(s, obj_id);
}

bool NuRaftStateMachine::apply_snapshot(snapshot & s)
{
    ServiceProfileEvents::increment(ServiceProfileEvents::apply_received_snapshot_count, 1);
    //TODO: double buffer load or multi thread load
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    LOG_INFO(log, "apply snapshot term {}, last log index {}, size {}", s.get_last_log_term(), s.get_last_log_idx(), s.size());
    return snap_mgr->parseSnapshot(s, storage);
}

void NuRaftStateMachine::free_user_snp_ctx(void *& user_snp_ctx)
{
    // In this example, `read_logical_snp_obj` doesn't create
    // `user_snp_ctx`. Nothing to do in this function.
    if (user_snp_ctx != nullptr)
    {
        free(user_snp_ctx);
        user_snp_ctx = nullptr;
    }
}

ptr<snapshot> NuRaftStateMachine::last_snapshot()
{
    // Just return the latest snapshot.
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    LOG_INFO(log, "last_snapshot invoke");
    return snap_mgr->lastSnapshot();
}

bool NuRaftStateMachine::exists(const std::string & path)
{
    return (storage.container.count(path) == 1);
}

KeeperNode & NuRaftStateMachine::getNode(const std::string & path)
{
    auto node_ptr = storage.container.get(path);
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

}

#ifdef __clang__
#    pragma clang diagnostic pop
#endif
