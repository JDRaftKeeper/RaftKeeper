#include <Service/SvsKeeperDispatcher.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <Common/setThreadName.h>
#include <Common/checkStackSize.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
    extern const int RAFT_ERROR;
}

namespace fs = std::filesystem;

SvsKeeperDispatcher::SvsKeeperDispatcher()
    : configuration_and_settings(std::make_shared<KeeperConfigurationAndSettings>()), log(&Poco::Logger::get("SvsKeeperDispatcher"))
    , svskeeper_sync_processor(requests_commit_event), svskeeper_commit_processor(requests_commit_event, responses_queue)
{
}

void SvsKeeperDispatcher::requestThreadFakeZk(size_t thread_index)
{
    setThreadName(("SerK - " + std::to_string(thread_index)).c_str());

    /// Result of requests batch from previous iteration
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    SvsKeeperStorage::RequestsForSessions prev_batch;

    while (!shutdown_called)
    {
        SvsKeeperStorage::RequestForSession request_for_session;

        UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

        if (requests_queue->tryPop(thread_index, request_for_session, max_wait))
        {
            //            LOG_TRACE(log, "1 requests_queue tryPop session {}, xid {}", request_for_session.session_id, request_for_session.request->xid);

            if (shutdown_called)
                break;

            try
            {
                svskeeper_sync_processor.processRequest(request_for_session);
                svskeeper_commit_processor.processRequest(request_for_session);
            }
            catch (...)
            {

                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void SvsKeeperDispatcher::requestThread(size_t thread_index)
{
    setThreadName(("SerK - " + std::to_string(thread_index)).c_str());

    /// Result of requests batch from previous iteration
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    SvsKeeperStorage::RequestsForSessions prev_batch;

    while (!shutdown_called)
    {
        SvsKeeperStorage::RequestForSession request_for_session;

        UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

        if (requests_queue->tryPop(thread_index, request_for_session, max_wait))
        {
//            LOG_TRACE(log, "1 requests_queue tryPop session {}, xid {}", request_for_session.session_id, request_for_session.request->xid);

            if (shutdown_called)
                break;

            try
            {
                std::vector<SvsKeeperStorage::RequestForSession> request_batch;
                bool has_read_request = request_for_session.request->isReadRequest();

                if (!has_read_request)
                {
                    request_batch.emplace_back(request_for_session);

                    /// async handle client request. Until prev_batch last request has notified.
//                    while (canAccumulateBatch(prev_result, prev_batch, request_batch.size()))
//                    {
//                    requests_queue.
                    while (request_batch.size() < 1000 && requests_queue->tryPop(thread_index, request_for_session))
                    {
//                        requests_queue.
//                        if (requests_queue->tryPop(thread_index, request_for_session, 1))
//                        {
//                            LOG_TRACE(log, "2 requests_queue tryPop session {}, xid {}", request_for_session.session_id, request_for_session.request->xid);

                            if (request_for_session.request->isReadRequest())
                            {
//                                LOG_TRACE(log, "tryPop batch read request succ");
                                has_read_request = true;
                                break;
                            }
                            else
                            {
                                request_batch.emplace_back(request_for_session);
//                                LOG_TRACE(log, "tryPop batch write request succ, request_batch size {}", request_batch.size());
                            }
//                        }

                        if (shutdown_called)
                            break;
                    }
                }

                if (prev_result)
                {
                    waitResultAndHandleError(prev_result, prev_batch);
                    prev_result.reset();
                    prev_batch.clear();
                }

                if (shutdown_called)
                    return;

                /// 1. First process the batch request
                if (!request_batch.empty())
                {
                    auto result = server->putRequestBatch(request_batch);

                    prev_batch = request_batch;
                    prev_result = result;

                    request_batch.clear();
                }

                /// 2. Second, process the current request
                if (has_read_request)
                {
                    /// wait prev write request result
                    if (prev_result)
                    {
                        waitResultAndHandleError(prev_result, prev_batch);
                        prev_result.reset();
                        prev_batch.clear();
                    }

                    requests_commit_event.waitForCommit(request_for_session.session_id);

//                    LOG_TRACE(log, "processReadRequest {}, {}", request_for_session.session_id, request_for_session.request->xid);
                    server->processReadRequest(request_for_session);
//                    LOG_TRACE(log, "processReadRequest succ {}, {}", request_for_session.session_id, request_for_session.request->xid);
                }
            }
            catch (...)
            {
                prev_result.reset();
                prev_batch.clear();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}


void SvsKeeperDispatcher::requestThreadAtomicConsistency(size_t thread_index)
{
    setThreadName(("SerK - " + std::to_string(thread_index)).c_str());

    /// Result of requests batch from previous iteration
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    SvsKeeperStorage::RequestsForSessions prev_batch;

    while (!shutdown_called)
    {
        SvsKeeperStorage::RequestForSession request_for_session;

        UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

        if (requests_queue->tryPop(thread_index, request_for_session, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                std::vector<SvsKeeperStorage::RequestForSession> request_batch;
                bool has_read_request = request_for_session.request->isReadRequest();

                if (!has_read_request)
                {
                    request_batch.emplace_back(request_for_session);

                    while (request_batch.size() < 1000 && requests_queue->tryPop(thread_index, request_for_session))
                    {
                        if (request_for_session.request->isReadRequest())
                        {
                            has_read_request = true;
                            break;
                        }
                        else
                        {
                            request_batch.emplace_back(request_for_session);
                        }

                        if (shutdown_called)
                            break;
                    }
                }

                if (prev_result)
                {
                    waitResultAndHandleError(prev_result, prev_batch);
                }

                if (request_batch.empty() && has_read_request && !prev_batch.empty())
                {
                    requests_commit_event.waitForCommit(prev_batch.back().session_id, prev_batch.back().request->xid);
                }

                prev_result.reset();
                prev_batch.clear();

                if (shutdown_called)
                    return;

                /// 1. First process the batch request
                if (!request_batch.empty())
                {
                    auto result = server->putRequestBatch(request_batch);

                    prev_batch = std::move(request_batch);
                    prev_result = result;
                }

                /// 2. Second, process the current request
                if (has_read_request)
                {
                    /// wait prev write request result
                    if (prev_result)
                    {
                        waitResultAndHandleError(prev_result, prev_batch);
                    }

                    if (!prev_batch.empty())
                        requests_commit_event.waitForCommit(prev_batch.back().session_id, prev_batch.back().request->xid);

                    prev_result.reset();
                    prev_batch.clear();

                    server->processReadRequest(request_for_session);
                }
            }
            catch (...)
            {
                prev_result.reset();
                prev_batch.clear();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}


void SvsKeeperDispatcher::requestThreadFakeZooKeeper(size_t thread_index)
{
    setThreadName(("SerK - " + std::to_string(thread_index)).c_str());

    /// Result of requests batch from previous iteration
    nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> result = nullptr;
    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
//    SvsKeeperStorage::RequestsForSessions prev_batch;

    SvsKeeperStorage::RequestsForSessions to_append_batch;
    std::unordered_set<int64_t> batch_session_ids;

    while (!shutdown_called)
    {
        try
        {
            SvsKeeperStorage::RequestForSession request_for_session;

            UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

            bool pop_succ = false;
            if (to_append_batch.empty())
            {
                pop_succ = requests_queue->tryPop(thread_index, request_for_session, max_wait);
            }
            else
            {
                if (!requests_queue->tryPop(thread_index, request_for_session))
                {
                    if (!to_append_batch.empty())
                    {
                        result = server->putRequestBatch(to_append_batch);
                        waitResultAndHandleError(result, to_append_batch);
                        result.reset();
                        to_append_batch.clear();
                    }
                    continue;
                }
                pop_succ = true;
            }

            if (pop_succ)
            {
                bool has_read_request = false;
                if (request_for_session.request->isReadRequest())
                {
                    has_read_request = true;
                }
                else
                {
                    to_append_batch.emplace_back(request_for_session);
                    batch_session_ids.emplace(request_for_session.session_id);
                }

                if (to_append_batch.size() > 1000 || (has_read_request && batch_session_ids.contains(request_for_session.session_id)))
                {
                    if (!to_append_batch.empty())
                    {
                        result = server->putRequestBatch(to_append_batch);
                        waitResultAndHandleError(result, to_append_batch);
                        result.reset();
                        to_append_batch.clear();
                    }
                }

                if (has_read_request)
                {
                    requests_commit_event.waitForCommit(request_for_session.session_id);
                    server->processReadRequest(request_for_session);
                }
            }
        }
        catch (...)
        {
            result.reset();
            to_append_batch.clear();
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

    }
}


/// The accumulate batch can only be saved when the result of the previous request is not received or is not committed.
bool SvsKeeperDispatcher::canAccumulateBatch(nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch, size_t request_batch_size)
{
    /// reach max batch size
    if (request_batch_size >= 100)
    {
//        LOG_TRACE(log, "111");
        return false;
    }


    /// not have prev_result, mabey first request or after read request
    if (!prev_result)
    {
//        LOG_TRACE(log, "222");
        return false;
    }

//    LOG_TRACE(log, "prev_result->has_result {}, prev_result->get_result_code {}", prev_result->has_result(), prev_result->get_result_code());
    /// prev_result not have result return
    if (!prev_result->has_result() && prev_result->get_result_code() == nuraft::cmd_result_code::RESULT_NOT_EXIST_YET)
    {
//        LOG_TRACE(log, "333");
        return true;
    }

    /// prev_result have result return, but last request not commit
    if (prev_result->has_result() && prev_result->get_result_code() == nuraft::cmd_result_code::OK)
    {
//        LOG_TRACE(log, "444");
        if (!prev_batch.empty() && !requests_commit_event.hasNotified(prev_batch.back().session_id, prev_batch.back().request->xid))
        {
//            LOG_TRACE(log, "555");
            return true;
        }
    }
//    LOG_TRACE(log, "666");
    /// prev_result has error result
    return false;
}

bool SvsKeeperDispatcher::waitResultAndHandleError(nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch)
{
    /// Forcefully process all previous pending requests

    if (!prev_result->has_result())
        prev_result->get();

    bool result_accepted = prev_result->get_accepted();

    if (result_accepted && prev_result->get_result_code() == nuraft::cmd_result_code::OK)
    {
        return true;
    }
    else
    {

        for (auto & request_session : prev_batch)
        {
            requests_commit_event.erase(request_session.session_id, request_session.request->xid);

            auto response = request_session.request->makeResponse();

            response->xid = request_session.request->xid;
            response->zxid = 0;

            response->error = prev_result->get_result_code() == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                                 : Coordination::Error::ZCONNECTIONLOSS;

            responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request_session.session_id, response});
        }

        if (!result_accepted)
            throw Exception(ErrorCodes::RAFT_ERROR,
                            "Request batch is not accepted.");
        else
            throw Exception(ErrorCodes::RAFT_ERROR,
                            "Request batch error, nuraft code {} and message: '{}'",
                            prev_result->get_result_code(),
                            prev_result->get_result_str());
    }
}

void SvsKeeperDispatcher::responseThread()
{
    setThreadName("SerKeeperRspT");
    while (!shutdown_called)
    {
        SvsKeeperStorage::ResponseForSession response_for_session;

        UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

        if (responses_queue.tryPop(response_for_session, max_wait))
        {
            if (shutdown_called)
                break;

            try
            {
                setResponse(response_for_session.session_id, response_for_session.response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void SvsKeeperDispatcher::setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_writer = session_to_response_callback.find(session_id);
    if (session_writer == session_to_response_callback.end())
        return;

    session_writer->second(response);
    /// Session closed, no more writes
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
        session_to_response_callback.erase(session_writer);
}

bool SvsKeeperDispatcher::putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        if (session_to_response_callback.count(session_id) == 0)
            return false;
    }

    SvsKeeperStorage::RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = session_id;

    LOG_TRACE(log, "[putRequest]SessionID/xid #{}#{},opnum {}", session_id, request->xid, request->getOpNum());

//    std::lock_guard lock(push_request_mutex);

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
    {
        if (!requests_queue->push(std::move(request_info)))
            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
    }
    else if (!requests_queue->tryPush(std::move(request_info), configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}

void SvsKeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");
    configuration_and_settings = KeeperConfigurationAndSettings::loadFromConfig(config, true);

    server = std::make_shared<SvsKeeperServer>(configuration_and_settings, config, responses_queue, requests_commit_event);
    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup();
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        server->waitInit();
        LOG_DEBUG(log, "Quorum initialized");

        server->reConfigIfNeed();
        LOG_DEBUG(log, "Server reconfiged");

        svskeeper_sync_processor.setRaftServer(server);
        svskeeper_commit_processor.setRaftServer(server);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    int thread_count = configuration_and_settings->thread_count;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);

#ifdef __THREAD_POOL_VEC__
    request_threads.reserve(thread_count);
    for (int i = 0; i < thread_count; ++i)
        request_threads.emplace_back(&SvsKeeperStorageDispatcher::requestThread, this);

    response_threads.reserve(thread_count);
    for (int i = 0; i < thread_count; ++i)
        response_threads.emplace_back(&SvsKeeperStorageDispatcher::responseThread, this);
#else
    request_thread = std::make_shared<ThreadPool>(thread_count);
    responses_thread = std::make_shared<ThreadPool>(1);
    for (int32_t i = 0; i < thread_count; i++)
    {
        request_thread->trySchedule([this, i] { requestThreadFakeZk(i); });
    }
    responses_thread->trySchedule([this] { responseThread(); });
#endif

    session_cleaner_thread = ThreadFromGlobalPool([this] { sessionCleanerTask(); });
    update_configuration_thread = ThreadFromGlobalPool([this] { updateConfigurationThread(); });
    updateConfiguration(config);

    LOG_DEBUG(log, "Dispatcher initialized");
}

void SvsKeeperDispatcher::shutdown()
{
    try
    {
        {
            std::lock_guard lock(push_request_mutex);

            if (shutdown_called)
                return;

            LOG_DEBUG(log, "Shutting down storage dispatcher");
            shutdown_called = true;

            if (update_configuration_thread.joinable())
                update_configuration_thread.join();

            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

#ifdef __THREAD_POOL_VEC__
            for (auto & request_thread : request_threads)
                request_thread.join();

            request_threads.clear();

            for (auto & response_thread : response_threads)
                response_thread.join();

            response_threads.clear();
#else

            requests_commit_event.notifiyAll();

            if (request_thread)
                request_thread->wait();
            if (responses_thread)
                responses_thread->wait();
#endif
        }

        svskeeper_sync_processor.shutdown();
        svskeeper_commit_processor.shutdown();

        if (server)
            server->shutdown();

        SvsKeeperStorage::RequestForSession request_for_session;
        while (requests_queue->tryPopAny(request_for_session))
        {
            auto response = request_for_session.request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            setResponse(request_for_session.session_id, response);
        }
        session_to_response_callback.clear();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_DEBUG(log, "Dispatcher shut down");
}

void SvsKeeperDispatcher::registerSession(int64_t session_id, ZooKeeperResponseCallback callback)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    if (!session_to_response_callback.try_emplace(session_id, callback).second)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", session_id);
}

void SvsKeeperDispatcher::sessionCleanerTask()
{
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            if (isLeader())
            {
                auto dead_sessions = server->getDeadSessions();
                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", dead_session);
                    Coordination::ZooKeeperRequestPtr request
                        = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    SvsKeeperStorage::RequestForSession request_info;
                    request_info.request = request;
                    request_info.session_id = dead_session;
                    {
                        std::lock_guard lock(push_request_mutex);
                        if (!requests_queue->push(std::move(request_info)))
                            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
                    }
                    finishSession(dead_session);
                    LOG_INFO(log, "Dead session close request pushed");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(configuration_and_settings->coordination_settings->dead_session_check_period_ms.totalMilliseconds()));
    }
}


void SvsKeeperDispatcher::updateConfigurationThread()
{
    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            if (!server->checkInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(std::chrono::milliseconds(5000));
                continue;
            }

            ConfigUpdateAction action;
            if (!update_configuration_queue.pop(action))
                break;


            /// We must wait this update from leader or apply it ourself (if we are leader)
            bool done = false;
            while (!done)
            {
                if (shutdown_called)
                    return;

                if (isLeader())
                {
                    done = server->applyConfigurationUpdate(action);
                    if (!done)
                        LOG_WARNING(log, "Cannot apply configuration update, maybe trying to remove leader node (ourself), will retry");
                }
                else
                {
                    done = server->waitConfigurationUpdate(action);
                    if (!done)
                        LOG_WARNING(log, "Cannot wait for configuration update, maybe we become leader, or maybe update is invalid, will try to wait one more time");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void SvsKeeperDispatcher::finishSession(int64_t session_id)
{
    std::lock_guard lock(session_to_response_callback_mutex);
    auto session_it = session_to_response_callback.find(session_id);
    if (session_it != session_to_response_callback.end())
        session_to_response_callback.erase(session_it);
}

void SvsKeeperDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto diff = server->getConfigurationDiff(config);
    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for RAFT");
    else if (diff.size() > 1)
        LOG_WARNING(log, "Configuration changed for more than one server ({}) from cluster, it's strictly not recommended", diff.size());
    else
        LOG_DEBUG(log, "Configuration change size ({})", diff.size());

    for (auto & change : diff)
    {
        bool push_result = update_configuration_queue.push(change);
        if (!push_result)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");
    }
}


void SvsKeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms)
{
    std::lock_guard lock(keeper_stats_mutex);
    keeper_stats.updateLatency(process_time_ms);
}

static uint64_t getDirSize(const fs::path & dir)
{
    checkStackSize();
    if (!fs::exists(dir))
        return 0;

    fs::directory_iterator it(dir);
    fs::directory_iterator end;

    uint64_t size{0};
    while (it != end)
    {
        if (it->is_regular_file())
            size += fs::file_size(*it);
        else
            size += getDirSize(it->path());
        ++it;
    }
    return size;
}

uint64_t SvsKeeperDispatcher::getLogDirSize() const
{
    return getDirSize(configuration_and_settings->log_storage_path);
}

uint64_t SvsKeeperDispatcher::getSnapDirSize() const
{
    return getDirSize(configuration_and_settings->snapshot_storage_path);
}

Keeper4LWInfo SvsKeeperDispatcher::getKeeper4LWInfo()
{
    Keeper4LWInfo result;
    result.is_follower = server->isFollower();
    result.is_standalone = !result.is_follower && server->getFollowerCount() == 0;
    result.is_leader = isLeader();
    result.is_observer = server->isObserver();
    result.has_leader = hasLeader();
    {
        std::lock_guard lock(push_request_mutex);
        result.outstanding_requests_count = requests_queue->size();
    }
    {
        std::lock_guard lock(session_to_response_callback_mutex);
        result.alive_connections_count = session_to_response_callback.size();
    }
    if (result.is_leader)
    {
        result.follower_count = server->getFollowerCount();
        result.synced_follower_count = server->getSyncedFollowerCount();
    }
    result.total_nodes_count = server->getKeeperStateMachine()->getNodesCount();
    result.last_zxid = server->getKeeperStateMachine()->getLastProcessedZxid();
    return result;
}

}
