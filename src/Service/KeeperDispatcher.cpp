#include <Poco/NumberFormatter.h>

#include <Common/checkStackSize.h>
#include <Common/setThreadName.h>

#include <Service/KeeperDispatcher.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <Service/formatHex.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}

namespace fs = std::filesystem;
using Poco::NumberFormatter;

KeeperDispatcher::KeeperDispatcher()
    : configuration_and_settings(std::make_shared<Settings>())
    , log(&Poco::Logger::get("KeeperDispatcher"))
    , request_processor(std::make_shared<RequestProcessor>(responses_queue))
    , request_accumulator(request_processor)
    , request_forwarder(request_processor)
    , new_session_internal_id_counter(1)
{
}

void KeeperDispatcher::requestThread(RunnerId runner_id)
{
    setThreadName(("ReqDspchr#" + std::to_string(runner_id)).c_str());

    /// Requests from previous iteration. We store them to be able
    /// to send errors to the client.
    KeeperStore::RequestsForSessions prev_batch;

    while (!shutdown_called)
    {
        RequestForSession request_for_session;

        UInt64 max_wait = configuration_and_settings->raft_settings->operation_timeout_ms;

        if (requests_queue->tryPop(runner_id, request_for_session, std::min(static_cast<uint64_t>(1000), max_wait)))
        {
            if (shutdown_called)
                break;

            try
            {
                if (unlikely(isSessionRequest(request_for_session.request)
                             || request_for_session.request->getOpNum() == Coordination::OpNum::Auth))
                {
                    LOG_TRACE(log, "Skip to push {} to request processor", request_for_session.toSimpleString());
                }
                else if (isLocalSession(request_for_session.session_id))
                {
                    LOG_TRACE(log, "Push {} to request processor", request_for_session.toSimpleString());
                    request_processor->push(request_for_session);
                }
                /// we should skip close requests from clear session task
                else if (!request_for_session.isForwardRequest() && request_for_session.request->getOpNum() != Coordination::OpNum::Close)
                {
                    LOG_WARNING(log, "Not local session {}", toHexString(request_for_session.session_id));
                }

                if (!request_for_session.request->isReadRequest() && server->isLeaderAlive())
                {
                    LOG_TRACE(log, "Leader is {}", server->getLeader());

                    if (server->isLeader())
                        request_accumulator.push(request_for_session);
                    else
                        request_forwarder.push(request_for_session);
                }
                else if (!request_for_session.request->isReadRequest() && !server->isLeaderAlive())
                {
                    request_accumulator.push(request_for_session);
                }
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void KeeperDispatcher::responseThread()
{
    setThreadName("RspDispatcher");

    ResponseForSession response_for_session;
    UInt64 max_wait = configuration_and_settings->raft_settings->operation_timeout_ms;

    while (!shutdown_called)
    {
        if (responses_queue.tryPop(response_for_session, std::min(max_wait, static_cast<UInt64>(1000))))
        {
            if (shutdown_called)
                break;

            try
            {
                invokeResponseCallBack(response_for_session.session_id, response_for_session.response);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void KeeperDispatcher::invokeResponseCallBack(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response)
{
    /// session request
    if (unlikely(isSessionRequest(response->getOpNum())))
    {
        std::lock_guard lock(response_callbacks_mutex);
        auto session_writer = session_response_callbacks.find(session_id); /// TODO session id == internal id?
        if (session_writer == session_response_callbacks.end())
            return;
        session_writer->second(response);
    }
    /// user request
    else
    {
        std::lock_guard lock(response_callbacks_mutex);
        auto session_writer = user_response_callbacks.find(session_id);
        if (session_writer == user_response_callbacks.end())
            return;

        session_writer->second(response);
        /// Session closed, no more writes
        if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
            unregisterUserResponseCallBackWithoutLock(session_id);
    }
}

void KeeperDispatcher::invokeForwardResponseCallBack(ForwardingClientId client_id, ForwardResponsePtr response)
{
    std::lock_guard lock(forward_response_callbacks_mutex);
    auto forward_response_writer = forward_response_callbacks.find(client_id);
    if (forward_response_writer == forward_response_callbacks.end())
        return;

    LOG_TRACE(
        log, "[invokeForwardResponseCallBack] server_id {}, client_id {}, response {}", client_id.first, client_id.second, response->toString());

    forward_response_writer->second(response);
}

bool KeeperDispatcher::pushSessionRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t internal_id)
{
    RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = internal_id;

    using namespace std::chrono;
    request_info.create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    LOG_TRACE(
        log,
        "Push new/update session request #{}#{}#{}",
        toHexString(internal_id),
        request->xid,
        Coordination::toString(request->getOpNum()));

    if (!requests_queue->tryPush(std::move(request_info), configuration_and_settings->raft_settings->operation_timeout_ms))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push session request to queue within operation timeout");
    return true;
}

bool KeeperDispatcher::pushRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id)
{
    {
        std::lock_guard lock(response_callbacks_mutex);
        /// session is expired by server
        if (user_response_callbacks.count(session_id) == 0)
            return false;
    }

    RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = session_id;

    using namespace std::chrono;
    request_info.create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    LOG_TRACE(log, "Push user request #{}#{}#{}", toHexString(session_id), request->xid, Coordination::toString(request->getOpNum()));

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
    {
        if (!requests_queue->push(std::move(request_info)))
            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
    }
    else if (!requests_queue->tryPush(std::move(request_info), configuration_and_settings->raft_settings->operation_timeout_ms))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}


bool KeeperDispatcher::pushForwardingRequest(size_t server_id, size_t client_id, ForwardRequestPtr request)
{
    RequestForSession && request_info = request->requestForSession();

    using namespace std::chrono;
    request_info.create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    request_info.server_id = server_id;
    request_info.client_id = client_id;

    LOG_TRACE(
        log,
        "Push forwarding request #{}#{}#{} which is from server {} client {}",
        toHexString(request_info.session_id),
        request_info.request->xid,
        Coordination::toString(request_info.request->getOpNum()),
        server_id,
        client_id);

    /// Put close requests without timeouts
    if (request_info.request->getOpNum() == Coordination::OpNum::Close)
    {
        if (!requests_queue->push(std::move(request_info)))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push request to queue");
    }
    else if (!requests_queue->tryPush(std::move(request_info), configuration_and_settings->raft_settings->operation_timeout_ms))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Cannot push forwarding request to queue within operation timeout");
    return true;
}

void KeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_INFO(log, "Initializing dispatcher");
    configuration_and_settings = Settings::loadFromConfig(config, true);

    size_t thread_count = configuration_and_settings->thread_count;
    UInt64 operation_timeout_ms = configuration_and_settings->raft_settings->operation_timeout_ms;

    server = std::make_shared<KeeperServer>(configuration_and_settings, config, responses_queue, request_processor);
    new_session_internal_id_counter = server->myId();
    /// Raft server needs to be able to handle commit when startup.
    request_processor->initialize(thread_count, server, shared_from_this(), operation_timeout_ms);

    try
    {
        LOG_INFO(log, "Waiting server to initialize");
        server->startup();
        LOG_INFO(log, "Server initialized, waiting for quorum");

        server->waitInit();
        LOG_INFO(log, "Quorum initialized");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    UInt64 session_sync_period_ms = configuration_and_settings->raft_settings->dead_session_check_period_ms * 2;
    request_forwarder.initialize(thread_count, server, shared_from_this(), session_sync_period_ms, operation_timeout_ms);
    request_accumulator.initialize(
        1, shared_from_this(), server, operation_timeout_ms, configuration_and_settings->raft_settings->max_batch_size);
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);

    request_thread = std::make_shared<ThreadPool>(thread_count);
    responses_thread = std::make_shared<ThreadPool>(1);

    for (size_t i = 0; i < thread_count; i++)
    {
        request_thread->trySchedule([this, i] { requestThread(i); });
    }
    responses_thread->trySchedule([this] { responseThread(); });

    session_cleaner_thread = ThreadFromGlobalPool([this] { deadSessionCleanThread(); });
    update_configuration_thread = ThreadFromGlobalPool([this] { updateConfigurationThread(); });

    updateConfiguration(config);
    LOG_INFO(log, "Dispatcher initialized");
}

void KeeperDispatcher::shutdown()
{
    try
    {
        {
            std::lock_guard lock(push_request_mutex);

            if (shutdown_called)
                return;

            LOG_INFO(log, "Shutting down dispatcher");
            shutdown_called = true;

            LOG_INFO(log, "Shutting down update_configuration_thread");
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();

            LOG_INFO(log, "Shutting down session_cleaner_thread");
            if (session_cleaner_thread.joinable())
                session_cleaner_thread.join();

            LOG_INFO(log, "Shutting down request_thread");
            if (request_thread)
                request_thread->wait();

            LOG_INFO(log, "Shutting down responses_thread");
            if (responses_thread)
                responses_thread->wait();
        }

        LOG_INFO(log, "Shutting down request forwarder");
        request_forwarder.shutdown();

        LOG_INFO(log, "Shutting down request accumulator");
        request_accumulator.shutdown();

        LOG_INFO(log, "Shutting down request processor");
        request_processor->shutdown();

        if (server)
        {
            LOG_INFO(log, "Shutting down server");
            server->shutdown();
        }

        LOG_INFO(log, "for unhandled requests sending session expired error to client.");
        RequestForSession request_for_session;
        while (requests_queue->tryPopAny(request_for_session))
        {
            auto response = request_for_session.request->makeResponse();
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            invokeResponseCallBack(request_for_session.session_id, response);
        }
        user_response_callbacks.clear();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    LOG_INFO(log, "Dispatcher shut down");
}

void KeeperDispatcher::registerSessionResponseCallback(int64_t id, ZooKeeperResponseCallback callback)
{
    LOG_DEBUG(log, "Register session response callback {}", toHexString(id));
    std::lock_guard lock(response_callbacks_mutex);
    if (!session_response_callbacks.try_emplace(id, callback).second)
        throw Exception(RK::ErrorCodes::LOGICAL_ERROR, "Session response callback with id {} has already registered", toHexString(id));
}

void KeeperDispatcher::unRegisterSessionResponseCallback(int64_t id)
{
    std::lock_guard lock(response_callbacks_mutex);
    unRegisterSessionResponseCallbackWithoutLock(id);
}

void KeeperDispatcher::unRegisterSessionResponseCallbackWithoutLock(int64_t id)
{
    LOG_DEBUG(log, "Unregister session response callback {}", toHexString(id));
    auto it = session_response_callbacks.find(id);
    if (it != session_response_callbacks.end())
        session_response_callbacks.erase(it);
}

[[maybe_unused]] void KeeperDispatcher::registerUserResponseCallBack(int64_t session_id, ZooKeeperResponseCallback callback, bool is_reconnected)
{
    std::lock_guard lock(response_callbacks_mutex);
    registerUserResponseCallBackWithoutLock(session_id, callback, is_reconnected);
}

void KeeperDispatcher::registerUserResponseCallBackWithoutLock(int64_t session_id, ZooKeeperResponseCallback callback, bool is_reconnected)
{
    if (session_id != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Session id cannot be 0");

    if (!user_response_callbacks.try_emplace(session_id, callback).second && !is_reconnected)
        throw Exception(RK::ErrorCodes::LOGICAL_ERROR, "Session with id {} already registered in dispatcher", toHexString(session_id));
}

void KeeperDispatcher::unregisterUserResponseCallBack(int64_t session_id)
{
    std::lock_guard lock(response_callbacks_mutex);
    unregisterUserResponseCallBackWithoutLock(session_id);
}

void KeeperDispatcher::unregisterUserResponseCallBackWithoutLock(int64_t session_id)
{
    LOG_DEBUG(log, "Unregister user response callback {}", toHexString(session_id));
    auto it = user_response_callbacks.find(session_id);
    if (it != user_response_callbacks.end())
        user_response_callbacks.erase(it);
}

void KeeperDispatcher::registerForwarderResponseCallBack(ForwardingClientId client_id, ForwardResponseCallback callback)
{
    std::lock_guard lock(forward_response_callbacks_mutex);

    if (forward_response_callbacks.contains(client_id))
    {
        LOG_WARNING(
            log,
            "Receive new forwarding connection from server_id {}, client_id {}, will destroy the older one",
            client_id.first,
            client_id.second);
        auto & call_back = forward_response_callbacks[client_id];
        auto response = std::make_shared<ForwardDestroyResponse>();
        call_back(response);
        forward_response_callbacks.erase(client_id);
    }

    forward_response_callbacks.emplace(client_id, callback);
}

void KeeperDispatcher::unRegisterForwarderResponseCallBack(ForwardingClientId client_id)
{
    std::lock_guard lock(forward_response_callbacks_mutex);
    auto forward_response_writer = forward_response_callbacks.find(client_id);
    if (forward_response_writer == forward_response_callbacks.end())
        return;

    forward_response_callbacks.erase(forward_response_writer);
}

void KeeperDispatcher::deadSessionCleanThread()
{
    setThreadName("DeadSessnClean");

    LOG_INFO(log, "Start dead session clean thread");
    while (true)
    {
        if (shutdown_called)
            break;

        try
        {
            if (isLeader())
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(configuration_and_settings->raft_settings->dead_session_check_period_ms));

                auto dead_sessions = server->getDeadSessions();
                if (!dead_sessions.empty())
                    LOG_INFO(log, "Found dead sessions {}", dead_sessions.size());

                for (int64_t dead_session : dead_sessions)
                {
                    LOG_INFO(log, "Found dead session {}, will try to close it", toHexString(dead_session));
                    Coordination::ZooKeeperRequestPtr request
                        = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
                    request->xid = Coordination::CLOSE_XID;
                    RequestForSession request_info;
                    request_info.request = request;
                    request_info.session_id = dead_session;
                    // request_info.is_internal = true;
                    using namespace std::chrono;
                    request_info.create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    {
                        std::lock_guard lock(push_request_mutex);
                        if (!requests_queue->push(std::move(request_info)))
                            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
                    }
                    // unregisterUserResponseCallBack(dead_session);
                    LOG_DEBUG(log, "Dead session close request pushed");
                }
            }
            else
            {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(configuration_and_settings->raft_settings->dead_session_check_period_ms));
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    LOG_INFO(log, "End dead session clean thread!");
}


void KeeperDispatcher::updateConfigurationThread()
{
    setThreadName("UpdateConfig");

    while (true)
    {
        if (shutdown_called)
            return;

        try
        {
            if (!server->checkInit())
            {
                LOG_INFO(log, "Server still not initialized, will not apply configuration until initialization finished");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                continue;
            }

            ConfigUpdateAction action;
            if (!update_configuration_queue.tryPop(action, 1000))
                continue;

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
                        LOG_WARNING(
                            log,
                            "Cannot wait for configuration update, maybe we become leader, or maybe update is invalid, will try to wait "
                            "one more time");
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

bool KeeperDispatcher::isLocalSession(int64_t session_id)
{
    std::lock_guard lock(response_callbacks_mutex);
    auto it = user_response_callbacks.find(session_id);
    return it != user_response_callbacks.end();
}

void KeeperDispatcher::filterLocalSessions(std::unordered_map<int64_t, int64_t> & session_to_expiration_time)
{
    std::lock_guard lock(response_callbacks_mutex);
    for (auto it = session_to_expiration_time.begin(); it != session_to_expiration_time.end();)
    {
        if (!user_response_callbacks.contains(it->first))
        {
            LOG_TRACE(log, "Not local session {}", toHexString(it->first));
            it = session_to_expiration_time.erase(it);
        }
        else
        {
            LOG_TRACE(log, "Local session {}", it->first);
            ++it;
        }
    }
}


void KeeperDispatcher::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto diff = server->getConfigurationDiff(config);
    if (diff.empty())
        LOG_TRACE(log, "Configuration update triggered, but nothing changed for RAFT");
    else if (diff.size() > 1)
        LOG_WARNING(log, "Configuration changed for more than one server ({}) from cluster, it's strictly not recommended", diff.size());
    else
        LOG_INFO(log, "Configuration change size ({})", diff.size());

    for (auto & change : diff)
    {
        bool push_result = update_configuration_queue.push(change);
        if (!push_result)
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot push configuration update to queue");
    }
}


void KeeperDispatcher::updateKeeperStatLatency(uint64_t process_time_ms)
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

uint64_t KeeperDispatcher::getLogDirSize() const
{
    return getDirSize(configuration_and_settings->log_dir);
}

uint64_t KeeperDispatcher::getSnapDirSize() const
{
    return getDirSize(configuration_and_settings->snapshot_dir);
}

Keeper4LWInfo KeeperDispatcher::getKeeper4LWInfo()
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
        std::lock_guard lock(response_callbacks_mutex);
        result.alive_connections_count = user_response_callbacks.size();
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
