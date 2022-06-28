#include <Service/SvsKeeperDispatcher.h>
#include <Common/DNSResolver.h>
#include <Common/isLocalAddress.h>
#include <Common/setThreadName.h>
#include <Common/checkStackSize.h>
#include <Service/WriteBufferFromFiFoBuffer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int SYSTEM_ERROR;
}

namespace fs = std::filesystem;

SvsKeeperDispatcher::SvsKeeperDispatcher()
    : configuration_and_settings(std::make_shared<KeeperConfigurationAndSettings>()), log(&Poco::Logger::get("SvsKeeperDispatcher"))
{
}

void SvsKeeperDispatcher::requestThread()
{
    setThreadName("SerKeeperReqT");

    SvsKeeperStorage::RequestForSession request;
    UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

    while (!shutdown_called)
    {
        /// TO prevent long time shutdown
        if (requests_queue.tryPop(request, std::min(max_wait, static_cast<UInt64>(1000))))
        {
            if (shutdown_called)
                break;

            try
            {
                LOG_TRACE(
                    log,
                    "Push request to keeper server : session {}, xid {}, opnum {}",
                    request.session_id,
                    request.request->xid,
                    Coordination::toString(request.request->getOpNum()));
                server->putRequest(request);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void SvsKeeperDispatcher::responseThread()
{
    setThreadName("SerKeeperRspT");

    SvsKeeperStorage::ResponseForSession response_for_session;
    UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());

    while (!shutdown_called)
    {
        if (responses_queue.tryPop(response_for_session, std::min(max_wait, static_cast<UInt64>(1000))))
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
    using namespace std::chrono;
    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    LOG_TRACE(log, "[putRequest]SessionID/xid #{}#{},opnum {}", session_id, request->xid, Coordination::toString(request->getOpNum()));

    //    std::lock_guard lock(push_request_mutex);

    /// Put close requests without timeouts
    if (request->getOpNum() == Coordination::OpNum::Close)
    {
        LOG_TRACE(log, "receive close request 0x{}", getHexUIntLowercase(session_id));
        if (!requests_queue.push(std::move(request_info)))
            throw Exception("Cannot push request to queue", ErrorCodes::SYSTEM_ERROR);
    }
    else if (!requests_queue.tryPush(std::move(request_info), configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds()))
        throw Exception("Cannot push request to queue within operation timeout", ErrorCodes::TIMEOUT_EXCEEDED);
    return true;
}

void SvsKeeperDispatcher::initialize(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Initializing storage dispatcher");
    configuration_and_settings = KeeperConfigurationAndSettings::loadFromConfig(config, true);

    server = std::make_unique<SvsKeeperServer>(configuration_and_settings, config, responses_queue);
    try
    {
        LOG_DEBUG(log, "Waiting server to initialize");
        server->startup();
        LOG_DEBUG(log, "Server initialized, waiting for quorum");

        server->waitInit();
        LOG_DEBUG(log, "Quorum initialized");

        server->reConfigIfNeed();
        LOG_DEBUG(log, "Server reconfiged");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        throw;
    }

    int thread_count = configuration_and_settings->thread_count;

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
    for (int i = 0; i < thread_count; i++)
    {
        request_thread->trySchedule([this] { requestThread(); });
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

            LOG_DEBUG(log, "Shutting down update_configuration_thread");
            if (update_configuration_thread.joinable())
                update_configuration_thread.join();

            LOG_DEBUG(log, "Shutting down session_cleaner_thread");
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
            LOG_DEBUG(log, "Shutting down request_thread");
            if (request_thread)
                request_thread->wait();

            LOG_DEBUG(log, "Shutting down responses_thread");
            if (responses_thread)
                responses_thread->wait();
#endif
        }

        if (server)
            server->shutdown();

        LOG_DEBUG(log, "for unhandled requests sending session expired error to client.");
        SvsKeeperStorage::RequestForSession request_for_session;
        while (requests_queue.tryPop(request_for_session))
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
                    using namespace std::chrono;
                    request_info.time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    {
                        std::lock_guard lock(push_request_mutex);
                        if (!requests_queue.push(std::move(request_info)))
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
    LOG_TRACE(log, "finish session 0x{}", getHexUIntLowercase(session_id));
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
        result.outstanding_requests_count = requests_queue.size();
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
