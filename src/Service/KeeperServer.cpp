#include <chrono>
#include <string>
#include <Service/KeeperServer.h>
#include <Service/LoggerWrapper.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <libnuraft/async.hxx>
#include <Poco/NumberFormatter.h>
#include <Common/Stopwatch.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

using Poco::NumberFormatter;

KeeperServer::KeeperServer(
    const SettingsPtr & settings_,
    const Poco::Util::AbstractConfiguration & config_,
    KeeperResponsesQueue & responses_queue_,
    std::shared_ptr<RequestProcessor> request_processor_)
    : server_id(settings_->my_id)
    , settings(settings_)
    , config(config_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("KeeperServer")))
{
    state_manager = cs_new<NuRaftStateManager>(server_id, config, settings_);

    state_machine = nuraft::cs_new<NuRaftStateMachine>(
        responses_queue_,
        settings->raft_settings,
        settings->snapshot_dir,
        settings->snapshot_create_interval,
        settings->raft_settings->max_stored_snapshots,
        new_session_id_callback_mutex,
        new_session_id_callback,
        state_manager->load_log_store(),
        checkAndGetSuperdigest(settings->super_digest),
        KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE,
        request_processor_);
}
namespace
{
    void initializeRaftParams(nuraft::raft_params & params, RaftSettingsPtr & raft_settings)
    {
        params.heart_beat_interval_ = raft_settings->heart_beat_interval_ms;
        params.election_timeout_lower_bound_ = raft_settings->election_timeout_lower_bound_ms;
        params.election_timeout_upper_bound_ = raft_settings->election_timeout_upper_bound_ms;
        params.reserved_log_items_ = raft_settings->reserved_log_items;
        params.snapshot_distance_ = raft_settings->snapshot_distance;
        params.client_req_timeout_ = raft_settings->operation_timeout_ms;
        params.return_method_ = nuraft::raft_params::blocking;
        params.parallel_log_appending_ = raft_settings->log_fsync_mode == FsyncMode::FSYNC_PARALLEL;
        params.auto_forwarding_ = true;
        params.auto_forwarding_req_timeout_ = raft_settings->operation_timeout_ms;
        // TODO set max_batch_size to NuRaft
    }
}

void KeeperServer::startup()
{
    auto raft_settings = settings->raft_settings;

    nuraft::raft_params params;
    initializeRaftParams(params, raft_settings);

    nuraft::asio_service::options asio_opts{};
    asio_opts.thread_pool_size_ = raft_settings->nuraft_thread_size;
    nuraft::raft_server::init_options init_options;

    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    UInt16 port = config.getInt("keeper.internal_port", 8103);

    raft_instance = launcher.init(
        state_machine,
        state_manager,
        nuraft::cs_new<LoggerWrapper>("NuRaft", raft_settings->raft_logs_level),
        port,
        asio_opts,
        params,
        init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot initialized RAFT instance");

    /// used raft_instance notify_log_append_completion
    if (raft_settings->log_fsync_mode == FsyncMode::FSYNC_PARALLEL)
        dynamic_cast<NuRaftFileLogStore &>(*state_manager->load_log_store()).setRaftServer(raft_instance);
}

int32 KeeperServer::getLeader()
{
    return raft_instance->get_leader();
}

void KeeperServer::shutdown()
{
    LOG_INFO(log, "Shutting down keeper server.");
    state_machine->shutdown();
    if (state_manager->load_log_store() && !state_manager->load_log_store()->flush())
        LOG_WARNING(log, "Log store flush error while server shutdown.");

    dynamic_cast<NuRaftFileLogStore &>(*state_manager->load_log_store()).shutdown();

    if (!launcher.shutdown(settings->raft_settings->shutdown_timeout))
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", 5);
    LOG_INFO(log, "Shut down keeper server done!");
}

void KeeperServer::putRequest(const RequestForSession & request_for_session)
{
    auto [session_id, request, time, server, client] = request_for_session;
    if (isLeaderAlive() && request->isReadRequest())
    {
        LOG_TRACE(
            log, "[put read request]SessionID/xid #{}#{}, opnum {}", session_id, request->xid, Coordination::toString(request->getOpNum()));
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        std::vector<ptr<buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, time, request));

        LOG_TRACE(
            log,
            "[put write request]SessionID/xid #{}#{}, opnum {}, entries {}",
            session_id,
            request->xid,
            Coordination::toString(request->getOpNum()),
            entries.size());

        ptr<nuraft::cmd_result<ptr<buffer>>> result;
        {
            result = raft_instance->append_entries(entries);
        }

        if (!result->has_result())
            result->get();

        if (result->get_accepted() && result->get_result_code() == nuraft::cmd_result_code::OK)
        {
            /// response pushed into queue by state machine
            return;
        }

        auto response = request->makeResponse();

        response->xid = request->xid;
        response->zxid = 0;

        response->error = result->get_result_code() == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                        : Coordination::Error::ZCONNECTIONLOSS;

        responses_queue.push(RK::ResponseForSession{session_id, response});
        if (!result->get_accepted())
            throw Exception(ErrorCodes::RAFT_ERROR, "Request session {} xid {} error, result is not accepted.", session_id, request->xid);
        else
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Request session {} xid {} error, nuraft code {} and message: '{}'",
                session_id,
                request->xid,
                result->get_result_code(),
                result->get_result_str());
    }
}

ptr<nuraft::cmd_result<ptr<buffer>>> KeeperServer::putRequestBatch(const std::vector<RequestForSession> & request_batch)
{
    LOG_DEBUG(log, "process the batch requests {}", request_batch.size());
    std::vector<ptr<buffer>> entries;
    for (const auto & request_session : request_batch)
    {
        LOG_TRACE(
            log,
            "push request to entries session {}, xid {}, opnum {}",
            request_session.session_id,
            request_session.request->xid,
            request_session.request->getOpNum());
        entries.push_back(getZooKeeperLogEntry(request_session.session_id, request_session.create_time, request_session.request));
    }
    /// append_entries write request
    ptr<nuraft::cmd_result<ptr<buffer>>> result = raft_instance->append_entries(entries);
    return result;
}

int64_t KeeperServer::getSessionID(int64_t session_timeout_ms)
{
    auto entry = buffer::alloc(sizeof(int64_t));
    /// Just special session request
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);

    int64_t sid;
    {
        Stopwatch sw;
        auto result = raft_instance->append_entries({entry});

        if (!result->has_result())
            result->get();

        sw.stop();
        LOG_TRACE(log, "append entries for new session cost {}ms", sw.elapsedMilliseconds());

        if (!result->get_accepted())
            throw Exception(ErrorCodes::RAFT_ERROR, "Cannot send session_id request to RAFT, reason {}", result->get_result_str());

        if (result->get_result_code() != nuraft::cmd_result_code::OK)
            throw Exception(ErrorCodes::RAFT_ERROR, "session_id request failed to RAFT");

        auto resp = result->get();
        if (resp == nullptr)
            throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr as session_id");

        nuraft::buffer_serializer bs_resp(resp);
        sid = bs_resp.get_i64();
    }

    {
        std::unique_lock session_id_lock(new_session_id_callback_mutex);
        if (!state_machine->getStore().containsSession(sid))
        {
            ptr<std::condition_variable> condition = std::make_shared<std::condition_variable>();
            new_session_id_callback.emplace(sid, condition);

            using namespace std::chrono_literals;
            auto status = condition->wait_for(session_id_lock, session_timeout_ms * 1ms);

            new_session_id_callback.erase(sid);
            if (status == std::cv_status::timeout)
            {
                throw Exception(ErrorCodes::RAFT_ERROR, "Time out, can not allocate session {}", sid);
            }
        }
    }

    LOG_DEBUG(log, "Got session {}", sid);
    return sid;
}

bool KeeperServer::updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
{
    LOG_DEBUG(log, "Updating session timeout for {}", NumberFormatter::formatHex(session_id, true));

    auto entry = buffer::alloc(sizeof(int64_t) + sizeof(int64_t));
    nuraft::buffer_serializer bs(entry);

    bs.put_i64(session_id);
    bs.put_i64(session_timeout_ms);

    auto result = raft_instance->append_entries({entry});

    if (!result->has_result())
        result->get();

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot update session timeout, reason {}", result->get_result_str());

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "Update session timeout failed to RAFT");

    if (!result->get())
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr when updating session timeout");

    auto buffer = ReadBufferFromNuraftBuffer(*result->get());
    int8_t is_success;
    Coordination::read(is_success, buffer);

    if (!is_success)
        return false;

    {
        std::unique_lock session_id_lock(new_session_id_callback_mutex);

        const auto & dead_session = getDeadSessions();
        if (std::count(dead_session.begin(), dead_session.end(), session_id))
        {
            ptr<std::condition_variable> condition = std::make_shared<std::condition_variable>();

            new_session_id_callback.emplace(session_id, condition);

            using namespace std::chrono_literals;
            auto status = condition->wait_for(session_id_lock, session_timeout_ms * 1ms);

            new_session_id_callback.erase(session_id);
            if (status == std::cv_status::timeout)
            {
                throw Exception(ErrorCodes::RAFT_ERROR, "Time out, can not allocate session {}", session_id);
            }
        }
    }

    return is_success;
}

void KeeperServer::handleRemoteSession(int64_t session_id, int64_t expiration_time)
{
    state_machine->getStore().handleRemoteSession(session_id, expiration_time);
}

[[maybe_unused]] int64_t KeeperServer::getSessionTimeout(int64_t session_id)
{
    LOG_DEBUG(log, "get session timeout for {}", session_id);
    if (state_machine->getStore().session_and_timeout.contains(session_id))
    {
        return state_machine->getStore().session_and_timeout.find(session_id)->second;
    }
    else
    {
        LOG_WARNING(log, "Not found session timeout for {}", session_id);
        return -1;
    }
}

bool KeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}


bool KeeperServer::isObserver() const
{
    auto cluster_config = state_manager->get_cluster_config();
    return cluster_config->get_server(server_id)->is_learner();
}

bool KeeperServer::isFollower() const
{
    return !isLeader() && !isObserver();
}

bool KeeperServer::isLeaderAlive() const
{
    /// nuraft leader_ and role_ not sync
    return raft_instance->is_leader_alive() && raft_instance->get_leader() != -1;
}

uint64_t KeeperServer::getFollowerCount() const
{
    return raft_instance->get_peer_info_all().size();
}

uint64_t KeeperServer::getSyncedFollowerCount() const
{
    uint64_t last_log_idx = raft_instance->get_last_log_idx();
    const auto followers = raft_instance->get_peer_info_all();

    uint64_t stale_followers = 0;

    const uint64_t stale_follower_gap = raft_instance->get_current_params().stale_log_gap_;
    for (const auto & fl : followers)
    {
        if (last_log_idx > fl.last_log_idx_ + stale_follower_gap)
            stale_followers++;
    }
    return followers.size() - stale_followers;
}

nuraft::cb_func::ReturnCode KeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    if (type == nuraft::cb_func::Type::BecomeFresh || type == nuraft::cb_func::Type::BecomeLeader)
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    }
    else if (type == nuraft::cb_func::NewConfig)
    {
        /// Update Forward connections
        std::unique_lock lock(forward_listener_mutex);
        if (update_forward_listener)
            update_forward_listener();
    }
    return nuraft::cb_func::ReturnCode::Ok;
}

void KeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = settings->raft_settings->startup_timeout;
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::vector<int64_t> KeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

ConfigUpdateActions KeeperServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_)
{
    return state_manager->getConfigurationDiff(config_);
}

bool KeeperServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added", task.server->get_id());
                added = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to add server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->add_srv(*task.server);
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(
                log,
                "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * 5));
            return false;
        }

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed", task.server->get_id());
                removed = true;
                break;
            }

            if (!isLeader())
            {
                LOG_INFO(log, "We are not leader anymore, will not try to remove server {}", task.server->get_id());
                break;
            }

            auto result = raft_instance->remove_srv(task.server->get_id());
            if (!result->get_accepted())
                LOG_INFO(
                    log,
                    "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry",
                    task.server->get_id(),
                    i + 1,
                    sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries",
                task.server->get_id(),
                settings->raft_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));

    return true;
}

bool KeeperServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) != nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully added by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to add server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to wait remove of server with id {}", task.server->get_id());

        for (size_t i = 0; i < settings->raft_settings->configuration_change_tries_count; ++i)
        {
            if (raft_instance->get_srv_config(task.server->get_id()) == nullptr)
            {
                LOG_INFO(log, "Server with id {} was successfully removed by leader", task.server->get_id());
                return true;
            }

            if (isLeader())
            {
                LOG_INFO(log, "We are leader now, probably we will have to remove server {}", task.server->get_id());
                return false;
            }

            LOG_DEBUG(log, "Wait for action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        return false;
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        return true;
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));
    return true;
}

uint64_t KeeperServer::createSnapshot()
{
    uint64_t log_idx = raft_instance->create_snapshot();
    if (log_idx != 0)
        LOG_INFO(log, "Snapshot creation scheduled with last committed log index {}.", log_idx);
    else
        LOG_WARNING(log, "Failed to schedule snapshot creation task.");
    return log_idx;
}

KeeperLogInfo KeeperServer::getKeeperLogInfo()
{
    KeeperLogInfo log_info;
    auto log_store = state_manager->load_log_store();
    if (log_store)
    {
        log_info.first_log_idx = log_store->start_index();
        log_info.first_log_term = log_store->term_at(log_info.first_log_idx);
    }

    if (raft_instance)
    {
        log_info.last_log_idx = raft_instance->get_last_log_idx();
        log_info.last_log_term = raft_instance->get_last_log_term();
        log_info.last_committed_log_idx = raft_instance->get_committed_log_idx();
        log_info.leader_committed_log_idx = raft_instance->get_leader_committed_log_idx();
        log_info.target_committed_log_idx = raft_instance->get_target_committed_log_idx();
        log_info.last_snapshot_idx = raft_instance->get_last_snapshot_idx();
    }

    return log_info;
}

bool KeeperServer::requestLeader()
{
    return isLeader() || raft_instance->request_leadership();
}

void KeeperServer::registerForWardListener(UpdateForwardListener forward_listener)
{
    std::unique_lock lock(forward_listener_mutex);
    update_forward_listener = forward_listener;
}

}
