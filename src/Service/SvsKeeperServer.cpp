#include <chrono>
#include <string>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Service/LoggerWrapper.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/ReadBufferFromNuraftBuffer.h>
#include <Service/SvsKeeperServer.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <libnuraft/async.hxx>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <boost/algorithm/string.hpp>
#include <Poco/NumberFormatter.h>

#ifndef TEST_TCPHANDLER
//#define TEST_TCPHANDLER
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int RAFT_ERROR;
    extern const int INVALID_CONFIG_PARAMETER;
}

using Poco::NumberFormatter;

namespace
{
std::string checkAndGetSuperdigest(const String & user_and_digest)
{
    if (user_and_digest.empty())
        return "";

    std::vector<std::string> scheme_and_id;
    boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
    if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

}
SvsKeeperServer::SvsKeeperServer(
    const KeeperConfigurationAndSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config_,
    SvsKeeperResponsesQueue & responses_queue_)
    : server_id(coordination_settings_->server_id)
    , coordination_and_settings(coordination_settings_)
    , config(config_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("RaftKeeperServer")))
{

    state_manager = cs_new<NuRaftStateManager>(
        server_id,
        coordination_settings_->host + ":" + std::to_string(coordination_settings_->tcp_port),
        coordination_settings_->log_storage_path,
        config,
        coordination_settings_->coordination_settings->force_sync);


    state_machine = nuraft::cs_new<NuRaftStateMachine>(
        responses_queue_,
        coordination_and_settings->coordination_settings,
        coordination_and_settings->snapshot_storage_path,
        coordination_and_settings->snapshot_start_time,
        coordination_and_settings->snapshot_end_time,
        coordination_and_settings->snapshot_create_interval,
        coordination_and_settings->coordination_settings->max_stored_snapshots,
        state_manager->load_log_store(),
        checkAndGetSuperdigest(coordination_and_settings->super_digest));
}


void SvsKeeperServer::startup()
{
    /*
    nuraft::nuraft_global_config g_config;
    g_config.num_commit_threads_ = 2;
    g_config.num_append_threads_ = 2;
    nuraft::nuraft_global_mgr::init(g_config);
    */
    auto coordination_settings = coordination_and_settings->coordination_settings;
    nuraft::raft_params params;
    params.heart_beat_interval_ = coordination_settings->heart_beat_interval_ms.totalMilliseconds();
    params.election_timeout_lower_bound_ = coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds();
    params.election_timeout_upper_bound_ = coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds();
    params.reserved_log_items_ = coordination_settings->reserved_log_items;
    params.snapshot_distance_ = coordination_settings->snapshot_distance;
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();

    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};
    asio_opts.thread_pool_size_ = coordination_settings->nuraft_thread_size;
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    UInt16 port = config.getInt("service.internal_port", 5103);

    raft_instance = launcher.init(
        state_machine,
        state_manager,
        nuraft::cs_new<LoggerWrapper>("RaftInstance", coordination_settings->raft_logs_level),
        port,
        asio_opts,
        params,
        init_options);

    if (!raft_instance)
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot allocate RAFT instance");
}

void SvsKeeperServer::addServer(const std::vector<std::string> & tokens)
{
    if (tokens.size() < 2)
    {
        LOG_ERROR(log, "Too few arguments.");
        return;
    }

    int server_id_to_add = atoi(tokens[0].c_str());
    if (!server_id_to_add || server_id_to_add == server_id)
    {
        LOG_WARNING(log, "Wrong server id: {}", server_id_to_add);
        return;
    }

    std::string endpoint_to_add = tokens[1];
    srv_config srv_conf_to_add(server_id_to_add, 1, endpoint_to_add, std::string(), false, 50);
    LOG_DEBUG(log, "Adding server {}, {}.", toString(server_id_to_add), endpoint_to_add);
    auto ret = raft_instance->add_srv(srv_conf_to_add);
    if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
        auto ret1 = raft_instance->add_srv(srv_conf_to_add);
        if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
        {
            LOG_ERROR(log, "Failed to add server: {}", ret1->get_result_code());
            return;
        }
    }

    // Wait until it appears in server list.
    const size_t MAX_TRY = 400;
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(server_id_to_add);
        if (conf)
        {
            LOG_DEBUG(log, "Add server done.");
            break;
        }
    }
    LOG_DEBUG(log, "Async request is in progress add server {}.", server_id_to_add);
}

void SvsKeeperServer::addServer(ptr<srv_config> srv_conf_to_add)
{
    LOG_DEBUG(log, "Adding server {}, {} after 10s.", toString(srv_conf_to_add->get_id()), srv_conf_to_add->get_endpoint());
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    auto ret = raft_instance->add_srv(*srv_conf_to_add);
    if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
    {
        LOG_DEBUG(log, "Retry adding server {}, {} after 30s.", toString(srv_conf_to_add->get_id()), srv_conf_to_add->get_endpoint());
        std::this_thread::sleep_for(std::chrono::milliseconds(30000));
        auto ret1 = raft_instance->add_srv(*srv_conf_to_add);
        if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
        {
            LOG_ERROR(log, "Failed to add server {} : {}", srv_conf_to_add->get_endpoint(), ret1->get_result_code());
            return;
        }
    }

    // Wait until it appears in server list.
    const size_t MAX_TRY = 4 * 60 * 10; // 10 minutes
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(srv_conf_to_add->get_id());
        if (conf)
        {
            LOG_DEBUG(log, "Add server {} done.", srv_conf_to_add->get_endpoint());
            return;
        }
    }
    LOG_DEBUG(log, "Async request is in progress add server {}.", srv_conf_to_add->get_endpoint());
}


void SvsKeeperServer::getServerList(std::vector<Server> & server_list)
{
    std::vector<ptr<srv_config>> configs;
    raft_instance->get_srv_config_all(configs);
    int leader_id = raft_instance->get_leader();

    for (auto & entry : configs)
    {
        ptr<srv_config> & srv = entry;
        //std::cout << "server id " << srv->get_id() << ": " << srv->get_endpoint();
        Server server;
        server.server_id = srv->get_id();
        server.endpoint = srv->get_endpoint();
        if (srv->get_id() == leader_id)
        {
            //std::cout << " (LEADER)";
            server.is_leader = true;
        }
        else
        {
            server.is_leader = false;
        }
        server_list.push_back(server);
        //std::cout << std::endl;
    }
}

void SvsKeeperServer::removeServer(const std::string & endpoint)
{
    std::vector<Server> server_list;
    getServerList(server_list);
    std::optional<UInt32> to_remove_id;
    for (auto it = server_list.begin(); it != server_list.end(); it++)
    {
        if (it->endpoint == endpoint)
        {
            to_remove_id = it->server_id;
            LOG_DEBUG(log, "Removing server {}, {} after 30s.", toString(it->server_id), endpoint);
            std::this_thread::sleep_for(std::chrono::milliseconds(30000));
            auto ret = raft_instance->remove_srv(it->server_id);
            if (!ret->get_accepted() || ret->get_result_code() != nuraft::cmd_result_code::OK)
            {
                LOG_DEBUG(log, "Retry removing server {}, {} after 30s.", toString(it->server_id), endpoint);
                std::this_thread::sleep_for(std::chrono::milliseconds(30000));
                auto ret1 = raft_instance->remove_srv(it->server_id);
                if (!ret1->get_accepted() || ret1->get_result_code() != nuraft::cmd_result_code::OK)
                {
                    LOG_ERROR(log, "Failed to remove server {} : {}", endpoint, ret1->get_result_code());
                    return;
                }
            }
            break;
        }
    }

    if (!to_remove_id)
        return;
    // Wait until it appears in server list.
    const size_t MAX_TRY = 400;
    for (size_t jj = 0; jj < MAX_TRY; ++jj)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ptr<srv_config> conf = raft_instance->get_srv_config(to_remove_id.value());
        if (!conf)
        {
            LOG_DEBUG(log, "Remove server {} done.", endpoint);
            return;
        }
    }
    LOG_DEBUG(log, "Async request is in progress remove server {}.", endpoint);
}


void SvsKeeperServer::shutdown()
{
    LOG_INFO(log, "Shutting down keeper server.");
    state_machine->shutdown();
    if (state_manager->load_log_store() && !state_manager->load_log_store()->flush())
        LOG_WARNING(log, "Log store flush error while server shutdown.");
    //    state_manager->flushLogStore();
    if (!launcher.shutdown(coordination_and_settings->coordination_settings->shutdown_timeout.totalSeconds()))
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", 5);
    LOG_INFO(log, "Shut down keeper server done!");
}

namespace
{
#ifdef TEST_TCPHANDLER
#else
    nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request)
    {
        DB::WriteBufferFromNuraftBuffer buf;
        DB::writeIntBinary(session_id, buf);
        request->write(buf);
        DB::writeIntBinary(time, buf);
        return buf.getBuffer();
    }
#endif
}

void SvsKeeperServer::putRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
    auto [session_id, request, time] = request_for_session;
#ifdef TEST_TCPHANDLER
    if (Coordination::ZooKeeperCreateRequest * zk_request = dynamic_cast<Coordination::ZooKeeperCreateRequest *>(request.get()))
    {
        auto response_ptr = request->makeResponse();
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);
        response.xid = zk_request->xid;
        response.zxid = state_machine->getStorage().getZXID();
        response.path_created = zk_request->path;
        response.error = Coordination::Error::ZOK;
        responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{session_id, response_ptr});
    }
#else
    if (isLeaderAlive() && request->isReadRequest())
    {
        LOG_TRACE(
            log,
            "[put read request]SessionID/xid #{}#{}, opnum {}",
            session_id,
            request->xid,
            Coordination::toString(request->getOpNum()));
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
//            std::lock_guard lock(append_entries_mutex);
            result = raft_instance->append_entries(entries);
        }

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

        responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{session_id, response});
        if (!result->get_accepted())
            throw Exception(ErrorCodes::RAFT_ERROR,
                            "Request session {} xid {} error, result is not accepted.",
                            session_id,
                            request->xid);
        else
            throw Exception(ErrorCodes::RAFT_ERROR,
                            "Request session {} xid {} error, nuraft code {} and message: '{}'",
                            session_id,
                            request->xid,
                            result->get_result_code(),
                            result->get_result_str());
    }
#endif
}

int64_t SvsKeeperServer::getSessionID(int64_t session_timeout_ms)
{
    auto entry = buffer::alloc(sizeof(int64_t));
    /// Just special session request
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);

    std::lock_guard lock(append_entries_mutex);

    auto result = raft_instance->append_entries({entry});

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot send session_id request to RAFT, reason {}", result->get_result_str());

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "session_id request failed to RAFT");

    auto resp = result->get();
    if (resp == nullptr)
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr as session_id");

    nuraft::buffer_serializer bs_resp(resp);
    int64_t sid = bs_resp.get_i64();
    LOG_DEBUG(log, "[getSessionID]SessionID #{}", sid);
    return sid;
}

bool SvsKeeperServer::updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms)
{
    LOG_DEBUG(log, "Updating session timeout for {}", NumberFormatter::formatHex(session_id, true));

    auto entry = buffer::alloc(sizeof(int64_t) + sizeof(int64_t));
    nuraft::buffer_serializer bs(entry);

    bs.put_i64(session_id);
    bs.put_i64(session_timeout_ms);

    auto result = raft_instance->append_entries({entry});

    if (!result->get_accepted())
        throw Exception(ErrorCodes::RAFT_ERROR, "Cannot update session timeout, reason {}", result->get_result_str());

    if (result->get_result_code() != nuraft::cmd_result_code::OK)
        throw Exception(ErrorCodes::RAFT_ERROR, "Update session timeout failed to RAFT");

    if (!result->get())
        throw Exception(ErrorCodes::RAFT_ERROR, "Received nullptr when updating session timeout");

    auto buffer = ReadBufferFromNuraftBuffer(*result->get());
    int8_t is_success;
    Coordination::read(is_success, buffer);

    return is_success;
}

bool SvsKeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}


bool SvsKeeperServer::isObserver() const
{
    auto cluster_config = state_manager->get_cluster_config();
    return cluster_config->get_server(server_id)->is_learner();
}

bool SvsKeeperServer::isFollower() const
{
    return !isLeader() && !isObserver();
}

bool SvsKeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
}

uint64_t SvsKeeperServer::getFollowerCount() const
{
    return raft_instance->get_peer_info_all().size();
}

uint64_t SvsKeeperServer::getSyncedFollowerCount() const
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

nuraft::cb_func::ReturnCode SvsKeeperServer::callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * /* param */)
{
    if (type == nuraft::cb_func::Type::BecomeFresh || type == nuraft::cb_func::Type::BecomeLeader)
    {
        std::unique_lock lock(initialized_mutex);
        initialized_flag = true;
        initialized_cv.notify_all();
    }
    return nuraft::cb_func::ReturnCode::Ok;
}

void SvsKeeperServer::waitInit()
{
    std::unique_lock lock(initialized_mutex);
    int64_t timeout = coordination_and_settings->coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::vector<int64_t> SvsKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

ConfigUpdateActions SvsKeeperServer::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_)
{
    return state_manager->getConfigurationDiff(config_);
}

bool SvsKeeperServer::applyConfigurationUpdate(const ConfigUpdateAction & task)
{
    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to add server with id {}", task.server->get_id());
        bool added = false;
        for (size_t i = 0; i < coordination_and_settings->coordination_settings->configuration_change_tries_count; ++i)
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
                LOG_INFO(log, "Command to add server {} was not accepted for the {} time, will sleep for {} ms and retry", task.server->get_id(), i + 1, sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action AddServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!added)
            throw Exception(ErrorCodes::RAFT_ERROR, "Configuration change to add server (id {}) was not accepted by RAFT after all {} retries", task.server->get_id(), coordination_and_settings->coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::RemoveServer)
    {
        LOG_INFO(log, "Will try to remove server with id {}", task.server->get_id());

        bool removed = false;
        if (task.server->get_id() == state_manager->server_id())
        {
            LOG_INFO(log, "Trying to remove leader node (ourself), so will yield leadership and some other node (new leader) will try remove us. "
                          "Probably you will have to run SYSTEM RELOAD CONFIG on the new leader node");

            raft_instance->yield_leadership();
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * 5));
            return false;
        }

        for (size_t i = 0; i < coordination_and_settings->coordination_settings->configuration_change_tries_count; ++i)
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
                LOG_INFO(log, "Command to remove server {} was not accepted for the {} time, will sleep for {} ms and retry", task.server->get_id(), i + 1, sleep_ms * (i + 1));

            LOG_DEBUG(log, "Wait for apply action RemoveServer {} done for {} ms", task.server->get_id(), sleep_ms * (i + 1));
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms * (i + 1)));
        }
        if (!removed)
            throw Exception(ErrorCodes::RAFT_ERROR, "Configuration change to remove server (id {}) was not accepted by RAFT after all {} retries", task.server->get_id(), coordination_and_settings->coordination_settings->configuration_change_tries_count);
    }
    else if (task.action_type == ConfigUpdateActionType::UpdatePriority)
        raft_instance->set_priority(task.server->get_id(), task.server->get_priority());
    else
        LOG_WARNING(log, "Unknown configuration update type {}", static_cast<uint64_t>(task.action_type));

    return true;
}

bool SvsKeeperServer::waitConfigurationUpdate(const ConfigUpdateAction & task)
{

    size_t sleep_ms = 500;
    if (task.action_type == ConfigUpdateActionType::AddServer)
    {
        LOG_INFO(log, "Will try to wait server with id {} to be added", task.server->get_id());
        for (size_t i = 0; i < coordination_and_settings->coordination_settings->configuration_change_tries_count; ++i)
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

        for (size_t i = 0; i < coordination_and_settings->coordination_settings->configuration_change_tries_count; ++i)
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

void SvsKeeperServer::reConfigIfNeed()
{
//    if (!isLeader())
//        return;
//
//    auto cur_cluster_config = state_manager->load_config();
//
//    std::vector<String> srvs_removed;
//    std::vector<ptr<srv_config>> srvs_added;
//
//    auto new_cluster_config = state_manager->parseClusterConfig(config, "service.remote_servers");
//    std::list<ptr<srv_config>> & new_srvs(new_cluster_config->get_servers());
//    std::list<ptr<srv_config>> & old_srvs(cur_cluster_config->get_servers());
//
//    for (auto it = new_srvs.begin(); it != new_srvs.end(); ++it)
//    {
//        if (!cur_cluster_config->get_server((*it)->get_id()))
//            srvs_added.push_back(*it);
//    }
//
//    for (auto it = old_srvs.begin(); it != old_srvs.end(); ++it)
//    {
//        if (!new_cluster_config->get_server((*it)->get_id()))
//            srvs_removed.push_back((*it)->get_endpoint());
//    }
//
//    for (auto & end_point : srvs_removed)
//    {
//        removeServer(end_point);
//    }
//
//    for (auto srv_add : srvs_added)
//    {
//        addServer(srv_add);
//    }

}

}
