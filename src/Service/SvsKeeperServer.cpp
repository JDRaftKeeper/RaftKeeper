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
#include <Service/SvsKeeperProfileEvents.h>
#include <libnuraft/async.hxx>
#include <Common/ZooKeeper/ZooKeeperIO.h>

#ifndef TEST_TCPHANDLER
//#define TEST_TCPHANDLER
#endif

namespace ServiceProfileEvents
{
extern const Event req_all;
extern const Event req_read;
extern const Event req_write;
}
namespace DB
{
namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

SvsKeeperServer::SvsKeeperServer(
    int server_id_,
    const SvsKeeperSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config,
    SvsKeeperResponsesQueue & responses_queue_)
    : server_id(server_id_)
    , coordination_settings(coordination_settings_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("RaftKeeperServer")))
{
    std::string log_dir = config.getString("service.log_dir", "./raft_log");
    std::string snapshot_dir = config.getString("service.snapshot_dir", "./raft_snapshot");
    UInt32 start_time = config.getInt("service.snapshot_start_time", 7200);
    UInt32 end_time = config.getInt("service.snapshot_end_time", 79200);
    UInt32 create_interval = config.getInt("service.snapshot_create_interval", 3600 * 1);
    std::string host = config.getString("service.host", "localhost");
    std::string port = config.getString("service.internal_port", "2281");
    min_server_id = INT32_MAX;
    parseClusterConfig(config, "service.remote_servers");
    state_manager = cs_new<NuRaftStateManager>(server_id, host + ":" + port, log_dir, myself_cluster_config);

    state_machine = nuraft::cs_new<NuRaftStateMachine>(
        responses_queue_,
        coordination_settings,
        snapshot_dir,
        start_time,
        end_time,
        create_interval,
        coordination_settings->max_stored_snapshots,
        state_manager->load_log_store());
}

void SvsKeeperServer::startup(const Poco::Util::AbstractConfiguration & config)
{
    /*
    nuraft::nuraft_global_config g_config;
    g_config.num_commit_threads_ = 2;
    g_config.num_append_threads_ = 2;
    nuraft::nuraft_global_mgr::init(g_config);
    */

    nuraft::raft_params params;
    params.heart_beat_interval_ = coordination_settings->heart_beat_interval_ms.totalMilliseconds();
    params.election_timeout_lower_bound_ = coordination_settings->election_timeout_lower_bound_ms.totalMilliseconds();
    params.election_timeout_upper_bound_ = coordination_settings->election_timeout_upper_bound_ms.totalMilliseconds();
    params.reserved_log_items_ = coordination_settings->reserved_log_items;
    params.snapshot_distance_ = coordination_settings->snapshot_distance;
    params.client_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds();
    params.auto_forwarding_ = coordination_settings->auto_forwarding;
    //    params.auto_forwarding_req_timeout_ = coordination_settings->operation_timeout_ms.totalMilliseconds() * 2;

    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opts{};
    asio_opts.thread_pool_size_ = coordination_settings->nuraft_thread_size;
    nuraft::raft_server::init_options init_options;
    init_options.skip_initial_election_timeout_ = state_manager->shouldStartAsFollower();
    init_options.raft_callback_ = [this](nuraft::cb_func::Type type, nuraft::cb_func::Param * param) { return callbackFunc(type, param); };

    UInt16 port = config.getInt("service.internal_port", 2281);

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
    LOG_DEBUG(log, "Adding server {}, {} after 30s.", toString(srv_conf_to_add->get_id()), srv_conf_to_add->get_endpoint());
    std::this_thread::sleep_for(std::chrono::milliseconds(30000));
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
    state_machine->shutdown();
    if (state_manager->load_log_store() && !state_manager->load_log_store()->flush())
        LOG_WARNING(log, "Log store flush error while server shutdown.");
    //    state_manager->flushLogStore();
    if (!launcher.shutdown(coordination_settings->shutdown_timeout.totalSeconds()))
        LOG_WARNING(log, "Failed to shutdown RAFT server in {} seconds", 5);
}

namespace
{
#ifdef TEST_TCPHANDLER
#else
    nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, const Coordination::ZooKeeperRequestPtr & request)
    {
        DB::WriteBufferFromNuraftBuffer buf;
        DB::writeIntBinary(session_id, buf);
        request->write(buf);
        return buf.getBuffer();
    }
#endif
}

SvsKeeperStorage::ResponsesForSessions SvsKeeperServer::singleProcessReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
    ServiceProfileEvents::increment(ServiceProfileEvents::req_all, 1);
    ServiceProfileEvents::increment(ServiceProfileEvents::req_read, 1);
    return state_machine->singleProcessReadRequest(request_for_session);
}

void SvsKeeperServer::putRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
    ServiceProfileEvents::increment(ServiceProfileEvents::req_all, 1);
    auto [session_id, request] = request_for_session;
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
        ServiceProfileEvents::increment(ServiceProfileEvents::req_read, 1);
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        ServiceProfileEvents::increment(ServiceProfileEvents::req_write, 1);
        std::vector<ptr<buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, request));

        LOG_TRACE(
            log, "[putRequest]SessionID/xid #{}#{}, opnum {}, entries {}", session_id, request->xid, request->getOpNum(), entries.size());

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
                                                                                        : Coordination::Error::ZSYSTEMERROR;

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

bool SvsKeeperServer::isLeader() const
{
    return raft_instance->is_leader();
}

bool SvsKeeperServer::isLeaderAlive() const
{
    return raft_instance->is_leader_alive();
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
    int64_t timeout = coordination_settings->startup_timeout.totalMilliseconds();
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag.load(); }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::vector<int64_t> SvsKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

void SvsKeeperServer::parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    specified_cluster_config = cs_new<cluster_config>();
    myself_cluster_config = cs_new<cluster_config>();

    for (const auto & key : keys)
    {
        if (startsWith(key, "server"))
        {
            int id_ = config.getInt(config_name + "." + key + ".server_id");
            String host = config.getString(config_name + "." + key + ".host");
            String port = config.getString(config_name + "." + key + ".port", "5103");
            String endpoint_ = host + ":" + port;
            bool learner_ = config.getBool(config_name + "." + key + ".learner", false);
            int priority_ = config.getInt(config_name + "." + key + ".priority", 1);
            auto server_config = cs_new<srv_config>(id_, 0, endpoint_, "", learner_, priority_);
            specified_cluster_config->get_servers().push_back(server_config);
            min_server_id = std::min(id_, min_server_id);
            if (id_ == server_id)
                myself_cluster_config->get_servers().push_back(server_config);
        }
        else if (key == "async_replication")
        {
            specified_cluster_config->set_async_replication(config.getBool(config_name + "." + key, false));
        }
    }

    std::string s;
    std::for_each(specified_cluster_config->get_servers().cbegin(), specified_cluster_config->get_servers().cend(), [&s](ptr<srv_config> srv) {
        s += " ";
        s += srv->get_endpoint();
    });

    LOG_INFO(log, "specified raft cluster config : {}", s);
}

void SvsKeeperServer::reConfigIfNeed()
{
    if (min_server_id != server_id)
        return;
    /// diff specified_cluster_config and state_manager->get_cluster_config()

    auto cur_cluster_config = state_manager->get_cluster_config();

    std::vector<String> srvs_removed;
    std::vector<ptr<srv_config>> srvs_added;

    std::list<ptr<srv_config>> & new_srvs(specified_cluster_config->get_servers());
    std::list<ptr<srv_config>> & old_srvs(cur_cluster_config->get_servers());

    for (auto it = new_srvs.begin(); it != new_srvs.end(); ++it)
    {
        if (!cur_cluster_config->get_server((*it)->get_id()))
            srvs_added.push_back(*it);
    }

    for (auto it = old_srvs.begin(); it != old_srvs.end(); ++it)
    {
        if (!specified_cluster_config->get_server((*it)->get_id()))
            srvs_removed.push_back((*it)->get_endpoint());
    }

    for (auto & end_point : srvs_removed)
    {
        removeServer(end_point);
    }

    for (auto srv_add : srvs_added)
    {
        addServer(srv_add);
    }

}

}
