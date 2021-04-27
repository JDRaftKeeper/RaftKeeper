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
#include <Common/ZooKeeper/ZooKeeperIO.h>

#ifndef TEST_TCPHANDLER
//#define TEST_TCPHANDLER
#endif


namespace DB
{
namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

SvsKeeperServer::SvsKeeperServer(
    int server_id_,
    const CoordinationSettingsPtr & coordination_settings_,
    const Poco::Util::AbstractConfiguration & config,
    ResponsesQueue & responses_queue_)
    : server_id(server_id_)
    , coordination_settings(coordination_settings_)
    , responses_queue(responses_queue_)
    , log(&(Poco::Logger::get("RaftKeeperServer")))
{
    std::string log_dir = config.getString("service.log_dir", "./raft_log");
    std::string snapshot_dir = config.getString("service.snapshot_dir", "./raft_snapshot");
    //UInt32 begin_second = 3600;
    //UInt32 create_interval = 3600 * 8;
    UInt32 start_time = config.getInt("service.snapshot_start_time", 3600);
    UInt32 create_interval = config.getInt("service.snapshot_create_interval", 3600 * 24);
    state_machine = nuraft::cs_new<NuRaftStateMachine>(responses_queue_, coordination_settings, snapshot_dir, start_time, create_interval);
    std::string host = config.getString("service.host", "localhost");
    std::string port = config.getString("service.internal_port", "2281");
    state_manager = cs_new<NuRaftStateManager>(server_id, host + ":" + port, log_dir, config, "service");
}

void SvsKeeperServer::startup(const Poco::Util::AbstractConfiguration & config)
{
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
    if (!ret->get_accepted())
    {
        LOG_ERROR(log, "Failed to add server: {}", ret->get_result_code());
        return;
    }

    // Wait until it appears in server list.
    const size_t MAX_TRY = 40;
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
    //    LOG_DEBUG(log, "Async request is in progress (check with `list` command)");
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
    for (auto it = server_list.begin(); it != server_list.end(); it++)
    {
        if (it->endpoint == endpoint)
        {
            raft_instance->remove_srv(it->server_id);
            break;
        }
    }
}


void SvsKeeperServer::shutdown()
{
    state_machine->shutdownStorage();
//    state_manager->flushLogStore();
    if (!launcher.shutdown(coordination_settings->shutdown_timeout.totalSeconds()))
        LOG_WARNING(&Poco::Logger::get("NuKeeperServer"), "Failed to shutdown RAFT server in {} seconds", 5);
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

void SvsKeeperServer::putRequest(const SvsKeeperStorage::RequestForSession & request_for_session)
{
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
        state_machine->processReadRequest(request_for_session);
    }
    else
    {
        std::vector<nuraft::ptr<nuraft::buffer>> entries;
        entries.push_back(getZooKeeperLogEntry(session_id, request));

        //std::lock_guard lock(append_entries_mutex);

        LOG_DEBUG(
            log, "[putRequest]SessionID/xid #{}#{}, opnum {}, entries {}", session_id, request->xid, request->getOpNum(), entries.size());

        auto result = raft_instance->append_entries(entries);

        if(result->get_accepted() && result->get_result_code() == nuraft::cmd_result_code::OK)
        {
            /// response pushed into queue by state machine
            return;
        }

        SvsKeeperStorage::ResponsesForSessions responses;
        auto response = request->makeResponse();

        response->xid = request->xid;
        response->zxid = 0;

        response->error = result->get_result_code() == nuraft::cmd_result_code::TIMEOUT ?
                          Coordination::Error::ZOPERATIONTIMEOUT : Coordination::Error::ZSYSTEMERROR;

        responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{session_id, response});
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
    auto entry = nuraft::buffer::alloc(sizeof(int64_t));
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
    if (!initialized_cv.wait_for(lock, std::chrono::milliseconds(timeout), [&] { return initialized_flag; }))
        throw Exception(ErrorCodes::RAFT_ERROR, "Failed to wait RAFT initialization");
}

std::unordered_set<int64_t> SvsKeeperServer::getDeadSessions()
{
    return state_machine->getDeadSessions();
}

}
