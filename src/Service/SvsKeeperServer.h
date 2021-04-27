#pragma once

#include <unordered_map>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/SvsKeeperSettings.h>
#include <Service/SvsKeeperStorage.h>
#include <libnuraft/nuraft.hxx>

namespace DB
{
struct Server
{
    UInt32 server_id;
    std::string endpoint;
    bool is_leader;
};

class SvsKeeperServer
{
private:
    int server_id;

    CoordinationSettingsPtr coordination_settings;

    nuraft::ptr<NuRaftStateMachine> state_machine;

    nuraft::ptr<NuRaftStateManager> state_manager;

    nuraft::raft_launcher launcher;

    nuraft::ptr<nuraft::raft_server> raft_instance;

    std::mutex append_entries_mutex;

    SvsKeeperResponsesQueue & responses_queue;

    Poco::Logger * log;

    std::mutex initialized_mutex;
    bool initialized_flag = false;
    std::condition_variable initialized_cv;

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

public:
    SvsKeeperServer(
        int server_id_,
        const CoordinationSettingsPtr & coordination_settings_,
        const Poco::Util::AbstractConfiguration & config,
        SvsKeeperResponsesQueue & responses_queue_);

    void startup(const Poco::Util::AbstractConfiguration & config);

    void addServer(const std::vector<std::string> & endpoint_list);

    void getServerList(std::vector<Server> & server_list);

    void removeServer(const std::string & endpoint);

    void putRequest(const SvsKeeperStorage::RequestForSession & request);

    int64_t getSessionID(int64_t session_timeout_ms);

    std::unordered_set<int64_t> getDeadSessions();

    bool isLeader() const;

    bool isLeaderAlive() const;

    void waitInit();

    void shutdown();

    UInt64 getNodeNum()
    {
        return state_machine->getNodeNum();
    }

    UInt64 getNodeSizeMB()
    {
        return state_machine->getNodeSizeMB();
    }

    /// no need to lock
    UInt64 getSessionNum()
    {
        return state_machine->getSessionNum();
    }
};

}
