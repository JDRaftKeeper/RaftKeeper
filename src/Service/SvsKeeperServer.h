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

    SvsKeeperSettingsPtr coordination_settings;

    nuraft::ptr<NuRaftStateMachine> state_machine;

    nuraft::ptr<NuRaftStateManager> state_manager;

    nuraft::raft_launcher launcher;

    nuraft::ptr<nuraft::raft_server> raft_instance;

    std::mutex append_entries_mutex;

    SvsKeeperResponsesQueue & responses_queue;

    Poco::Logger * log;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
//    std::atomic<bool> initial_batch_committed = false;

    ptr<cluster_config> specified_cluster_config;

    ptr<cluster_config> myself_cluster_config;

    int min_server_id;

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    void parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    void addServer(ptr<srv_config> srv_conf_to_add);

public:
    SvsKeeperServer(
        const KeeperConfigurationAndSettingsPtr & coordination_settings_,
        const Poco::Util::AbstractConfiguration & config,
        SvsKeeperResponsesQueue & responses_queue_);

    void startup(const Poco::Util::AbstractConfiguration & config);

    void addServer(const std::vector<std::string> & endpoint_list);

    void getServerList(std::vector<Server> & server_list);

    void removeServer(const std::string & endpoint);

    SvsKeeperStorage::ResponsesForSessions singleProcessReadRequest(const SvsKeeperStorage::RequestForSession & request_for_session);

    void putRequest(const SvsKeeperStorage::RequestForSession & request);

    int64_t getSessionID(int64_t session_timeout_ms);
    /// update session timeout
    /// @return whether session is expired when updating
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    std::vector<int64_t> getDeadSessions();

    bool isLeader() const;

    bool isLeaderAlive() const;

    void waitInit();

    void reConfigIfNeed();

    void shutdown();

    nuraft::ptr<NuRaftStateMachine> getKeeperStateMachine() const
    {
        return state_machine;
    }


    bool isFollower() const;

    bool isObserver() const;

    /// @return follower count if node is not leader return 0
    uint64_t getFollowerCount() const;

    /// @return synced follower count if node is not leader return 0
    uint64_t getSyncedFollowerCount() const;

};

}
