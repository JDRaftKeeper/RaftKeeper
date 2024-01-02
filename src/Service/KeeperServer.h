#pragma once

#include <unordered_map>
#include <Service/Keeper4LWInfo.h>
#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/Settings.h>
#include <Service/Types.h>
#include <libnuraft/nuraft.hxx>

namespace RK
{

/**
 * Encapsulate NuRaft launcher and NuRaft server,
 * and provides interfaces to interact with NuRaft and state machine.
 */
class KeeperServer
{
private:
    /// server id configured in config.xml
    uint32_t server_id;

    /// configurations
    SettingsPtr settings;

    /// NuRaft state machine who hold all data and sessions.
    nuraft::ptr<NuRaftStateMachine> state_machine;

    /// NuRaft cluster state manager.
    nuraft::ptr<NuRaftStateManager> state_manager;

    /// NuRaft cluster launcher.
    nuraft::raft_launcher launcher;

    /// NuRaft server which provides interface to interact with NuRaft.
    nuraft::ptr<nuraft::raft_server> raft_instance;

    /// configurations
    const Poco::Util::AbstractConfiguration & config;

    Poco::Logger * log;

    /// initialized flag
    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    using UpdateForwardListener = std::function<void()>;
    std::mutex forward_listener_mutex;
    UpdateForwardListener update_forward_listener;

    /// Will registered to NuRaft and wait BecomeFresh for follower and
    /// BecomeLeader for leader events, which means itself joins cluster.
    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

public:
    KeeperServer(
        const SettingsPtr & settings_,
        const Poco::Util::AbstractConfiguration & config_,
        KeeperResponsesQueue & responses_queue_,
        std::shared_ptr<RequestProcessor> request_processor_ = nullptr);

    /// Put write request into queue and keeper server will append it to NuRaft asynchronously.
    ptr<nuraft::cmd_result<ptr<buffer>>> putRequestBatch(const std::vector<RequestForSession> & request_batch);

    /// Allocate a new session id.
    int64_t newSession(int64_t session_timeout_ms);

    /// Update session timeout, used when client reconnected
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    /// Get expired sessions, used in clear session task.
    std::vector<int64_t> getDeadSessions();

    /// When leader receive session expiration infos from others,
    /// it will update the snapshot itself.
    void handleRemoteSession(int64_t session_id, int64_t expiration_time);

    /// Get the initialized timeout for a session.
    /// Return initialized timeout, or -1 if session not exist.
    [[maybe_unused]] int64_t getSessionTimeout(int64_t session_id);

    /// will invoke waitInit
    void startup();

    /// Wait NuRaft cluster initialized, when success,
    /// data is loaded and Raft quorum reached in majority of cluster.
    void waitInit();

    /// Return true if KeeperServer initialized
    bool checkInit() const { return initialized_flag; }

    /// Shutdown the sever, will recycle resources such as flush files
    /// and wait background thread terminate.
    void shutdown();

    /// return keeper state machine
    nuraft::ptr<NuRaftStateMachine> getKeeperStateMachine() const { return state_machine; }

    /// get current leader
    int32_t getLeader();

    /// Whether is leader, just invoke API from NuRaft.
    bool isLeader() const;

    /// Whether leader is alive, just invoke API from NuRaft.
    bool isLeaderAlive() const;

    bool isFollower() const;

    /// observer node who does not participate in leader selection and data replication quorum
    bool isObserver() const;

    /// @return follower count if node is not leader return 0
    uint64_t getFollowerCount() const;

    /// @return synced follower count if node is not leader return 0
    uint64_t getSyncedFollowerCount() const;

    /// Get configuration diff between current configuration in RAFT and in XML file.
    /// Notice that we only concern the cluster related configurations.
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_);

    /// Apply action for configuration update. Actually call raft_instance->remove_srv
    /// or raft_instance->add_srv.
    bool applyConfigurationUpdate(const ConfigUpdateAction & task);

    /// Wait configuration update for action. Used by followers.
    /// Return true if update was successfully received.
    bool waitConfigurationUpdate(const ConfigUpdateAction & task);

    /// Manually create snapshot. Return last committed log index.
    uint64_t createSnapshot();

    /// Return NuRaft log related information.
    KeeperLogInfo getKeeperLogInfo();

    /// Send request to become leader. Return true if scheduled task, or false.
    bool requestLeader();

    void registerForWardListener(UpdateForwardListener forward_listener);

    uint32_t myId() const
    {
        return server_id;
    }
};

}
