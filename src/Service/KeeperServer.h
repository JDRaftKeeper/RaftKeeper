/**
 * Copyright 2016-2021 ClickHouse, Inc.
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <unordered_map>
#include <Service/Keeper4LWInfo.h>
#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/NuRaftStateManager.h>
#include <Service/Settings.h>
#include <libnuraft/nuraft.hxx>

namespace RK
{
struct ServerInfo
{
    UInt32 server_id;
    std::string endpoint;
    bool is_leader;
};

class KeeperServer
{
private:
    int server_id;

    SettingsPtr settings;

    nuraft::ptr<NuRaftStateMachine> state_machine;

    nuraft::ptr<NuRaftStateManager> state_manager;

    nuraft::raft_launcher launcher;

    nuraft::ptr<nuraft::raft_server> raft_instance;

    std::mutex append_entries_mutex;

    const Poco::Util::AbstractConfiguration & config;

    SvsKeeperResponsesQueue & responses_queue;

    Poco::Logger * log;

    std::mutex initialized_mutex;
    std::atomic<bool> initialized_flag = false;
    std::condition_variable initialized_cv;
//    std::atomic<bool> initial_batch_committed = false;

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    nuraft::cb_func::ReturnCode callbackFunc(nuraft::cb_func::Type type, nuraft::cb_func::Param * param);

    void addServer(ptr<srv_config> srv_conf_to_add);

public:
    KeeperServer(
        const SettingsPtr & settings_,
        const Poco::Util::AbstractConfiguration & config_,
        SvsKeeperResponsesQueue & responses_queue_,
        std::shared_ptr<RequestProcessor> request_processor_ = nullptr);

    void startup();

    void addServer(const std::vector<std::string> & endpoint_list);

    void getServerList(std::vector<ServerInfo> & server_list);

    ptr<ForwardingConnection> getLeaderClient(size_t thread_idx);

    int32 getLeader();

    void removeServer(const std::string & endpoint);

    void putRequest(const KeeperStore::RequestForSession & request);

    ptr<nuraft::cmd_result<ptr<buffer>>> putRequestBatch(const std::vector<KeeperStore::RequestForSession> & request_batch);

    void processReadRequest(const KeeperStore::RequestForSession & request);

    int64_t getSessionID(int64_t session_timeout_ms);
    /// update session timeout
    /// @return whether success
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    std::vector<int64_t> getDeadSessions();

    void setSessionExpirationTime(int64_t session_id, int64_t expiration_time);

    int64_t getSessionTimeout(int64_t session_id);

    bool isLeader() const;

    bool isLeaderAlive() const;

    void waitInit();

    /// Return true if KeeperServer initialized
    bool checkInit() const
    {
        return initialized_flag;
    }

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

    /// Get configuration diff between current configuration in RAFT and in XML file
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config_);

    /// Apply action for configuration update. Actually call raft_instance->remove_srv or raft_instance->add_srv.
    /// Synchronously check for update results with retries.
    bool applyConfigurationUpdate(const ConfigUpdateAction & task);


    /// Wait configuration update for action. Used by followers.
    /// Return true if update was successfully received.
    bool waitConfigurationUpdate(const ConfigUpdateAction & task);

    /// Manually create snapshot.
    /// Return last committed log index.
    uint64_t createSnapshot();

    /// Raft log information
    KeeperLogInfo getKeeperLogInfo();

    bool requestLeader();
};

}
