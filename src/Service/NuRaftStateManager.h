#pragma once

#include <fstream>
#include <Service/NuRaftFileLogStore.h>
#include <libnuraft/buffer.hxx>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>
#include <Service/SvsKeeperSettings.h>
#include <Service/ForwardingClient.h>

namespace DB
{
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::srv_config;
using nuraft::srv_state;

using KeeperServerConfigPtr = nuraft::ptr<nuraft::srv_config>;

/// When our configuration changes the following action types
/// can happen
enum class ConfigUpdateActionType
{
    RemoveServer,
    AddServer,
    UpdatePriority,
};

/// Action to update configuration
struct ConfigUpdateAction
{
    ConfigUpdateActionType action_type;
    KeeperServerConfigPtr server;
};

using ConfigUpdateActions = std::vector<ConfigUpdateAction>;

class NuRaftStateManager : public nuraft::state_mgr
{
public:
    NuRaftStateManager(
        int id,
        const std::string & endpoint,
        const std::string & log_dir,
        const Poco::Util::AbstractConfiguration & config,
        KeeperConfigurationAndSettingsPtr coordination_settings_);

    ~NuRaftStateManager() override = default;

    ptr<cluster_config> parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_name, size_t thread_count) const;

    ptr<cluster_config> load_config() override;

    void save_config(const cluster_config & config) override;

    void save_state(const srv_state & state) override;

    ptr<srv_state> read_state() override;

    ptr<log_store> load_log_store() override { return curr_log_store; }

    int32 server_id() override { return my_server_id; }

    void system_exit(const int exit_code) override;

    bool shouldStartAsFollower() const { return start_as_follower_servers.count(my_server_id); }

    //ptr<srv_config> get_srv_config() const { return curr_srv_config; }

    ptr<cluster_config> get_cluster_config() const { return cur_cluster_config; }

    /// Get configuration diff between proposed XML and current state in RAFT
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const;

    ptr<ForwardingClient> getClient(int32 id, size_t thread_idx);

protected:
    NuRaftStateManager() { }

private:
    KeeperConfigurationAndSettingsPtr coordination_settings;

    int my_server_id;
    std::string endpoint;

    std::unordered_set<int> start_as_follower_servers;

    std::string log_dir;
    ptr<log_store> curr_log_store;

    ptr<cluster_config> cur_cluster_config;

    mutable std::mutex clients_mutex;
    mutable std::unordered_map<UInt32, std::vector<ptr<ForwardingClient>>> clients;

protected:
    Poco::Logger * log;
    std::string srv_state_file;
    std::string cluster_config_file;
};

}
