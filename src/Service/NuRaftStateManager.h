#pragma once

#include <fstream>
#include <Service/NuRaftFileLogStore.h>
#include <Service/Settings.h>
#include <Service/Types.h>
#include <libnuraft/buffer.hxx>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/LayeredConfiguration.h>
#include "Common/StringUtils.h"
#include <common/logger_useful.h>

namespace RK
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
    NuRaftStateManager(int id, const Poco::Util::AbstractConfiguration & config_, SettingsPtr settings_);

    ~NuRaftStateManager() override = default;

    ptr<cluster_config> parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const String & config_name);

    ptr<cluster_config> load_config() override;

    void save_config(const cluster_config & config) override;

    void save_state(const srv_state & state) override;

    ptr<srv_state> read_state() override;

    ptr<log_store> load_log_store() override { return curr_log_store; }

    int32 server_id() override { return my_id; }

    void system_exit(const int exit_code) override;

    bool shouldStartAsFollower() const { return start_as_follower_servers.count(my_id); }

    //ptr<srv_config> get_srv_config() const { return curr_srv_config; }

    ptr<cluster_config> get_cluster_config() const { return cur_cluster_config; }

    /// Get configuration diff between proposed XML and current state in RAFT
    ConfigUpdateActions getConfigurationDiff(const Poco::Util::AbstractConfiguration & config);

protected:
    NuRaftStateManager() = default;

private:
    SettingsPtr settings;
    int my_id;

    String my_host;
    int32_t my_internal_port;

    std::unordered_set<int> start_as_follower_servers;

    String log_dir;
    ptr<log_store> curr_log_store;

    ptr<cluster_config> cur_cluster_config;

protected:
    Poco::Logger * log;
    String srv_state_file;
    String cluster_config_file;
};

}
