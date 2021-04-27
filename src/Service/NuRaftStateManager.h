#pragma once

#include <fstream>
#include <Service/NuRaftFileLogStore.h>
#include <libnuraft/buffer.hxx>
#include <libnuraft/nuraft.hxx>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/StringUtils/StringUtils.h>
#include <common/logger_useful.h>

namespace DB
{
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::log_store;
using nuraft::ptr;
using nuraft::srv_config;
using nuraft::srv_state;


class NuRaftStateManager : public nuraft::state_mgr
{
public:
    NuRaftStateManager(
        int id,
        const std::string & endpoint,
        const std::string & log_dir,
        const Poco::Util::AbstractConfiguration & config_,
        const String & config_name_);

    ~NuRaftStateManager() override = default;

    void parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

    ptr<cluster_config> load_config() override;

    void save_config(const cluster_config & config) override;

    void save_state(const srv_state & state) override;

    ptr<srv_state> read_state() override;

    ptr<log_store> load_log_store() override { return curr_log_store; }

    int32 server_id() override { return my_server_id; }

    void system_exit(const int exit_code) override;

    bool shouldStartAsFollower() const { return start_as_follower_servers.count(my_server_id); }

    //ptr<srv_config> get_srv_config() const { return curr_srv_config; }

protected:
    NuRaftStateManager() { }

private:
    int my_server_id;
    std::string endpoint;

    ptr<cluster_config> cur_cluster_config;
    std::unordered_set<int> start_as_follower_servers;

    std::string log_dir;
    ptr<log_store> curr_log_store;

protected:
    Poco::Logger * log;
    std::string srv_state_file;
};

}
