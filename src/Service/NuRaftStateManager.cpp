#include <filesystem>
#include <Common/IO/ReadBufferFromFile.h>
#include <Common/IO/VarInt.h>
#include <Common/IO/WriteBufferFromFile.h>
#include <Service/NuRaftStateManager.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>

namespace fs = std::filesystem;

namespace RK
{
using namespace nuraft;

NuRaftStateManager::NuRaftStateManager(
    int id_,
    const Poco::Util::AbstractConfiguration & config_,
    SettingsPtr settings_)
    : settings(settings_), my_id(id_), my_host(settings_->host), my_internal_port(settings_->internal_port), log_dir(settings_->log_dir)
{
    log = &(Poco::Logger::get("NuRaftStateManager"));
    curr_log_store = cs_new<NuRaftFileLogStore>(
        log_dir, false, settings->raft_settings->log_fsync_mode, settings->raft_settings->log_fsync_interval);

    srv_state_file = fs::path(log_dir) / "srv_state";
    cluster_config_file = fs::path(log_dir) / "cluster_config";
    cur_cluster_config = parseClusterConfig(config_, "keeper.cluster");
}

ptr<cluster_config> NuRaftStateManager::load_config()
{
    if (!Poco::File(cluster_config_file).exists())
    {
        LOG_INFO(log, "load config with initial cluster config.");
        return cur_cluster_config;
    }

    std::unique_ptr<ReadBufferFromFile> read_file_buf = std::make_unique<ReadBufferFromFile>(cluster_config_file, 4096);
    size_t size;
    readVarUInt(size, *read_file_buf);
    ptr<nuraft::buffer> buf = nuraft::buffer::alloc(size);
    read_file_buf->readStrict(reinterpret_cast<char *>(buf->data()), size);
    cur_cluster_config = nuraft::cluster_config::deserialize(*buf);
    LOG_INFO(log, "load config with log index {}", cur_cluster_config->get_log_idx());
    return cur_cluster_config;
}

void NuRaftStateManager::save_config(const cluster_config & config)
{
    std::unique_ptr<WriteBufferFromFile> out_file_buf
        = std::make_unique<WriteBufferFromFile>(cluster_config_file, 4096, O_WRONLY | O_TRUNC | O_CREAT);
    nuraft::ptr<nuraft::buffer> data = config.serialize();
    writeVarUInt(data->size(), *out_file_buf);
    out_file_buf->write(reinterpret_cast<char *>(data->data()), data->size());
    out_file_buf->finalize();
    out_file_buf->sync();
    LOG_INFO(log, "save config with log index {}", config.get_log_idx());
    cur_cluster_config = cluster_config::deserialize(*data);
}

void NuRaftStateManager::save_state(const srv_state & state)
{
    std::ofstream out(srv_state_file, std::ios::binary | std::ios::trunc);

    ptr<buffer> data = state.serialize();
    out.write(reinterpret_cast<char *>(data->data()), data->size());
    out.close();

    LOG_INFO(log, "save srv_state with term {} and vote_for {}", state.get_term(), state.get_voted_for());
}

ptr<srv_state> NuRaftStateManager::read_state()
{
    std::ifstream in(srv_state_file, std::ios::binary);

    if (!in)
    {
        LOG_WARNING(log, "Raft srv_state file not exist, maybe this is the first startup.");
        return cs_new<srv_state>();
    }
    in.seekg(0, std::ios::end);
    unsigned int size = in.tellg();

    in.seekg(0, std::ios::beg);
    char data[size];

    in.read(data, size);
    in.close();

    ptr<nuraft::buffer> buf = nuraft::buffer::alloc(size);
    buf->put_raw(reinterpret_cast<const byte *>(data), size);

    ptr<srv_state> ret = srv_state::deserialize(*buf.get());
    LOG_INFO(log, "load srv_state: term {}, vote_for {} from disk", ret->get_term(), ret->get_voted_for());

    return ret;
}

void NuRaftStateManager::system_exit(const int exit_code)
{
    LOG_ERROR(log, "Raft system exit with code {}", exit_code);
}

ptr<cluster_config> NuRaftStateManager::parseClusterConfig(
    const Poco::Util::AbstractConfiguration & config, const std::string & config_name) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    auto ret_cluster_config = cs_new<cluster_config>();

    for (const auto & key : keys)
    {
        if (startsWith(key, "server"))
        {
            int id = config.getInt(config_name + "." + key + ".id");
            String host = config.getString(config_name + "." + key + ".host");
            String internal_port = config.getString(config_name + "." + key + ".internal_port", "8103");
            String endpoint = host + ":" + internal_port;
            bool learner = config.getBool(config_name + "." + key + ".learner", false);
            int priority = config.getInt(config_name + "." + key + ".priority", 1);
            ret_cluster_config->get_servers().push_back(cs_new<srv_config>(id, 0, endpoint, "", learner, priority));
        }
    }

    /// If user does not configure cluster, put myself to NuRaft.
    if (ret_cluster_config->get_servers().empty())
    {
        auto my_endpoint = my_host + ":" + std::to_string(my_internal_port);
        ret_cluster_config->get_servers().push_back(cs_new<srv_config>(my_id, 0, my_endpoint, "", false, 1));
    }

    std::string s;
    std::for_each(ret_cluster_config->get_servers().cbegin(), ret_cluster_config->get_servers().cend(), [&s](ptr<srv_config> srv)
    {
        s += " ";
        s += srv->get_endpoint();
    });

    LOG_INFO(log, "raft cluster config : {}", s);
    return ret_cluster_config;
}

ConfigUpdateActions NuRaftStateManager::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const
{
    auto new_cluster_config = parseClusterConfig(config, "keeper.cluster");

    std::unordered_map<int, KeeperServerConfigPtr> new_ids, old_ids;
    for (const auto & new_server : new_cluster_config->get_servers())
        new_ids[new_server->get_id()] = new_server;

    {
        for (const auto & old_server : cur_cluster_config->get_servers())
            old_ids[old_server->get_id()] = old_server;
    }

    ConfigUpdateActions result;

    /// First that remove old ones
    for (auto [old_id, server_config] : old_ids)
    {
        if (!new_ids.count(old_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::RemoveServer, server_config});
    }

    /// After of all add new servers
    for (auto [new_id, server_config] : new_ids)
    {
        if (!old_ids.count(new_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::AddServer, server_config});
    }

    {
        /// And update priority if required
        for (const auto & old_server : cur_cluster_config->get_servers())
        {
            for (const auto & new_server : new_cluster_config->get_servers())
            {
                if (old_server->get_id() == new_server->get_id())
                {
                    if (old_server->get_priority() != new_server->get_priority())
                    {
                        result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::UpdatePriority, new_server});
                    }
                    break;
                }
            }
        }
    }

    return result;
}

}
