#include <filesystem>
#include <Service/NuRaftInMemoryLogStore.h>
#include <Service/NuRaftStateManager.h>
#include <libnuraft/nuraft.hxx>

namespace fs = std::filesystem;

namespace DB
{
using namespace nuraft;

NuRaftStateManager::NuRaftStateManager(
    int id_,
    const std::string & endpoint_,
    const std::string & log_dir_,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_name)
    : my_server_id(id_), endpoint(endpoint_), log_dir(log_dir_)
{
    log = &(Poco::Logger::get("RaftStateManager"));
    curr_log_store = cs_new<NuRaftFileLogStore>(log_dir);

    srv_state_file = fs::path(log_dir) / "srv_state";
    parseClusterConfig(config, config_name + "." + "remote_servers");
}

ptr<cluster_config> NuRaftStateManager::load_config()
{
    return cur_cluster_config;
}

void NuRaftStateManager::save_config(const cluster_config & config)
{
    /// do nothing because cluster config info already in log store
    LOG_INFO(log, "save config with log index {}", config.get_log_idx());
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
        LOG_WARNING(log, "Raft srv_state file not exist");
        return cs_new<srv_state>();
    }
    in.seekg(0, std::ios::end);
    unsigned int size = in.tellg();

    in.seekg(0, std::ios::beg);
    char * data = new char[size];

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

void NuRaftStateManager::parseClusterConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    cur_cluster_config = cs_new<cluster_config>();

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
            cur_cluster_config->get_servers().push_back(cs_new<srv_config>(id_, 0, endpoint_, "", learner_, priority_));
        }
        else if (key == "async_replication")
        {
            cur_cluster_config->set_async_replication(config.getBool(config_name + "." + key, false));
        }
    }

    std::string s;
    std::for_each(cur_cluster_config->get_servers().cbegin(), cur_cluster_config->get_servers().cend(), [&s](ptr<srv_config> srv) {
        s += " ";
        s += srv->get_endpoint();
    });

    LOG_INFO(log, "raft cluster config : {}", s);
}

}
