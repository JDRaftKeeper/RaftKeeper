#include <filesystem>
#include <IO/ReadBufferFromFile.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromFile.h>
#include <Service/NuRaftInMemoryLogStore.h>
#include <Service/NuRaftStateManager.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>

namespace fs = std::filesystem;

namespace DB
{
using namespace nuraft;

NuRaftStateManager::NuRaftStateManager(
    int id_,
    const std::string & endpoint_,
    const std::string & log_dir_,
    ptr<cluster_config> myself_cluster_config_,
    bool force_sync)
    : my_server_id(id_), endpoint(endpoint_), log_dir(log_dir_), cur_cluster_config(myself_cluster_config_)
{
    log = &(Poco::Logger::get("RaftStateManager"));
    curr_log_store = cs_new<NuRaftFileLogStore>(log_dir, false , force_sync);

    srv_state_file = fs::path(log_dir) / "srv_state";
    cluster_config_file = fs::path(log_dir) / "cluster_config";
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
    std::unique_ptr<WriteBufferFromFile> out_file_buf = std::make_unique<WriteBufferFromFile>(cluster_config_file, 4096, O_WRONLY | O_TRUNC | O_CREAT);
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
        LOG_WARNING(log, "Raft srv_state file not exist");
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
