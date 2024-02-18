#pragma once

#include <Poco/Util/Application.h>

#include <loggers/Loggers.h>

#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>

namespace RK
{
class TestServer : public Poco::Util::Application, public Loggers
{
public:
    TestServer();
    ~TestServer() override;
    void init(int argc, char ** argv);
};

static const String LOG_DIR = "./test_raft_log";
[[maybe_unused]] static const String SNAP_DIR = "./test_raft_snapshot";

void cleanAll();
void cleanDirectory(const String & log_dir, bool remove_dir = true);

ptr<log_entry> createLogEntry(UInt64 term, const String & key, const String & data);
UInt64 appendEntry(ptr<LogSegmentStore> store, UInt64 term, String & key, String & data);

ptr<Coordination::ZooKeeperCreateRequest> getZookeeperCreateRequest(ptr<log_entry> log);
void setNode(KeeperStore & storage, const String & key, const String & value, bool is_ephemeral = false, int64_t session_id = 1);

void createZNodeLog(NuRaftStateMachine & machine, const String & key, const String & data, ptr<NuRaftFileLogStore> store, UInt64 term);
void createZNode(NuRaftStateMachine & machine, const String & key, const String & data);
void setZNode(NuRaftStateMachine & machine, const String & key, const String & data);
void removeZNode(NuRaftStateMachine & machine, const String & key);

}
