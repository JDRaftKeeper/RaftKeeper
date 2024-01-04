#pragma once

#include <Service/KeeperStore.h>
#include <Service/NuRaftStateMachine.h>
#include <loggers/Loggers.h>
#include <Poco/Util/Application.h>

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
static const String SNAP_DIR = "./test_raft_snapshot";

void cleanAll();
void cleanDirectory(const String & log_dir, bool remove_dir = true);

ptr<LogEntryPB> createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, String & key, String & data);

void createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, String & key, String & data, ptr<LogEntryPB> & entry_pb);

void createEntry(UInt64 term, LogOpTypePB op, String & key, String & data, std::vector<ptr<log_entry>> & entry_vec);

void createZNode(NuRaftStateMachine & machine, String & key, String & data);

void setNode(KeeperStore & storage, const String key, const String value, bool is_ephemeral = false, int64_t session_id = 1);

}
