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

static const std::string LOG_DIR = "./test_raft_log";
static const std::string SNAP_DIR = "./test_raft_snapshot";

void cleanAll();
void cleanDirectory(const std::string & log_dir, bool remove_dir = true);

ptr<LogEntryPB> createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data);

void createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data, ptr<LogEntryPB> & entry_pb);

void createEntry(UInt64 term, LogOpTypePB op, std::string & key, std::string & data, std::vector<ptr<log_entry>> & entry_vec);

void createZNode(NuRaftStateMachine & machine, std::string & key, std::string & data);

void setNode(KeeperStore & storage, const std::string key, const std::string value, bool is_ephemeral = false, int64_t session_id = 1);

}
