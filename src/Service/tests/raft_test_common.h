#pragma once

#include <Service/NuRaftStateMachine.h>
#include <Service/SvsKeeperStorage.h>
#include <loggers/Loggers.h>
#include <Poco/Util/Application.h>

namespace DB
{
//#define _RAFT_UNIT_TEST_

class TestServer : public Poco::Util::Application, public Loggers
{
public:
    TestServer();
    ~TestServer() override;
    void init(int argc, char ** argv);
};

static const std::string LOG_DIR = "./test_raft_log";
static const std::string SNAP_DIR = "./test_raft_snapshot";

void cleanDirectory(const std::string & log_dir, bool remove_dir = true);

ptr<LogEntryPB> createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data);

void createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data, ptr<LogEntryPB> & entry_pb);

void createEntry(UInt64 term, LogOpTypePB op, std::string & key, std::string & data, std::vector<ptr<log_entry>> & entry_vec);

void createZNode(NuRaftStateMachine & machine, std::string & key, std::string & data);

void setNode(SvsKeeperStorage & storage, const std::string key, const std::string value);

}
