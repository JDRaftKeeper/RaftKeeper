#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/proto/Log.pb.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <loggers/Loggers.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/argsToConfig.h>
#include <Service/RequestsCommitEvent.h>

using namespace nuraft;
using namespace DB;
using namespace Coordination;

namespace DB
{
void cleanAll() {
    Poco::File log(LOG_DIR);
    Poco::File snap(LOG_DIR);
    if (log.exists())
        log.remove(true);
    if (snap.exists())
        snap.remove(true);
}

void createZNodeLog(NuRaftStateMachine & machine, std::string & key, std::string & data, ptr<NuRaftFileLogStore> store, UInt64 term)
{
    //Poco::Logger * log = &(Poco::Logger::get("RaftStateMachine"));
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    SvsKeeperStorage::RequestForSession session_request;
    auto request = cs_new<ZooKeeperCreateRequest>();
    session_request.request = request;
    request->path = key;
    request->data = data;
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    //LOG_INFO(log, "index {}", index);
    if (store != nullptr)
    {
        //auto entry_pb = createEntryPB(term, 0, op, key, data);
        //ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
        ptr<log_entry> entry_log = cs_new<log_entry>(term, buf);
        store->append(entry_log);
    }

    machine.commit(index, *(buf.get()), true);
}

void createZNode(NuRaftStateMachine & machine, std::string & key, std::string & data)
{
    createZNodeLog(machine, key, data, nullptr, 0);
}
void setZNode(NuRaftStateMachine & machine, std::string & key, std::string & data)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    SvsKeeperStorage::RequestForSession session_request;
    auto request = cs_new<ZooKeeperSetRequest>();
    session_request.request = request;
    request->path = key;
    request->data = data;
    //request->is_ephemeral = false;
    //request->is_sequential = false;
    //request->acls = default_acls;
    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    machine.commit(index, *(buf.get()));
}

void removeZNode(NuRaftStateMachine & machine, std::string & key)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    SvsKeeperStorage::RequestForSession session_request;
    auto request = cs_new<ZooKeeperRemoveRequest>();
    session_request.request = request;
    request->path = key;
    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    machine.commit(index, *(buf.get()));
}

}

TEST(RaftStateMachine, createSnapshotTime)
{
    BackendTimer timer;
    timer.begin_second = 7200;
    timer.end_second = 79200;
    timer.interval = 1 * 3600;
    timer.randomWindow = 60; // 60 seconds

    //empty
    ASSERT_TRUE(timer.isActionTime("", 1614190200));

    //currtime 2021-02-24 3:01:01
    ASSERT_TRUE(timer.isActionTime("20210224020000", 1614106861));

    //currtime 2021-02-24 22:00:00
    ASSERT_TRUE(timer.isActionTime("20210224020000", 1614175200));

    //currtime 2021-02-24 2:59:59
    ASSERT_FALSE(timer.isActionTime("20210224020000", 1614106799));

    //currtime 2021-02-24 1:59:59
    ASSERT_FALSE(timer.isActionTime("20210224020000", 1614103199));

    //currtime 2021-02-24 22:00:01
    ASSERT_FALSE(timer.isActionTime("20210224020000", 1614175201));
}

TEST(RaftStateMachine, serializeAndParse)
{
    std::string snap_dir(SNAP_DIR + "/0");
    SvsKeeperResponsesQueue queue;
    SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();

    //NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);

    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    //UInt64 index = machine.last_commit_index() + 1;
    SvsKeeperStorage::RequestForSession session_request;
    session_request.session_id = 1;
    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "1";
    request->data = "a";
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    session_request.request = request;

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    SvsKeeperStorage::RequestForSession session_request_2 = NuRaftStateMachine::parseRequest(*(buf.get()));
    if (session_request_2.request->getOpNum() == OpNum::Create)
    {
        ZooKeeperCreateRequest * request_2 = static_cast<ZooKeeperCreateRequest *>(session_request_2.request.get());
        ASSERT_EQ(request_2->path, request->path);
        ASSERT_EQ(request_2->data, request->data);
    }
    
    //machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, appendEntry)
{
    std::string snap_dir(SNAP_DIR + "/1");
    cleanDirectory(snap_dir);

    SvsKeeperResponsesQueue queue;
    SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    RequestsCommitEvent wait_commits;
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, wait_commits);
    std::string key("/table1");
    std::string data("CREATE TABLE table1;");
    createZNode(machine, key, data);
    KeeperNode & node = machine.getNode(key);
    ASSERT_EQ(node.data, data);

    machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, modifyEntry)
{
    std::string snap_dir(SNAP_DIR + "/2");
    cleanDirectory(snap_dir);

    SvsKeeperResponsesQueue queue;
    SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    RequestsCommitEvent wait_commits;
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, wait_commits);
    std::string key("/table1");
    std::string data1("CREATE TABLE table1;");
    //LogOpTypePB op = OP_TYPE_CREATE;
    createZNode(machine, key, data1);
    KeeperNode & node1 = machine.getNode(key);
    ASSERT_EQ(node1.data, data1);

    std::string data2("CREATE TABLE table2;");
    //op = OP_TYPE_SET;
    setZNode(machine, key, data2);

    KeeperNode & node2 = machine.getNode(key);
    ASSERT_EQ(node2.data, data2);

    removeZNode(machine, key);
    removeZNode(machine, key);
    removeZNode(machine, key);
    KeeperNode & node3 = machine.getNode(key);
    ASSERT_TRUE(node3.data.empty());

    machine.shutdown();
    cleanDirectory(snap_dir);
}


TEST(RaftStateMachine, createSnapshot)
{
    auto log = &(Poco::Logger::get("Test_RaftStateMachine"));
    std::string snap_dir(SNAP_DIR + "/3");
    cleanDirectory(snap_dir);

    SvsKeeperResponsesQueue queue;
    SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    RequestsCommitEvent wait_commits;
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, wait_commits);
    LOG_INFO(log, "init last commit index {}", machine.last_commit_index());

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt32 last_index = 35;
    for (auto i = 0; i < last_index; i++)
    {
        std::string key = "/" + std::to_string(i + 1);
        std::string data = "table_" + key;
        createZNode(machine, key, data);
    }

    sleep(1);

    LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());

    ASSERT_EQ(machine.last_commit_index(), machine.getLastCommittedIndex());

    UInt64 term = 1;
    snapshot meta(last_index, term, config);
    machine.create_snapshot(meta);
    ASSERT_EQ(machine.getStorage().container.size(), 36);
    machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, syncSnapshot)
{
    std::string snap_dir_1(SNAP_DIR + "/4");
    std::string snap_dir_2(SNAP_DIR + "/5");
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);

    SvsKeeperResponsesQueue queue;
    SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    RequestsCommitEvent wait_commits;
    NuRaftStateMachine machine_source(queue, setting_ptr, snap_dir_1, 0, 3600, 10, 3, wait_commits);
    NuRaftStateMachine machine_target(queue, setting_ptr, snap_dir_2, 0, 3600, 10, 3, wait_commits);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt64 term = 1;
    UInt32 last_index = 1024;
    for (auto i = 0; i < last_index; i++)
    {
        std::string key = "/" + std::to_string(i + 1);
        std::string data = "table_" + key;
        //LogOpTypePB op = OP_TYPE_CREATE;
        createZNode(machine_source, key, data);
    }
    snapshot meta(last_index, term, config);
    machine_source.create_snapshot(meta);

    ptr<buffer> data_out;
    void * user_snp_ctx;
    bool is_last_obj = false;
    ulong obj_id = 0;
    while (!is_last_obj)
    {
        machine_source.read_logical_snp_obj(meta, user_snp_ctx, obj_id, data_out, is_last_obj);
        bool is_first = (obj_id == 0);
        machine_target.save_logical_snp_obj(meta, obj_id, *(data_out.get()), is_first, is_last_obj);
    }
    machine_target.apply_snapshot(meta);
    ASSERT_EQ(machine_target.getStorage().container.size(), last_index + 1);

    for (auto i = 1; i < obj_id; i++)
    {
        ASSERT_TRUE(machine_target.exist_snapshot_object(meta, i));
    }

    machine_source.shutdown();
    machine_target.shutdown();
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);
}

TEST(RaftStateMachine, initStateMachine)
{
    auto *log = &(Poco::Logger::get("Test_RaftStateMachine"));
    std::string snap_dir(SNAP_DIR + "/6");
    std::string log_dir(LOG_DIR + "/6");

    cleanDirectory(snap_dir, true);
    cleanDirectory(log_dir, true);

    cleanAll();

    //Create
    {
        SvsKeeperResponsesQueue queue;
        SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);
        RequestsCommitEvent wait_commits;
        NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, wait_commits, log_store);

        ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
        UInt32 last_index = 128;
        UInt64 term = 1;

        for (auto i = 0; i < last_index; i++)
        {
            UInt32 index = i + 1;
            std::string key = "/" + std::to_string(index);
            std::string data = "table_" + key;
            createZNodeLog(machine, key, data, log_store, term);
        }
        sleep(1);
        LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());
        ASSERT_EQ(machine.last_commit_index(), machine.getLastCommittedIndex());
        snapshot meta(last_index, term, config);
        machine.create_snapshot(meta);

        for (auto i = 0; i < last_index; i++)
        {
            UInt32 index = last_index + i + 1;
            std::string key = "/" + std::to_string(index);
            std::string data = "table_" + key;
            createZNodeLog(machine, key, data, log_store, term);
        }
        sleep(1);
        LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());


        ASSERT_EQ(machine.getStorage().container.size(), 257);
        machine.shutdown();
    }

    // Load
    {
        SvsKeeperResponsesQueue queue;
        SvsKeeperSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);
        RequestsCommitEvent wait_commits;
        NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, wait_commits, log_store);
        LOG_INFO(log, "init last commit index {}", machine.last_commit_index());
        ASSERT_EQ(machine.last_commit_index(), 256);
        machine.shutdown();
    }

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

