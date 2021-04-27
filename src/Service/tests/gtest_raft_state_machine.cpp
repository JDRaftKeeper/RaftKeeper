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

using namespace nuraft;
using namespace DB;
using namespace Coordination;

namespace DB
{
void createZNode(NuRaftStateMachine & machine, std::string & key, std::string & data)
{
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
    //Poco::Logger * log = &(Poco::Logger::get("RaftStateMachine"));
    //LOG_INFO(log, "Path {}, data {}", request->path, request->data);
    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    machine.commit(index, *(buf.get()));
}

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

TEST(RaftStateMachine, createSnapshotTime)
{
    BackendTimer timer;
    timer.begin_second = 7200;
    timer.interval = 24 * 3600;
    timer.window = 600;
    //currtime 2021-02-25 2:10:00
    ASSERT_TRUE(timer.isActionTime("20210224020000", 1614190200));
    //currtime 2021-02-25 2:00:00
    ASSERT_TRUE(timer.isActionTime("20210224021000", 1614189600));

    //currtime 2021-02-25 1:59:59
    ASSERT_FALSE(timer.isActionTime("20210224021000", 1614189599));

    //currtime 2021-02-25 2:09:59
    ASSERT_FALSE(timer.isActionTime("20210224022000", 1614190199));

    //currtime 2021-02-25 2:10:01
    ASSERT_FALSE(timer.isActionTime("20210224020000", 1614190201));
}

TEST(RaftStateMachine, serializeAndParse)
{
    std::string snap_dir(SNAP_DIR + "/0");
    ResponsesQueue queue;
    CoordinationSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);

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
}

TEST(RaftStateMachine, appendEntry)
{
    std::string snap_dir(SNAP_DIR + "/1");
    ResponsesQueue queue;
    CoordinationSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);
    cleanDirectory(snap_dir);
    std::string key("/table1");
    std::string data("CREATE TABLE table1;");
    createZNode(machine, key, data);
    KeeperNode & node = machine.getNode(key);
    ASSERT_EQ(node.data, data);
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, modifyEntry)
{
    std::string snap_dir(SNAP_DIR + "/2");
    ResponsesQueue queue;
    CoordinationSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);
    cleanDirectory(snap_dir);
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
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, createSnapshot)
{
    std::string snap_dir(SNAP_DIR + "/3");
    ResponsesQueue queue;
    CoordinationSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);
    cleanDirectory(snap_dir);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt32 last_index = 35;
    for (auto i = 0; i < last_index; i++)
    {
        std::string key = "/" + std::to_string(i + 1);
        std::string data = "table_" + key;
        createZNode(machine, key, data);
    }
    UInt64 term = 1;
    snapshot meta(last_index, term, config);
    machine.create_snapshot(meta);
    ASSERT_EQ(machine.getStorage().container.size(), 36);
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, syncSnapshot)
{
    std::string snap_dir_1(SNAP_DIR + "/4");
    std::string snap_dir_2(SNAP_DIR + "/5");
    ResponsesQueue queue;
    CoordinationSettingsPtr setting_ptr = cs_new<SvsKeeperSettings>();
    NuRaftStateMachine machine_source(queue, setting_ptr, snap_dir_1, 0, 3600, 10, 3);
    NuRaftStateMachine machine_target(queue, setting_ptr, snap_dir_2, 0, 3600, 10, 3);
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);
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
    //cleanDirectory(snap_dir_1);
    //cleanDirectory(snap_dir_2);
}
