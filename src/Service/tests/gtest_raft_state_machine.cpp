#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/KeeperCommon.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>
#include <Poco/Logger.h>

using namespace nuraft;
using namespace RK;
using namespace Coordination;

TEST(RaftStateMachine, serializeAndParse)
{
    String snap_dir(SNAP_DIR + "/0");
    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    //NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3);

    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    //UInt64 index = machine.last_commit_index() + 1;
    RequestForSession session_request;
    session_request.session_id = 1;
    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "1";
    request->data = "a";
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    session_request.request = request;

    using namespace std::chrono;
    session_request.create_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(session_request);
    RequestForSession session_request_2 = NuRaftStateMachine::parseRequest(*(buf.get()));
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
    String snap_dir(SNAP_DIR + "/1");
    cleanDirectory(snap_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data("CREATE TABLE table1;");
    createZNode(machine, key, data);
    KeeperNode & node = machine.getNode(key);
    ASSERT_EQ(node.data, data);

    machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, modifyEntry)
{
    String snap_dir(SNAP_DIR + "/2");
    cleanDirectory(snap_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data1("CREATE TABLE table1;");
    //LogOpTypePB op = OP_TYPE_CREATE;
    createZNode(machine, key, data1);
    KeeperNode & node1 = machine.getNode(key);
    ASSERT_EQ(node1.data, data1);

    String data2("CREATE TABLE table2;");
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
    auto *log = &(Poco::Logger::get("Test_RaftStateMachine"));
    String snap_dir(SNAP_DIR + "/3");
    cleanDirectory(snap_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    LOG_INFO(log, "init last commit index {}", machine.last_commit_index());

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt32 last_index = 35;
    for (auto i = 0; i < last_index; i++)
    {
        String key = "/" + std::to_string(i + 1);
        String data = "table_" + key;
        createZNode(machine, key, data);
    }

    sleep(1);

    LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());

    ASSERT_EQ(machine.last_commit_index(), machine.getLastCommittedIndex());

    UInt64 term = 1;
    snapshot meta(last_index, term, config);
    machine.create_snapshot(meta);
    ASSERT_EQ(machine.getStore().container.size(), 36);
    machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, syncSnapshot)
{
    String snap_dir_1(SNAP_DIR + "/4");
    String snap_dir_2(SNAP_DIR + "/5");
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine_source(
        queue, setting_ptr, snap_dir_1, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    NuRaftStateMachine machine_target(
        queue, setting_ptr, snap_dir_2, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt64 term = 1;
    UInt32 last_index = 1024;
    for (auto i = 0; i < last_index; i++)
    {
        String key = "/" + std::to_string(i + 1);
        String data = "table_" + key;
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
    ASSERT_EQ(machine_target.getStore().container.size(), last_index + 1);

    for (auto i = 1; i < obj_id; i++)
    {
        ASSERT_TRUE(machine_target.existSnapshotObject(meta, i));
    }

    machine_source.shutdown();
    machine_target.shutdown();
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);
}

TEST(RaftStateMachine, initStateMachine)
{
    auto * log = &(Poco::Logger::get("Test_RaftStateMachine"));
    String snap_dir(SNAP_DIR + "/6");
    String log_dir(LOG_DIR + "/6");

    cleanDirectory(snap_dir, true);
    cleanDirectory(log_dir, true);

    cleanAll();

    //Create
    {
        KeeperResponsesQueue queue;
        RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);

        std::mutex new_session_id_callback_mutex;
        std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

        NuRaftStateMachine machine(
            queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);

        ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
        UInt32 last_index = 128;
        UInt64 term = 1;

        for (auto i = 0; i < last_index; i++)
        {
            UInt32 index = i + 1;
            String key = "/" + std::to_string(index);
            String data = "table_" + key;
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
            String key = "/" + std::to_string(index);
            String data = "table_" + key;
            createZNodeLog(machine, key, data, log_store, term);
        }
        sleep(1);
        LOG_INFO(log, "get sm/tm last commit index {},{}", machine.last_commit_index(), machine.getLastCommittedIndex());


        ASSERT_EQ(machine.getStore().container.size(), 257);
        machine.shutdown();
    }

    // Load
    {
        KeeperResponsesQueue queue;
        RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
        ptr<NuRaftFileLogStore> log_store = cs_new<NuRaftFileLogStore>(log_dir);

        std::mutex new_session_id_callback_mutex;
        std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

        NuRaftStateMachine machine(
            queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);
        LOG_INFO(log, "init last commit index {}", machine.last_commit_index());
        ASSERT_EQ(machine.last_commit_index(), 256);
        machine.shutdown();
    }

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}
