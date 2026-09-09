#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/KeeperCommon.h>
#include <Service/tests/raft_test_common.h>
#include <Common/IO/ReadBufferFromMemory.h>
#include <Common/IO/WriteBufferFromString.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <set>

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

    session_request.create_time = getCurrentTimeMilliseconds();
    session_request.process_time = getCurrentWallTimeMilliseconds();

    ptr<buffer> buf = serializeKeeperRequest(session_request);
    ptr<RequestForSession> session_request_2 = deserializeKeeperRequest(*(buf.get()));
    if (session_request_2->request->getOpNum() == OpNum::Create)
    {
        ZooKeeperCreateRequest * request_2 = static_cast<ZooKeeperCreateRequest *>(session_request_2->request.get());
        ASSERT_EQ(request_2->path, request->path);
        ASSERT_EQ(request_2->data, request->data);
    }

    //machine.shutdown();
    cleanDirectory(snap_dir);
}

TEST(RaftStateMachine, appendEntry)
{
    String snap_dir(SNAP_DIR + "/1");
    String log_dir(LOG_DIR + "/1");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data("CREATE TABLE table1;");
    createZNode(machine, key, data);
    KeeperNode & node = machine.getNode(key);
    ASSERT_EQ(node.data, data);

    machine.shutdown();

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, modifyEntry)
{
    String snap_dir(SNAP_DIR + "/2");
    String log_dir(LOG_DIR + "/2");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    String key("/table1");
    String data1("CREATE TABLE table1;");
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
    cleanDirectory(log_dir);
}


TEST(RaftStateMachine, createSnapshot)
{
    auto *log = &(Poco::Logger::get("Test_RaftStateMachine"));
    String snap_dir(SNAP_DIR + "/3");
    String log_dir(LOG_DIR + "/3");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
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
    /// 35 created nodes + "/" + mode-dependent system nodes (see KeeperStore::initializeSystemNodes):
    /// 2 (/zookeeper, /zookeeper/config) in ZooKeeper mode,
    /// 3 (/keeper, /keeper/api_version, /keeper/feature_flags) in ClickHouse mode.
#ifdef COMPATIBLE_MODE_ZOOKEEPER
    ASSERT_EQ(machine.getStore().getNodesCount(), 38);
#else
    ASSERT_EQ(machine.getStore().getNodesCount(), 39);
#endif
    machine.shutdown();

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, syncSnapshot)
{
    String snap_dir_1(SNAP_DIR + "/4");
    String snap_dir_2(SNAP_DIR + "/5");
    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);

    String log_dir_1(LOG_DIR + "/4");
    String log_dir_2(LOG_DIR + "/5");
    cleanDirectory(log_dir_1);
    cleanDirectory(log_dir_2);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine_source(
        queue, setting_ptr, snap_dir_1, log_dir_1, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    NuRaftStateMachine machine_target(
        queue, setting_ptr, snap_dir_2, log_dir_2, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    UInt64 term = 1;
    UInt32 last_index = 1024;
    for (auto i = 0; i < last_index; i++)
    {
        String key = "/" + std::to_string(i + 1);
        String data = "table_" + key;
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
    /// Mode-dependent system nodes: 2 (/zookeeper, /zookeeper/config) in ZooKeeper mode ->
    /// created + "/" + 2 = last_index + 3; 3 in ClickHouse mode -> last_index + 4.
#ifdef COMPATIBLE_MODE_ZOOKEEPER
    ASSERT_EQ(machine_target.getStore().getNodesCount(), last_index + 3);
#else
    ASSERT_EQ(machine_target.getStore().getNodesCount(), last_index + 4);
#endif

    for (auto i = 1; i < obj_id; i++)
    {
        ASSERT_TRUE(machine_target.existSnapshotObject(meta, i));
    }

    machine_source.shutdown();
    machine_target.shutdown();

    cleanDirectory(snap_dir_1);
    cleanDirectory(snap_dir_2);
    cleanDirectory(log_dir_1);
    cleanDirectory(log_dir_2);
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
            queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);

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


        /// Mode-dependent system nodes: ZooKeeper mode -> 256 created + "/" + 2 = 259;
        /// ClickHouse mode -> 256 created + "/" + 3 = 260.
#ifdef COMPATIBLE_MODE_ZOOKEEPER
        ASSERT_EQ(machine.getStore().getNodesCount(), 259);
#else
        ASSERT_EQ(machine.getStore().getNodesCount(), 260);
#endif
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
            queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback, log_store);
        LOG_INFO(log, "init last commit index {}", machine.last_commit_index());
        ASSERT_EQ(machine.last_commit_index(), 256);
        machine.shutdown();
    }

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadDoesNotIncreaseZxid)
{
    String snap_dir(SNAP_DIR + "/multiread_zxid");
    String log_dir(LOG_DIR + "/multiread_zxid");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    /// Set up: create a session and a test node
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", /*is_ephemeral=*/false, session_id);

    /// Record zxid before MultiRead
    int64_t zxid_before = machine.getStore().getZxid();

    /// Build MultiRead request
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 100;

    for (int i = 0; i < 5; ++i)
    {
        auto get_req = cs_new<ZooKeeperGetRequest>();
        get_req->path = "/test_node";
        get_req->xid = 100;
        multi_read->requests.push_back(get_req);
    }

    /// Process
    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Verify zxid didn't change
    ASSERT_EQ(machine.getStore().getZxid(), zxid_before);

    /// Verify response
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    ASSERT_EQ(response_for_session.response->getOpNum(), OpNum::MultiRead);
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 5u);
    for (size_t i = 0; i < 5; ++i)
        ASSERT_EQ(multi_response.responses[i]->error, Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadRegistersSubrequestWatches)
{
    String snap_dir(SNAP_DIR + "/multiread_watch");
    String log_dir(LOG_DIR + "/multiread_watch");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node_a", "data_a", /*is_ephemeral=*/false, session_id);
    setNode(machine.getStore(), "test_node_b", "data_b", /*is_ephemeral=*/false, session_id);

    /// Count watches before
    uint64_t watches_before = machine.getStore().getTotalWatchesCount();

    /// Build MultiRead with watched subrequests on different paths
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 200;

    /// Get with watch on path A
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node_a";
        req->has_watch = true;
        req->xid = 200;
        multi_read->requests.push_back(req);
    }
    /// Exists with watch on path B
    {
        auto req = cs_new<ZooKeeperExistsRequest>();
        req->path = "/test_node_b";
        req->has_watch = true;
        req->xid = 200;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Both subrequests registered watches
    ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 2);

    /// Verify response
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadHandlesIndividualErrors)
{
    String snap_dir(SNAP_DIR + "/multiread_error");
    String log_dir(LOG_DIR + "/multiread_error");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);

    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", /*is_ephemeral=*/false, session_id);

    /// Build MultiRead: one valid path, one nonexistent path
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 300;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node";
        req->xid = 300;
        multi_read->requests.push_back(req);
    }
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/nonexistent_path";
        req->xid = 300;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Each subrequest has its own error — first succeeds, second fails with ZNONODE
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZNONODE);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadRejectsWriteOps)
{
    String snap_dir(SNAP_DIR + "/multiread_reject");
    String log_dir(LOG_DIR + "/multiread_reject");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "test_node", "test_data", false, session_id);

    /// Build MultiRead with a Create subrequest — should be rejected
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 400;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/test_node";
        req->xid = 400;
        multi_read->requests.push_back(req);
    }
    {
        Coordination::ACLs acls;
        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        acls.emplace_back(std::move(acl));
        auto req = cs_new<ZooKeeperCreateRequest>();
        req->path = "/test_node/should_fail";
        req->data = "bad";
        req->acls = acls;
        req->xid = 400;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    /// The bad sub-op must produce a clean per-subrequest error response — a throw
    /// here would leave the client hanging (read-path exceptions send no response).
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    /// The valid read sub-op still succeeds.
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    /// The write sub-op is rejected with ZBADARGUMENTS at its own position.
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZBADARGUMENTS);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadExistsWatchOnNonExistentNode)
{
    String snap_dir(SNAP_DIR + "/multiread_watch_nonexistent");
    String log_dir(LOG_DIR + "/multiread_watch_nonexistent");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    uint64_t watches_before = machine.getStore().getTotalWatchesCount();

    /// Exists with watch on a path that does NOT exist — watch should still be registered
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 500;

    {
        auto req = cs_new<ZooKeeperExistsRequest>();
        req->path = "/nonexistent_path";
        req->has_watch = true;
        req->xid = 500;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Watch must be registered even on ZNONODE for Exists
    ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 1);

    /// Response should carry ZNONODE error
    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 1u);
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZNONODE);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiReadAuthCheckPerSubrequest)
{
    String snap_dir(SNAP_DIR + "/multiread_acl");
    String log_dir(LOG_DIR + "/multiread_acl");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Create node A with world-readable ACL (accessible)
    setNode(machine.getStore(), "public_node", "public_data", false, session_id);

    /// Create node B with auth-only ACL (not readable by this session)
    {
        Coordination::ACLs restricted_acls;
        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "digest";
        acl.id = "user:password";
        restricted_acls.emplace_back(std::move(acl));

        auto create_req = cs_new<ZooKeeperCreateRequest>();
        create_req->path = "/restricted_node";
        create_req->data = "secret";
        create_req->is_ephemeral = false;
        create_req->is_sequential = false;
        create_req->acls = restricted_acls;
        create_req->xid = 600;

        KeeperStore::KeeperResponsesQueue rsp_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(
            rsp_queue, {create_req, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/true);
    }

    /// MultiRead with Get on both nodes
    auto multi_read = cs_new<ZooKeeperMultiRequest>();
    multi_read->operation_type = ZooKeeperMultiRequest::OperationType::Read;
    multi_read->xid = 601;

    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/public_node";
        req->xid = 601;
        multi_read->requests.push_back(req);
    }
    {
        auto req = cs_new<ZooKeeperGetRequest>();
        req->path = "/restricted_node";
        req->xid = 601;
        multi_read->requests.push_back(req);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {multi_read, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    ResponseForSession response_for_session;
    ASSERT_TRUE(response_queue.tryPop(response_for_session));
    auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
    ASSERT_EQ(multi_response.responses.size(), 2u);
    /// public_node: readable by world:anyone
    ASSERT_EQ(multi_response.responses[0]->error, Error::ZOK);
    /// restricted_node: auth-only ACL, session has no auth → ZNOAUTH
    ASSERT_EQ(multi_response.responses[1]->error, Error::ZNOAUTH);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, RemoveRecursive)
{
    String snap_dir(SNAP_DIR + "/rem_rec");
    String log_dir(LOG_DIR + "/rem_rec");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Build: /a -> /a/b, /a/b/c, /a/d
    setNode(machine.getStore(), "a", "root", false, session_id);
    setNode(machine.getStore(), "a/b", "child_b", false, session_id);
    setNode(machine.getStore(), "a/b/c", "grandchild", false, session_id);
    setNode(machine.getStore(), "a/d", "child_d", false, session_id);
    ASSERT_TRUE(machine.getStore().getNode("/a") != nullptr);
    ASSERT_TRUE(machine.getStore().getNode("/a/b/c") != nullptr);

    /// Record parent (/) child count before removal
    auto root_before = machine.getStore().getNode("/");
    int32_t root_children_before = root_before->stat.numChildren;

    auto req = cs_new<ZooKeeperRemoveRecursiveRequest>();
    req->path = "/a";
    req->xid = 1;

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(
        response_queue, {req, session_id, time}, {}, true, false);

    ASSERT_EQ(machine.getStore().getNode("/a"), nullptr);
    ASSERT_EQ(machine.getStore().getNode("/a/b"), nullptr);
    ASSERT_EQ(machine.getStore().getNode("/a/b/c"), nullptr);
    ASSERT_EQ(machine.getStore().getNode("/a/d"), nullptr);

    /// Parent stat must reflect the removed child (regression: issue #1)
    auto root_after = machine.getStore().getNode("/");
    ASSERT_EQ(root_after->stat.numChildren, root_children_before - 1);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    ASSERT_EQ(r.response->error, Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, EphemeralCleanupBumpsParentCversion)
{
    String snap_dir(SNAP_DIR + "/eph_cv");
    String log_dir(LOG_DIR + "/eph_cv");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    setNode(machine.getStore(), "eph_parent", "p", false, session_id);
    int32_t cversion_before = machine.getStore().getNode("/eph_parent")->stat.cversion;

    /// Creating the ephemeral child bumps the parent cversion by exactly 1 in both modes
    /// (in ZooKeeper mode the visible value is reconstructed from the stored counters).
    setNode(machine.getStore(), "eph_parent/eph_child", "c", true, session_id);
    ASSERT_EQ(machine.getStore().getNode("/eph_parent")->stat.cversion, cversion_before + 1);

    /// Closing the session removes the ephemeral child.
    auto close_req = cs_new<ZooKeeperCloseRequest>();
    close_req->xid = 2;
    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {close_req, session_id, time}, {}, true, false);

    ASSERT_EQ(machine.getStore().getNode("/eph_parent/eph_child"), nullptr);

    /// The ephemeral expiry must advance the parent's cversion the same way an explicit child
    /// removal does. In ZooKeeper mode the stored cversion only counts creates (the visible value
    /// is rebuilt in statForResponse), so there is no stored bump; in ClickHouse mode the stored
    /// value itself counts removals, and skipping the bump here used to leave a stale cversion.
#ifdef COMPATIBLE_MODE_ZOOKEEPER
    ASSERT_EQ(machine.getStore().getNode("/eph_parent")->stat.cversion, cversion_before + 1);
#else
    ASSERT_EQ(machine.getStore().getNode("/eph_parent")->stat.cversion, cversion_before + 2);
#endif

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

#ifndef COMPATIBLE_MODE_ZOOKEEPER
TEST(RaftStateMachine, MultiRollbackRestoresParentCversion)
{
    /// ClickHouse-mode only: a child Set bumps the parent's cversion, and Set's undo must restore
    /// it on the *live* tree node. A later RemoveRecursive in the same multi removes the parent and
    /// its undo re-adds a clone, so undo that writes to the parent object captured at process time
    /// would leak the cversion bump (regression: captured stale parent pointer in StoreRequestSet).
    String snap_dir(SNAP_DIR + "/multi_rb_cv");
    String log_dir(LOG_DIR + "/multi_rb_cv");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    setNode(machine.getStore(), "a", "root", false, session_id);
    setNode(machine.getStore(), "a/b", "child", false, session_id);
    /// Captured before the multi; the failing transaction below must restore exactly these values.
    int32_t root_cversion_before = machine.getStore().getNode("/")->stat.cversion;
    int32_t a_cversion_before = machine.getStore().getNode("/a")->stat.cversion;

    auto set_req = cs_new<ZooKeeperSetRequest>();
    set_req->path = "/a/b";
    set_req->data = "changed";
    set_req->version = -1;

    auto rr_req = cs_new<ZooKeeperRemoveRecursiveRequest>();
    rr_req->path = "/a";

    /// Guaranteed-failing last op: Remove a path deleted by the RemoveRecursive above.
    auto rm_req = cs_new<ZooKeeperRemoveRequest>();
    rm_req->path = "/a/b";
    rm_req->version = -1;

    auto multi_req = cs_new<ZooKeeperMultiRequest>();
    multi_req->requests = {set_req, rr_req, rm_req};
    multi_req->xid = 1;

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {multi_req, session_id, time}, {}, true, false);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    /// The multi fails and rolls back.
    ASSERT_EQ(r.response->error, Error::ZOK);
    auto & multi_resp = dynamic_cast<ZooKeeperMultiResponse &>(*r.response);
    ASSERT_EQ(multi_resp.responses[2]->error, Error::ZNONODE);

    /// The tree must be byte-identical to the pre-multi state: nodes back with original data...
    auto a_node = machine.getStore().getNode("/a");
    auto ab_node = machine.getStore().getNode("/a/b");
    ASSERT_TRUE(a_node != nullptr);
    ASSERT_TRUE(ab_node != nullptr);
    ASSERT_EQ(ab_node->data, "child");

    /// ...including cversions: /a's Set-bump and /'s RemoveRecursive-bump both rolled back.
    ASSERT_EQ(a_node->stat.cversion, a_cversion_before);
    ASSERT_EQ(machine.getStore().getNode("/")->stat.cversion, root_cversion_before);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}
#endif

TEST(RaftStateMachine, TryRemove)
{
    String snap_dir(SNAP_DIR + "/tryrem");
    String log_dir(LOG_DIR + "/tryrem");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "exists_node", "data", false, session_id);

    /// TryRemove existing node
    {
        auto req = cs_new<ZooKeeperRemoveRequest>();
        req->path = "/exists_node";
        req->try_remove = true;
        req->xid = 1;

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

        ResponseForSession r;
        ASSERT_TRUE(response_queue.tryPop(r));
        ASSERT_EQ(r.response->error, Error::ZOK);
        ASSERT_EQ(machine.getStore().getNode("/exists_node"), nullptr);
    }

    /// TryRemove nonexistent — succeeds
    {
        auto req = cs_new<ZooKeeperRemoveRequest>();
        req->path = "/nonexistent";
        req->try_remove = true;
        req->xid = 2;

        /// Register a data watch on the missing node, then verify TryRemove
        /// on a nonexistent path does NOT fire it.
        uint64_t watches_before = machine.getStore().getTotalWatchesCount();
        {
            /// Exists (unlike Get) registers a data watch even on a missing path
            auto exists_req = cs_new<ZooKeeperExistsRequest>();
            exists_req->path = "/nonexistent";
            exists_req->has_watch = true;
            exists_req->xid = 3;

            KeeperStore::KeeperResponsesQueue watch_queue;
            int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
            machine.getStore().processRequest(watch_queue, {exists_req, session_id, time}, {}, true, false);
            ResponseForSession reg;
            ASSERT_TRUE(watch_queue.tryPop(reg));
        }
        ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 1);

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

        ResponseForSession r;
        ASSERT_TRUE(response_queue.tryPop(r));
        ASSERT_EQ(r.response->error, Error::ZOK);

        /// No watch event fired and the watch survives: the node was never deleted
        ASSERT_FALSE(response_queue.tryPop(r));
        ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 1);
    }

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, CheckStat)
{
    String snap_dir(SNAP_DIR + "/chkstat");
    String log_dir(LOG_DIR + "/chkstat");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "check_node", "data", false, session_id);

    auto node = machine.getStore().getNode("/check_node");
    auto view = node->statForResponse();
    int32_t v = view.version;
    int32_t cv = view.cversion;
    int32_t av = view.aversion;

    /// Matching stat
    {
        auto req = cs_new<ZooKeeperCheckStatRequest>();
        req->path = "/check_node";
        req->version = v;
        req->stat_to_check.cversion = cv;
        req->stat_to_check.aversion = av;
        req->xid = 1;

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

        ResponseForSession r;
        ASSERT_TRUE(response_queue.tryPop(r));
        ASSERT_EQ(r.response->error, Error::ZOK);
    }

    /// Wrong version
    {
        auto req = cs_new<ZooKeeperCheckStatRequest>();
        req->path = "/check_node";
        req->version = v + 1;
        req->xid = 2;

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

        ResponseForSession r;
        ASSERT_TRUE(response_queue.tryPop(r));
        ASSERT_EQ(r.response->error, Error::ZBADVERSION);
    }

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, TryRemoveBestEffort)
{
    String snap_dir(SNAP_DIR + "/tryremove");
    String log_dir(LOG_DIR + "/tryremove");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    setNode(machine.getStore(), "parent", "d", false, session_id);
    setNode(machine.getStore(), "parent/child", "d", false, session_id);

    auto try_remove = [&](const String & path, int32_t version, int32_t xid)
    {
        auto req = cs_new<ZooKeeperRemoveRequest>();
        req->path = path;
        req->try_remove = true;
        req->version = version;
        req->xid = xid;
        KeeperStore::KeeperResponsesQueue rq;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(rq, {req, session_id, time}, {}, true, false);
        ResponseForSession r;
        EXPECT_TRUE(rq.tryPop(r));
        return r.response->error;
    };

    /// Non-empty node: best-effort TryRemove is a silent no-op success, node survives.
    ASSERT_EQ(try_remove("/parent", -1, 1), Error::ZOK);
    ASSERT_NE(machine.getStore().getNode("/parent"), nullptr);

    /// Wrong version: same, ZOK and node survives.
    ASSERT_EQ(try_remove("/parent/child", 999, 2), Error::ZOK);
    ASSERT_NE(machine.getStore().getNode("/parent/child"), nullptr);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, RemoveRecursiveRejectsRoot)
{
    String snap_dir(SNAP_DIR + "/rmrec_root");
    String log_dir(LOG_DIR + "/rmrec_root");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "keep_me", "d", false, session_id);

    auto req = cs_new<ZooKeeperRemoveRecursiveRequest>();
    req->path = "/";
    req->remove_nodes_limit = 100;
    req->xid = 1;

    KeeperStore::KeeperResponsesQueue rq;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(rq, {req, session_id, time}, {}, true, false);

    ResponseForSession r;
    ASSERT_TRUE(rq.tryPop(r));
    ASSERT_EQ(r.response->error, Error::ZBADARGUMENTS);
    /// Tree untouched.
    ASSERT_NE(machine.getStore().getNode("/keep_me"), nullptr);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, CheckStatFullFields)
{
    String snap_dir(SNAP_DIR + "/checkstat_full");
    String log_dir(LOG_DIR + "/checkstat_full");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "cnode", "hello", false, session_id);

    auto view = machine.getStore().getNode("/cnode")->statForResponse();

    auto check = [&](const std::function<void(ZooKeeperCheckStatRequest &)> & fill, int32_t xid)
    {
        auto req = cs_new<ZooKeeperCheckStatRequest>();
        req->path = "/cnode";
        fill(*req);
        req->xid = xid;
        KeeperStore::KeeperResponsesQueue rq;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(rq, {req, session_id, time}, {}, true, false);
        ResponseForSession r;
        EXPECT_TRUE(rq.tryPop(r));
        return r.response->error;
    };

    /// dataLength mismatch is now caught (was ignored before - only version/cversion/aversion were checked).
    ASSERT_EQ(check([&](auto & q) { q.stat_to_check.dataLength = view.dataLength + 1; }, 1), Error::ZBADVERSION);
    /// pzxid mismatch caught too.
    ASSERT_EQ(check([&](auto & q) { q.stat_to_check.pzxid = view.pzxid + 1; }, 2), Error::ZBADVERSION);
    /// Matching full stat -> ZOK.
    ASSERT_EQ(check([&](auto & q) { q.stat_to_check.dataLength = view.dataLength; q.stat_to_check.pzxid = view.pzxid; q.stat_to_check.czxid = view.czxid; }, 3), Error::ZOK);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, GetACLStatConsistentWithGet)
{
    String snap_dir(SNAP_DIR + "/getacl_stat");
    String log_dir(LOG_DIR + "/getacl_stat");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);
    setNode(machine.getStore(), "gnode", "data", false, session_id);
    /// Create a child so cversion/numChildren are non-trivial and the statForResponse transform matters.
    setNode(machine.getStore(), "gnode/c", "d", false, session_id);

    auto run = [&](ZooKeeperRequestPtr req, int32_t xid) -> Coordination::Stat
    {
        req->xid = xid;
        KeeperStore::KeeperResponsesQueue rq;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(rq, {req, session_id, time}, {}, true, false);
        ResponseForSession r;
        EXPECT_TRUE(rq.tryPop(r));
        if (auto * g = dynamic_cast<ZooKeeperGetResponse *>(r.response.get()))
            return g->stat;
        return dynamic_cast<ZooKeeperGetACLResponse &>(*r.response).stat;
    };

    auto get_req = cs_new<ZooKeeperGetRequest>();
    get_req->path = "/gnode";
    auto get_stat = run(get_req, 1);

    auto acl_req = cs_new<ZooKeeperGetACLRequest>();
    acl_req->path = "/gnode";
    auto acl_stat = run(acl_req, 2);

    /// GetACL must report the same client-facing stat as Get (both via statForResponse).
    ASSERT_EQ(acl_stat.cversion, get_stat.cversion);
    ASSERT_EQ(acl_stat.numChildren, get_stat.numChildren);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, ListRecursive)
{
    String snap_dir(SNAP_DIR + "/listrec");
    String log_dir(LOG_DIR + "/listrec");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Build: /subtree -> /subtree/x, /subtree/y, /subtree/y/z
    setNode(machine.getStore(), "subtree", "root", false, session_id);
    setNode(machine.getStore(), "subtree/x", "x_data", false, session_id);
    setNode(machine.getStore(), "subtree/y", "y_data", false, session_id);
    setNode(machine.getStore(), "subtree/y/z", "z_data", false, session_id);

    auto req = cs_new<ZooKeeperListRecursiveRequest>();
    req->path = "/subtree";
    req->xid = 1;

    /// ListRecursive is read-only: zxid must not advance (regression: issue #2)
    int64_t zxid_before = machine.getStore().getZxid();

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

    ASSERT_EQ(machine.getStore().getZxid(), zxid_before);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    ASSERT_EQ(r.response->error, Error::ZOK);

    auto & list_resp = dynamic_cast<ZooKeeperListRecursiveResponse &>(*r.response);
    std::vector<String> names;
    for (auto it = list_resp.names.begin(); it != list_resp.names.end(); ++it)
        names.emplace_back(*it);
    std::sort(names.begin(), names.end());
    ASSERT_EQ(names.size(), 3u);
    ASSERT_EQ(names[0], "/subtree/x");
    ASSERT_EQ(names[1], "/subtree/y");
    ASSERT_EQ(names[2], "/subtree/y/z");

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, FilteredListWithStatsAndData)
{
    String snap_dir(SNAP_DIR + "/flist_stats");
    String log_dir(LOG_DIR + "/flist_stats");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    setNode(machine.getStore(), "flist", "root", false, session_id);
    setNode(machine.getStore(), "flist/x", "data_x", false, session_id);
    setNode(machine.getStore(), "flist/y", "data_y", false, session_id);

    auto req = cs_new<ZooKeeperFilteredListRequest>();
    req->path = "/flist";
    req->xid = 1;
    req->list_with_stats_and_data = true;
    req->with_stat = true;
    req->with_data = true;
    ASSERT_EQ(req->getOpNum(), OpNum::FilteredListWithStatsAndData);

    int64_t zxid_before = machine.getStore().getZxid();

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {req, session_id, time}, {}, true, false);

    ASSERT_EQ(machine.getStore().getZxid(), zxid_before);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    ASSERT_EQ(r.response->error, Error::ZOK);

    auto & resp = dynamic_cast<ZooKeeperFilteredListWithStatsAndDataResponse &>(*r.response);
    ASSERT_EQ(resp.names.size(), 2u);
    ASSERT_EQ(resp.stats.size(), 2u);
    ASSERT_EQ(resp.data.size(), 2u);

    size_t i = 0;
    for (auto it = resp.names.begin(); it != resp.names.end(); ++it, ++i)
    {
        String name = (*it).toString();
        if (name == "x")
            ASSERT_EQ(resp.data[i], "data_x");
        else if (name == "y")
            ASSERT_EQ(resp.data[i], "data_y");
        else
            FAIL() << "unexpected child " << name;
    }

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiWriteWithCheckStatAndTryRemove)
{
    String snap_dir(SNAP_DIR + "/multi_checkstat_tryremove");
    String log_dir(LOG_DIR + "/multi_checkstat_tryremove");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    setNode(machine.getStore(), "mnode", "data", false, session_id);
    auto node = machine.getStore().getNode("/mnode");
    int32_t ver = node->stat.version;

    /// Multi (write): CheckStat(correct version) + TryRemove(existing) must both succeed.
    auto multi = cs_new<ZooKeeperMultiRequest>();
    multi->operation_type = ZooKeeperMultiRequest::OperationType::Write;
    multi->xid = 10;
    {
        auto cs = cs_new<ZooKeeperCheckStatRequest>();
        cs->path = "/mnode";
        cs->version = ver;
        multi->requests.push_back(cs);
    }
    {
        auto tr = cs_new<ZooKeeperRemoveRequest>();
        tr->path = "/mnode";
        tr->try_remove = true;
        tr->version = -1;
        multi->requests.push_back(tr);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {multi, session_id, time}, {}, true, false);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    auto & multi_resp = dynamic_cast<ZooKeeperMultiResponse &>(*r.response);
    ASSERT_EQ(multi_resp.responses.size(), 2u);
    ASSERT_EQ(multi_resp.responses[0]->error, Error::ZOK);
    ASSERT_EQ(multi_resp.responses[1]->error, Error::ZOK);
    /// Node removed by the TryRemove sub-op
    ASSERT_EQ(machine.getStore().getNode("/mnode"), nullptr);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiRemoveRecursiveRollback)
{
    String snap_dir(SNAP_DIR + "/multi_remrec_rollback");
    String log_dir(LOG_DIR + "/multi_remrec_rollback");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Build subtree /r -> /r/a, /r/a/b, /r/c
    setNode(machine.getStore(), "r", "root", false, session_id);
    setNode(machine.getStore(), "r/a", "a_data", false, session_id);
    setNode(machine.getStore(), "r/a/b", "b_data", false, session_id);
    setNode(machine.getStore(), "r/c", "c_data", false, session_id);

    auto root_parent = machine.getStore().getNode("/");
    int32_t parent_children_before = root_parent->stat.numChildren;

    /// Multi (write): RemoveRecursive(/r) then a Check that FAILS (wrong version on a
    /// node that no longer needs to exist) -> whole multi must roll back, restoring /r.
    auto multi = cs_new<ZooKeeperMultiRequest>();
    multi->operation_type = ZooKeeperMultiRequest::OperationType::Write;
    multi->xid = 20;
    {
        auto rr = cs_new<ZooKeeperRemoveRecursiveRequest>();
        rr->path = "/r";
        multi->requests.push_back(rr);
    }
    {
        /// Check on a nonexistent path -> ZNONODE -> triggers rollback
        auto ck = cs_new<ZooKeeperCheckRequest>();
        ck->path = "/does_not_exist";
        ck->version = -1;
        multi->requests.push_back(ck);
    }

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {multi, session_id, time}, {}, true, false);

    ResponseForSession r;
    ASSERT_TRUE(response_queue.tryPop(r));
    /// Multi failed, so the whole subtree must be restored intact.
    ASSERT_NE(machine.getStore().getNode("/r"), nullptr);
    ASSERT_NE(machine.getStore().getNode("/r/a"), nullptr);
    ASSERT_NE(machine.getStore().getNode("/r/a/b"), nullptr);
    ASSERT_NE(machine.getStore().getNode("/r/c"), nullptr);
    /// Data preserved
    ASSERT_EQ(machine.getStore().getNode("/r/a/b")->data, "b_data");
    /// Parent link + stat restored
    ASSERT_TRUE(machine.getStore().getNode("/")->children.count("r") == 1);
    ASSERT_EQ(machine.getStore().getNode("/")->stat.numChildren, parent_children_before);
    /// /r still has both children
    ASSERT_EQ(machine.getStore().getNode("/r")->children.size(), 2u);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(RaftStateMachine, MultiRejectsUnsupportedSubOpWithoutAbort)
{
    String snap_dir(SNAP_DIR + "/multi_bad_subop");
    String log_dir(LOG_DIR + "/multi_bad_subop");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// Multi with GetACL: not supported as a sub-op. Must fail with a clean error
    /// instead of throwing at apply time, where RequestProcessor aborts the server.
    {
        auto multi = cs_new<ZooKeeperMultiRequest>();
        multi->operation_type = ZooKeeperMultiRequest::OperationType::Write;
        multi->xid = 700;

        auto get_acl = cs_new<ZooKeeperGetACLRequest>();
        get_acl->path = "/some_node";
        get_acl->xid = 700;
        multi->requests.push_back(get_acl);

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {multi, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

        ResponseForSession response_for_session;
        ASSERT_TRUE(response_queue.tryPop(response_for_session));
        /// Top-level header error stays ZOK, matching how a normal multi-write
        /// failure is reported: ZooKeeperResponse::writeNoCopy only serializes the
        /// per-op body when the top-level error is ZOK, so a non-ZOK top-level error
        /// here would suppress the body entirely instead of surfacing the real error.
        ASSERT_EQ(response_for_session.response->error, Error::ZOK);
        auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
        ASSERT_EQ(multi_response.responses.size(), 1u);
        ASSERT_EQ(multi_response.responses[0]->error, Error::ZBADARGUMENTS);
    }

    /// Mixed read/write multi: also a clean error, no throw.
    {
        auto multi = cs_new<ZooKeeperMultiRequest>();
        multi->operation_type = ZooKeeperMultiRequest::OperationType::Unspecified;
        multi->xid = 701;

        auto set_req = cs_new<ZooKeeperSetRequest>();
        set_req->path = "/some_node";
        set_req->data = "x";
        set_req->version = -1;
        set_req->xid = 701;
        multi->requests.push_back(set_req);

        auto get_req = cs_new<ZooKeeperGetRequest>();
        get_req->path = "/some_node";
        get_req->xid = 701;
        multi->requests.push_back(get_req);

        KeeperStore::KeeperResponsesQueue response_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(response_queue, {multi, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

        ResponseForSession response_for_session;
        ASSERT_TRUE(response_queue.tryPop(response_for_session));
        ASSERT_EQ(response_for_session.response->error, Error::ZOK);
        auto & multi_response = dynamic_cast<ZooKeeperMultiResponse &>(*response_for_session.response);
        ASSERT_EQ(multi_response.responses.size(), 2u);
        ASSERT_EQ(multi_response.responses[0]->error, Error::ZBADARGUMENTS);
        ASSERT_EQ(multi_response.responses[1]->error, Error::ZBADARGUMENTS);
    }

    /// The store must still work after both rejected multis.
    setNode(machine.getStore(), "still_alive", "yes", false, session_id);
    ASSERT_EQ(machine.getStore().getNode("/still_alive")->data, "yes");

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}

TEST(ZooKeeperCreateRequest, RejectsTTLAndContainerModesWithoutWireDesync)
{
    /// ZK 3.5+ TTL create modes append a trailing int64 ttl after the flags. It must
    /// be consumed, and the mode rejected, instead of silently creating a permanent
    /// node and leaving the next request to parse garbage.
    {
        WriteBufferFromOwnString buf;
        Coordination::write(String("/ttl_node"), buf);
        Coordination::write(String("data"), buf);
        ACLs acls;
        Coordination::write(acls, buf);
        Coordination::write(int32_t{5}, buf); /// PERSISTENT_WITH_TTL
        Coordination::write(int64_t{60000}, buf);

        auto request = cs_new<ZooKeeperCreateRequest>();
        ReadBufferFromMemory in(buf.str().data(), buf.str().size());
        EXPECT_THROW(request->readImpl(in), Coordination::Exception);
        EXPECT_TRUE(in.eof()) << "The trailing ttl must be consumed, otherwise the next request on the connection parses garbage";
    }

    {
        WriteBufferFromOwnString buf;
        Coordination::write(String("/seq_ttl_node"), buf);
        Coordination::write(String("data"), buf);
        ACLs acls;
        Coordination::write(acls, buf);
        Coordination::write(int32_t{6}, buf); /// PERSISTENT_SEQUENTIAL_WITH_TTL
        Coordination::write(int64_t{60000}, buf);

        auto request = cs_new<ZooKeeperCreateRequest>();
        ReadBufferFromMemory in(buf.str().data(), buf.str().size());
        EXPECT_THROW(request->readImpl(in), Coordination::Exception);
        EXPECT_TRUE(in.eof());
    }

    /// Container mode is rejected with a clean error (matches ClickHouse Keeper).
    {
        WriteBufferFromOwnString buf;
        Coordination::write(String("/container_node"), buf);
        Coordination::write(String("data"), buf);
        ACLs acls;
        Coordination::write(acls, buf);
        Coordination::write(int32_t{4}, buf); /// CONTAINER

        auto request = cs_new<ZooKeeperCreateRequest>();
        ReadBufferFromMemory in(buf.str().data(), buf.str().size());
        EXPECT_THROW(request->readImpl(in), Coordination::Exception);
        EXPECT_TRUE(in.eof());
    }

    /// Unknown create mode is rejected too.
    {
        WriteBufferFromOwnString buf;
        Coordination::write(String("/bad_node"), buf);
        Coordination::write(String("data"), buf);
        ACLs acls;
        Coordination::write(acls, buf);
        Coordination::write(int32_t{42}, buf);

        auto request = cs_new<ZooKeeperCreateRequest>();
        ReadBufferFromMemory in(buf.str().data(), buf.str().size());
        EXPECT_THROW(request->readImpl(in), Coordination::Exception);
        EXPECT_TRUE(in.eof());
    }
}

TEST(RaftStateMachine, SetWatchesRestoresChildAndExistWatches)
{
    String snap_dir(SNAP_DIR + "/set_watches_restore");
    String log_dir(LOG_DIR + "/set_watches_restore");
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, log_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int64_t session_id = machine.getStore().getSessionID(30000);

    /// /leaf (no children, pzxid never moves), then /parent and /parent/child
    /// (the child create bumps /parent's pzxid).
    setNode(machine.getStore(), "leaf", "l", false, session_id);
    setNode(machine.getStore(), "parent", "p", false, session_id);
    setNode(machine.getStore(), "parent/child", "c", false, session_id);

    /// A reconnect SetWatches with relative_zxid 1: /parent's pzxid moved, so that
    /// child watch must fire CHILD (not be spuriously DELETED and dropped); the exist
    /// watch on an existing node must fire CREATED; a data watch on a missing node
    /// must fire DELETED; an unchanged child watch (/leaf) must re-register silently.
    /// In this direct-processRequest test path, create() runs with the pre-increment
    /// zxid, so /leaf gets pzxid 0 and /parent's child create sets its pzxid to 2;
    /// production commits pass the zxid in first, but the comparisons hold either way.
    auto set_watches = cs_new<ZooKeeperSetWatchesRequest>();
    set_watches->relative_zxid = 1;
    set_watches->xid = 702;
    set_watches->data_watches.emplace_back("/data_gone");
    set_watches->exist_watches.emplace_back("/parent");
    set_watches->exist_watches.emplace_back("/exist_gone");
    set_watches->list_watches.emplace_back("/parent");
    set_watches->list_watches.emplace_back("/leaf");

    /// Another session also watches /parent. The restore must fire only the
    /// re-registering session's own watch — session2's watch must survive and must
    /// not receive a spurious event.
    int64_t session2 = machine.getStore().getSessionID(30000);
    {
        auto get_req = cs_new<ZooKeeperGetRequest>();
        get_req->path = "/parent";
        get_req->has_watch = true;
        get_req->xid = 703;
        KeeperStore::KeeperResponsesQueue rsp_queue;
        int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
        machine.getStore().processRequest(rsp_queue, {get_req, session2, time}, {}, /*check_acl=*/true, /*ignore_response=*/true);
    }

    uint64_t watches_before = machine.getStore().getTotalWatchesCount();

    KeeperStore::KeeperResponsesQueue response_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    machine.getStore().processRequest(response_queue, {set_watches, session_id, time}, {}, /*check_acl=*/true, /*ignore_response=*/false);

    /// Collect the watch events fired.
    std::multiset<std::pair<String, int>> fired;
    ResponseForSession response_for_session;
    while (response_queue.tryPop(response_for_session))
    {
        if (auto * watch_response = dynamic_cast<Coordination::ZooKeeperWatchResponse *>(response_for_session.response.get()))
        {
            fired.emplace(watch_response->path, watch_response->type);
            /// Fired events must only ever go to the re-registering session.
            ASSERT_EQ(response_for_session.session_id, session_id);
        }
    }

    ASSERT_EQ(fired.count(std::make_pair(String("/data_gone"), Coordination::Event::DELETED)), 1u);
    ASSERT_EQ(fired.count(std::make_pair(String("/parent"), Coordination::Event::CREATED)), 1u);
    ASSERT_EQ(fired.count(std::make_pair(String("/parent"), Coordination::Event::CHILD)), 1u);
    /// No spurious DELETED for the existing /parent child watch.
    ASSERT_EQ(fired.count(std::make_pair(String("/parent"), Coordination::Event::DELETED)), 0u);
    /// The unchanged child watch on /leaf must not fire anything.
    ASSERT_EQ(fired.count(std::make_pair(String("/leaf"), Coordination::Event::CHILD)), 0u);
    ASSERT_EQ(fired.count(std::make_pair(String("/leaf"), Coordination::Event::DELETED)), 0u);

    /// Silent registrations survive (exist watch on /exist_gone + child watch on
    /// /leaf), and session2's /parent watch was not consumed by the restore.
    ASSERT_EQ(machine.getStore().getTotalWatchesCount(), watches_before + 2);

    machine.shutdown();
    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}
