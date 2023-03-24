#include <string>
#include <unordered_map>
#include <Service/ACLMap.h>
#include <Service/KeeperStore.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/Settings.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>

using namespace nuraft;
using namespace RK;
using namespace Coordination;


namespace RK
{
void setNode(KeeperStore & storage, const std::string key, const std::string value, bool is_ephemeral, int64_t session_id)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    storage.addSessionID(session_id, 30000);

    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = is_ephemeral;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;
    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, session_id, time, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

ptr<buffer> createSessionLog(int64_t session_timeout_ms)
{
    auto entry = buffer::alloc(sizeof(int64_t));
    nuraft::buffer_serializer bs(entry);
    bs.put_i64(session_timeout_ms);
    return entry;
}

ptr<buffer> updateSessionLog(int64_t session_id, int64_t session_timeout_ms)
{
    auto entry = buffer::alloc(sizeof(int64_t) + sizeof(int64_t));
    nuraft::buffer_serializer bs(entry);

    bs.put_i64(session_id);
    bs.put_i64(session_timeout_ms);
    return entry;
}

ptr<buffer> closeSessionLog(int64_t session_id)
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Close);
    request->xid = Coordination::CLOSE_XID;
    KeeperStore::RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = session_id;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    request_info.create_time = time;
    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(request_info);
    return buf;
}

ptr<buffer> createLog(int64_t session_id, const std::string & key, const std::string & data, bool is_ephemeral = false)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    auto session_request = cs_new<KeeperStore::RequestForSession>();
    auto request = cs_new<ZooKeeperCreateRequest>();
    session_request->request = request;
    session_request->session_id = session_id;
    request->path = key;
    request->data = data;
    request->is_ephemeral = is_ephemeral;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;

    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    session_request->create_time = time;

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(*session_request);
    return buf;
}

ptr<buffer> setLog(int64_t session_id, const std::string & key, const std::string value, const int32_t version = -1)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    auto session_request = cs_new<KeeperStore::RequestForSession>();
    auto request = cs_new<ZooKeeperSetRequest>();
    session_request->request = request;
    session_request->session_id = session_id;
    request->path = key;
    request->data = value;
    request->version = version;
    request->xid = 1;

    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    session_request->create_time = time;

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(*session_request);
    return buf;
}

ptr<buffer> removeLog(int64_t session_id, const std::string & key)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    auto session_request = cs_new<KeeperStore::RequestForSession>();
    auto request = cs_new<ZooKeeperRemoveRequest>();
    session_request->request = request;
    session_request->session_id = session_id;
    request->path = key;
    request->xid = 1;

    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    session_request->create_time = time;

    ptr<buffer> buf = NuRaftStateMachine::serializeRequest(*session_request);
    return buf;
}

void appendToLogStore(ptr<NuRaftFileLogStore> store, ptr<buffer> buf, int64_t term = 1)
{
    ptr<log_entry> entry_log = cs_new<log_entry>(term, buf);
    store->append(entry_log);
}

void commitLog(NuRaftStateMachine & machine, ptr<buffer> buf)
{
    machine.commit(machine.last_commit_index() + 1, *buf, true);
}

void setACLNode(
    KeeperStore & storage,
    const std::string key,
    const std::string value,
    int32_t permissions,
    const std::string & scheme,
    const std::string & id)
{
    ACLs default_acls;
    ACL acl;
    //    acl.permissions = ACL::All;
    acl.permissions = permissions;
    acl.scheme = scheme;
    acl.id = id;
    default_acls.emplace_back(std::move(acl));

    //    'digest', 'user1:password1'

    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;

    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, 1, time, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

void setACLNode(KeeperStore & storage, const std::string key, const std::string value, const ACLs & acls)
{
    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = acls;
    request->xid = 1;

    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, 1, time, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

void addAuth(KeeperStore & storage, uint64_t session_id, const std::string & scheme, const std::string & id)
{
    //    'digest', 'user1:password1'
    //    String scheme = "digest";
    //    String data = "user1:password1";

    auto request = cs_new<ZooKeeperAuthRequest>();
    request->scheme = scheme;
    request->data = id;

    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, session_id, time, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

ACLs getACL(KeeperStore & storage, const std::string key)
{
    auto request = cs_new<ZooKeeperGetACLRequest>();
    request->path = key;

    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, 1, time, {}, /* check_acl = */ true, /*ignore_response*/ false);

    KeeperStore::ResponseForSession responses;
    responses_queue.tryPop(responses);
    return dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*responses.response).acl;
}

void setEphemeralNode(KeeperStore & storage, const std::string key, const std::string value)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = key;
    request->data = value;
    request->is_ephemeral = true;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;
    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, request, 1, time, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

void assertStateMachineEquals(KeeperStore & storage, KeeperStore & ano_storage)
{
    /// assert unit map
    ASSERT_EQ(storage.zxid, ano_storage.zxid);
    ASSERT_EQ(storage.session_id_counter, ano_storage.session_id_counter);

    /// assert size
    ASSERT_EQ(storage.container.size(), ano_storage.container.size());
    ASSERT_EQ(storage.ephemerals.size(), ano_storage.ephemerals.size());
    ASSERT_EQ(storage.session_and_timeout.size(), ano_storage.session_and_timeout.size());


    /// assert container
    for (uint32_t i = 0; i < KeeperStore::MAP_BLOCK_NUM; i++)
    {
        auto & map = storage.container.getMap(i);
        auto & ano_map = ano_storage.container.getMap(i);

        map.forEach([&ano_map](const auto & key, const auto & value) {
            /// TODO only compare data
            const auto * l = dynamic_cast<const KeeperNode *>(value.get());
            const auto * r = dynamic_cast<const KeeperNode *>(ano_map.get(key).get());
            ASSERT_EQ(l->data, r->data);
            //            ASSERT_EQ(*l, *r);
        });
    }

    /// assert ephemeral nodes
    for (const auto & it : storage.ephemerals)
    {
        ASSERT_TRUE(ano_storage.ephemerals.contains(it.first));
        auto ano_paths = ano_storage.ephemerals.at(it.first);
        ASSERT_EQ(it.second.size(), ano_paths.size());
        for (const auto & path_it : it.second)
        {
            ASSERT_TRUE(ano_paths.contains(path_it));
        }
    }

    /// assert session_and_timeout
    for (auto it : storage.session_and_timeout)
    {
        ASSERT_TRUE(ano_storage.session_and_timeout.contains(it.first));
        ASSERT_EQ(it.second, ano_storage.session_and_timeout.at(it.first));
    }

    auto filter_auth = [](KeeperStore::SessionAndAuth & auth_ids) {
        for (auto it = auth_ids.begin(); it != auth_ids.end();)
        {
            if (it->second.empty())
                auth_ids.erase(it++);
            else
                it++;
        }
    };

    filter_auth(storage.session_and_auth);
    filter_auth(ano_storage.session_and_auth);

    /// assert session_and_auth
    ASSERT_EQ(storage.session_and_auth, ano_storage.session_and_auth);

    /// assert acl
    ASSERT_EQ(storage.acl_map, ano_storage.acl_map);
}

}

TEST(RaftSnapshot, whenToSnapshot)
{
    BackendTimer timer;
    /// after 2:00
    timer.begin_second = 7200;
    /// every 1 day
    timer.interval = 24 * 3600;

    // first snapshot
    bool is_time = timer.isActionTime(0, 0);
    ASSERT_EQ(is_time, true);
}


TEST(RaftSnapshot, createSnapshot_1)
{
    std::string snap_dir(SNAP_DIR + "/1");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 10);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    snapshot snap_meta(1, 1, config);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore storage(raft_settings->dead_session_check_period_ms);

    setNode(storage, "1", "table_1");
    ASSERT_EQ(storage.container.size(), 2); /// it's has "/" and "/1"
    size_t object_size = snap_mgr.createSnapshot(snap_meta, storage);
    ASSERT_EQ(object_size, 1 + 1 + 1 + 1);
    cleanDirectory(snap_dir);
}

TEST(RaftSnapshot, createSnapshot_2)
{
    std::string snap_dir(SNAP_DIR + "/2");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 100);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore store(raft_settings->dead_session_check_period_ms);

    UInt32 last_index = 1024;
    UInt32 term = 1;
    for (int i = 0; i < last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(store, key, value);
    }
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr.createSnapshot(meta, store);
    ASSERT_EQ(object_size, 11 + 1 + 1 + 1);
    cleanDirectory(snap_dir);
}

TEST(RaftSnapshot, readAndSaveSnapshot)
{
    std::string snap_read_dir(SNAP_DIR + "/3");
    std::string snap_save_dir(SNAP_DIR + "/4");
    cleanDirectory(snap_read_dir);
    cleanDirectory(snap_save_dir);

    UInt32 last_index = 1024;
    UInt32 term = 1;
    KeeperSnapshotManager snap_mgr_read(snap_read_dir, 3, 100);
    KeeperSnapshotManager snap_mgr_save(snap_save_dir, 3, 100);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore store(raft_settings->dead_session_check_period_ms);

    for (int i = 0; i < last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(store, key, value);
    }
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr_read.createSnapshot(meta, store);
    ASSERT_EQ(object_size, 11 + 1 + 1 + 1);

    ulong obj_id = 0;
    snap_mgr_save.receiveSnapshot(meta);
    while (true)
    {
        obj_id++;
        if (!snap_mgr_read.existSnapshotObject(meta, obj_id))
        {
            break;
        }
        ptr<buffer> buffer;
        snap_mgr_read.loadSnapshotObject(meta, obj_id, buffer);
        if (buffer != nullptr)
        {
            snap_mgr_save.saveSnapshotObject(meta, obj_id, *(buffer.get()));
        }
    }
    for (auto i = 1; i < obj_id; i++)
    {
        ASSERT_TRUE(snap_mgr_save.existSnapshotObject(meta, i));
    }
    cleanDirectory(snap_read_dir);
    cleanDirectory(snap_save_dir);
}

void parseSnapshot(const SnapshotVersion create_version, const SnapshotVersion parse_version)
{
    std::string snap_dir(SNAP_DIR + "/5");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 100);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore store(raft_settings->dead_session_check_period_ms);

    /// session 1
    store.getSessionID(3000);

    addAuth(store, 1, "digest", "user1:password1"); /// set acl to session
    UInt32 last_index = 2048;
    UInt32 term = 1;
    for (int i = 1; i <= 1024; i++)
    {
        std::string key = std::to_string(i);
        std::string value = "table_" + key;

        if (i == 1020)
        {
            ACLs acls;
            ACL acl1;
            acl1.permissions = ACL::All;
            acl1.scheme = "digest";
            acl1.id = "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=";

            ACL acl2;
            acl2.permissions = ACL::All;
            acl2.scheme = "digest";
            acl2.id = "user1:CGujN0OWj2wmttV5NJgM2ja68PQ=";
            acls.emplace_back(std::move(acl1));
            acls.emplace_back(std::move(acl2));

            /// set vector acl to "/1020" node
            setACLNode(store, key, value, acls);
        }
        else if (i == 1022)
        {
            /// set read permission to "/1022" node
            setACLNode(store, key, value, ACL::Read, "digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=");
        }
        else if (i == 1024)
        {
            /// Set a password different from session 1
            setACLNode(store, key, value, ACL::All, "digest", "user1:CGujN0OWj2wmttV5NJgM2ja68PQ=");
        }
        else if (i % 2)
            setNode(store, key, value);
        else
            setACLNode(store, key, value, ACL::All, "digest", "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI="); /// set acl to even number node
    }

    for (int i = 0; i < 1024; i++)

    {
        std::string key = std::to_string(i);
        std::string value = "table_" + key;

        /// create EphemeralNode to even number, session 1 auth is "digest", "user1:password1"
        setEphemeralNode(store, "/2/" + key, value);
    }

    setEphemeralNode(store, "/1020/test112", "test211"); /// Success, parent acls Include (ACL::All, "digest", "user1:password1")
    setEphemeralNode(store, "/1022/test112", "test211"); /// Failure, no permission
    setEphemeralNode(store, "/1024/test113", "test311"); /// Failure, different password

    /// session 2
    store.getSessionID(3000);

    /// session 3
    store.getSessionID(6000);

    for (size_t i = 0; i < 10000; ++i)
    {
        store.getSessionID(6000);
    }

    ASSERT_EQ(store.container.size(), 2050); /// Include "/" node

    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr.createSnapshot(meta, store, store.zxid, store.session_id_counter);

    /// Normal node objects、Sessions、Others(int_map)、ACL_MAP
    ASSERT_EQ(object_size, 21 + 3);

    KeeperStore new_storage(raft_settings->dead_session_check_period_ms);

    ASSERT_TRUE(snap_mgr.parseSnapshot(meta, new_storage));

    /// compare container
    ASSERT_EQ(new_storage.container.size(), 2050); /// Include "/" node, "/1020/test112"
    ASSERT_EQ(new_storage.container.size(), store.container.size());
    for (UInt32 i = 0; i < store.container.getBlockNum(); i++)
    {
        auto & inner_map = store.container.getMap(i);
        for (auto it = inner_map.getMap().begin(); it != inner_map.getMap().end(); it++)
        {
            auto new_node = new_storage.container.get(it->first);
            ASSERT_TRUE(new_node != nullptr);
            ASSERT_EQ(new_node->data, it->second->data);
            if (create_version >= V1 && parse_version >= V1)
            {
                ASSERT_EQ(new_node->acl_id, it->second->acl_id);
            }

            ASSERT_EQ(new_node->is_ephemeral, it->second->is_ephemeral);
            ASSERT_EQ(new_node->is_sequental, it->second->is_sequental);
            ASSERT_EQ(new_node->stat, it->second->stat);
            ASSERT_EQ(new_node->children, it->second->children);
        }
    }
    ASSERT_EQ(new_storage.container.get("/1020/test112")->data, "test211");

    ASSERT_TRUE(true) << "compare container.";

    /// compare ephemeral
    ASSERT_EQ(new_storage.ephemerals.size(), store.ephemerals.size());
    ASSERT_EQ(store.ephemerals.size(), 1);
    for (const auto & [session_id, paths] : store.ephemerals)
    {
        ASSERT_FALSE(new_storage.ephemerals.find(session_id) == new_storage.ephemerals.end());
        ASSERT_EQ(paths, new_storage.ephemerals.find(session_id)->second);
    }

    ASSERT_TRUE(true) << "compare ephemeral.";

    /// compare sessions
    ASSERT_EQ(store.session_and_timeout.size(), 10003);
    ASSERT_EQ(store.session_and_timeout.size(), new_storage.session_and_timeout.size());
    ASSERT_EQ(store.session_and_timeout, new_storage.session_and_timeout);

    ASSERT_TRUE(true) << "compare sessions.";

    /// compare Others(int_map)
    ASSERT_EQ(store.session_id_counter, 10004);
    ASSERT_EQ(store.session_id_counter, new_storage.session_id_counter);
    ASSERT_EQ(store.zxid, new_storage.zxid);

    ASSERT_TRUE(true) << "compare Others(int_map).";


    /// compare session_and_auth
    if (create_version >= V1 && parse_version >= V1)
    {
        ASSERT_EQ(store.session_and_auth, new_storage.session_and_auth);
    }

    ASSERT_TRUE(true) << "compare session_and_auth.";

    /// compare ACLs
    if (create_version >= V1 && parse_version >= V1)
    {
        /// include : vector acl, (ACL::All, "digest", "user1:password1"), (ACL::Read, "digest", "user1:password1"), (ACL::All, "digest", "user1:password")
        ASSERT_EQ(new_storage.acl_map.getMapping().size(), 4);
        ASSERT_EQ(store.acl_map.getMapping(), new_storage.acl_map.getMapping());

        const auto & acls = new_storage.acl_map.convertNumber(store.container.get("/1020")->acl_id);
        ASSERT_EQ(acls.size(), 2);
        ASSERT_EQ(acls[0].id, "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=");
        ASSERT_EQ(acls[1].id, "user1:CGujN0OWj2wmttV5NJgM2ja68PQ=");

        for (const auto & acl : new_storage.acl_map.convertNumber(store.container.get("/1022")->acl_id))
        {
            ASSERT_EQ(acl.permissions, ACL::Read);
        }

        for (const auto & acl : new_storage.acl_map.convertNumber(store.container.get("/1024")->acl_id))
        {
            ASSERT_EQ(acl.permissions, ACL::All);
            ASSERT_EQ(acl.id, "user1:CGujN0OWj2wmttV5NJgM2ja68PQ=");
        }

        const auto & const_acl_usage_counter = store.acl_map.getUsageCounter();
        auto & acl_usage_counter = const_cast<decltype(store.acl_map.getUsageCounter()) &>(const_acl_usage_counter);
        const auto & const_new_acl_usage_counter = new_storage.acl_map.getUsageCounter();
        auto & new_acl_usage_counter = const_cast<decltype(new_storage.acl_map.getUsageCounter()) &>(const_new_acl_usage_counter);

        std::cout << "acl_usage_counter.size()" << acl_usage_counter.size() << std::endl;
        std::cout << "new_acl_usage_counter.size()" << new_acl_usage_counter.size() << std::endl;
        ASSERT_EQ(acl_usage_counter, new_acl_usage_counter);

        const auto & acls_1020 = getACL(new_storage, "/1020");
        ASSERT_EQ(acls_1020[0].id, "user1:XDkd2dsEuhc9ImU3q8pa8UOdtpI=");
        ASSERT_EQ(acls_1020[1].id, "user1:CGujN0OWj2wmttV5NJgM2ja68PQ=");
        // end of compare
    }

    ASSERT_TRUE(true) << "compare ACLs.";

    for (int i = last_index; i < 2 * last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(store, key, value);
    }

    ASSERT_EQ(store.container.size(), 4098);
    sleep(1); /// snapshot_create_interval minest is 1
    snapshot meta2(2 * last_index, term, config);
    object_size = snap_mgr.createSnapshot(meta2, store);

    KeeperSnapshotManager new_snap_mgr(snap_dir, 1, 100);
    ASSERT_EQ(new_snap_mgr.loadSnapshotMetas(), 2);
    ASSERT_EQ(new_snap_mgr.lastSnapshot()->get_last_log_idx(), 4096);

    ASSERT_EQ(new_snap_mgr.removeSnapshots(), 1);

    cleanDirectory(snap_dir);
}

TEST(RaftSnapshot, parseSnapshot)
{
    parseSnapshot(V0, V0);
    sleep(1); /// snapshot_create_interval minest is 1
    parseSnapshot(V1, V1);
}

TEST(RaftSnapshot, createSnapshotWithFuzzyLog)
{
    auto * log = &(Poco::Logger::get("Test_RaftSnapshot"));
    std::string snap_dir(SNAP_DIR + "/6");
    std::string log_dir(LOG_DIR + "/6");

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);

    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();
    ptr<NuRaftFileLogStore> store = cs_new<NuRaftFileLogStore>(log_dir);

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 0, 3600, 10, 3, new_session_id_callback_mutex, new_session_id_callback, store);

    int64_t last_log_term = store->term_at(store->next_slot() - 1);
    int64_t term = last_log_term == 0 ? 1 : last_log_term;

    /// create 2 session
    for (int i = 0; i < 2; i++)
    {
        auto buf = createSessionLog(3000);
        appendToLogStore(store, buf, term);
        commitLog(machine, buf);
    }

    /// use the first session
    int64_t session_id = machine.getStore().session_id_counter - 2;
    ASSERT_EQ(session_id, 1);

    /// create 10 znodes
    for (int i = 0; i < 10; i++)
    {
        auto buf = createLog(session_id, "/key" + std::to_string(i), "v" + std::to_string(i), false);
        appendToLogStore(store, buf, term);
        commitLog(machine, buf);
    }

    /// create 2 ephemeral znodes
    for (int i = 10; i < 12; i++)
    {
        auto buf = createLog(session_id, "/key" + std::to_string(i), "v" + std::to_string(i), true);
        appendToLogStore(store, buf, term);
        commitLog(machine, buf);
    }

    ptr<cluster_config> last_config = cs_new<cluster_config>();
    snapshot s(machine.last_commit_index(), term, last_config);

    std::mutex mutex;
    std::condition_variable cv;

    std::atomic<bool> snapshot_done = false;
    cmd_result<bool>::handler_type handler = [log, &snapshot_done, &mutex, &cv](bool, ptr<std::exception> &) {
        LOG_INFO(log, "snapshot done");
        std::unique_lock lock(mutex);
        cv.notify_all();
        snapshot_done = true;
    };

    /// Now we have 12 znodes and 2 sessions.
    /// Asynchronously create snapshot.
    /// Creating snapshot thread will sleep 20ms before save sessions and 10ms when saving a znode.
    /// Will sleep 140ms totally.
    machine.create_snapshot(s, handler);

    /// 1. make previous create session log fuzzy
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    /// 2. make fuzzy user request log
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    for (int i = 0; i < 10; i++)
    {
        auto buf = createLog(session_id, "/key" + std::to_string(i), "v" + std::to_string(i), false);
        appendToLogStore(store, buf, term);
        commitLog(machine, buf);
    }
    for (int i = 10; i < 12; i++)
    {
        auto buf = createLog(session_id, "/key" + std::to_string(i), "v" + std::to_string(i), true);
        appendToLogStore(store, buf, term);
        commitLog(machine, buf);
    }

    /// wait snapshot done
    {
        std::unique_lock lock(mutex);
        if (!snapshot_done)
        {
            cv.wait_for(lock, std::chrono::seconds(10));
        }
    }

    LOG_INFO(log, "create snapshot with fuzzy log complete");

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    KeeperResponsesQueue ano_queue;
    ptr<NuRaftFileLogStore> ano_store = cs_new<NuRaftFileLogStore>(log_dir);

    NuRaftStateMachine ano_machine(
        ano_queue, setting_ptr, snap_dir, 0, 3600, 10, 3, new_session_id_callback_mutex, new_session_id_callback, ano_store);

    assertStateMachineEquals(machine.getStore(), ano_machine.getStore());

    machine.shutdown();
    ano_machine.shutdown();

    cleanDirectory(snap_dir);
    cleanDirectory(log_dir);
}
