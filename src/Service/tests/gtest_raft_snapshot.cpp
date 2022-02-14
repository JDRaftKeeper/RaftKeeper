#include <string>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/SvsKeeperSettings.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/proto/Log.pb.h>
//#include <Service/ACLMap.h>
#include <unordered_map>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>

using namespace nuraft;
using namespace DB;
using namespace Coordination;

//const std::string LOG_DIR = "./test_raft_log";
//const std::string SNAP_DIR = "./test_raft_snapshot";

namespace DB
{
/*
void setNode(NodeMap & node_map, const std::string key, const std::string value)
{
    ptr<DataNode> node = cs_new<DataNode>();
    node->setData(value);
    node_map[key] = node;
}
*/

//const int SvsKeeperStorage::MAP_BLOCK_NUM = 16;

void setNode(SvsKeeperStorage & storage, const std::string key, const std::string value)
{
    ACLs default_acls;
    ACL acl;
    acl.permissions = ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    auto request = cs_new<ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;
    SvsKeeperStorage::SvsKeeperResponsesQueue responses_queue;
    storage.processRequest(responses_queue ,request, 0, {}, /* check_acl = */ false, /*ignore_response*/true);
}

void setEphemeralNode(SvsKeeperStorage & storage, const std::string key, const std::string value)
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
    SvsKeeperStorage::SvsKeeperResponsesQueue responses_queue;
    storage.processRequest(responses_queue ,request, 0, {}, /* check_acl = */ false, /*ignore_response*/true);
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
    bool is_time = timer.isActionTime("", 0);
    ASSERT_EQ(is_time, true);
}


TEST(RaftSnapshot, createSnapshot_1)
{
    std::string snap_dir(SNAP_DIR + "/1");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 10);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    snapshot snap_meta(1, 1, config);

    SvsKeeperSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    setNode(storage, "1", "table_1");
    ASSERT_EQ(storage.container.size(), 2); /// it's has "/" and "/1"
    size_t object_size = snap_mgr.createSnapshot(snap_meta, storage);
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM + 1 + 1 + 1);
    cleanDirectory(snap_dir);
}

TEST(RaftSnapshot, createSnapshot_2)
{
    std::string snap_dir(SNAP_DIR + "/2");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 100);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    SvsKeeperSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    UInt32 last_index = 1024;
    UInt32 term = 1;
    for (int i = 0; i < last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(storage, key, value);
    }
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr.createSnapshot(meta, storage);
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM + 1 + 1 + 1);
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

    SvsKeeperSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    for (int i = 0; i < last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(storage, key, value);
    }
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr_read.createSnapshot(meta, storage);
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM + 1 + 1 + 1);

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

TEST(RaftSnapshot, parseSnapshot)
{
    std::string snap_dir(SNAP_DIR + "/5");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 3, 100);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    SvsKeeperSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    /// session 1
    storage.getSessionID(3000);

    UInt32 last_index = 2048;
    UInt32 term = 1;

    for (int i = 1; i <= 1024; i++)
    {
        std::string key = std::to_string(i);
        std::string value = "table_" + key;
        setNode(storage, key, value);
    }

    for (int i = 0; i < 1024; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;

        /// create EphemeralNode to even number, session 1 auth is "digest", "user1:password1"
        setEphemeralNode(storage, "/2/" + key, value);
    }

    setEphemeralNode(storage, "/1020/test112", "test211"); /// Success, parent acls Include (ACL::All, "digest", "user1:password1")
    setEphemeralNode(storage, "/1022/test112", "test211"); /// Failure, no permission
    setEphemeralNode(storage, "/1024/test113", "test311"); /// Failure, different password

    /// session 2
    storage.getSessionID(3000);

    /// session 3
    storage.getSessionID(6000);

    for (size_t i = 0; i < 10000; ++i)
    {
        storage.getSessionID(6000);
    }

    ASSERT_EQ(storage.container.size(),2052); /// Include "/" node

    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr.createSnapshot(meta, storage);

    /// Normal node objects、Ephemeral node objects、Sessions、Others(int_map)
    ASSERT_EQ(object_size, 2 * SvsKeeperStorage::MAP_BLOCK_NUM + 1 + 1 + 1);

    SvsKeeperStorage new_storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    ASSERT_TRUE(snap_mgr.parseSnapshot(meta, new_storage));

    /// compare container
    ASSERT_EQ(new_storage.container.size(),2052); /// Include "/" node, "/1020/test112"
    ASSERT_EQ(new_storage.container.size(), storage.container.size());
    for (UInt32 i = 0; i < storage.container.getBlockNum(); i++)
    {
        auto & inner_map = storage.container.getMap(i);
        for (auto it = inner_map.getMap().begin(); it != inner_map.getMap().end(); it++)
        {
            auto new_node = new_storage.container.get(it->first);
            ASSERT_TRUE(new_node != nullptr);
            ASSERT_EQ(new_node->data, it->second->data);

            ASSERT_EQ(new_node->is_ephemeral, it->second->is_ephemeral);
            ASSERT_EQ(new_node->is_sequental, it->second->is_sequental);
            ASSERT_EQ(new_node->stat, it->second->stat);
            ASSERT_EQ(new_node->children, it->second->children);
        }
    }
    ASSERT_EQ(new_storage.container.get("/1020/test112")->data, "test211");

    /// compare ephemeral
    ASSERT_EQ(new_storage.ephemerals.size(), storage.ephemerals.size());
    ASSERT_EQ(storage.ephemerals.size(),1);
    for (const auto & [session_id, paths] : storage.ephemerals)
    {
        ASSERT_FALSE(new_storage.ephemerals.find(session_id) == new_storage.ephemerals.end());
        ASSERT_EQ(paths, new_storage.ephemerals.find(session_id)->second);
    }

    /// compare sessions
    ASSERT_EQ(storage.session_and_timeout.size(),10004);
    ASSERT_EQ(storage.session_and_timeout.size(), new_storage.session_and_timeout.size());
    ASSERT_EQ(storage.session_and_timeout, new_storage.session_and_timeout);

    /// compare Others(int_map)
    ASSERT_EQ(storage.session_id_counter,10004);
    ASSERT_EQ(storage.session_id_counter, new_storage.session_id_counter);
    ASSERT_EQ(storage.zxid, new_storage.zxid);

    for (int i = last_index; i < 2 * last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(storage, key, value);
    }
    ASSERT_EQ(storage.container.size(),4100);
    sleep(1); /// snapshot_create_interval minest is 1
    snapshot meta2(2 * last_index, term, config);
    object_size = snap_mgr.createSnapshot(meta2, storage);

    KeeperSnapshotManager new_snap_mgr(snap_dir, 1, 100);
    ASSERT_EQ(new_snap_mgr.loadSnapshotMetas(), 2);
    ASSERT_EQ(new_snap_mgr.lastSnapshot()->get_last_log_idx(), 4096);

    ASSERT_EQ(new_snap_mgr.removeSnapshots(), 1);

    cleanDirectory(snap_dir);
}
