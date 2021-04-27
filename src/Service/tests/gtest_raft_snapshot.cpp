#include <string>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/SvsKeeperSettings.h>
#include <Service/SvsKeeperStorage.h>
#include <Service/proto/Log.pb.h>
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
    storage.processRequest(request, 0);
}

}

TEST(RaftSnapshot, createSnapshot_1)
{
    std::string snap_dir(SNAP_DIR + "/1");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 10, 3);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);
    snapshot snap_meta(1, 1, config);

    CoordinationSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    setNode(storage, "1", "table_1");
    ASSERT_EQ(storage.container.size(), 2); /// it's has "/" and "/1"
    size_t object_size = snap_mgr.createSnapshot(snap_meta, storage);
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM);
    cleanDirectory(snap_dir);
}

TEST(RaftSnapshot, createSnapshot_2)
{
    std::string snap_dir(SNAP_DIR + "/2");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 100, 3);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    CoordinationSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
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
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM);
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
    KeeperSnapshotManager snap_mgr_read(snap_read_dir, 100, 3);
    KeeperSnapshotManager snap_mgr_save(snap_save_dir, 100, 3);

    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    CoordinationSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
    SvsKeeperStorage storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    for (int i = 0; i < last_index; i++)
    {
        std::string key = std::to_string(i + 1);
        std::string value = "table_" + key;
        setNode(storage, key, value);
    }
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr_read.createSnapshot(meta, storage);
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM);

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
    KeeperSnapshotManager snap_mgr(snap_dir, 100, 3);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    CoordinationSettingsPtr coordination_settings(std::make_shared<SvsKeeperSettings>());
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
    ASSERT_EQ(object_size, SvsKeeperStorage::MAP_BLOCK_NUM);

    SvsKeeperStorage new_storage(coordination_settings->dead_session_check_period_ms.totalMilliseconds());

    ASSERT_TRUE(snap_mgr.parseSnapshot(meta, new_storage));
    for (UInt32 i = 0; i < storage.container.getBlockNum(); i++)
    {
        auto & inner_map = storage.container.getMap(i);
        for (auto it = inner_map.getMap().begin(); it != inner_map.getMap().end(); it++)
        {
            auto new_node = storage.container.get(it->first);
            ASSERT_TRUE(new_node != nullptr);
            ASSERT_EQ(new_node->data, it->second->data);
        }
    }
    cleanDirectory(snap_dir);
}