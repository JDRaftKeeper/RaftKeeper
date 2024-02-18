#include <Poco/File.h>

#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>

#include <Service/KeeperUtils.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/tests/raft_test_common.h>


using namespace nuraft;
using namespace RK;


TEST(RaftLog, writeAndReadUInt32)
{
    auto ofs = cs_new<std::fstream>();
    ofs->open("./tmptest", std::ios::out | std::ios::binary);
    UInt32 x1 = 25, x2 = 3421592344;
    writeUInt32(ofs, x1);
    writeUInt32(ofs, x2);
    ofs->flush();

    auto ifs = cs_new<std::fstream>();
    ifs->open("./tmptest", std::ios::in | std::ios::binary);

    ifs->seekg(0, ifs->beg);
    UInt32 y1, y2;
    readUInt32(ifs, y1);
    readUInt32(ifs, y2);

    ofs->close();
    ifs->close();
    ASSERT_EQ(x1, y1);
    ASSERT_EQ(x2, y2);
}

TEST(RaftLog, serializeStr)
{
    String str("a string buffer");
    size_t size = str.size();
    ptr<buffer> log_buf = buffer::alloc(sizeof(uint32_t) + size);
    log_buf->put(str);
    log_buf->pos(0);
    String str2(log_buf->get_str());
    ASSERT_EQ(str, str2);
}

TEST(RaftLog, serializeRaw)
{
    const char * str = "a string buffer";
    size_t size = sizeof(uint64_t) + strlen(str);
    ptr<buffer> log_buf = buffer::alloc(size);
    log_buf->put(static_cast<ulong>(strlen(str)));
    log_buf->put_raw(reinterpret_cast<const byte *>(str), strlen(str));
    log_buf->pos(0);
    uint64_t len = log_buf->get_ulong();
    const char * save_buf = reinterpret_cast<const char *>(log_buf->get_raw(len));
    ASSERT_EQ(std::memcmp(save_buf, str, strlen(str)), 0);
}

TEST(RaftLog, serializeEntry)
{
    UInt64 term = 1;
    String key("/ck/table/table1");
    String data("CREATE TABLE table1;");

    auto log = createLogEntry(term, key, data);

    ptr<buffer> log_buf = log->serialize();
    ptr<log_entry> deserialized_log = log_entry::deserialize(*log_buf);

    ASSERT_EQ(deserialized_log->get_term(), term);
    auto zk_create_request = getZookeeperCreateRequest(deserialized_log);

    ASSERT_EQ(zk_create_request->path, key);
    ASSERT_EQ(zk_create_request->data, data);
}

TEST(RaftLog, appendEntry)
{
    String log_dir(LOG_DIR + "/1");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);

    UInt64 term = 1;
    String key("/ck/table/table1");
    String data("CREATE TABLE table1;");

    ASSERT_EQ(appendEntry(log_store, term, key, data), 1);
    cleanDirectory(log_dir);
}

TEST(RaftLog, appendSomeEntry)
{
    String log_dir(LOG_DIR + "/2");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);

    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }
    log_store->close();
    cleanDirectory(log_dir);
}

TEST(RaftLog, loadLog)
{
    String log_dir(LOG_DIR + "/3");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);
    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }
    ASSERT_EQ(log_store->close(), 0);
    //Load prev log segment from disk when log_store init.
    ASSERT_EQ(log_store->init(), 0);
    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 4);
    }
    cleanDirectory(log_dir);
}


TEST(RaftLog, splitSegment)
{
    String log_dir(LOG_DIR + "/4");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(200, 10), 0); //81 byte / log
    for (int i = 0; i < 12; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }
    ASSERT_EQ(log_store->getClosedSegments().size(), 5);
    ASSERT_EQ(log_store->close(), 0);
    cleanDirectory(log_dir);
}

TEST(RaftLog, removeSegment)
{
    String log_dir(LOG_DIR + "/5");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(200, 3), 0);
    //5 segment
    for (int i = 0; i < 10; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }

    //[1,2],[3,4],[5,6],[7，8],[9,open]
    ASSERT_EQ(log_store->getClosedSegments().size(), 4);
    ASSERT_EQ(log_store->removeSegment(3), 0); //remove first segment[1,2]
    ASSERT_EQ(log_store->getClosedSegments().size(), 3);
    ASSERT_EQ(log_store->firstLogIndex(), 3);
    ASSERT_EQ(log_store->lastLogIndex(), 10);
    ASSERT_EQ(log_store->removeSegment(), 0); //remove more than MAX_SEGMENT_COUNT segment
    ASSERT_EQ(log_store->getClosedSegments().size(), 2); //2 finish_segment + 1 open_segment = 3
    ASSERT_EQ(log_store->firstLogIndex(), 5);
    ASSERT_EQ(log_store->lastLogIndex(), 10);
    ASSERT_EQ(log_store->close(), 0);
    //cleanDirectory(log_dir);
}

TEST(RaftLog, truncateLog)
{
    String log_dir(LOG_DIR + "/6");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(200, 3), 0);
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }
    ASSERT_EQ(log_store->getClosedSegments().size(), 7);
    ASSERT_EQ(log_store->lastLogIndex(), 16);

    ASSERT_EQ(log_store->truncateLog(15), 0); //truncate open segment
    ASSERT_EQ(log_store->lastLogIndex(), 15);

    ptr<log_entry> log = log_store->getEntry(15);
    ASSERT_EQ(log->get_term(), 1);
    ASSERT_EQ(log->get_val_type(), app_log);
    auto zk_create_request = getZookeeperCreateRequest(log);
    ASSERT_EQ("/ck/table/table1", zk_create_request->path);
    ASSERT_EQ("CREATE TABLE table1;", zk_create_request->data);

    ASSERT_EQ(log_store->getClosedSegments().size(), 7);
    ASSERT_EQ(log_store->truncateLog(13), 0); //truncate close and open segment
    ASSERT_EQ(log_store->getClosedSegments().size(), 6);
    ASSERT_EQ(log_store->lastLogIndex(), 13); //truncate close and open segment

    ASSERT_EQ(log_store->truncateLog(2), 0); //truncate close and open segment

    ptr<log_entry> log2 = log_store->getEntry(2);
    ASSERT_EQ(log2->get_term(), 1);
    ASSERT_EQ(log2->get_val_type(), app_log);
    auto zk_create_request2 = getZookeeperCreateRequest(log2);
    ASSERT_EQ("/ck/table/table1", zk_create_request2->path);
    ASSERT_EQ("CREATE TABLE table1;", zk_create_request2->data);

    ASSERT_EQ(log_store->truncateLog(1), 0); //truncate close and open segment

    ptr<log_entry> log3 = log_store->getEntry(1);
    ASSERT_EQ(log3->get_term(), 1);
    ASSERT_EQ(log3->get_val_type(), app_log);
    auto zk_create_request3 = getZookeeperCreateRequest(log3);
    ASSERT_EQ("/ck/table/table1", zk_create_request3->path);
    ASSERT_EQ("CREATE TABLE table1;", zk_create_request3->data);

    ASSERT_EQ(log_store->close(), 0);
    cleanDirectory(log_dir);
}

TEST(RaftLog, writeAt)
{
    String log_dir(LOG_DIR + "/9");
    cleanDirectory(log_dir);
    ptr<NuRaftFileLogStore> file_store = cs_new<NuRaftFileLogStore>(log_dir, true);

    UInt64 term = 1;
    String key("/ck");
    String data("CRE;");
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        ptr<log_entry> log = createLogEntry(term, key, data);
        ASSERT_EQ(file_store->append(log), i + 1);
    }

    ptr<cluster_config> new_conf = cs_new<cluster_config>(454570345, 454569083, false);
    ptr<srv_config> srv_conf1 = cs_new<srv_config>(4, 0, "10.199.141.7:8103", "", 0, 1);
    ptr<srv_config> srv_conf2 = cs_new<srv_config>(13, 0, "10.199.141.8:8103", "", 0, 1);
    ptr<srv_config> srv_conf3 = cs_new<srv_config>(15, 0, "10.199.141.6:8103", "", 0, 1);
    new_conf->get_servers().push_back(srv_conf1);
    new_conf->get_servers().push_back(srv_conf2);
    new_conf->get_servers().push_back(srv_conf3);
    new_conf->set_user_ctx("");
    ptr<buffer> new_conf_buf(new_conf->serialize());
    ptr<log_entry> entry(cs_new<log_entry>(2, new_conf_buf, log_val_type::conf));

    file_store->write_at(8, entry);

    ptr<log_entry> log = file_store->entry_at(8);
    ptr<cluster_config> new_conf1 = cluster_config::deserialize(log->get_buf());

    ASSERT_EQ(new_conf1->get_servers().size(), 3);
    ASSERT_EQ(new_conf1->get_log_idx(), 454570345);
    ASSERT_EQ(new_conf1->get_prev_log_idx(), 454569083);
    ASSERT_EQ(new_conf1->get_user_ctx(), "");
    ASSERT_EQ(new_conf1->is_async_replication(), false);
    ASSERT_EQ(log->get_term(), 2);
    ASSERT_EQ(log->get_val_type(), conf);

    auto entry_log = createLogEntry(term, "/ck/table/table2", "CREATE TABLE table2;");
    file_store->write_at(9, entry_log);

    term = 2;
    auto entry_log1 = createLogEntry(term, "/ck/table/table2222222222222222222221", "CREATE TABLE table222222222222222222222333;");
    file_store->write_at(10, entry_log1);

    ptr<log_entry> log1 = file_store->entry_at(10);
    ASSERT_EQ(log1->get_term(), 2);
    ASSERT_EQ(log1->get_val_type(), app_log);
    auto zk_request1 = getZookeeperCreateRequest(log1);
    ASSERT_EQ("/ck/table/table2222222222222222222221", zk_request1->path);
    ASSERT_EQ("CREATE TABLE table222222222222222222222333;", zk_request1->data);

    key = "/ck/table/table22222222222233312222221";
    data = "CREATE TABLE table22222222221111123222222222333;";
    term = 3;
    ptr<log_entry> entry_log2 = createLogEntry(term, key, data);
    ASSERT_EQ(file_store->append(entry_log2), 11);

    ptr<log_entry> log2 = file_store->entry_at(11);
    ASSERT_EQ(log2->get_term(), 3);
    ASSERT_EQ(log2->get_val_type(), app_log);
    auto zk_request2 = getZookeeperCreateRequest(log2);
    ASSERT_EQ("/ck/table/table22222222222233312222221", zk_request2->path);
    ASSERT_EQ("CREATE TABLE table22222222221111123222222222333;", zk_request2->data);
}


TEST(RaftLog, compact)
{
    String log_dir(LOG_DIR + "/10");
    cleanDirectory(log_dir);
    ptr<NuRaftFileLogStore> file_store
        = cs_new<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC_PARALLEL, 1000, static_cast<UInt32>(200), static_cast<UInt32>(3));

    UInt64 term = 1;
    String key("/ck/table/table1");
    String data("CREATE TABLE table1;");
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        ptr<log_entry> entry_log = createLogEntry(term, key, data);
        ASSERT_EQ(file_store->append(entry_log), i + 1);
    }

    ASSERT_EQ(file_store->segmentStore()->getClosedSegments().size(), 7);

    file_store->compact(3);

    ASSERT_EQ(file_store->start_index(), 3);
    ASSERT_EQ(file_store->segmentStore()->lastLogIndex(), 16);

    ASSERT_EQ(file_store->segmentStore()->getClosedSegments().size(), 6);
    ptr<log_entry> log1 = file_store->entry_at(3);
    ASSERT_EQ(log1->get_term(), 1);
    ASSERT_EQ(log1->get_val_type(), app_log);
    auto zk_request1 = getZookeeperCreateRequest(log1);
    ASSERT_EQ("/ck/table/table1", zk_request1->path);
    ASSERT_EQ("CREATE TABLE table1;", zk_request1->data);

    key = "/ck/table/table22222222222233312222221";
    data = "CREATE TABLE table22222222221111123222222222333;";
    ptr<log_entry> entry_log2 = createLogEntry(term, key, data);
    ASSERT_EQ(file_store->append(entry_log2), 17);

    ASSERT_EQ(file_store->segmentStore()->lastLogIndex(), 17);
    ptr<log_entry> log2 = file_store->entry_at(17);
    ASSERT_EQ(log2->get_term(), 1);
    ASSERT_EQ(log2->get_val_type(), app_log);
    auto zk_request2 = getZookeeperCreateRequest(log2);
    ASSERT_EQ("/ck/table/table22222222222233312222221", zk_request2->path);
    ASSERT_EQ("CREATE TABLE table22222222221111123222222222333;", zk_request2->data);
}

TEST(RaftLog, getEntry)
{
    String log_dir(LOG_DIR + "/7");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(100, 3), 0);
    UInt64 term = 1;
    String key("/ck/table/table1");
    String data("CREATE TABLE table1;");
    for (int i = 0; i < 6; i++)
    {
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }

    for (int i = 0; i < 6; i++)
    {
        ptr<log_entry> log = log_store->getEntry(i + 1);
        ASSERT_EQ(log->get_term(), term);
        ASSERT_EQ(log->get_val_type(), app_log);

        ASSERT_EQ(log->get_term(), 1);
        ASSERT_EQ(log->get_val_type(), app_log);
        auto zk_request = getZookeeperCreateRequest(log);
        ASSERT_EQ("/ck/table/table1", zk_request->path);
        ASSERT_EQ("CREATE TABLE table1;", zk_request->data);
    }
    log_store->close();
    cleanDirectory(log_dir);
}

TEST(RaftLog, getEntries)
{
    String log_dir(LOG_DIR + "/8");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(250, 3), 0); //69 * 4 = 276
    for (int i = 0; i < 8; i++)
    {
        UInt64 term = 1;
        String key("/ck/table/table1");
        String data("CREATE TABLE table1;");
        ASSERT_EQ(appendEntry(log_store, term, key, data), i + 1);
    }
    ptr<std::vector<ptr<log_entry>>> ret = cs_new<std::vector<ptr<log_entry>>>();
    log_store->getEntries(1, 3, ret);
    ASSERT_EQ(ret->size(), 3);
    ret->clear();
    log_store->getEntries(4, 8, ret);
    ASSERT_EQ(ret->size(), 5);
    log_store->close();
    cleanDirectory(log_dir);
}

int main(int argc, char ** argv)
{
    RK::TestServer app;
    app.init(argc, argv);
    app.run();
    //thread_env = new ThreadEnvironment();
    testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    //delete thread_env;
    return ret;
}
