#include <Service/NuRaftCommon.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/proto/Log.pb.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <loggers/Loggers.h>
#include <Poco/File.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <common/argsToConfig.h>


using namespace nuraft;
using namespace DB;

namespace DB
{
TestServer::TestServer() = default;
TestServer::~TestServer()
{
}

void TestServer::init(int argc, char ** argv)
{
    namespace po = boost::program_options;
    /// Don't parse options with Poco library, we prefer neat boost::program_options
    stopOptionsProcessing();
    /// Save received data into the internal config.
    config().setBool("stacktrace", true);
    config().setBool("logger.console", true);
    config().setString("logger.log", "./test.logs");
    const char * tag = argv[1];
    const char * loglevel = "information";
    if (argc >= 2)
    {
        if (strcmp(tag, "debug"))
        {
            loglevel = "debug";
        }
    }
    config().setString("logger.level", loglevel);

    config().setString("logger.level", "information");

    config().setBool("ignore-error", false);

    std::vector<std::string> arguments;
    for (int arg_num = 1; arg_num < argc; ++arg_num)
        arguments.emplace_back(argv[arg_num]);
    argsToConfig(arguments, config(), 100);

    if (config().has("logger.console") || config().has("logger.level") || config().has("logger.log"))
    {
        // force enable logging
        config().setString("logger", "logger");
        // sensitive data rules are not used here
        buildLoggers(config(), logger(), "clickhouse-local");
    }
}

void cleanDirectory(const std::string & log_dir, bool remove_dir)
{
    Poco::File dir_obj(log_dir);
    if (dir_obj.exists())
    {
        std::vector<std::string> files;
        dir_obj.list(files);
        for (auto file : files)
        {
            Poco::File(log_dir + "/" + file).remove();
        }
        if (remove_dir)
        {
            dir_obj.remove();
        }
    }
}

ptr<LogEntryPB> createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data)
{
    ptr<LogEntryPB> entry_pb = cs_new<LogEntryPB>();
    entry_pb->set_entry_type(ENTRY_TYPE_DATA);
    entry_pb->mutable_log_index()->set_term(term);
    entry_pb->mutable_log_index()->set_index(index);
    LogDataPB * data_pb = entry_pb->add_data();
    data_pb->set_op_type(op);
    data_pb->set_key(key);
    data_pb->set_data(data);
    return entry_pb;
}

ptr<LogEntryPB> createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, const std::string & key, const std::string & data)
{
    ptr<LogEntryPB> entry_pb = cs_new<LogEntryPB>();
    entry_pb->set_entry_type(ENTRY_TYPE_DATA);
    entry_pb->mutable_log_index()->set_term(term);
    entry_pb->mutable_log_index()->set_index(index);
    LogDataPB * data_pb = entry_pb->add_data();
    data_pb->set_op_type(op);
    data_pb->set_key(key);
    data_pb->set_data(data);
    return entry_pb;
}

void createEntryPB(UInt64 term, UInt64 index, LogOpTypePB op, std::string & key, std::string & data, ptr<LogEntryPB> & entry_pb)
{
    entry_pb = cs_new<LogEntryPB>();
    entry_pb->set_entry_type(ENTRY_TYPE_DATA);
    entry_pb->mutable_log_index()->set_term(term);
    entry_pb->mutable_log_index()->set_index(index);
    LogDataPB * data_pb = entry_pb->add_data();
    data_pb->set_op_type(op);
    data_pb->set_key(key);
    data_pb->set_data(data);
}

void createEntry(UInt64 term, LogOpTypePB op, std::string & key, std::string & data, std::vector<ptr<log_entry>> & entry_vec)
{
    auto entry_pb = createEntryPB(term, 0, op, key, data);
    ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
    ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);
    entry_vec.push_back(entry_log);
}

}

UInt64 appendEntry(ptr<LogSegmentStore> store, UInt64 term, LogOpTypePB op, std::string & key, std::string & data)
{
    auto entry_pb = createEntryPB(term, 0, op, key, data);
    ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
    ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);
    return store->appendEntry(entry_log);
}

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
    std::string str("a string buffer");
    size_t size = str.size();
    ptr<buffer> msg_buf = buffer::alloc(sizeof(uint32_t) + size);
    msg_buf->put(str);
    msg_buf->pos(0);
    std::string str2(msg_buf->get_str());
    ASSERT_EQ(str, str2);
}

TEST(RaftLog, serializeRaw)
{
    const char * str = "a string buffer";
    size_t size = sizeof(long) + strlen(str);
    ptr<buffer> msg_buf = buffer::alloc(size);
    msg_buf->put(static_cast<ulong>(strlen(str)));
    msg_buf->put_raw(reinterpret_cast<const byte *>(str), strlen(str));
    msg_buf->pos(0);
    long len = msg_buf->get_ulong();
    const char * save_buf = reinterpret_cast<const char *>(msg_buf->get_raw(len));
    ASSERT_EQ(std::memcmp(save_buf, str, strlen(str)), 0);
}

TEST(RaftLog, serializeProto)
{
    //Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    UInt64 term = 1, index = 1;
    std::string key("/ck/table/table1");
    std::string data("CREATE TABLE table1;");
    LogOpTypePB op = OP_TYPE_CREATE;
    auto entry_pb = createEntryPB(term, index, op, key, data);

    ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
    ptr<LogEntryPB> entry_pb_1 = LogEntry::parsePB(msg_buf);

    //test proto buf serialize
    ASSERT_EQ(entry_pb_1->log_index().term(), term);
    ASSERT_EQ(entry_pb_1->log_index().index(), index);
    ASSERT_EQ(entry_pb_1->data_size(), 1);
    ASSERT_EQ(entry_pb_1->data(0).op_type(), op);
    ASSERT_EQ(entry_pb_1->data(0).key(), key);
    ASSERT_EQ(entry_pb_1->data(0).data(), data);
}

TEST(RaftLog, serializeStr2Raw)
{
    //Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    std::string str("a string buffer");
    ulong term = 1;
    size_t size = sizeof(uint32_t) + str.size();
    ptr<buffer> msg_buf_1 = buffer::alloc(size);
    msg_buf_1->put(str);
    msg_buf_1->pos(0);

    ptr<buffer> entry_buf_1 = buffer::alloc(sizeof(ulong) + msg_buf_1->size());
    entry_buf_1->put(term);
    entry_buf_1->put(*msg_buf_1);
    entry_buf_1->pos(0);

    const char * save_buf = reinterpret_cast<const char *>(entry_buf_1->get_raw(entry_buf_1->size()));

    ptr<buffer> entry_buf_2 = buffer::alloc(entry_buf_1->size());
    entry_buf_2->put_raw(reinterpret_cast<const byte *>(save_buf), entry_buf_1->size());
    entry_buf_2->pos(0);
    ASSERT_EQ(entry_buf_2->get_ulong(), term);

    //ASSERT_EQ(strlen(str), len);
    //const char * str2 = reinterpret_cast<const char *>(msg_buf_2->get_raw(len));
    //ASSERT_EQ(std::memcmp(str2, str, len), 0);
}

TEST(RaftLog, serializeEntry)
{
    UInt64 term = 1, index = 1;
    std::string key("/ck/table/table1");
    std::string data("CREATE TABLE table1;");
    LogOpTypePB op = OP_TYPE_CREATE;

    auto entry_pb_1 = createEntryPB(term, index, op, key, data);

    ptr<buffer> msg_buf_1 = LogEntry::serializePB(entry_pb_1);

    ptr<log_entry> entry_log_1 = cs_new<log_entry>(term, msg_buf_1);

    ptr<buffer> entry_buf;
    size_t buf_size;
    const char * entry_str = LogEntry::serializeEntry(entry_log_1, entry_buf, buf_size);
    ptr<log_entry> entry_log_2 = LogEntry::parseEntry(entry_str, term, buf_size);
    ASSERT_EQ(entry_log_1->get_term(), entry_log_2->get_term());

    ptr<LogEntryPB> entry_pb_2 = LogEntry::parsePB(entry_log_2->get_buf());
    ASSERT_EQ(entry_pb_1->log_index().term(), term);
    ASSERT_EQ(entry_pb_1->log_index().index(), index);
    ASSERT_EQ(entry_pb_1->data_size(), 1);
    ASSERT_EQ(entry_pb_1->data(0).op_type(), op);
    ASSERT_STREQ(entry_pb_1->data(0).key().c_str(), key.c_str());
    ASSERT_STREQ(entry_pb_1->data(0).data().c_str(), data.c_str());
}

TEST(RaftLog, appendEntry)
{
    std::string log_dir(LOG_DIR + "/1");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);

    UInt64 term = 1;
    std::string key("/ck/table/table1");
    std::string data("CREATE TABLE table1;");
    LogOpTypePB op = OP_TYPE_CREATE;

    ASSERT_EQ(appendEntry(log_store, term, op, key, data), 1);
    cleanDirectory(log_dir);
}

TEST(RaftLog, appendSomeEntry)
{
    std::string log_dir(LOG_DIR + "/2");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);

    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }
    log_store->close();
    cleanDirectory(log_dir);
}

TEST(RaftLog, loadLog)
{
    std::string log_dir(LOG_DIR + "/3");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(), 0);
    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }
    ASSERT_EQ(log_store->close(), 0);
    //Load prev log segment from disk when log_store init.
    ASSERT_EQ(log_store->init(), 0);
    for (int i = 0; i < 3; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 4);
    }
    cleanDirectory(log_dir);
}


TEST(RaftLog, splitSegment)
{
    std::string log_dir(LOG_DIR + "/4");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(200, 10), 0); //75 byte / log
    for (int i = 0; i < 12; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }
    ASSERT_EQ(log_store->getSegments().size(), 3);
    ASSERT_EQ(log_store->close(), 0);
    cleanDirectory(log_dir);
}

TEST(RaftLog, removeSegment)
{
    std::string log_dir(LOG_DIR + "/5");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(100, 3), 0);
    //5 segment
    for (int i = 0; i < 10; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }

    //[1,2],[3,4],[5,6],[7，8],[9,open]
    ASSERT_EQ(log_store->getSegments().size(), 4);    
    ASSERT_EQ(log_store->removeSegment(3), 0); //remove first segment[1,2]    
    ASSERT_EQ(log_store->getSegments().size(), 3);
    ASSERT_EQ(log_store->firstLogIndex(), 3);
    ASSERT_EQ(log_store->lastLogIndex(), 10);
    ASSERT_EQ(log_store->removeSegment(), 0); //remove more than MAX_SEGMENT_COUNT segment    
    ASSERT_EQ(log_store->getSegments().size(), 2); //2 finish_segment + 1 open_segment = 3
    ASSERT_EQ(log_store->firstLogIndex(), 5);
    ASSERT_EQ(log_store->lastLogIndex(), 10);
    ASSERT_EQ(log_store->close(), 0);
    //cleanDirectory(log_dir);
}

TEST(RaftLog, truncateLog)
{
    std::string log_dir(LOG_DIR + "/6");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(100, 3), 0);
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }
    ASSERT_EQ(log_store->getSegments().size(), 7);
    ASSERT_EQ(log_store->lastLogIndex(),16);

    ASSERT_EQ(log_store->truncateLog(15), 0); //tuncate open segment
    ASSERT_EQ(log_store->lastLogIndex(),15);

    ptr<log_entry> log = log_store->getEntry(15);
    ASSERT_EQ(log->get_term(), 1);
    ASSERT_EQ(log->get_val_type(), app_log);
    ptr<LogEntryPB> pb = LogEntry::parsePB(log->get_buf());
    ASSERT_EQ(pb->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb->data_size(), 1);
    ASSERT_EQ("/ck/table/table1", pb->data(0).key());
    ASSERT_EQ("CREATE TABLE table1;", pb->data(0).data());

    ASSERT_EQ(log_store->getSegments().size(), 7);
    ASSERT_EQ(log_store->truncateLog(13), 0); //tuncate close and open segment
    ASSERT_EQ(log_store->getSegments().size(), 6);
    ASSERT_EQ(log_store->lastLogIndex(), 13); //tuncate close and open segment

    ASSERT_EQ(log_store->truncateLog(2), 0); //tuncate close and open segment

    ptr<log_entry> log2 = log_store->getEntry(2);
    ASSERT_EQ(log2->get_term(), 1);
    ASSERT_EQ(log2->get_val_type(), app_log);
    ptr<LogEntryPB> pb2 = LogEntry::parsePB(log2->get_buf());
    ASSERT_EQ(pb2->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb2->data_size(), 1);
    ASSERT_EQ("/ck/table/table1", pb2->data(0).key());
    ASSERT_EQ("CREATE TABLE table1;", pb2->data(0).data());

    ASSERT_EQ(log_store->truncateLog(1), 0); //tuncate close and open segment

    ptr<log_entry> log3 = log_store->getEntry(1);
    ASSERT_EQ(log3->get_term(), 1);
    ASSERT_EQ(log3->get_val_type(), app_log);
    ptr<LogEntryPB> pb3 = LogEntry::parsePB(log3->get_buf());
    ASSERT_EQ(pb3->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb3->data_size(), 1);
    ASSERT_EQ("/ck/table/table1", pb3->data(0).key());
    ASSERT_EQ("CREATE TABLE table1;", pb3->data(0).data());

    ASSERT_EQ(log_store->close(), 0);
    //cleanDirectory(log_dir);
}

TEST(RaftLog, writeAt)
{
    std::string log_dir(LOG_DIR + "/9");
    cleanDirectory(log_dir);
    ptr<NuRaftFileLogStore> file_store = cs_new<NuRaftFileLogStore>(log_dir, true);

    UInt64 term = 1;
    std::string key("/ck");
    std::string data("CRE;");
    LogOpTypePB op = OP_TYPE_CREATE;
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        auto entry_pb = createEntryPB(term, 0, op, key, data);
        ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
        ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);
        ASSERT_EQ(file_store->append(entry_log), i + 1);
    }

    ptr<cluster_config> new_conf = cs_new<cluster_config>
        ( 454570345,
          454569083, false );
    ptr<srv_config> srv_conf1 = cs_new<srv_config>(4, 0, "10.199.141.7:5103", "", 0, 1);
    ptr<srv_config> srv_conf2 = cs_new<srv_config>(13, 0, "10.199.141.8:5103", "", 0, 1);
    ptr<srv_config> srv_conf3 = cs_new<srv_config>(15, 0, "10.199.141.6:5103", "", 0, 1);
    new_conf->get_servers().push_back(srv_conf1);
    new_conf->get_servers().push_back(srv_conf2);
    new_conf->get_servers().push_back(srv_conf3);
    new_conf->set_user_ctx( "" );
    ptr<buffer> new_conf_buf( new_conf->serialize() );
    ptr<log_entry> entry( cs_new<log_entry>( 2, new_conf_buf, log_val_type::conf ) );

    file_store->write_at(8, entry);

    ptr<log_entry> log = file_store->entry_at(8);
    ptr<cluster_config> new_conf1 =
        cluster_config::deserialize(log->get_buf());

    ASSERT_EQ(new_conf1->get_servers().size(), 3);
    ASSERT_EQ(new_conf1->get_log_idx(), 454570345);
    ASSERT_EQ(new_conf1->get_prev_log_idx(), 454569083);
    ASSERT_EQ(new_conf1->get_user_ctx(), "");
    ASSERT_EQ(new_conf1->is_async_replication(), false);
    ASSERT_EQ(log->get_term(), 2);
    ASSERT_EQ(log->get_val_type(), conf);

    auto entry_pb = createEntryPB(term, 0, op, "/ck/table/table2", "CREATE TABLE table2;");
    ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
    ptr<log_entry> entry_log = cs_new<log_entry>(2, msg_buf);
    file_store->write_at(9, entry_log);

    auto entry_pb1 = createEntryPB(term, 0, op, "/ck/table/table2222222222222222222221", "CREATE TABLE table222222222222222222222333;");
    ptr<buffer> msg_buf1 = LogEntry::serializePB(entry_pb1);
    ptr<log_entry> entry_log1 = cs_new<log_entry>(2, msg_buf1);
    file_store->write_at(10, entry_log1);

    ptr<log_entry> log1 = file_store->entry_at(10);
    ASSERT_EQ(log1->get_term(), 2);
    ASSERT_EQ(log1->get_val_type(), app_log);
    ptr<LogEntryPB> pb1 = LogEntry::parsePB(log1->get_buf());
    ASSERT_EQ(pb1->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb1->data_size(), 1);
    ASSERT_EQ("/ck/table/table2222222222222222222221", pb1->data(0).key());
    ASSERT_EQ("CREATE TABLE table222222222222222222222333;", pb1->data(0).data());

    key = "/ck/table/table22222222222233312222221";
    data = "CREATE TABLE table22222222221111123222222222333;";
    auto entry_pb2 = createEntryPB(term, 0, op, key, data);
    ptr<buffer> msg_buf2 = LogEntry::serializePB(entry_pb2);
    ptr<log_entry> entry_log2 = cs_new<log_entry>(term, msg_buf2);
    ASSERT_EQ(file_store->append(entry_log2), 11);

    ptr<log_entry> log2 = file_store->entry_at(11);
    ASSERT_EQ(log2->get_term(), 1);
    ASSERT_EQ(log2->get_val_type(), app_log);
    ptr<LogEntryPB> pb2 = LogEntry::parsePB(log2->get_buf());
    ASSERT_EQ(pb2->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb2->data_size(), 1);
    ASSERT_EQ("/ck/table/table22222222222233312222221", pb2->data(0).key());
    ASSERT_EQ("CREATE TABLE table22222222221111123222222222333;", pb2->data(0).data());

//    ASSERT_EQ(file_store->close(), 0);
    //cleanDirectory(log_dir);
}


TEST(RaftLog, compact)
{
    std::string log_dir(LOG_DIR + "/10");
    cleanDirectory(log_dir);
    ptr<NuRaftFileLogStore> file_store = cs_new<NuRaftFileLogStore>(log_dir, true, 100, 3);

    UInt64 term = 1;
    std::string key("/ck/table/table1");
    std::string data("CREATE TABLE table1;");
    LogOpTypePB op = OP_TYPE_CREATE;
    //8 segment, index 1-16
    for (int i = 0; i < 16; i++)
    {
        auto entry_pb = createEntryPB(term, 0, op, key, data);
        ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
        ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);
        ASSERT_EQ(file_store->append(entry_log), i + 1);
    }

    ASSERT_EQ(file_store->segmentStore()->getSegments().size(), 7);

    file_store->compact(3);

    ASSERT_EQ(file_store->start_index(), 3);
    ASSERT_EQ(file_store->segmentStore()->lastLogIndex(), 16);

    ASSERT_EQ(file_store->segmentStore()->getSegments().size(), 6);
    ptr<log_entry> log1 = file_store->entry_at(3);
    ASSERT_EQ(log1->get_term(), 1);
    ASSERT_EQ(log1->get_val_type(), app_log);
    ptr<LogEntryPB> pb1 = LogEntry::parsePB(log1->get_buf());
    ASSERT_EQ(pb1->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb1->data_size(), 1);
    ASSERT_EQ(key, pb1->data(0).key());
    ASSERT_EQ(data, pb1->data(0).data());

    key = "/ck/table/table22222222222233312222221";
    data = "CREATE TABLE table22222222221111123222222222333;";
    auto entry_pb2 = createEntryPB(term, 0, op, key, data);
    ptr<buffer> msg_buf2 = LogEntry::serializePB(entry_pb2);
    ptr<log_entry> entry_log2 = cs_new<log_entry>(term, msg_buf2);
    ASSERT_EQ(file_store->append(entry_log2), 17);

    ASSERT_EQ(file_store->segmentStore()->lastLogIndex(), 17);
    ptr<log_entry> log2 = file_store->entry_at(17);
    ASSERT_EQ(log2->get_term(), 1);
    ASSERT_EQ(log2->get_val_type(), app_log);
    ptr<LogEntryPB> pb2 = LogEntry::parsePB(log2->get_buf());
    ASSERT_EQ(pb2->entry_type(), OP_TYPE_CREATE);
    ASSERT_EQ(pb2->data_size(), 1);
    ASSERT_EQ("/ck/table/table22222222222233312222221", pb2->data(0).key());
    ASSERT_EQ("CREATE TABLE table22222222221111123222222222333;", pb2->data(0).data());

//    ASSERT_EQ(file_store->close(), 0);
    //cleanDirectory(log_dir);
}

TEST(RaftLog, getEntry)
{
    std::string log_dir(LOG_DIR + "/7");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(100, 3), 0);
    UInt64 term = 1;
    std::string key("/ck/table/table1");
    std::string data("CREATE TABLE table1;");
    for (int i = 0; i < 6; i++)
    {
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
    }

    for (int i = 0; i < 6; i++)
    {
        ptr<log_entry> log = log_store->getEntry(i + 1);
        ASSERT_EQ(log->get_term(), term);
        ASSERT_EQ(log->get_val_type(), app_log);

        ptr<LogEntryPB> pb = LogEntry::parsePB(log->get_buf());
        //ASSERT_EQ(pb->log_index().term(), term);
        //ASSERT_EQ(pb->log_index().index(), i + 1);
        ASSERT_EQ(pb->entry_type(), OP_TYPE_CREATE);
        ASSERT_EQ(pb->data_size(), 1);
        ASSERT_EQ(key, pb->data(0).key());
        ASSERT_EQ(data, pb->data(0).data());
        //Poco::Logger * LOG = &(Poco::Logger::get("LogTest"));
        //LOG_INFO(LOG, "PB:{},{},{}", pb->log_index().term(), pb->log_index().index(), pb->entry_type());
    }
    log_store->close();
    cleanDirectory(log_dir);
}

TEST(RaftLog, getEntries)
{
    std::string log_dir(LOG_DIR + "/8");
    cleanDirectory(log_dir);
    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ASSERT_EQ(log_store->init(250, 3), 0); //69 * 4 = 276
    for (int i = 0; i < 8; i++)
    {
        UInt64 term = 1;
        std::string key("/ck/table/table1");
        std::string data("CREATE TABLE table1;");
        LogOpTypePB op = OP_TYPE_CREATE;
        ASSERT_EQ(appendEntry(log_store, term, op, key, data), i + 1);
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

//#define ASSERT_EQ_LOG(log, v1, v2) \ -
//    { \ -
//        if (v1 != v2) \ -
//            LOG_WARNING(log, "v1 {}, v2 {} not equal.", v1, v2); \-
//    }

/*
TEST(RaftLog, logSegmentThread)
{
    Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    std::string log_dir(LOG_DIR + "/10");
    cleanDirectory(log_dir);

    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    //10M
    UInt32 max_seg_count = 10;
    ASSERT_EQ(log_store->init(10000000, max_seg_count), 0);

    int key_bytes = 256;
    int value_bytes = 1024;
    //256 byte
    std::string key;
    for (int i = 0; i < key_bytes; i++)
    {
        key.append("k");
    }
    //1024 byte
    std::string data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }

    //std::mutex index_mutex;
    std::vector<int> thread_vec = {8};
    std::atomic<int> log_index = 0;
    int log_count = 100000;
    for (auto thread_count : thread_vec)
    {
        //int end_index = log_index + log_count;
        int thread_log_count = log_count / thread_count;
        FreeThreadPool thread_pool(thread_count);
        Stopwatch watch;
        watch.start();
        for (int thread_idx = 0; thread_idx < thread_count; thread_idx++)
        {
            thread_pool.trySchedule(
                [&log_store, &log_index, thread_count, thread_idx, thread_log_count, log_count, max_seg_count, &key, &data] {
                    UInt64 log_idx(0);
                    UInt64 term = 1;
                    LogOpTypePB op = OP_TYPE_CREATE;

                    auto * thread_log = &Poco::Logger::get("client_thread");
                    LOG_INFO(
                        thread_log,
                        "Begin run thread size {}/{}, append count {}/{}",
                        thread_idx,
                        thread_count,
                        thread_log_count,
                        log_count);
                    try
                    {
                        for (auto idx = 0; idx < thread_log_count; idx++)
                        {
                            ptr<LogEntryPB> entry_pb;
                            createEntryPB(term, 0, op, key, data, entry_pb);
                            ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
                            ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);

                            log_idx = log_store->appendEntry(entry_log);

                            if (log_idx % 1 == 0)
                            {
                                ptr<log_entry> new_log = log_store->getEntry(log_idx);
                                ASSERT_EQ_LOG(thread_log, new_log->get_term(), term);
                                //ASSERT_EQ_LOG(thread_log, new_log->get_val_type(), app_log);
                                ptr<LogEntryPB> pb = LogEntry::parsePB(new_log->get_buf());
                                //ASSERT_EQ_LOG(thread_log, pb->entry_type(), OP_TYPE_CREATE);
                                ASSERT_EQ_LOG(thread_log, pb->data_size(), 1);
                                ASSERT_EQ_LOG(thread_log, key, pb->data(0).key());
                                ASSERT_EQ_LOG(thread_log, data, pb->data(0).data());
                            }

                            log_index.store(log_idx, std::memory_order_relaxed);
                            if (log_store->getSegments().size() + 1 >= max_seg_count)
                            {
                                //remove segment
                                log_store->removeSegment();
                            }
                        }
                    }
                    catch (std::exception & ex)
                    {
                        LOG_ERROR(thread_log, "thread exception : {}", ex.what());
                    }
                });
        }
        //LOG_INFO(log, "Max thread count {}, running {}", thread_pool.getMaxThreads(), thread_pool.active());
        thread_pool.wait();
        watch.stop();
        int mill_second = watch.elapsedMilliseconds();
        int log_size = ((key_bytes + value_bytes) + sizeof(UInt32) * 4 + sizeof(UInt32) * 6);
        double total_size = 1.0 * log_size * log_count / 1000 / 1000; //M
        double byte_rate = 1.0 * total_size / mill_second * 1000;
        double count_rate = 1.0 * log_count / mill_second * 1000;
        LOG_INFO(
            log,
            "Append performance : thread_count {}, size {} Byte/One Log, count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS "
            "{}",
            thread_pool.getMaxThreads(),
            log_size,
            log_count,
            total_size,
            mill_second,
            byte_rate,
            count_rate);
    }
    //cleanDirectory(log_dir);
}
*/

int main(int argc, char ** argv)
{
    DB::TestServer app;
    app.init(argc, argv);
    app.run();
    //thread_env = new ThreadEnvironment();
    testing::InitGoogleTest(&argc, argv);
    int ret = RUN_ALL_TESTS();
    //delete thread_env;
    return ret;
}
