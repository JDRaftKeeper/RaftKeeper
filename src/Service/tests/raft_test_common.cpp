#include <Poco/File.h>

#include <boost/program_options.hpp>
#include <common/argsToConfig.h>
#include <ZooKeeper/ZooKeeperIO.h>

#include <Service/NuRaftFileLogStore.h>
#include <Service/tests/raft_test_common.h>


namespace RK
{

TestServer::TestServer() = default;
TestServer::~TestServer() = default;

void TestServer::init(int argc, char ** argv)
{
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

    std::vector<String> arguments;
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

void cleanDirectory(const String & log_dir, bool remove_dir)
{
    Poco::File dir_obj(log_dir);
    if (dir_obj.exists())
    {
        std::vector<String> files;
        dir_obj.list(files);
        for (auto & file : files)
        {
            Poco::File(log_dir + "/" + file).remove();
        }
        if (remove_dir)
        {
            dir_obj.remove();
        }
    }
}

void cleanAll()
{
    Poco::File log(LOG_DIR);
    Poco::File snap(LOG_DIR);
    if (log.exists())
        log.remove(true);
    if (snap.exists())
        snap.remove(true);
}

ptr<log_entry> createLogEntry(UInt64 term, const String & key, const String & data)
{
    auto zk_create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    zk_create_request->path = key;
    zk_create_request->data = data;
    RequestForSession request_with_session(zk_create_request, 1, 1);
    auto serialized_request = serializeKeeperRequest(request_with_session);
    return std::make_shared<log_entry>(term, serialized_request);
}

UInt64 appendEntry(ptr<LogSegmentStore> store, UInt64 term, String & key, String & data)
{
    return store->appendEntry(createLogEntry(term, key, data));
}


ptr<Coordination::ZooKeeperCreateRequest> getZookeeperCreateRequest(ptr<log_entry> log)
{
    ReadBufferFromMemory buf(log->get_buf().data_begin(), log->get_buf().size());
    int64_t session_id;
    readIntBinary(session_id, buf);
    int32_t req_body_len;
    Coordination::read(req_body_len, buf);
    auto request = Coordination::ZooKeeperRequest::read(buf);
    int64_t time;
    readIntBinary(time, buf);
    ptr<Coordination::ZooKeeperCreateRequest> zk_create_request;
    return std::static_pointer_cast<Coordination::ZooKeeperCreateRequest>(request);
}

void setNode(KeeperStore & storage, const String & key, const String & value, bool is_ephemeral, int64_t session_id)
{
    Coordination::ACLs default_acls;
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    storage.addSessionID(session_id, 30000);

    auto request = cs_new<Coordination::ZooKeeperCreateRequest>();
    request->path = "/" + key;
    request->data = value;
    request->is_ephemeral = is_ephemeral;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;
    KeeperStore::KeeperResponsesQueue responses_queue;
    int64_t time = std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    storage.processRequest(responses_queue, {request, session_id, time}, {}, /* check_acl = */ true, /*ignore_response*/ true);
}

uint64_t createSession(NuRaftStateMachine & machine)
{
    return machine.getStore().getSessionID(30000);
}

void createZNodeLog(NuRaftStateMachine & machine, const String & key, const String & data, ptr<NuRaftFileLogStore> store, UInt64 term)
{
    Coordination::ACLs default_acls;
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    RequestForSession session_request;
    session_request.session_id = createSession(machine);
    auto request = cs_new<Coordination::ZooKeeperCreateRequest>();
    session_request.request = request;
    request->path = key;
    request->data = data;
    request->is_ephemeral = false;
    request->is_sequential = false;
    request->acls = default_acls;
    request->xid = 1;

    session_request.create_time = getCurrentTimeMilliseconds();
    session_request.process_time = getCurrentWallTimeMilliseconds();

    ptr<buffer> buf = serializeKeeperRequest(session_request);
    //LOG_INFO(log, "index {}", index);
    if (store != nullptr)
    {
        ptr<log_entry> entry_log = cs_new<log_entry>(term, buf);
        store->append(entry_log);
    }

    machine.commit(index, *(buf.get()), true);
}

void createZNode(NuRaftStateMachine & machine, const String & key, const String & data)
{
    createZNodeLog(machine, key, data, nullptr, 0);
}

void setZNode(NuRaftStateMachine & machine, const String & key, const String & data)
{
    Coordination::ACLs default_acls;
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    RequestForSession session_request;
    session_request.session_id = createSession(machine);
    auto request = cs_new<Coordination::ZooKeeperSetRequest>();
    session_request.request = request;
    request->path = key;
    request->data = data;
    //request->is_ephemeral = false;
    //request->is_sequential = false;
    //request->acls = default_acls;

    session_request.create_time = getCurrentTimeMilliseconds();
    session_request.process_time = getCurrentWallTimeMilliseconds();

    ptr<buffer> buf = serializeKeeperRequest(session_request);
    machine.commit(index, *(buf.get()));
}

void removeZNode(NuRaftStateMachine & machine, const String & key)
{
    Coordination::ACLs default_acls;
    Coordination::ACL acl;
    acl.permissions = Coordination::ACL::All;
    acl.scheme = "world";
    acl.id = "anyone";
    default_acls.emplace_back(std::move(acl));

    UInt64 index = machine.last_commit_index() + 1;
    RequestForSession session_request;
    session_request.session_id = createSession(machine);
    auto request = cs_new<Coordination::ZooKeeperRemoveRequest>();
    session_request.request = request;
    request->path = key;

    session_request.create_time = getCurrentTimeMilliseconds();
    session_request.process_time = getCurrentWallTimeMilliseconds();

    ptr<buffer> buf = serializeKeeperRequest(session_request);
    machine.commit(index, *(buf.get()));
}

}
