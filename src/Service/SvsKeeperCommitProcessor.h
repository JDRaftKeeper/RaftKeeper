#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

using Request = SvsKeeperStorage::RequestForSession;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

struct ErrorRequest
{
    bool accepted;
    nuraft::cmd_result_code error_code;
    int64_t session_id;
    Coordination::XID xid;
    Coordination::OpNum opnum;
};

class SvsKeeperDispatcher;

class SvsKeeperCommitProcessor
{

public:
    using RequestForSession = SvsKeeperStorage::RequestForSession;

    SvsKeeperCommitProcessor(SvsKeeperResponsesQueue & responses_queue_)
        : responses_queue(responses_queue_), log(&Poco::Logger::get("SvsKeeperCommitProcessor"))
    {
    }

    void processRequest(Request request_for_session);

    void run();

    void processReadRequests(size_t thread_idx);

    void stateMachineProcessRequest(const RequestForSession & reuqest) const;

    void shutdown();

    void commit(Request request);

    void onError(bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid, Coordination::OpNum opnum);

    void initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_, UInt64 operation_timeout_ms_);

private:
    ptr<RequestsQueue> requests_queue;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

    std::shared_ptr<SvsKeeperServer> server;

    SvsKeeperResponsesQueue & responses_queue;

    size_t thread_count;

    ThreadPoolPtr request_thread;

//    SvsKeeperThreadSafeQueue<SvsKeeperStorage::RequestForSession> committed_queue;
    ConcurrentBoundedQueue<SvsKeeperStorage::RequestForSession> committed_queue{1000};

    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;

    using RequestForSessions = std::vector<SvsKeeperStorage::RequestForSession>;

    std::unordered_map<size_t, std::unordered_map<int64_t, RequestForSessions>> thread_pending_requests;

    mutable std::mutex mutex;
    /// key : session_id xid
    std::unordered_map<UInt128, ErrorRequest> errors;

    std::condition_variable cv;

    Poco::Logger * log;

    UInt64 operation_timeout_ms = 10000;
};

}
