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
    Request request;
};

class SvsKeeperCommitProcessor
{


public:
    SvsKeeperCommitProcessor(SvsKeeperResponsesQueue & responses_queue_)
        : responses_queue(responses_queue_), log(&Poco::Logger::get("SvsKeeperCommitProcessor"))
    {
    }

    void processRequest(Request request_for_session);

    void run();

    void processReadRequests(size_t thread_idx);

    void shutdown();

    void commit(Request request);

    void onError(bool accepted, nuraft::cmd_result_code error_code, Request request);

    void initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, UInt64 operation_timeout_ms_);

private:
    ptr<RequestsQueue> requests_queue;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

    std::shared_ptr<SvsKeeperServer> server;

    SvsKeeperResponsesQueue & responses_queue;

    size_t thread_count;

    ThreadPoolPtr request_thread;

    SvsKeeperThreadSafeQueue<SvsKeeperStorage::RequestForSession> committed_queue;

    using RequestForSessions = std::vector<SvsKeeperStorage::RequestForSession>;

    std::unordered_map<size_t, std::unordered_map<int64_t, RequestForSessions>> thread_pending_requests;

    mutable std::mutex errors_mutex;
    /// key : session_id xid
    std::unordered_map<UInt128, ErrorRequest> errors;

    std::mutex mutex;

    std::condition_variable cv;

    Poco::Logger * log;

    UInt64 operation_timeout_ms = 10000;
};

}
