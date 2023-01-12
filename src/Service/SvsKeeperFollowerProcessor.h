#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>
#include <Service/SvsKeeperCommitProcessor.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

class SvsKeeperFollowerProcessor
{
using Request = SvsKeeperStorage::RequestForSession;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

public:
    SvsKeeperFollowerProcessor(std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor_)
        : svskeeper_commit_processor(svskeeper_commit_processor_) , log(&Poco::Logger::get("FollowerRequestProcessor"))
    {
    }

    void processRequest(Request request_for_session);

    void run(size_t thread_idx);

    void shutdown();

    void runReceive(size_t thread_idx);

    void initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_, UInt64 session_sync_period_ms_);


private:
    size_t thread_count;

    ptr<RequestsQueue> requests_queue;

    std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor;

    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;

//    std::vector<std::unique_ptr<std::mutex>> mutexes;
//
//    /// session, xid, request
//    using SessionXidRequest = std::unordered_map<int64_t, std::map<int64_t , Request>>;
//    std::unordered_map<size_t, SessionXidRequest> thread_requests;

    Poco::Logger * log;

    ThreadPoolPtr request_thread;

    ThreadPoolPtr response_thread;

    bool shutdown_called{false};

    std::shared_ptr<SvsKeeperServer> server;

    UInt64 session_sync_period_ms = 500;

    std::atomic<UInt8> session_sync_idx{0};

    Stopwatch session_sync_time_watch;
};

}
