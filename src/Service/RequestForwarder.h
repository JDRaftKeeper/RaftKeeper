#pragma once

#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>
#include <Common/Stopwatch.h>
#include <Service/Types.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

class RequestForwarder
{
public:
    explicit RequestForwarder(std::shared_ptr<RequestProcessor> request_processor_)
        : request_processor(request_processor_), log(&Poco::Logger::get("RequestForwarder"))
    {
    }

    void push(RequestForSession request_for_session);

    void run(RunnerId runner_id);

    void shutdown();

    void runReceive(RunnerId runner_id);

    void initialize(
        size_t thread_count_,
        std::shared_ptr<KeeperServer> server_,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        UInt64 session_sync_period_ms_);


private:
    size_t thread_count;

    ptr<RequestsQueue> requests_queue;

    std::shared_ptr<RequestProcessor> request_processor;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    Poco::Logger * log;

    ThreadPoolPtr request_thread;

    ThreadPoolPtr response_thread;

    bool shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    UInt64 session_sync_period_ms = 500;

    std::atomic<UInt8> session_sync_idx{0};

    Stopwatch session_sync_time_watch;
};

}
