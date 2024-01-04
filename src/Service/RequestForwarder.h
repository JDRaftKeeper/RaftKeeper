#pragma once

#include <chrono>

#include <Common/Stopwatch.h>

#include <Service/ForwardConnection.h>
#include <Service/ForwardRequest.h>
#include <Service/ForwardResponse.h>
#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>
#include <Service/Types.h>


namespace RK
{

using clock = std::chrono::steady_clock;

class RequestForwarder
{
public:
    explicit RequestForwarder(std::shared_ptr<RequestProcessor> request_processor_)
        : request_processor(request_processor_), log(&Poco::Logger::get("RequestForwarder"))
    {
    }

    void push(const RequestForSession & request_for_session);

    void runSend(RunnerId runner_id);
    void runReceive(RunnerId runner_id);

    void initialize(
        size_t thread_count_,
        std::shared_ptr<KeeperServer> server_,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        UInt64 session_sync_period_ms_,
        UInt64 operation_timeout_ms_);

    void shutdown();

    std::shared_ptr<RequestProcessor> request_processor;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

private:
    void initConnections();

    /// void runSessionSync(RunnerId runner_id);
    /// void runSessionSyncReceive(RunnerId runner_id);

    void processResponse(RunnerId runner_id, ForwardResponsePtr forward_response_ptr);
    bool removeFromQueue(RunnerId runner_id, ForwardResponsePtr forward_response_ptr);

    bool processTimeoutRequest(RunnerId runner_id, ForwardRequestPtr newFront);

    size_t thread_count;
    ptr<RequestsQueue> requests_queue;

    Poco::Logger * log;

    ThreadPoolPtr request_thread;
    ThreadPoolPtr response_thread;

    ThreadFromGlobalPool session_sync_thread;

    std::atomic<bool> shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    UInt64 session_sync_period_ms;
    std::atomic<UInt64> session_sync_idx{0};
    Stopwatch session_sync_time_watch;

    using ForwardRequestQueue = ThreadSafeQueue<ForwardRequestPtr, std::list<ForwardRequestPtr>>;
    using ForwardRequestQueuePtr = std::unique_ptr<ForwardRequestQueue>;
    std::vector<ForwardRequestQueuePtr> forward_request_queue;

    Poco::Timespan operation_timeout;

    using ConnectionPool = std::vector<ptr<ForwardConnection>>;
    std::unordered_map<UInt32, ConnectionPool> connections;
    std::mutex connections_mutex;

    using EndPoint = String; /// host:port
    std::unordered_map<UInt32, EndPoint> cluster_config_forward;
};

}
