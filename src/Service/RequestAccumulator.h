#pragma once

#include <Service/KeeperCommon.h>
#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>

namespace RK
{

/** Accumulate requests into a batch to promote performance.
 * Request in a batch must be all write request.
 *
 * The batch is transferred to Raft and goes through log replication flow.
 */
class RequestAccumulator
{
    using RequestForSession = RequestForSession;
    using NuRaftResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;

public:
    explicit RequestAccumulator(std::shared_ptr<RequestProcessor> request_processor_)
        : log(&Poco::Logger::get("RequestAccumulator")), request_processor(request_processor_)
    {
    }

    void push(const RequestForSession & request_for_session);

    bool waitResultAndHandleError(NuRaftResult prev_result, const RequestsForSessions & prev_batch);

    void run();

    void shutdown();

    void initialize(
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        std::shared_ptr<KeeperServer> server_,
        UInt64 operation_timeout_ms_,
        UInt64 max_batch_size_);

private:
    Poco::Logger * log;

    ptr<ConcurrentBoundedQueue<RequestForSession>> requests_queue;
    ThreadFromGlobalPool request_thread;

    std::atomic<bool> shutdown_called{false};
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    std::shared_ptr<KeeperServer> server;
    std::shared_ptr<RequestProcessor> request_processor;

    UInt64 operation_timeout_ms;
    UInt64 max_batch_size;
};

}
