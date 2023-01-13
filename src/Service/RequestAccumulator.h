#pragma once

#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>

namespace DB
{

class RequestAccumulator
{
    using RequestForSession = SvsKeeperStorage::RequestForSession;
    using ThreadPoolPtr = std::shared_ptr<ThreadPool>;
    using NuRaftResult = nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>>;
    using RunnerId = size_t;

public:
    explicit RequestAccumulator(std::shared_ptr<RequestProcessor> request_processor_) : request_processor(request_processor_) { }

    void processRequest(RequestForSession request_for_session);

    bool waitResultAndHandleError(NuRaftResult prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch);

    void run(RunnerId runner_id);

    void shutdown();

    void initialize(
        size_t thread_count,
        std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_,
        std::shared_ptr<SvsKeeperServer> server_,
        UInt64 operation_timeout_ms_,
        UInt64 max_batch_size_);

private:
    ptr<RequestsQueue> requests_queue;
    ThreadPoolPtr request_thread;

    bool shutdown_called{false};
    std::shared_ptr<SvsKeeperDispatcher> keeper_dispatcher;

    std::shared_ptr<SvsKeeperServer> server;
    std::shared_ptr<RequestProcessor> request_processor;

    UInt64 operation_timeout_ms;
    UInt64 max_batch_size;
};

}
