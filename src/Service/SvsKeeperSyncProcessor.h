#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>
#include <Service/SvsKeeperCommitProcessor.h>

namespace DB
{

class SvsKeeperSyncProcessor
{
using Request = SvsKeeperStorage::RequestForSession;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

public:
    SvsKeeperSyncProcessor(std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor_)
        : svskeeper_commit_processor(svskeeper_commit_processor_)
    {
    }

    void processRequest(Request request_for_session);

    bool waitResultAndHandleError(nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch);

    void run(size_t thread_idx);

    void shutdown();

    void initialize(size_t thread_count, std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_, std::shared_ptr<SvsKeeperServer> server_, UInt64 operation_timeout_ms_, UInt64 max_batch_size_);

private:
    ptr<RequestsQueue> requests_queue;

    ThreadPoolPtr request_thread;

    bool shutdown_called{false};

    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;

    std::shared_ptr<SvsKeeperServer> server;

    std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor;

    UInt64 operation_timeout_ms{10000};

    UInt64 max_batch_size{1000};
};

}
