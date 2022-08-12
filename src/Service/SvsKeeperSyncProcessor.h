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

    void processRequest(Request request_for_session)
    {
        requests_queue->push(request_for_session);
    }

    bool waitResultAndHandleError(nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch)
    {
        /// Forcefully process all previous pending requests

        if (!prev_result->has_result())
            prev_result->get();

        bool result_accepted = prev_result->get_accepted();

        if (result_accepted && prev_result->get_result_code() == nuraft::cmd_result_code::OK)
        {
            return true;
        }
        else
        {
            for (auto & request_session : prev_batch)
            {
                svskeeper_commit_processor->onError(result_accepted, prev_result->get_result_code(), request_session);
            }
            return false;
        }
    }

    void run(size_t thread_idx)
    {
        nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> result = nullptr;
        /// Requests from previous iteration. We store them to be able
        /// to send errors to the client.
        //    SvsKeeperStorage::RequestsForSessions prev_batch;

        SvsKeeperStorage::RequestsForSessions to_append_batch;

        while (!shutdown_called)
        {
            UInt64 max_wait = operation_timeout_ms;

            SvsKeeperStorage::RequestForSession request_for_session;

            bool pop_succ = false;
            if (to_append_batch.empty())
            {
                pop_succ = requests_queue->tryPop(thread_idx, request_for_session, max_wait);
            }
            else
            {
                if (!requests_queue->tryPop(thread_idx, request_for_session))
                {
                    result = server->putRequestBatch(to_append_batch);
                    waitResultAndHandleError(result, to_append_batch);
                    result.reset();
                    to_append_batch.clear();

                    continue;
                }
                pop_succ = true;
            }

            if (pop_succ)
            {
                to_append_batch.emplace_back(request_for_session);

                if (to_append_batch.size() > 1000)
                {
                    result = server->putRequestBatch(to_append_batch);
                    waitResultAndHandleError(result, to_append_batch);
                    result.reset();
                    to_append_batch.clear();

                }
            }
        }
    }

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;

        SvsKeeperStorage::RequestForSession request_for_session;
        while (requests_queue->tryPopAny(request_for_session))
        {
            svskeeper_commit_processor->onError(false, nuraft::cmd_result_code::CANCELLED, request_for_session);
        }
    }

    void initialize(size_t thread_count, std::shared_ptr<SvsKeeperServer> server_, UInt64 operation_timeout_ms_)
    {
        operation_timeout_ms = operation_timeout_ms_;
        server = server_;
        requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
        request_thread = std::make_shared<ThreadPool>(thread_count);
        for (size_t i = 0; i < thread_count; i++)
        {
            request_thread->trySchedule([this, i] { run(i); });
        }
    }


private:
    ptr<RequestsQueue> requests_queue;

    ThreadPoolPtr request_thread;

    bool shutdown_called{false};

    std::shared_ptr<SvsKeeperServer> server;

    std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor;

    UInt64 operation_timeout_ms = 10000;
};

}
