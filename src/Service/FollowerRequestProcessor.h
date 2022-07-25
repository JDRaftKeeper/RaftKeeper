#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>
#include <Service/SvsKeeperCommitProcessor.h>

namespace DB
{

class FollowerRequestProcessor
{
using Request = SvsKeeperStorage::RequestForSession;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

public:

    FollowerRequestProcessor()
    {
    }

    void processRequest(Request request_for_session)
    {
        requests_queue->push(request_for_session);
    }

    void run(size_t thread_idx)
    {
        while (!shutdown_called)
        {
            UInt64 max_wait = operation_timeout_ms;

            SvsKeeperStorage::RequestForSession request_for_session;

            if (requests_queue->tryPop(thread_idx, request_for_session, max_wait))
            {
                server->getLeaderClient(thread_idx)->send(request_for_session);
            }
        }
    }

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;

        request_thread->wait();

        SvsKeeperStorage::RequestForSession request_for_session;
        while (requests_queue->tryPopAny(request_for_session))
        {
            /// TODO ?
            server->getLeaderClient(0)->send(request_for_session);
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

    UInt64 operation_timeout_ms = 10000;
};

}
