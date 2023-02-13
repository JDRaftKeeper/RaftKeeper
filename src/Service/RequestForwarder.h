/**
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <Service/KeeperServer.h>
#include <Service/RequestProcessor.h>
#include <Service/RequestsQueue.h>
#include <Common/Stopwatch.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

class RequestForwarder
{
    using ThreadPoolPtr = std::shared_ptr<ThreadPool>;
    using RunnerId = size_t;

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

    //    std::vector<std::unique_ptr<std::mutex>> mutexes;
    //
    //    /// session, xid, request
    //    using SessionXidRequest = std::unordered_map<int64_t, std::map<int64_t , Request>>;
    //    std::unordered_map<size_t, SessionXidRequest> thread_requests;

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
