/**
 * Copyright 2016-2023 ClickHouse, Inc.
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

#if defined(OS_LINUX) || defined(__FreeBSD__)

#include <condition_variable>
#include <future>
#include <mutex>
#include <map>
#include <IO/AIO.h>
#include <Common/ThreadPool.h>


namespace RK
{

class AIOContextPool : private boost::noncopyable
{
    static const auto max_concurrent_events = 128;
    static const auto timeout_sec = 1;

    AIOContext aio_context{max_concurrent_events};

    using ID = size_t;
    using BytesRead = ssize_t;

    /// Autoincremental id used to identify completed requests
    ID next_id{};
    mutable std::mutex mutex;
    mutable std::condition_variable have_resources;
    std::map<ID, std::promise<BytesRead>> promises;

    std::atomic<bool> cancelled{false};
    ThreadFromGlobalPool io_completion_monitor{&AIOContextPool::doMonitor, this};

    ~AIOContextPool();

    void doMonitor();
    void waitForCompletion();
    int getCompletionEvents(io_event events[], const int max_events) const;
    void fulfillPromises(const io_event events[], const int num_events);
    void notifyProducers(const int num_producers) const;
    void reportExceptionToAnyProducer();

public:
    static AIOContextPool & instance();

    /// Request AIO read operation for iocb, returns a future with number of bytes read
    std::future<BytesRead> post(struct iocb & iocb);
};

}

#endif
