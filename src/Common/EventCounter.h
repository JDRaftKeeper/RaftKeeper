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

#include <cstdint>
#include <mutex>
#include <condition_variable>


/** Allow to subscribe for multiple events and wait for them one by one in arbitrary order.
  */
class EventCounter
{
private:
    size_t events_happened = 0;
    size_t events_waited = 0;

    mutable std::mutex mutex;
    std::condition_variable condvar;

public:
    void notify()
    {
        {
            std::lock_guard lock(mutex);
            ++events_happened;
        }
        condvar.notify_all();
    }

    void wait()
    {
        std::unique_lock lock(mutex);
        condvar.wait(lock, [&]{ return events_happened > events_waited; });
        ++events_waited;
    }

    template <typename Duration>
    bool waitFor(Duration && duration)
    {
        std::unique_lock lock(mutex);
        if (condvar.wait(lock, std::forward<Duration>(duration), [&]{ return events_happened > events_waited; }))
        {
            ++events_waited;
            return true;
        }
        return false;
    }
};
