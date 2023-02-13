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
#include <atomic>

#include <Common/Exception.h>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>


/// Test what happens if local ThreadPool cannot create a ThreadFromGlobalPool.
/// There was a bug: if local ThreadPool cannot allocate even a single thread,
///  the job will be scheduled but never get executed.


TEST(ThreadPool, GlobalFull1)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    global_pool.setQueueSize(capacity);
    global_pool.wait();

    std::atomic<size_t> counter = 0;
    static constexpr size_t num_jobs = capacity + 1;

    auto func = [&] { ++counter; while (counter != num_jobs) {} };

    ThreadPool pool(num_jobs);

    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    for (size_t i = capacity; i < num_jobs; ++i)
    {
        EXPECT_THROW(pool.scheduleOrThrowOnError(func), RK::Exception);
        ++counter;
    }

    pool.wait();
    EXPECT_EQ(counter, num_jobs);

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}


TEST(ThreadPool, GlobalFull2)
{
    GlobalThreadPool & global_pool = GlobalThreadPool::instance();

    static constexpr size_t capacity = 5;

    global_pool.setMaxThreads(capacity);
    global_pool.setMaxFreeThreads(1);
    global_pool.setQueueSize(capacity);

    /// ThreadFromGlobalPool from local thread pools from previous test case have exited
    ///  but their threads from global_pool may not have finished (they still have to exit).
    /// If we will not wait here, we can get "Cannot schedule a task exception" earlier than we expect in this test.
    global_pool.wait();

    std::atomic<size_t> counter = 0;
    auto func = [&] { ++counter; while (counter != capacity + 1) {} };

    ThreadPool pool(capacity, 0, capacity);
    for (size_t i = 0; i < capacity; ++i)
        pool.scheduleOrThrowOnError(func);

    ThreadPool another_pool(1);
    EXPECT_THROW(another_pool.scheduleOrThrowOnError(func), RK::Exception);

    ++counter;

    pool.wait();

    global_pool.wait();

    for (size_t i = 0; i < capacity; ++i)
        another_pool.scheduleOrThrowOnError([&] { ++counter; });

    another_pool.wait();
    EXPECT_EQ(counter, capacity * 2 + 1);

    global_pool.setMaxThreads(10000);
    global_pool.setMaxFreeThreads(1000);
    global_pool.setQueueSize(10000);
}
