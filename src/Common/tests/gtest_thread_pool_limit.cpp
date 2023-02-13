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
#include <iostream>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>

/// Test for thread self-removal when number of free threads in pool is too large.
/// Just checks that nothing weird happens.

template <typename Pool>
int test()
{
    Pool pool(10, 2, 10);

    std::atomic<int> counter{0};
    for (size_t i = 0; i < 10; ++i)
        pool.scheduleOrThrowOnError([&]{ ++counter; });
    pool.wait();

    return counter;
}

TEST(ThreadPool, ThreadRemoval)
{
    EXPECT_EQ(test<FreeThreadPool>(), 10);
    EXPECT_EQ(test<ThreadPool>(), 10);
}
