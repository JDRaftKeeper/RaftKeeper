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
#include <iostream>
#include <stdexcept>
#include <Common/ThreadPool.h>

#include <gtest/gtest.h>


static bool check()
{
    ThreadPool pool(10);

    /// The throwing thread.
    pool.scheduleOrThrowOnError([] { throw std::runtime_error("Hello, world!"); });

    try
    {
        while (true)
        {
            /// An exception from the throwing thread will be rethrown from this method
            /// as soon as the throwing thread executed.

            /// This innocent thread may or may not be executed, the following possibilities exist:
            /// 1. The throwing thread has already thrown exception and the attempt to schedule the innocent thread will rethrow it.
            /// 2. The throwing thread has not executed, the innocent thread will be scheduled and executed.
            /// 3. The throwing thread has not executed, the innocent thread will be scheduled but before it will be executed,
            ///    the throwing thread will be executed and throw exception and it will prevent starting of execution of the innocent thread
            ///    the method will return and the exception will be rethrown only on call to "wait" or on next call on next loop iteration as (1).
            pool.scheduleOrThrowOnError([]{});

            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    catch (const std::runtime_error &)
    {
        pool.wait();
        return true;
    }

    __builtin_unreachable();
}


TEST(ThreadPool, ExceptionFromSchedule)
{
    EXPECT_TRUE(check());
}
