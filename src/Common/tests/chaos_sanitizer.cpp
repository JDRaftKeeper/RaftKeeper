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
#include <thread>
#include <iostream>
#include <atomic>

#include <common/sleep.h>

#include <IO/ReadHelpers.h>

#include <Common/Exception.h>
#include <Common/ThreadFuzzer.h>


/** Proves that ThreadFuzzer helps to find concurrency bugs.
  *
  * for i in {1..10}; do ./chaos_sanitizer 1000000; done
  * for i in {1..10}; do THREAD_FUZZER_CPU_TIME_PERIOD_US=1000 THREAD_FUZZER_SLEEP_PROBABILITY=0.1 THREAD_FUZZER_SLEEP_TIME_US=100000 ./chaos_sanitizer 1000000; done
  */
int main(int argc, char ** argv)
{
    const size_t num_iterations = argc >= 2 ? RK::parse<size_t>(argv[1]) : 1000000000;

    std::cerr << (RK::ThreadFuzzer::instance().isEffective() ? "ThreadFuzzer is enabled.\n" : "ThreadFuzzer is not enabled.\n");

    std::atomic<size_t> counter1 = 0;
    std::atomic<size_t> counter2 = 0;

    /// These threads are synchronized by sleep (that's intentionally incorrect).

    std::thread t1([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            counter1.store(counter1.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);

        sleepForNanoseconds(100000000);

        for (size_t i = 0; i < num_iterations; ++i)
            counter2.store(counter2.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    });

    std::thread t2([&]
    {
        for (size_t i = 0; i < num_iterations; ++i)
            counter2.store(counter2.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);

        sleepForNanoseconds(100000000);

        for (size_t i = 0; i < num_iterations; ++i)
            counter1.store(counter1.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
    });

    t1.join();
    t2.join();

    std::cerr << "Result: " << counter1 << ", " << counter2 << "\n";

    return 0;
}
