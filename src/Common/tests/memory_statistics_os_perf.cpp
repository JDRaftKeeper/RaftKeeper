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
#if defined(OS_LINUX)
#include <Common/MemoryStatisticsOS.h>
#include <iostream>

#endif

int main([[maybe_unused]] int argc, [[maybe_unused]] char ** argv)
{
#if defined(OS_LINUX)
    using namespace RK;

    size_t num_iterations = argc >= 2 ? std::stoull(argv[1]) : 1000000;
    MemoryStatisticsOS stats;

    uint64_t counter = 0;
    for (size_t i = 0; i < num_iterations; ++i)
    {
        MemoryStatisticsOS::Data data = stats.get();
        counter += data.resident;
    }

    if (num_iterations)
        std::cerr << (counter / num_iterations) << '\n';
#endif

    (void)argc;
    (void)argv;

    return 0;
}
