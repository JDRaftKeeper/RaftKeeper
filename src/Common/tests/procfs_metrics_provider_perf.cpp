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
#if defined(__linux__)
#include <Common/ProcfsMetricsProvider.h>

#include <iostream>
#include <linux/taskstats.h>
#endif


#if defined(__linux__)
int main(int argc, char ** argv)
{
    using namespace RK;

    size_t num_iterations = argc >= 2 ? std::stoull(argv[1]) : 1000000;

    if (!ProcfsMetricsProvider::isAvailable())
    {
        std::cerr << "Procfs statistics is not available on this system" << std::endl;
        return -1;
    }

    ProcfsMetricsProvider stats_provider(0);

    ::taskstats stats;
    stats_provider.getTaskStats(stats);

    const auto start_cpu_time = stats.cpu_run_virtual_total;
    for (size_t i = 0; i < num_iterations; ++i)
    {
        stats_provider.getTaskStats(stats);
    }

    if (num_iterations)
        std::cerr << stats.cpu_run_virtual_total - start_cpu_time << '\n';
    return 0;
}
#else
int main()
{
}
#endif
