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

#include <sys/types.h>
#include <boost/noncopyable.hpp>


#if defined(__linux__)
struct taskstats;

namespace RK
{
/// Provides several essential per-task metrics by reading data from Procfs (when available).
class ProcfsMetricsProvider : private boost::noncopyable
{
public:
    ProcfsMetricsProvider(const pid_t /*tid*/);
    ~ProcfsMetricsProvider();

    /// Updates only a part of taskstats struct's fields:
    ///  - cpu_run_virtual_total, cpu_delay_total (when /proc/thread-self/schedstat is available)
    ///  - blkio_delay_total                      (when /proc/thread-self/stat is available)
    ///  - rchar, wchar, read_bytes, write_bytes  (when /prod/thread-self/io is available)
    /// See: man procfs
    void getTaskStats(::taskstats & out_stats) const;

    /// Tells whether this metrics (via Procfs) is provided on the current platform
    static bool isAvailable() noexcept;

private:
    void readParseAndSetThreadCPUStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadBlkIOStat(::taskstats & out_stats, char *, size_t) const;
    void readParseAndSetThreadIOStat(::taskstats & out_stats, char *, size_t) const;

private:
    int thread_schedstat_fd = -1;
    int thread_stat_fd = -1;
    int thread_io_fd = -1;

    /// This field is used for compatibility with TasksStatsCounters::incrementProfileEvents()
    unsigned short stats_version = 1;
};

}
#endif
