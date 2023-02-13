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
#include <sys/resource.h>
#include "Stopwatch.h"

StopwatchRUsage::Timestamp StopwatchRUsage::Timestamp::current()
{
    StopwatchRUsage::Timestamp res;

    ::rusage rusage {};
#if !defined(__APPLE__)
    ::getrusage(RUSAGE_THREAD, &rusage);
#endif
    res.user_ns = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec * 1000UL;
    res.sys_ns = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec * 1000UL;
    return res;
}
