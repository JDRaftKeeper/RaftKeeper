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
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/SipHash.h>
#include <common/getThreadId.h>
#include <common/types.h>


namespace RK
{
    namespace ErrorCodes
    {
        extern const int CANNOT_CLOCK_GETTIME;
    }
}


RK::UInt64 randomSeed()
{
    struct timespec times;
    if (clock_gettime(CLOCK_MONOTONIC, &times))
        RK::throwFromErrno("Cannot clock_gettime.", RK::ErrorCodes::CANNOT_CLOCK_GETTIME);

    /// Not cryptographically secure as time, pid and stack address can be predictable.

    SipHash hash;
    hash.update(times.tv_nsec);
    hash.update(times.tv_sec);
    hash.update(getThreadId());
    hash.update(&times);
    return hash.get64();
}
