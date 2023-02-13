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
#if defined(OS_LINUX)
#include <Poco/Timespan.h>

namespace RK
{

/// Wrapper over timerfd.
class TimerDescriptor
{
private:
    int timer_fd;

public:
    explicit TimerDescriptor(int clockid = CLOCK_MONOTONIC, int flags = 0);
    ~TimerDescriptor();

    TimerDescriptor(const TimerDescriptor &) = delete;
    TimerDescriptor & operator=(const TimerDescriptor &) = delete;
    TimerDescriptor(TimerDescriptor && other);
    TimerDescriptor & operator=(TimerDescriptor &&) = default;

    int getDescriptor() const { return timer_fd; }

    void reset() const;
    void drain() const;
    void setRelative(const Poco::Timespan & timespan) const;
};

}
#endif
