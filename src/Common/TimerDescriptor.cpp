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
#include <Common/TimerDescriptor.h>
#include <Common/Exception.h>

#include <sys/timerfd.h>
#include <fcntl.h>
#include <unistd.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_CREATE_TIMER;
    extern const int CANNOT_SET_TIMER_PERIOD;
    extern const int CANNOT_FCNTL;
    extern const int CANNOT_READ_FROM_SOCKET;
}

TimerDescriptor::TimerDescriptor(int clockid, int flags)
{
    timer_fd = timerfd_create(clockid, flags);
    if (timer_fd == -1)
        throw Exception(ErrorCodes::CANNOT_CREATE_TIMER, "Cannot create timer_fd descriptor");

    if (-1 == fcntl(timer_fd, F_SETFL, O_NONBLOCK))
        throwFromErrno("Cannot set O_NONBLOCK for timer_fd", ErrorCodes::CANNOT_FCNTL);
}

TimerDescriptor::TimerDescriptor(TimerDescriptor && other) : timer_fd(other.timer_fd)
{
    other.timer_fd = -1;
}

TimerDescriptor::~TimerDescriptor()
{
    /// Do not check for result cause cannot throw exception.
    if (timer_fd != -1)
        close(timer_fd);
}

void TimerDescriptor::reset() const
{
    itimerspec spec;
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = 0;
    spec.it_value.tv_nsec = 0;

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throwFromErrno("Cannot reset timer_fd", ErrorCodes::CANNOT_SET_TIMER_PERIOD);

    /// Drain socket.
    /// It may be possible that alarm happened and socket is readable.
    drain();
}

void TimerDescriptor::drain() const
{
    /// It is expected that socket returns 8 bytes when readable.
    /// Read in loop anyway cause signal may interrupt read call.
    uint64_t buf;
    while (true)
    {
        ssize_t res = ::read(timer_fd, &buf, sizeof(buf));
        if (res < 0)
        {
            if (errno == EAGAIN)
                break;

            if (errno != EINTR)
                throwFromErrno("Cannot drain timer_fd", ErrorCodes::CANNOT_READ_FROM_SOCKET);
        }
    }
}

void TimerDescriptor::setRelative(const Poco::Timespan & timespan) const
{
    itimerspec spec;
    spec.it_interval.tv_nsec = 0;
    spec.it_interval.tv_sec = 0;
    spec.it_value.tv_sec = timespan.totalSeconds();
    spec.it_value.tv_nsec = timespan.useconds() * 1000;

    if (-1 == timerfd_settime(timer_fd, 0 /*relative timer */, &spec, nullptr))
        throwFromErrno("Cannot set time for timer_fd", ErrorCodes::CANNOT_SET_TIMER_PERIOD);
}

}
#endif
