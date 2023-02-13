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

#include <chrono>
#include <string>
#include <sstream>
#include <cctz/time_zone.h>


namespace ext
{
    inline std::string to_string(const std::time_t & time)
    {
        return cctz::format("%Y-%m-%d %H:%M:%S", std::chrono::system_clock::from_time_t(time), cctz::local_time_zone());
    }

    template <typename Clock, typename Duration = typename Clock::duration>
    std::string to_string(const std::chrono::time_point<Clock, Duration> & tp)
    {
        // Don't use DateLUT because it shows weird characters for
        // TimePoint::max(). I wish we could use C++20 format, but it's not
        // there yet.
        // return DateLUT::instance().timeToString(std::chrono::system_clock::to_time_t(tp));

        auto in_time_t = std::chrono::system_clock::to_time_t(tp);
        return to_string(in_time_t);
    }

    template <typename Rep, typename Period = std::ratio<1>>
    std::string to_string(const std::chrono::duration<Rep, Period> & duration)
    {
        auto seconds_as_int = std::chrono::duration_cast<std::chrono::seconds>(duration);
        if (seconds_as_int == duration)
            return std::to_string(seconds_as_int.count()) + "s";
        auto seconds_as_double = std::chrono::duration_cast<std::chrono::duration<double>>(duration);
        return std::to_string(seconds_as_double.count()) + "s";
    }

    template <typename Clock, typename Duration = typename Clock::duration>
    std::ostream & operator<<(std::ostream & o, const std::chrono::time_point<Clock, Duration> & tp)
    {
        return o << to_string(tp);
    }

    template <typename Rep, typename Period = std::ratio<1>>
    std::ostream & operator<<(std::ostream & o, const std::chrono::duration<Rep, Period> & duration)
    {
        return o << to_string(duration);
    }
}
