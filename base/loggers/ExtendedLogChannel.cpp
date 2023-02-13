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
#include "ExtendedLogChannel.h"

#include <sys/time.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <common/getThreadId.h>


namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_GETTIMEOFDAY;
}

ExtendedLogMessage ExtendedLogMessage::getFrom(const Poco::Message & base)
{
    ExtendedLogMessage msg_ext(base);

    ::timeval tv;
    if (0 != gettimeofday(&tv, nullptr))
        RK::throwFromErrno("Cannot gettimeofday", ErrorCodes::CANNOT_GETTIMEOFDAY);

    msg_ext.time_seconds = static_cast<UInt32>(tv.tv_sec);
    msg_ext.time_microseconds = static_cast<UInt32>(tv.tv_usec);
    msg_ext.time_in_microseconds = static_cast<UInt64>((tv.tv_sec) * 1000000U + (tv.tv_usec));

    if (current_thread)
    {
        auto query_id_ref = CurrentThread::getQueryId();
        if (query_id_ref.size)
            msg_ext.query_id.assign(query_id_ref.data, query_id_ref.size);
    }

    msg_ext.thread_id = getThreadId();

    return msg_ext;
}

}
