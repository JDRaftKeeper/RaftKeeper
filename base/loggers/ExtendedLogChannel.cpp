#include "ExtendedLogChannel.h"

#include <sys/time.h>
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

    msg_ext.thread_id = getThreadId();

    return msg_ext;
}

}
