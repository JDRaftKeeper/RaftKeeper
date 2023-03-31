#include "LoggerWrapper.h"
#include <Common/Exception.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int INVALID_LOG_LEVEL;
}

NuRaftLogLevel parseNuRaftLogLevel(const String & level)
{
    NuRaftLogLevel log_level;
    if (level == "trace")
        log_level = RAFT_LOG_TRACE;
    else if (level == "debug")
        log_level = RAFT_LOG_DEBUG;
    else if (level == "information")
        log_level = RAFT_LOG_INFORMATION;
    else if (level == "warning")
        log_level = RAFT_LOG_WARNING;
    else if (level == "error")
        log_level = RAFT_LOG_ERROR;
    else if (level == "fatal")
        log_level = RAFT_LOG_FATAL;
    else
        throw Exception("Valid log level values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal'", ErrorCodes::INVALID_LOG_LEVEL);
    return log_level;
}

String nuRaftLogLevelToString(NuRaftLogLevel level)
{
    String log_level;
    if (level == RAFT_LOG_TRACE)
        log_level = "trace";
    else if (level == RAFT_LOG_DEBUG)
        log_level = "debug";
    else if (level == RAFT_LOG_INFORMATION)
        log_level = "information";
    else if (level == RAFT_LOG_WARNING)
        log_level = "warning";
    else if (level == RAFT_LOG_ERROR)
        log_level = "error";
    else if (level == RAFT_LOG_FATAL)
        log_level = "fatal";
    else
        throw Exception("Valid log level", ErrorCodes::INVALID_LOG_LEVEL);
    return log_level;
}

Poco::Message::Priority toPocoLogLevel(NuRaftLogLevel level)
{
    using Poco::Message;
    int poco_log_level;
    switch (level)
    {
        case RAFT_LOG_FATAL:
            poco_log_level = Message::Priority::PRIO_FATAL;
            break;
        case RAFT_LOG_ERROR:
            poco_log_level = Message::Priority::PRIO_ERROR;
            break;
        case RAFT_LOG_WARNING:
            poco_log_level = Message::Priority::PRIO_WARNING;
            break;
        case RAFT_LOG_INFORMATION:
            poco_log_level = Message::Priority::PRIO_INFORMATION;
            break;
        case RAFT_LOG_DEBUG:
            poco_log_level = Message::Priority::PRIO_DEBUG;
            break;
        case RAFT_LOG_TRACE:
            poco_log_level = Message::Priority::PRIO_TRACE;
            break;
    }
    return static_cast<Poco::Message::Priority>(poco_log_level);
}

}
