#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_LOG_LEVEL;
}
using LogLevel = Poco::Message::Priority;

inline LogLevel parseLogLevel(const String & level)
{
    LogLevel log_level;
    if (level == "trace")
        log_level = LogLevel::PRIO_TRACE;
    else if (level == "debug")
        log_level = LogLevel::PRIO_DEBUG;
    else if (level == "information")
        log_level = LogLevel::PRIO_INFORMATION;
    else if (level == "warning")
        log_level = LogLevel::PRIO_WARNING;
    else if (level == "error")
        log_level = LogLevel::PRIO_ERROR;
    else if (level == "fatal")
        log_level = LogLevel::PRIO_FATAL;
    else
        throw Exception("Valid log level values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal'", ErrorCodes::INVALID_LOG_LEVEL);
    return log_level;
}

inline String logLevelToString(LogLevel level)
{
    String log_level;
    if (level == LogLevel::PRIO_TRACE)
        log_level = "trace";
    else if (level == LogLevel::PRIO_DEBUG)
        log_level = "debug";
    else if (level == LogLevel::PRIO_INFORMATION)
        log_level = "information";
    else if (level == LogLevel::PRIO_WARNING)
        log_level = "warning";
    else if (level == LogLevel::PRIO_ERROR)
        log_level = "error";
    else if (level == LogLevel::PRIO_FATAL)
        log_level = "fatal";
    else
        throw Exception("Valid log level", ErrorCodes::INVALID_LOG_LEVEL);
    return log_level;
}

class LoggerWrapper : public nuraft::logger
{
private:
    static inline const int LEVEL_MAX = static_cast<int>(LogLevel::PRIO_TRACE);
    static inline const int LEVEL_MIN = static_cast<int>(LogLevel::PRIO_FATAL);

public:
    LoggerWrapper(const std::string & name, LogLevel level_)
        : log(&Poco::Logger::get(name))
        , level(level_)
    {
        log->setLevel(static_cast<int>(level));
    }

    void put_details(
        int level_,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LOG_IMPL(log, static_cast<LogLevel>(level_), msg);
    }

    void set_level(int level_) override
    {
        level_ = std::min(LEVEL_MAX, std::max(LEVEL_MIN, level_));
        log->setLevel(static_cast<LogLevel>(level_));
    }

    int get_level() override
    {
        return static_cast<int>(level);
    }

private:
    Poco::Logger * log;
    std::atomic<LogLevel> level;
};

}
