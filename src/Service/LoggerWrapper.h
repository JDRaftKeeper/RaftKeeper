#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>

namespace RK
{

enum NuRaftLogLevel
{
    LOG_FATAL = 1,
    LOG_ERROR,
    LOG_WARNING,
    LOG_INFORMATION,
    LOG_DEBUG,
    LOG_TRACE
};

NuRaftLogLevel parseNuRaftLogLevel(const String & level);

String nuRaftLogLevelToString(NuRaftLogLevel level);

Poco::Message::Priority toPocoLogLevel(NuRaftLogLevel level);

class LoggerWrapper : public nuraft::logger
{
private:
    static inline const int LEVEL_MAX = static_cast<int>(LOG_TRACE);
    static inline const int LEVEL_MIN = static_cast<int>(LOG_FATAL);

public:
    LoggerWrapper(const std::string & name, NuRaftLogLevel level_)
        : log(&Poco::Logger::get(name))
        , nuraft_log_level(level_)
    {
        log->setLevel(toPocoLogLevel(static_cast<NuRaftLogLevel>(nuraft_log_level)));
    }

    void put_details(
        int level_,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LOG_IMPL(log, toPocoLogLevel(static_cast<NuRaftLogLevel>(level_)), msg);
    }

    void set_level(int level_) override
    {
        level_ = std::min(LEVEL_MAX, std::max(LEVEL_MIN, level_));
        nuraft_log_level = static_cast<NuRaftLogLevel>(level_);
        log->setLevel(toPocoLogLevel(static_cast<NuRaftLogLevel>(nuraft_log_level)));
    }

    int get_level() override
    {
        return static_cast<int>(nuraft_log_level);
    }

private:
    Poco::Logger * log;
    std::atomic<NuRaftLogLevel> nuraft_log_level;
};

}
