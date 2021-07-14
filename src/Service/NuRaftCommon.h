#pragma once

#include <fstream>
#include <time.h>
#include <Service/Crc32.h>
#include <libnuraft/nuraft.hxx>
#include <Common/ZooKeeper/IKeeper.h>
#include <common/logger_useful.h>


namespace DB
{
struct RaftWatchResponse : Coordination::WatchResponse
{
    std::string endpoint;
    std::string callback;
};

using RaftWatchCallback = std::function<void(const RaftWatchResponse &)>;

struct BackendTimer
{
    static constexpr char TIME_FMT[] = "%Y%m%d%H%M%S";

    //Only [2:00 - 22:00] can create snapshot
    UInt32 begin_second = 7200;
    UInt32 end_second = 79200;
    //default min interval is 1 hour
    UInt32 interval = 1 * 3600;

    inline UInt32 getTodaySeconds(struct tm * curr_tm) { return curr_tm->tm_hour * 3600 + curr_tm->tm_min * 60 + curr_tm->tm_sec; }

    static void getInitTime(std::string & init_str) { init_str = "20210101000000"; }

    static void getCurrentTime(std::string & date_str)
    {
        time_t curr_time;
        time(&curr_time);
        char tmp_buf[24];
        std::strftime(tmp_buf, sizeof(tmp_buf), TIME_FMT, localtime(&curr_time));
        date_str = tmp_buf;
    }

    static time_t parseTime(const std::string & date_str)
    {
        struct tm prev_tm;
        memset(&prev_tm, 0, sizeof(tm));
        strptime(date_str.data(), TIME_FMT, &prev_tm);
        time_t prev_time = mktime(&prev_tm);
        return prev_time;
    }

    bool isActionTime(const std::string & prev_date, time_t curr_time)
    {
        /// first snapshot
        if (prev_date.empty())
        {
            return true;
        }

        if (curr_time == 0L)
        {
            time(&curr_time);
        }
        struct tm * curr_tm;
        curr_tm = localtime(&curr_time);
        UInt32 today_second = getTodaySeconds(curr_tm);
        if (today_second < begin_second || today_second > end_second)
        {
            return false;
        }

        struct tm prev_tm;
        memset(&prev_tm, 0, sizeof(tm));
        strptime(prev_date.data(), TIME_FMT, &prev_tm);
        time_t prev_time = mktime(&prev_tm);

        return difftime(curr_time, prev_time) >= interval;
    }

    bool isActionTime(time_t & prev_time, time_t & curr_time)
    {
        if (curr_time == 0L)
        {
            time(&curr_time);
        }
        return difftime(curr_time, prev_time) >= interval;
    }
};


class Directory
{
public:
    static int createDir(const std::string & path);
};

inline int readUInt32(nuraft::ptr<std::fstream> & fs, UInt32 & x)
{
    errno = 0;
    char * buf = reinterpret_cast<char *>(&x);
    fs->read(buf, sizeof(UInt32));
    return fs->good() ? 0 : -1;
}

inline int writeUInt32(nuraft::ptr<std::fstream> & fs, const UInt32 & x)
{
    errno = 0;
    fs->write(reinterpret_cast<const char *>(&x), sizeof(UInt32));
    return fs->good() ? 0 : -1;
}

inline int readUInt64(nuraft::ptr<std::fstream> & fs, UInt64 & x)
{
    errno = 0;
    char * buf = reinterpret_cast<char *>(&x);
    fs->read(buf, sizeof(UInt64));
    return fs->good() ? 0 : -1;
}

inline int writeUInt64(nuraft::ptr<std::fstream> & fs, const UInt64 & x)
{
    errno = 0;
    fs->write(reinterpret_cast<const char *>(&x), sizeof(UInt64));
    return fs->good() ? 0 : -1;
}


}
