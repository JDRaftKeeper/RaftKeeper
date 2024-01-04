#pragma once

#include <fstream>
#include <time.h>
#include <Service/Crc32.h>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>


namespace RK
{

String checkAndGetSuperdigest(const String & user_and_digest);
nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request);
nuraft::ptr<nuraft::log_entry> makeClone(const nuraft::ptr<nuraft::log_entry> & entry);

struct BackendTimer
{
    static constexpr char TIME_FMT[] = "%Y%m%d%H%M%S";

    /// default min interval is 1 hour
    UInt32 interval = 1 * 3600;
    UInt32 random_window = 1200; //20 minutes

    static void getCurrentTime(String & date_str)
    {
        time_t curr_time;
        time(&curr_time);
        char tmp_buf[24];
        std::strftime(tmp_buf, sizeof(tmp_buf), TIME_FMT, localtime(&curr_time));
        date_str = tmp_buf;
    }

    static time_t parseTime(const String & date_str)
    {
        struct tm prev_tm;
        memset(&prev_tm, 0, sizeof(tm));
        strptime(date_str.data(), TIME_FMT, &prev_tm);
        time_t prev_time = mktime(&prev_tm);
        return prev_time;
    }

    bool isActionTime(const time_t & prev_time, time_t curr_time) const
    {
        if (curr_time == 0L)
            time(&curr_time);
        return difftime(curr_time, prev_time) >= (interval + rand() % random_window);
    }
};


class Directory
{
public:
    static int createDir(const String & path);
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
