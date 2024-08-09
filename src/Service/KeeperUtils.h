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

/// Serialize ZooKeeper request to log
nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request);
nuraft::ptr<nuraft::log_entry> makeClone(const nuraft::ptr<nuraft::log_entry> & entry);

/// Parent of a path, for example: got '/a/b' from '/a/b/c'
String getParentPath(const String & path);
/// Base name of a path, for example: got 'c' from '/a/b/c'
String getBaseName(const String & path);

String base64Encode(const String & decoded);
String getSHA1(const String & userdata);
String generateDigest(const String & userdata);
String checkAndGetSuperdigest(const String & user_and_digest);

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
