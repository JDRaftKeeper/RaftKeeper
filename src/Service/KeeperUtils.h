#pragma once

#include <fstream>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/nuraft.hxx>
#include <Service/KeeperCommon.h>


namespace RK
{

inline UInt64 getCurrentTimeMilliseconds()
{
    return duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

inline UInt64 getCurrentTimeMicroseconds()
{
    return duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

/// Serialize and deserialize ZooKeeper request to log
nuraft::ptr<nuraft::buffer> serializeKeeperRequest(const RequestForSession & request);
nuraft::ptr<RequestForSession> deserializeKeeperRequest(nuraft::buffer & data);

nuraft::ptr<nuraft::log_entry> cloneLogEntry(const nuraft::ptr<nuraft::log_entry> & entry);

/// Parent of a path, for example: got '/a/b' from '/a/b/c'
String getParentPath(const String & path);
/// Base name of a path, for example: got 'c' from '/a/b/c'
String getBaseName(const String & path);

String base64Encode(const String & decoded);
String getSHA1(const String & userdata);
String generateDigest(const String & userdata);
String checkAndGetSuperDigest(const String & user_and_digest);

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
