#include <Poco/File.h>

#include <Common/IO/WriteHelpers.h>
#include <boost/algorithm/string/split.hpp>

#include <Service/KeeperUtils.h>
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>

using namespace nuraft;

namespace RK
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

int Directory::createDir(const String & dir)
{
    Poco::File(dir).createDirectories();
    return 0;
}

String checkAndGetSuperdigest(const String & user_and_digest)
{
    if (user_and_digest.empty())
        return "";

    std::vector<String> scheme_and_id;
    boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
    if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect superdigest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

nuraft::ptr<nuraft::buffer> getZooKeeperLogEntry(int64_t session_id, int64_t time, const Coordination::ZooKeeperRequestPtr & request)
{
    RK::WriteBufferFromNuraftBuffer buf;
    RK::writeIntBinary(session_id, buf);
    request->write(buf);
    Coordination::write(time, buf);
    return buf.getBuffer();
}


ptr<log_entry> makeClone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
}

}
