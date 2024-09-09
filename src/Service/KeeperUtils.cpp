#include <Service/KeeperUtils.h>

#include <Poco/Base64Encoder.h>
#include <Poco/SHA1Engine.h>

#include <Common/IO/ReadHelpers.h>
#include <Common/IO/WriteHelpers.h>
#include <boost/algorithm/string/split.hpp>

#include <Service/formatHex.h>
#include <Service/ReadBufferFromNuRaftBuffer.h>
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

String checkAndGetSuperDigest(const String & user_and_digest)
{
    if (user_and_digest.empty())
        return "";

    std::vector<String> scheme_and_id;
    boost::split(scheme_and_id, user_and_digest, [](char c) { return c == ':'; });
    if (scheme_and_id.size() != 2 || scheme_and_id[0] != "super")
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect super digest in keeper_server config. Must be 'super:base64string'");

    return user_and_digest;
}

ptr<buffer> serializeKeeperRequest(const RequestForSession & request)
{
    WriteBufferFromNuraftBuffer out;
    writeIntBinary(request.session_id, out);
    request.request->write(out);
    Coordination::write(request.create_time, out);
    return out.getBuffer();
}

ptr<RequestForSession> deserializeKeeperRequest(nuraft::buffer & data)
{
    ptr<RequestForSession> request = cs_new<RequestForSession>();
    ReadBufferFromNuRaftBuffer buffer(data);
    readIntBinary(request->session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    //    bool is_internal;
    //    Coordination::read(is_internal, buffer);

    request->request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->request->xid = xid;
    request->request->readImpl(buffer);

    Coordination::read(request->create_time, buffer);

    return request;
}

ptr<log_entry> cloneLogEntry(const ptr<log_entry> & entry)
{
    ptr<log_entry> cloned = cs_new<log_entry>(
        entry->get_term(),
        buffer::clone(entry->get_buf()),
        entry->get_val_type(),
        entry->get_timestamp(),
        entry->has_crc32(),
        entry->get_crc32(),
        false);
    return cloned;
}

String getBaseName(const String & path)
{
    size_t basename_start = path.rfind('/');
    return String{&path[basename_start + 1], path.length() - basename_start - 1};
}


String getParentPath(const String & path)
{
    auto rslash_pos = path.rfind('/');
    if (rslash_pos > 0)
        return path.substr(0, rslash_pos);
    return "/";
}

String base64Encode(const String & decoded)
{
    std::ostringstream ostr; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ostr.exceptions(std::ios::failbit);
    Poco::Base64Encoder encoder(ostr);
    encoder.rdbuf()->setLineLength(0);
    encoder << decoded;
    encoder.close();
    return ostr.str();
}

String getSHA1(const String & userdata)
{
    Poco::SHA1Engine engine;
    engine.update(userdata);
    const auto & digest_id = engine.digest();
    return String{digest_id.begin(), digest_id.end()};
}

String generateDigest(const String & userdata)
{
    std::vector<String> user_password;
    boost::split(user_password, userdata, [](char c) { return c == ':'; });
    return user_password[0] + ":" + base64Encode(getSHA1(userdata));
}

}
