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

ptr<buffer> serializeKeeperRequest(const RequestForSession & session_request)
{
    WriteBufferFromNuraftBuffer out;
    /// TODO unify digital encoding mode, see deserializeKeeperRequest
    writeIntBinary(session_request.session_id, out);
    session_request.request->write(out);
    Coordination::write(session_request.create_time, out);
    return out.getBuffer();
}

RequestForSession deserializeKeeperRequest(nuraft::buffer & data)
{
    ReadBufferFromNuRaftBuffer buffer(data);
    RequestForSession request_for_session;
    /// TODO unify digital encoding mode
    readIntBinary(request_for_session.session_id, buffer);

    int32_t length;
    Coordination::read(length, buffer);

    int32_t xid;
    Coordination::read(xid, buffer);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buffer);

    //    bool is_internal;
    //    Coordination::read(is_internal, buffer);

    request_for_session.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_for_session.request->xid = xid;
    request_for_session.request->readImpl(buffer);

    if (!buffer.eof())
        Coordination::read(request_for_session.create_time, buffer);
    else /// backward compatibility
        request_for_session.create_time
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    auto * log = &(Poco::Logger::get("NuRaftStateMachine"));
    LOG_TRACE(
        log,
        "Parsed request session id {}, length {}, xid {}, opnum {}",
        toHexString(request_for_session.session_id),
        length,
        xid,
        Coordination::toString(opnum));

    return request_for_session;
}

ptr<log_entry> makeClone(const ptr<log_entry> & entry)
{
    ptr<log_entry> clone = cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type());
    return clone;
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
