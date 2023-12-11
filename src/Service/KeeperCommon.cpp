#include <Service/KeeperCommon.h>
#include <Service/formatHex.h>

namespace RK
{

String ErrorRequest::toString() const
{
    return fmt::format(
        "#{}#xid#{} accepted:{} error_code:{}",
        toHexString(session_id),
        xid,
        Coordination::toString(opnum),
        accepted,
        error_code);
}

RequestId ErrorRequest::getRequestId() const
{
    return {session_id, xid};
}

String RequestId::toString() const
{
    return fmt::format("#{}#{}", toHexString(session_id), xid);
}

bool RequestId::operator==(const RequestId & other) const
{
    return session_id == other.session_id && xid == other.xid;
}

std::size_t RequestId::RequestIdHash::operator()(const RequestId & request_id) const
{
    std::size_t seed = 0;
    std::hash<int64_t> hash64;
    std::hash<int32_t> hash32;

    seed ^= hash64(request_id.session_id);
    seed ^= hash32(request_id.xid);

    return seed;
}

String RequestForSession::toString() const
{
    return fmt::format(
        "[session:{} request:{} create_time:{} server_id:{} client_id:{}]",
        toHexString(session_id),
        request->toString(),
        create_time,
        server_id,
        client_id);
}

String RequestForSession::toSimpleString() const
{
    return fmt::format("#{}#{}#{}", toHexString(session_id), request->xid, Coordination::toString(request->getOpNum()));
}

RequestId RequestForSession::getRequestId() const
{
    return {session_id, request->xid};
}

}
