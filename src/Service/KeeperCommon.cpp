#include <Service/KeeperCommon.h>
#include <Service/formatHex.h>

namespace RK
{

String ErrorRequest::toString() const
{
    return fmt::format(
        "[session_id:{}, xid:{}, opnum:{}, accepted:{}, error_code:{}]",
        toHexString(session_id),
        xid,
        Coordination::toString(opnum),
        accepted,
        error_code);
}

String ErrorRequest::getRequestId() const
{
    return RequestId{session_id, xid}.toString();
}

String RequestId::toString() const
{
    return fmt::format("[session_id:{}, xid:{}]", toHexString(session_id), xid);
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
        "[session_id: {}, xid:{}, opnum:{}, create_time:{}, server_id:{}, client_id:{}, request:{}]",
        toHexString(session_id),
        request->xid,
        Coordination::toString(request->getOpNum()),
        create_time,
        server_id,
        client_id,
        request->toString());
}

String RequestForSession::toSimpleString() const
{
    return fmt::format(
        "[session_id:{}, xid:{}, opnum:{}]", toHexString(session_id), request->xid, Coordination::toString(request->getOpNum()));
}

RequestId RequestForSession::getRequestId() const
{
    return {session_id, request->xid};
}

}
