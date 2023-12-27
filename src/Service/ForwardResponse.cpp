
#include <Service/ForwardRequest.h>
#include <Service/ForwardResponse.h>
#include <Service/RequestForwarder.h>


namespace RK
{


namespace ErrorCodes
{
    extern const int UNEXPECTED_FORWARD_PACKET;
}

std::string toString(ForwardType type)
{
    switch (type)
    {
        case ForwardType::Handshake:
            return "Handshake";
        case ForwardType::SyncSessions:
            return "SyncSessions";
        case ForwardType::NewSession:
            return "NewSession";
        case ForwardType::UpdateSession:
            return "UpdateSession";
        case ForwardType::User:
            return "User";
        case ForwardType::Destroy:
            return "Destroy";
        default:
            break;
    }
    int32_t raw_type = static_cast<int32_t>(type);
    throw Exception("ForwardType " + std::to_string(raw_type) + " is unknown", ErrorCodes::UNEXPECTED_FORWARD_PACKET);
}

void ForwardSyncSessionsResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);
}

void ForwardSyncSessionsResponse::writeImpl(WriteBuffer &) const
{
}

bool ForwardSyncSessionsResponse::match(const ForwardRequestPtr & forward_request) const
{
    return forward_request->forwardType() == forwardType();
}

void ForwardNewSessionResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);
    Coordination::read(internal_id, buf);
}

void ForwardNewSessionResponse::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(internal_id, buf);
}

void ForwardNewSessionResponse::onError(RequestForwarder & request_forwarder) const
{
    request_forwarder.request_processor->onError(
        accepted,
        static_cast<nuraft::cmd_result_code>(error_code),
        internal_id,
        Coordination::NEW_SESSION_XID,
        Coordination::OpNum::NewSession);
}

bool ForwardNewSessionResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto * session_request = dynamic_cast<ForwardNewSessionRequest *>(forward_request.get());
    if (session_request)
    {
        auto * zk_session_request = dynamic_cast<ZooKeeperNewSessionRequest *>(session_request->request.get());
        if (zk_session_request)
        {
            return zk_session_request->internal_id == internal_id;
        }
    }

    return false;
}

void ForwardUpdateSessionResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);
    Coordination::read(session_id, buf);
}

void ForwardUpdateSessionResponse::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(session_id, buf);
}

void ForwardUpdateSessionResponse::onError([[maybe_unused]] RequestForwarder & request_forwarder) const
{
    request_forwarder.request_processor->onError(
        accepted,
        static_cast<nuraft::cmd_result_code>(error_code),
        session_id,
        Coordination::UPDATE_SESSION_XID,
        Coordination::OpNum::UpdateSession);
}

bool ForwardUpdateSessionResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto * session_request = dynamic_cast<ForwardUpdateSessionRequest *>(forward_request.get());
    if (session_request)
    {
        auto * zk_session_request = dynamic_cast<ZooKeeperUpdateSessionRequest *>(session_request->request.get());
        if (zk_session_request)
        {
            return zk_session_request->session_id == session_id;
        }
    }

    return false;
}

void ForwardUserRequestResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);

    Coordination::read(session_id, buf);
    Coordination::read(xid, buf);
    Coordination::read(opnum, buf);
}

void ForwardUserRequestResponse::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(session_id, buf);
    Coordination::write(xid, buf);
    Coordination::write(opnum, buf);
}

void ForwardUserRequestResponse::onError(RequestForwarder & request_forwarder) const
{
    request_forwarder.request_processor->onError(accepted, static_cast<nuraft::cmd_result_code>(error_code), session_id, xid, opnum);
}

bool ForwardUserRequestResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto * forward_request_ptr = dynamic_cast<ForwardUserRequest *>(forward_request.get());
    if (forward_request_ptr)
    {
        return forward_request_ptr->request.session_id == session_id && forward_request_ptr->request.request->xid == xid;
    }

    return false;
}


}
