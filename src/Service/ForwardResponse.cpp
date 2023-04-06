
#include <Service/ForwardResponse.h>
#include <Service/ForwardRequest.h>
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
        case ForwardType::Sessions:
            return "Sessions";
        case ForwardType::GetSession:
            return "GetSession";
        case ForwardType::UpdateSession:
            return "UpdateSession";
        case ForwardType::Operation:
            return "Op";
        default:
            break;
    }
    int32_t raw_type = static_cast<int32_t>(type);
    throw Exception("ForwardType " + std::to_string(raw_type) + " is unknown", ErrorCodes::UNEXPECTED_FORWARD_PACKET);
}

void ForwardSessionResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);
}

void ForwardSessionResponse::writeImpl(WriteBuffer &) const
{

}

bool ForwardSessionResponse::match(const ForwardRequestPtr & forward_request) const
{
    return forward_request->forwardType() == forwardType();
}

void ForwardGetSessionResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);
    Coordination::read(internal_id, buf);
}

void ForwardGetSessionResponse::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(internal_id, buf);
}

void ForwardGetSessionResponse::onError([[maybe_unused]]RequestForwarder & request_forwarder) const
{
    /// dispatcher on error
}

bool ForwardGetSessionResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto session_request = dynamic_cast<ForwardGetSessionRequest *>(forward_request.get());
    if (session_request)
    {
        auto zk_session_request = dynamic_cast<ZooKeeperSessionIDRequest *>(session_request->request.get());
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
    /// dispatcher on error
}

bool ForwardUpdateSessionResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto session_request = dynamic_cast<ForwardUpdateSessionRequest *>(forward_request.get());
    if (session_request)
    {
        auto zk_session_request = dynamic_cast<ZooKeeperUpdateSessionRequest *>(session_request->request.get());
        if (zk_session_request)
        {
            return zk_session_request->session_id == session_id;
        }
    }

    return false;
}

void ForwardOpResponse::readImpl(ReadBuffer & buf)
{
    Coordination::read(accepted, buf);
    Coordination::read(error_code, buf);

    Coordination::read(session_id, buf);
    Coordination::read(xid, buf);
    Coordination::read(opnum, buf);
}

void ForwardOpResponse::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(session_id, buf);
    Coordination::write(xid, buf);
    Coordination::write(opnum, buf);
}

void ForwardOpResponse::onError(RequestForwarder & request_forwarder) const
{
    request_forwarder.request_processor->onError(accepted, static_cast<nuraft::cmd_result_code>(error_code), session_id, xid, opnum);
}

bool ForwardOpResponse::match(const ForwardRequestPtr & forward_request) const
{
    auto forward_request_ptr =  dynamic_cast<ForwardOpRequest *>(forward_request.get());
    if (forward_request_ptr)
    {
        return forward_request_ptr->request.session_id && forward_request_ptr->request.request->xid;
    }

    return false;
}


}
