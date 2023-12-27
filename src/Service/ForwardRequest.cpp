#include <Service/ForwardRequest.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <Service/RequestForwarder.h>
#include <Common/Exception.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_FORWARD_PACKET;
    extern const int NOT_IMPLEMENTED;
}

void ForwardRequest::write(WriteBuffer & out) const
{
    Coordination::write(static_cast<int8_t>(forwardType()), out);
    writeImpl(out);
    out.next();
}

void ForwardHandshakeRequest::readImpl(ReadBuffer & buf)
{
    Coordination::read(server_id, buf);
    Coordination::read(client_id, buf);
}

void ForwardHandshakeRequest::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(server_id, buf);
    Coordination::write(client_id, buf);
}

ForwardResponsePtr ForwardHandshakeRequest::makeResponse() const
{
    return std::make_shared<ForwardHandshakeResponse>();
}

RequestForSession ForwardHandshakeRequest::requestForSession() const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented.");
}

void ForwardSyncSessionsRequest::readImpl(ReadBuffer & buf)
{
    int32_t session_size;
    read(session_size, buf);
    for (int32_t i = 0; i < session_size; ++i)
    {
        int64_t session_id;
        read(session_id, buf);
        int64_t expiration_time;
        read(expiration_time, buf);
        session_expiration_time.emplace(session_id, expiration_time);
    }
}

void ForwardSyncSessionsRequest::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(static_cast<int32_t>(session_expiration_time.size() * 16 + 4), buf);
    Coordination::write(static_cast<int32_t>(session_expiration_time.size()), buf);
    for (const auto & session_expiration : session_expiration_time)
    {
        Coordination::write(session_expiration.first, buf);
        Coordination::write(session_expiration.second, buf);
    }
}

ForwardResponsePtr ForwardSyncSessionsRequest::makeResponse() const
{
    return std::make_shared<ForwardSyncSessionsResponse>();
}

RequestForSession ForwardSyncSessionsRequest::requestForSession() const
{
    RequestForSession request_info; /// TODO throw exception
    return request_info;
}

void ForwardNewSessionRequest::readImpl(ReadBuffer & buf)
{
    int32_t xid;
    Coordination::read(xid, buf);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buf);

    request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;

    request->readImpl(buf);
}

void ForwardNewSessionRequest::writeImpl(WriteBuffer & buf) const
{
    WriteBufferFromOwnString out_buf;
    Coordination::write(request->xid, out_buf);
    Coordination::write(request->getOpNum(), out_buf);
    request->writeImpl(out_buf);
    Coordination::write(out_buf.str(), buf);
}

ForwardResponsePtr ForwardNewSessionRequest::makeResponse() const
{
    auto res = std::make_shared<ForwardNewSessionResponse>();
    res->internal_id = dynamic_cast<ZooKeeperNewSessionRequest *>(request.get())->internal_id;
    return res;
}

RequestForSession ForwardNewSessionRequest::requestForSession() const
{
    RequestForSession request_info;
    request_info.request = request;
    request_info.session_id = dynamic_cast<ZooKeeperNewSessionRequest *>(request.get())->internal_id;
    return request_info;
}

void ForwardUpdateSessionRequest::readImpl(ReadBuffer & buf)
{
    int32_t xid;
    Coordination::read(xid, buf);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buf);

    request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;

    request->readImpl(buf);
}

void ForwardUpdateSessionRequest::writeImpl(WriteBuffer & buf) const
{
    WriteBufferFromOwnString out_buf;
    Coordination::write(request->xid, out_buf);
    Coordination::write(request->getOpNum(), out_buf);
    request->writeImpl(out_buf);
    Coordination::write(out_buf.str(), buf);
}


ForwardResponsePtr ForwardUpdateSessionRequest::makeResponse() const
{
    auto res = std::make_shared<ForwardUpdateSessionResponse>();
    res->session_id = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get())->session_id;
    return res;
}

RequestForSession ForwardUpdateSessionRequest::requestForSession() const
{
    RequestForSession request_for_session;
    request_for_session.request = request;
    request_for_session.session_id = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get())->session_id;
    return request_for_session;
}

void ForwardUserRequest::readImpl(ReadBuffer & buf)
{
    Coordination::read(request.session_id, buf);

    int32_t xid;
    Coordination::read(xid, buf);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buf);

    request.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request.request->xid = xid;
    request.request->readImpl(buf);

//    bool is_internal;
//    Coordination::read(is_internal, buf);
//    request.is_internal = is_internal;
}

void ForwardUserRequest::writeImpl(WriteBuffer & buf) const
{
    WriteBufferFromOwnString out_buf;
    Coordination::write(request.session_id, out_buf);
    Coordination::write(request.request->xid, out_buf);
    Coordination::write(request.request->getOpNum(), out_buf);
    request.request->writeImpl(out_buf);
    Coordination::write(out_buf.str(), buf);
//    Coordination::write(request.is_internal, buf);
}


ForwardResponsePtr ForwardUserRequest::makeResponse() const
{
    auto res = std::make_shared<ForwardUserRequestResponse>();
    res->session_id = request.session_id;
    res->xid = request.request->xid;
    res->opnum = request.request->getOpNum();
    return res;
}

RequestForSession ForwardUserRequest::requestForSession() const
{
    return request;
}

ForwardRequestPtr ForwardRequestFactory::get(ForwardType type) const
{
    auto it = type_to_request.find(type);
    if (it == type_to_request.end())
        throw Exception("Unknown request type " + toString(type), ErrorCodes::UNEXPECTED_FORWARD_PACKET);

    return it->second();
}

ForwardRequestFactory & ForwardRequestFactory::instance()
{
    static ForwardRequestFactory factory;
    return factory;
}

template<ForwardType num, typename RequestT>
void registerForwardRequest(ForwardRequestFactory & factory)
{
    factory.registerRequest(num, []()
    {
        auto res = std::make_shared<RequestT>();
        return res;
    });
}


void ForwardRequestFactory::registerRequest(ForwardType type, Creator creator)
{
    if (!type_to_request.try_emplace(type, creator).second)
        throw Exception("Request type " + toString(type) + " already registered", ErrorCodes::UNEXPECTED_FORWARD_PACKET);
}


ForwardRequestFactory::ForwardRequestFactory()
{
    registerForwardRequest<ForwardType::User, ForwardUserRequest>(*this);
    registerForwardRequest<ForwardType::SyncSessions, ForwardSyncSessionsRequest>(*this);
    registerForwardRequest<ForwardType::NewSession, ForwardNewSessionRequest>(*this);
    registerForwardRequest<ForwardType::UpdateSession, ForwardUpdateSessionRequest>(*this);
}

}
