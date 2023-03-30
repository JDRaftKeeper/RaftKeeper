
#include <Service/ForwardRequest.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <Service/RequestForwarder.h>
#include <Common/Exception.h>

namespace RK
{

void ForwardRequest::write(WriteBuffer & out) const
{
    Coordination::write(forwardType(), out);
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

KeeperStore::RequestForSession ForwardHandshakeRequest::requestForSession() const
{
    RequestForSession reuqest_info;
    return reuqest_info;
}

void ForwardSessionRequest::readImpl(ReadBuffer &)
{
    /// TODO
}

void ForwardSessionRequest::writeImpl(WriteBuffer & buf) const
{
    Coordination::write(static_cast<int32_t>(session_expiration_time.size()), buf);
    for (const auto & session_expiration : session_expiration_time)
    {
        Coordination::write(session_expiration.first, buf);
        Coordination::write(session_expiration.second, buf);
    }
}

ForwardResponsePtr ForwardSessionRequest::makeResponse() const
{
    return std::make_shared<ForwardSessionResponse>();
}

KeeperStore::RequestForSession ForwardSessionRequest::requestForSession() const
{
    RequestForSession reuqest_info;
    return reuqest_info;
}

void ForwardGetSessionRequest::readImpl(ReadBuffer & buf)
{
    int32_t xid;
    Coordination::read(xid, buf);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buf);

    request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;

    request->readImpl(buf);
}

void ForwardGetSessionRequest::writeImpl(WriteBuffer & buf) const
{
    WriteBufferFromOwnString out_buf;
    Coordination::write(request->xid, out_buf);
    Coordination::write(request->getOpNum(), out_buf);
    request->writeImpl(out_buf);
    Coordination::write(out_buf.str(), buf);
}

ForwardResponsePtr ForwardGetSessionRequest::makeResponse() const
{
    auto res = std::make_shared<ForwardGetSessionResponse>();
    res->accepted = false;
    res->error_code = nuraft::cmd_result_code::FAILED;
    res->internal_id = dynamic_cast<ZooKeeperSessionIDRequest *>(request.get())->internal_id;
    return res;
}

KeeperStore::RequestForSession ForwardGetSessionRequest::requestForSession() const
{
    RequestForSession reuqest_info;
    reuqest_info.request = request;
    reuqest_info.session_id = -1;
    return reuqest_info;
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
    auto res = std::make_shared<ForwardOpResponse>();
    res->accepted = false;
    res->error_code = nuraft::cmd_result_code::FAILED;
    res->session_id = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get())->session_id;
    return res;
}

KeeperStore::RequestForSession ForwardUpdateSessionRequest::requestForSession() const
{
    RequestForSession reuqest_info;
    reuqest_info.request = request;
    reuqest_info.session_id = -1;
    return reuqest_info;
}

void ForwardOpRequest::readImpl(ReadBuffer & buf)
{
    Coordination::read(request.session_id, buf);

    int32_t xid;
    Coordination::read(xid, buf);

    Coordination::OpNum opnum;
    Coordination::read(opnum, buf);

    request.request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request.request->xid = xid;
    request.request->readImpl(buf);
}

void ForwardOpRequest::writeImpl(WriteBuffer & buf) const
{
    WriteBufferFromOwnString out_buf;
    Coordination::write(request.session_id, out_buf);
    Coordination::write(request.request->xid, out_buf);
    Coordination::write(request.request->getOpNum(), out_buf);
    request.request->writeImpl(out_buf);
    Coordination::write(out_buf.str(), buf);
}


ForwardResponsePtr ForwardOpRequest::makeResponse() const
{
    auto res = std::make_shared<ForwardOpResponse>();
    res->accepted = false;
    res->error_code = nuraft::cmd_result_code::FAILED;
    res->session_id = request.session_id;
    res->xid = request.request->xid;
    res->opnum = request.request->getOpNum();
    return res;
}

KeeperStore::RequestForSession ForwardOpRequest::requestForSession() const
{
    return request;
}

ForwardRequestPtr ForwardRequestFactory::get(ForwardType type) const
{
    auto it = op_num_to_request.find(type);
    if (it == op_num_to_request.end())
        throw Exception("Unknown operation type " + std::to_string(type), 0); /// TODO

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


void ForwardRequestFactory::registerRequest(ForwardType op_num, Creator creator)
{
    if (!op_num_to_request.try_emplace(op_num, creator).second)
        ;
//        throw Exception("Request type " + toString(op_num) + " already registered", 0); TODO
}


ForwardRequestFactory::ForwardRequestFactory()
{
    registerForwardRequest<ForwardType::Op, ForwardOpRequest>(*this);
    registerForwardRequest<ForwardType::Sessions, ForwardSessionRequest>(*this);
    registerForwardRequest<ForwardType::GetSession, ForwardGetSessionRequest>(*this);
    registerForwardRequest<ForwardType::UpdateSession, ForwardUpdateSessionRequest>(*this);
}

}
