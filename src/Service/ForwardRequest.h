#pragma once

#include <chrono>
#include <unordered_map>
#include <Service/KeeperStore.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <Service/ForwardResponse.h>

namespace RK
{

using namespace Coordination;

using clock = std::chrono::steady_clock;

class RequestForwarder;


struct ForwardRequest
{
    clock::time_point send_time;

    void write(WriteBuffer & out) const;

    virtual ForwardType forwardType() const = 0;

    virtual void readImpl(ReadBuffer &) = 0;
    virtual void writeImpl(WriteBuffer &) const = 0;

    virtual ForwardResponsePtr makeResponse() const = 0;
    virtual RequestForSession requestForSession() const = 0;

    virtual String toString() const = 0;

    virtual ~ForwardRequest()= default;
};

using ForwardRequestPtr = std::shared_ptr<ForwardRequest>;

struct ForwardHandshakeRequest : public ForwardRequest
{
    int32_t server_id; /// server_id is my id
    int32_t client_id;

    ForwardType forwardType() const override { return ForwardType::Handshake; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", server_id: " + std::to_string(server_id) + ", client_id: " + std::to_string(client_id);
    }
};

struct ForwardSyncSessionsRequest : public ForwardRequest
{
    std::unordered_map<int64_t, int64_t> session_expiration_time;

    ForwardSyncSessionsRequest() = default;

    explicit ForwardSyncSessionsRequest(std::unordered_map<int64_t, int64_t> && session_expiration_time_)
        : session_expiration_time(std::move(session_expiration_time_))
    {
    }

    ForwardType forwardType() const override { return ForwardType::SyncSessions; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", sessions size " + std::to_string(session_expiration_time.size());
    }

};

struct ForwardNewSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    ForwardType forwardType() const override { return ForwardType::NewSession; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        auto * request_ptr = dynamic_cast<ZooKeeperNewSessionRequest *>(request.get());
        return "ForwardType: " + RK::toString(forwardType()) + ", session " + std::to_string(request_ptr->internal_id) + " xid " + std::to_string(request_ptr->session_timeout_ms);
    }
};

struct ForwardUpdateSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    ForwardType forwardType() const override { return ForwardType::UpdateSession; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        auto * request_ptr = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get());
        return "ForwardType: " + RK::toString(forwardType()) + ", session " + toHexString(request_ptr->session_id) + " xid " + std::to_string(request_ptr->xid);
    }
};


struct ForwardUserRequest : public ForwardRequest
{
    RequestForSession request;

    ForwardType forwardType() const override { return ForwardType::User; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", session " + toHexString(request.session_id) + ", xid " + std::to_string(request.request->xid);
    }
};


class ForwardRequestFactory final : private boost::noncopyable
{

public:
    using Creator = std::function<ForwardRequestPtr()>;
    using TypeToRequest = std::unordered_map<ForwardType, Creator>;

    static ForwardRequestFactory & instance();

    ForwardRequestPtr get(ForwardType op_num) const;

    static ForwardRequestPtr convertFromRequest(const RequestForSession & request_for_session)
    {
        auto opnum = request_for_session.request->getOpNum();
        switch (opnum)
        {
            case Coordination::OpNum::NewSession:
            {
                auto forward_request = std::make_shared<ForwardNewSessionRequest>();
                forward_request->request = request_for_session.request;
                return forward_request;
            }
            case Coordination::OpNum::UpdateSession:
            {
                auto forward_request = std::make_shared<ForwardUpdateSessionRequest>();
                forward_request->request = request_for_session.request;
                return forward_request;
            }
            default:
            {
                auto forward_request = std::make_shared<ForwardUserRequest>();
                forward_request->request = request_for_session;
                return forward_request;
            }
        }
    }

    void registerRequest(ForwardType op_num, Creator creator);

private:
    TypeToRequest type_to_request;

    ForwardRequestFactory();
};

}
