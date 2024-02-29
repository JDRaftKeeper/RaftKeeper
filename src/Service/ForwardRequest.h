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

    virtual ForwardType forwardType() const = 0;

    void write(WriteBuffer & out) const;
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

    inline ForwardType forwardType() const override { return ForwardType::Handshake; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return fmt::format("#{}#{}#{}", RK::toString(forwardType()), server_id, client_id);
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

    inline ForwardType forwardType() const override { return ForwardType::SyncSessions; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return fmt::format("#{}#{}", RK::toString(forwardType()), session_expiration_time.size());
    }

};


struct ForwardNewSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    inline ForwardType forwardType() const override { return ForwardType::NewSession; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        auto * request_ptr = dynamic_cast<ZooKeeperNewSessionRequest *>(request.get());
        return fmt::format("#{}#{}#{}", RK::toString(forwardType()), request_ptr->internal_id, request_ptr->session_timeout_ms);
    }
};


struct ForwardUpdateSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    inline ForwardType forwardType() const override { return ForwardType::UpdateSession; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        auto * request_ptr = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get());
        return fmt::format("#{}#{}#{}", RK::toString(forwardType()), request_ptr->session_id, request_ptr->session_timeout_ms);
    }
};


struct ForwardUserRequest : public ForwardRequest
{
    RequestForSession request;

    inline ForwardType forwardType() const override { return ForwardType::User; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;
    RequestForSession requestForSession() const override;

    String toString() const override
    {
        return fmt::format("#{}#{}#{}", RK::toString(forwardType()), request.session_id, request.request->xid);
    }
};


class ForwardRequestFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<ForwardRequestPtr()>;
    using TypeToRequest = std::unordered_map<ForwardType, Creator>;

    static ForwardRequestFactory & instance();
    static ForwardRequestPtr convertFromRequest(const RequestForSession & request_for_session);

    ForwardRequestPtr get(ForwardType op_num) const;
    void registerRequest(ForwardType type, Creator creator);

private:
    ForwardRequestFactory();

    TypeToRequest type_to_request;
};

}
