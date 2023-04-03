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

    virtual KeeperStore::RequestForSession requestForSession() const = 0;

    virtual ~ForwardRequest()= default;
};

using ForwardRequestPtr = std::shared_ptr<ForwardRequest>;

struct ForwardHandshakeRequest : public ForwardRequest
{
    int32_t server_id;
    int32_t client_id;

    ForwardType forwardType() const override { return ForwardType::Handshake; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    virtual ForwardResponsePtr makeResponse() const override;

    KeeperStore::RequestForSession requestForSession() const override;
};

struct ForwardSessionRequest : public ForwardRequest
{
    std::unordered_map<int64_t, int64_t> session_expiration_time;

    ForwardSessionRequest() = default;

    ForwardSessionRequest(std::unordered_map<int64_t, int64_t> && session_expiration_time_)
        : session_expiration_time(std::forward<std::unordered_map<int64_t, int64_t>>(session_expiration_time_))
    {
    }

    ForwardType forwardType() const override { return ForwardType::Sessions; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    virtual ForwardResponsePtr makeResponse() const override;

    KeeperStore::RequestForSession requestForSession() const override;

};

struct ForwardGetSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    ForwardType forwardType() const override { return ForwardType::GetSession; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    virtual ForwardResponsePtr makeResponse() const override;

    KeeperStore::RequestForSession requestForSession() const override;
};

struct ForwardUpdateSessionRequest : public ForwardRequest
{
    Coordination::ZooKeeperRequestPtr request;

    ForwardType forwardType() const override { return ForwardType::UpdateSession; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    virtual ForwardResponsePtr makeResponse() const override;

    KeeperStore::RequestForSession requestForSession() const override;
};


struct ForwardOpRequest : public ForwardRequest
{
    KeeperStore::RequestForSession request;

    ForwardType forwardType() const override { return ForwardType::Op; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    ForwardResponsePtr makeResponse() const override;

    KeeperStore::RequestForSession requestForSession() const override;
};


class ForwardRequestFactory final : private boost::noncopyable
{

public:
    using Creator = std::function<ForwardRequestPtr()>;
    using TypeToRequest = std::unordered_map<ForwardType, Creator>;

    static ForwardRequestFactory & instance();

    ForwardRequestPtr get(ForwardType op_num) const;

    static ForwardRequestPtr convertFromRequest(const KeeperStore::RequestForSession & request_for_session)
    {
        auto opnum = request_for_session.request->getOpNum();
        switch (opnum)
        {
            case Coordination::OpNum::SessionID:
            {
                auto session_id_res = std::make_shared<ForwardGetSessionRequest>();
                session_id_res->request = request_for_session.request;
                return session_id_res;
            }
            case Coordination::OpNum::UpdateSession:
            {
                auto update_session_res = std::make_shared<ForwardUpdateSessionRequest>();
                update_session_res->request = request_for_session.request;
                return update_session_res;
            }
            default:
            {
                auto op_res = std::make_shared<ForwardOpRequest>();
                op_res->request = request_for_session;
                return op_res;
            }
        }
    }

    void registerRequest(ForwardType op_num, Creator creator);

private:
    TypeToRequest op_num_to_request;

    ForwardRequestFactory();
};

}
