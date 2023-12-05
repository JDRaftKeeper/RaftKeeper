#pragma once

#include <ZooKeeper/ZooKeeperIO.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <libnuraft/async.hxx>

namespace RK
{

using namespace Coordination;

class RequestForwarder;
struct ForwardRequest;
using ForwardRequestPtr = std::shared_ptr<ForwardRequest>;

enum ForwardType : int8_t
{
    Unknown = -1,
    Handshake = 1,
    Sessions = 2, /// all local sessions
    GetSession = 3, /// get session id
    UpdateSession = 4, /// session reconnect
    Operation = 5, /// all write requests after the connection is established
    Destroy = 6,
};

std::string toString(ForwardType type);


struct ForwardResponse
{
    bool accepted{true};
    int32_t error_code{nuraft::cmd_result_code::OK};

    void write(WriteBuffer & buf) const
    {
        Coordination::write(forwardType(), buf);
        Coordination::write(accepted, buf);
        Coordination::write(error_code, buf);
        writeImpl(buf);
    }

    virtual ForwardType forwardType() const = 0;

    virtual void readImpl(ReadBuffer &) = 0;

    virtual void writeImpl(WriteBuffer &) const = 0;
    virtual void onError(RequestForwarder & request_forwarder) const = 0;

    virtual bool match(const ForwardRequestPtr & forward_request) const = 0;

    void setAppendEntryResult(bool raft_accept, nuraft::cmd_result_code code)
    {
        accepted = raft_accept;
        error_code = code;
    }

    virtual ~ForwardResponse()= default;

    virtual String toString() const = 0;
};

using ForwardResponsePtr = std::shared_ptr<ForwardResponse>;


struct ForwardHandshakeResponse : public ForwardResponse
{
    ForwardType forwardType() const override { return ForwardType::Handshake; }

    void readImpl(ReadBuffer & buf) override
    {
        Coordination::read(accepted, buf);
        Coordination::read(error_code, buf);
    }

    void writeImpl(WriteBuffer &) const override {}

    void onError(RequestForwarder &) const override {}

    bool match(const ForwardRequestPtr &) const override { return false; }

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code);
    }
};

struct ForwardSessionResponse : public ForwardResponse
{
    ForwardType forwardType() const override { return ForwardType::Sessions; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    void onError(RequestForwarder &) const override {}

    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code);
    }
};


struct ForwardGetSessionResponse : public ForwardResponse
{
    int64_t internal_id;

    ForwardType forwardType() const override { return ForwardType::GetSession; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    void onError(RequestForwarder & request_forwarder) const override;

    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code) + " internal_id " + std::to_string(internal_id);
    }
};

struct ForwardUpdateSessionResponse : public ForwardResponse
{
    int64_t session_id;

    ForwardType forwardType() const override { return ForwardType::UpdateSession; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    void onError(RequestForwarder & request_forwarder) const override;

    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code) + " session " + std::to_string(session_id);
    }
};

struct ForwardOpResponse : public ForwardResponse
{
    int64_t session_id;
    int64_t xid;
    Coordination::OpNum opnum;

    ForwardType forwardType() const override { return ForwardType::Operation; }

    void readImpl(ReadBuffer &) override;

    void writeImpl(WriteBuffer &) const override;

    void onError(RequestForwarder & request_forwarder) const override;

    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code) + " session " + std::to_string(session_id) + " xid " + std::to_string(xid) + " opnum "
            + Coordination::toString(opnum);
    }
};

struct ForwardDestryResponse : public ForwardResponse
{
    ForwardType forwardType() const override { return ForwardType::Destroy; }

    void readImpl(ReadBuffer & buf) override
    {
        Coordination::read(accepted, buf);
        Coordination::read(error_code, buf);
    }

    void writeImpl(WriteBuffer &) const override {}

    void onError(RequestForwarder &) const override {}

    bool match(const ForwardRequestPtr &) const override { return false; }

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code);
    }
};

}
