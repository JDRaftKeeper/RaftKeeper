#pragma once

#include <ZooKeeper/ZooKeeperIO.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <libnuraft/async.hxx>
#include <Service/formatHex.h>

namespace RK
{

class RequestForwarder;
struct ForwardRequest;
using ForwardRequestPtr = std::shared_ptr<ForwardRequest>;

enum class ForwardType : int8_t
{
    Unknown = -1,
    Handshake = 1,         /// Forwarder handshake
    SyncSessions = 2,      /// Follower will send all local sessions to leader periodically
    NewSession = 3,        /// New session request
    UpdateSession = 4,     /// Update session request when client reconnecting
    User = 5,              /// All write requests after the connection is established
    Destroy = 6,           /// Only used in server side to indicate that the connection is stale and server should close it
};

String toString(ForwardType type);


struct ForwardResponse
{
    bool accepted{true};
    int32_t error_code{nuraft::cmd_result_code::OK};

    void write(WriteBuffer & buf) const
    {
        Coordination::write(static_cast<int8_t>(forwardType()), buf);
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

struct ForwardSyncSessionsResponse : public ForwardResponse
{
    ForwardType forwardType() const override { return ForwardType::SyncSessions; }

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


struct ForwardNewSessionResponse : public ForwardResponse
{
    int64_t internal_id;

    ForwardType forwardType() const override { return ForwardType::NewSession; }

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

    void onError(RequestForwarder & forwarder) const override;
    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return "ForwardType: " + RK::toString(forwardType()) + ", accepted " + std::to_string(accepted) + " error_code "
            + std::to_string(error_code) + " session " + std::to_string(session_id);
    }
};

struct ForwardUserRequestResponse : public ForwardResponse
{
    int64_t session_id;
    int64_t xid;
    Coordination::OpNum opnum;

    ForwardType forwardType() const override { return ForwardType::User; }

    void readImpl(ReadBuffer &) override;
    void writeImpl(WriteBuffer &) const override;

    void onError(RequestForwarder & forwarder) const override;
    bool match(const ForwardRequestPtr & forward_request) const override;

    String toString() const override
    {
        return fmt::format("#{}#{}#{}#{}, accepted {}, error_code {}", RK::toString(forwardType()),
            toHexString(session_id), xid, Coordination::toString(opnum), accepted, error_code);
    }
};

struct ForwardDestroyResponse : public ForwardResponse
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
