#pragma once

#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Service/KeeperStore.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <libnuraft/async.hxx>
#include <Poco/Net/StreamSocket.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <common/logger_useful.h>

namespace RK
{

enum PkgType : int8_t
{
    Unknown = -1,
    Handshake = 1,
    Session = 2,
    Data = 3,
    /// TODO remove Result
    Result = 4
};

struct ForwardResponse
{
    static constexpr int64_t non_session_id = -1;
    static constexpr int64_t non_xid = -1;

    PkgType protocol{-1};

    /// result info
    bool accepted{true};
    int32_t error_code{nuraft::cmd_result_code::OK};

    /// source info
    int64_t session_id{non_session_id};
    int64_t xid{non_xid};
    Coordination::OpNum opnum{Coordination::OpNum::Error};

    void write(WriteBufferFromFiFoBuffer & buf) const
    {
        Coordination::write(protocol, buf);
        Coordination::write(accepted, buf);
        Coordination::write(error_code, buf);
        Coordination::write(session_id, buf);
        Coordination::write(xid, buf);
        Coordination::write(opnum, buf);
    }

    String toString() const
    {
        String res;

        switch (protocol)
        {
            case Handshake:
                res += "Handshake";
                break;
            case Session:
                res += "Session";
                break;
            case Data:
                res += "Data";
                break;
            case Result:
                res += "Result";
                break;
            default:
                res += "Unknown";
                break;
        }
        res += ", accepted: " + std::to_string(accepted);
        res += ", error_code: " + std::to_string(error_code);
        res += ", session_id: " + std::to_string(session_id);
        res += ", xid: " + std::to_string(xid);
        res += ", opnum: " + Coordination::toString(opnum);
        return res;
    }
};

class ForwardingConnection
{
public:
    ForwardingConnection(int32_t server_id_, int32_t thread_id_, String endpoint_, Poco::Timespan operation_timeout_ms)
        : my_server_id(server_id_)
        , thread_id(thread_id_)
        , endpoint(endpoint_)
        , operation_timeout(operation_timeout_ms)
        , log(&Poco::Logger::get("ForwardingConnection"))
    {
    }

    void connect(Poco::Timespan connection_timeout);
    void send(KeeperStore::RequestForSession request_for_session);
    bool receive(ForwardResponse & response);
    void disconnect();

    void sendHandshake();

    void receiveHandshake();

    void sendSession(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time);

    bool poll(UInt64 max_wait);

    bool isConnected() const { return connected; }

    ~ForwardingConnection()
    {
        try
        {
            disconnect();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    int32_t my_server_id;
    int32_t thread_id;
    bool connected{false};
    String endpoint;
    Poco::Timespan operation_timeout;
    Poco::Net::StreamSocket socket;
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;
};
}
