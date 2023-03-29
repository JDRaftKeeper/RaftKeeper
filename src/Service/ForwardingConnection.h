#pragma once

#include <Service/KeeperStore.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <libnuraft/async.hxx>
#include <Poco/Net/StreamSocket.h>
#include <Common/IO/ReadBufferFromPocoSocket.h>
#include <Common/IO/WriteBufferFromPocoSocket.h>
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

    /// {session_id, xid} is actually request id
    int64_t session_id{non_session_id};
    int64_t xid{non_xid};

    Coordination::OpNum opnum{Coordination::OpNum::Error};

    /// serialize a ForwardResponse
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
        res += ", session_id: " + toHexString(session_id);
        res += ", xid: " + std::to_string(xid);
        res += ", opnum: " + Coordination::toString(opnum);
        return res;
    }
};

/**
 * Client for forward request to leader.
 */
class ForwardingConnection
{
public:
    ForwardingConnection(int32_t server_id_, int32_t thread_id_, String endpoint_, Poco::Timespan socket_timeout_)
        : my_server_id(server_id_)
        , thread_id(thread_id_)
        , endpoint(endpoint_)
        , socket_timeout(socket_timeout_)
        , log(&Poco::Logger::get("ForwardingConnection"))
    {
    }

    void connect();
    void disconnect();

    /// Send hand shake to forwarding server,
    /// server will register me.
    void sendHandshake();

    [[maybe_unused]] [[maybe_unused]] void receiveHandshake();

    void send(KeeperStore::RequestForSession request_for_session);
    bool receive(ForwardResponse & response);

    /// Send session to leader every session_sync_period_ms.
    void sendSession(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time);

    /// Used to wait response.
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
            /// We must continue to execute all callbacks,
            /// because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

private:
    int32_t my_server_id;
    int32_t thread_id;

    bool connected{false};
    /// server address who is cluster leader.
    String endpoint;

    /// socket send and receive timeout, but it not work for
    /// socket is non-blocking
    ///     For sending: loop to send n length @see WriteBufferFromPocoSocket.
    ///     For receiving: use poll to wait socket to be available.
    Poco::Timespan socket_timeout;
    Poco::Net::StreamSocket socket;

    /// socket read and write buffer
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;
};
}
