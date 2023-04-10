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
#include <Service/ForwardRequest.h>
#include <Service/ForwardResponse.h>

namespace RK
{

class ForwardingConnection
{
public:
    ForwardingConnection(int32_t server_id_, int32_t thread_id_, String endpoint_, Poco::Timespan socket_timeout_)
        : my_server_id(server_id_)
        , client_id(thread_id_)
        , endpoint(endpoint_)
        , socket_timeout(socket_timeout_)
        , log(&Poco::Logger::get("ForwardingConnection"))
    {
    }

    void connect();

    void send(ForwardRequestPtr request);
    void receive(ForwardResponsePtr & response);

    void disconnect();

    /// Send hand shake to forwarding server,
    /// server will register me.
    void sendHandshake();

    bool receiveHandshake();

    bool poll(UInt64 timeout_microseconds);

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
    int32_t client_id;

    std::atomic<bool> connected{false};

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
