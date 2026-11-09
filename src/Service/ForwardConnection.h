#pragma once

#include <shared_mutex>
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

class ForwardConnection
{
public:
    ForwardConnection(int32_t server_id_, int32_t client_id_, String endpoint_, Poco::Timespan socket_timeout_)
        : my_server_id(server_id_)
        , client_id(client_id_)
        , endpoint(endpoint_)
        , socket_timeout(socket_timeout_)
        , log(&Poco::Logger::get(fmt::format("ForwardConnection#{}#{}#{}",
            my_server_id, client_id, endpoint)))
    {
    }

    void connect();
    void disconnect();

    void send(ForwardRequestPtr request);
    void receive(ForwardResponsePtr & response);

    bool poll(UInt64 timeout_microseconds);

    bool isConnected() const { return connected; }

    ~ForwardConnection()
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
    /// Lifecycle helpers require the exclusive socket lock.
    void sendHandshake();
    bool receiveHandshake();
    void disconnectUnlocked();

    /// Retries count when connecting
    static constexpr size_t num_retries = 3;

    int32_t my_server_id;
    int32_t client_id;

    std::atomic<bool> connected{false};

    /// Remote endpoint
    String endpoint;

    /// socket send and receive timeout, but it not work for
    /// socket is non-blocking
    ///     For sending: loop to send n length @see WriteBufferFromPocoSocket.
    ///     For receiving: use poll to wait socket to be available.
    Poco::Timespan socket_timeout;

    /// One sender and one receiver may use the socket concurrently. Closing/replacing it
    /// and resetting its buffers must wait for both directions (including poll) to finish.
    std::shared_mutex socket_mutex;
    Poco::Net::StreamSocket socket;

    /// socket read and write buffer
    std::optional<ReadBufferFromPocoSocket> in;
    std::optional<WriteBufferFromPocoSocket> out;

    Poco::Logger * log;
};
}
