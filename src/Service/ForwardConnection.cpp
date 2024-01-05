
#include <Common/IO/WriteHelpers.h>

#include <Service/ForwardConnection.h>
#include <ZooKeeper/ZooKeeperIO.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int UNEXPECTED_FORWARD_PACKET;
    extern const int RAFT_FORWARD_ERROR;
    extern const int FORWARD_NOT_CONNECTED;
}

void ForwardConnection::connect()
{
    auto connection_timeout = socket_timeout.totalMicroseconds() / 3;
    Poco::Net::SocketAddress address{endpoint};

    for (size_t i = 0; i < num_retries; i++)
    {
        try
        {
            LOG_TRACE(log, "Try connect forward server {}", endpoint);

            /// Reset the state of previous attempt.
            socket = Poco::Net::StreamSocket();

            socket.connect(address, connection_timeout);

            socket.setReceiveTimeout(socket_timeout);
            socket.setSendTimeout(socket_timeout);
            socket.setNoDelay(true);

            in.emplace(socket);
            out.emplace(socket);

            sendHandshake();
            LOG_TRACE(log, "Sent handshake to {}", endpoint);

            if (!receiveHandshake())
                throw Exception(ErrorCodes::RAFT_FORWARD_ERROR, "Handshake with {} failed", endpoint);

            connected = true;
            LOG_TRACE(log, "Connect to {} success", endpoint);
            break;
        }
        catch (...)
        {
            if (i == (num_retries - 1))
                throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Connect to forward server {} failed", endpoint);
            else
                Poco::Thread::current()->sleep(1000);
        }
    }
}

void ForwardConnection::disconnect()
{
    if (connected)
    {
        socket.close();
        connected = false;
        /// reset errno if any
        errno = 0;
    }
}

void ForwardConnection::send(ForwardRequestPtr request)
{
    LOG_TRACE(log, "Forward request {} to endpoint {}", request->toString(), endpoint);

    if (unlikely(!connected))
        connect();

    try
    {
        request->write(*out);
    }
    catch (...)
    {
        disconnect();
        throw;
    }
}

bool ForwardConnection::poll(UInt64 timeout_microseconds)
{
    if (!connected)
        return false;
    return in->poll(timeout_microseconds);
}

void ForwardConnection::receive(ForwardResponsePtr & response)
{
    if (!connected)
        throw Exception(ErrorCodes::FORWARD_NOT_CONNECTED, "Forwarding connection disconnected");

    /// There are two situations,
    ///     1. Feedback not accepted.
    ///     2. Receiving network packets failed, which cannot determine whether the opposite end is accepted.
    try
    {
        int8_t type;
        Coordination::read(type, *in);

        ForwardType response_type = static_cast<ForwardType>(type);
        switch (response_type)
        {
            case ForwardType::SyncSessions:
                response = std::make_shared<ForwardSyncSessionsResponse>();
                break;
            case ForwardType::NewSession:
                response = std::make_shared<ForwardNewSessionResponse>();
                break;
            case ForwardType::UpdateSession:
                response = std::make_shared<ForwardUpdateSessionResponse>();
                break;
            case ForwardType::User:
                response = std::make_shared<ForwardUserRequestResponse>();
                break;
            default:
                throw Exception("Unexpected forward package type " + toString(response_type), ErrorCodes::UNEXPECTED_FORWARD_PACKET);
        }

        response->readImpl(*in);
    }
    catch (Exception & e)
    {
        tryLogCurrentException(log, "Exception while receiving forward result from " + endpoint);

        /// If it is a network exception occur, we does not know whether server process the request.
        /// But here we just make it not accepted and leave client to determine how to process.

        disconnect();
        throw e;
    }
}

void ForwardConnection::sendHandshake()
{
    LOG_TRACE(log, "Send handshake to leader {}, my_server_id {}, client_id {}", endpoint, my_server_id, client_id);
    ForwardHandshakeRequest handshake;
    handshake.server_id = my_server_id;
    handshake.client_id = client_id;
    handshake.write(*out);
}


bool ForwardConnection::receiveHandshake()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == static_cast<int8_t>(ForwardType::Handshake));

    ForwardHandshakeResponse handshake;
    handshake.readImpl(*in);

    return handshake.accepted;
}

}
