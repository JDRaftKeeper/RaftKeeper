
#include <Service/ForwardingConnection.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <Common/IO/WriteHelpers.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
    extern const int UNEXPECTED_FORWARD_PACKET;
    extern const int RAFT_FORWARDING_ERROR;
    extern const int FORWARDING_DISCONNECTED;
}

void ForwardingConnection::connect()
{
    auto connection_timeout = socket_timeout.totalMicroseconds() / 3;

    Poco::Net::SocketAddress address{endpoint};
    static constexpr size_t num_tries = 3;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
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
                throw Exception("Receive handshake failed from " + endpoint, ErrorCodes::RAFT_FORWARDING_ERROR);

            LOG_TRACE(log, "Received handshake from {}", endpoint);

            connected = true;
            LOG_TRACE(log, "Connect to {} success", endpoint);
            break;
        }
        catch (...)
        {
            LOG_ERROR(log, "Exception when connect to server {}, {}: {}", endpoint, address.toString(), getCurrentExceptionMessage(true));
        }
    }
}

void ForwardingConnection::disconnect()
{
    if (connected)
    {
        socket.close();
        connected = false;
        /// reset errno if any
        errno = 0;
    }
}

void ForwardingConnection::send(ForwardRequestPtr request)
{
    if (!connected)
        connect();

    if (!connected)
        throw Exception("Connect to server failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);

    LOG_TRACE(log, "Forwarding request {} to endpoint {}", request->toString(), endpoint);

    try
    {
        request->write(*out);
    }
    catch (...)
    {
        disconnect();
        throw Exception(ErrorCodes::NETWORK_ERROR, "Exception while send request to {}", endpoint);
    }
}

bool ForwardingConnection::poll(UInt64 timeout_microseconds)
{
    if (!connected)
        return false;
    return in->poll(timeout_microseconds);
}

void ForwardingConnection::receive(ForwardResponsePtr & response)
{
    if (!connected)
        throw Exception("Forwarding connection disconnected", ErrorCodes::FORWARDING_DISCONNECTED);

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
            case Sessions:
                response = std::make_shared<ForwardSessionResponse>();
                break;
            case GetSession:
                response = std::make_shared<ForwardGetSessionResponse>();
                break;
            case UpdateSession:
                response = std::make_shared<ForwardUpdateSessionResponse>();
                break;
            case Operation:
                response = std::make_shared<ForwardOpResponse>();
                break;
            default:
                throw Exception("Unexpected forward package type " + std::to_string(response_type), ErrorCodes::UNEXPECTED_FORWARD_PACKET);
        }

        response->readImpl(*in);
    }
    catch (Exception & e)
    {
        tryLogCurrentException(log, "Exception while receiving forward result " + endpoint);

        /// If it is a network exception occur, we does not know whether server process the request.
        /// But here we just make it not accepted and leave client to determine how to process.

        disconnect();
        throw e;
    }
}

void ForwardingConnection::sendHandshake()
{
    LOG_TRACE(log, "send hand shake to {}, my_server_id {}, client_id {}", endpoint, my_server_id, client_id);
    Coordination::write(ForwardType::Handshake, *out);
    Coordination::write(my_server_id, *out);
    Coordination::write(client_id, *out);
    out->next();
}


bool ForwardingConnection::receiveHandshake()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == ForwardType::Handshake);

    bool accepted;
    Coordination::read(accepted, *in);

    int32_t code;
    Coordination::read(code, *in);
    return accepted;
}

}
