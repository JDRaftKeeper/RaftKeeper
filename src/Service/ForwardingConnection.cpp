
#include <Service/ForwardingConnection.h>
#include <ZooKeeper/ZooKeeperIO.h>
#include <Common/IO/WriteHelpers.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
}

std::string toString(PkgType pkg_type)
{
    switch (pkg_type)
    {
        case Unknown:
            return "Unknown";
        case Handshake:
            return "Handshake";
        case Session:
            return "Session";
        case Data:
            return "Data";
    }
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

            /// non blocking socket
            socket.setNoDelay(true);

            in.emplace(socket);
            out.emplace(socket);

            sendHandshake();
            LOG_TRACE(log, "Sent handshake {}", endpoint);

            // TODO receiveHandshake
            // receiveHandshake();
            // LOG_TRACE(log, "received handshake {}", endpoint);

            connected = true;
            LOG_TRACE(log, "Connect success {}", endpoint);
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

void ForwardingConnection::send(const KeeperStore::RequestForSession & request_for_session)
{
    if (!connected)
        connect();

    if (!connected)
        throw Exception("Connect to server failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);

    LOG_TRACE(
        log,
        "Forwarding session {}, xid {} to server {}",
        toHexString(request_for_session.session_id),
        request_for_session.request->xid,
        endpoint);

    try
    {
        ForwardRequest::writeData(*out, request_for_session);
    }
    catch (...)
    {
        disconnect();
        throw Exception(ErrorCodes::NETWORK_ERROR, "Exception while send request to {}", endpoint);
    }
}

bool ForwardingConnection::poll(UInt64 max_wait)
{
    if (!connected)
        return false;
    return in->poll(max_wait);
}

bool ForwardingConnection::receive(ForwardResponse & response)
{
    if (!connected)
        return false;

    /// There are two situations,
    ///     1. Feedback not accepted.
    ///     2. Receiving network packets failed, which cannot determine whether the opposite end is accepted.
    try
    {
        int8_t type;
        Coordination::read(type, *in);
        response.pkg_type = static_cast<PkgType>(type);

        Coordination::read(response.accepted, *in);

        int32_t code;
        Coordination::read(code, *in);
        response.error_code = code;

        Coordination::read(response.session_id, *in);
        Coordination::read(response.xid, *in);
        Coordination::read(response.opnum, *in);

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Exception while receiving forward result " + endpoint);

        /// If it is a network exception occur, we does not know whether server process the request.
        /// But here we just make it not accepted and leave client to determine how to process.

        response.accepted = false;
        disconnect();

        return false;
    }
}

void ForwardingConnection::sendSession(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time)
{
    if (!connected)
        connect();

    if (!connected)
        throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "Connect to server {} failed when send session info", endpoint);

    LOG_TRACE(log, "Send session info to server {}", endpoint);

    try
    {
        Coordination::write(PkgType::Session, *out);
        Coordination::write(static_cast<int32_t>(session_to_expiration_time.size() * 16), *out);

        for (const auto & session_expiration_time : session_to_expiration_time)
        {
            LOG_TRACE(log, "Send session {}, expiration time {}", session_expiration_time.first, session_expiration_time.second);
            Coordination::write(session_expiration_time.first, *out);
            Coordination::write(session_expiration_time.second, *out);
        }

        out->next();
    }
    catch (...)
    {
        disconnect();
        throw Exception(ErrorCodes::NETWORK_ERROR, "Exception while send session to ", endpoint);
    }
}

void ForwardingConnection::sendHandshake()
{
    LOG_TRACE(log, "send hand shake to {}, my_server_id {}, thread_id {}", endpoint, my_server_id, thread_id);
    Coordination::write(PkgType::Handshake, *out);
    Coordination::write(my_server_id, *out);
    Coordination::write(thread_id, *out);
    out->next();
}


[[maybe_unused]] void ForwardingConnection::receiveHandshake()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == PkgType::Handshake);

    bool accepted;
    Coordination::read(accepted, *in);

    int32_t code;
    Coordination::read(code, *in);

    int64_t session_id;
    Coordination::read(session_id, *in);

    int64_t xid;
    Coordination::read(xid, *in);

    int32_t opnum;
    Coordination::read(opnum, *in);
}

}
