
#include <Service/ForwardingConnection.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
    extern const int RAFT_ERROR;
}

void ForwardingConnection::connect(Poco::Timespan connection_timeout)
{
    Poco::Net::SocketAddress address{endpoint};
    static constexpr size_t num_tries = 3;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        try
        {
            LOG_TRACE(log, "Try connect {}", endpoint);
            /// Reset the state of previous attempt.

            socket = Poco::Net::StreamSocket();

            socket.connect(address, connection_timeout);

            socket.setReceiveTimeout(operation_timeout);
            socket.setSendTimeout(operation_timeout);
            socket.setNoDelay(true);

            in.emplace(socket);
            out.emplace(socket);

            sendHandshake();
            LOG_TRACE(log, "Sent handshake {}", endpoint);

            // TODO receiveHandshake
//            receiveHandshake();
//            LOG_TRACE(log, "received handshake {}", endpoint);

            connected = true;
            LOG_TRACE(log, "Connect succ {}", endpoint);
            break;
        }
        catch (...)
        {
            LOG_ERROR(log, "Got exception connection {}, {}: {}", endpoint, address.toString(), getCurrentExceptionMessage(true));
        }
    }
}

void ForwardingConnection::disconnect()
{
    if (connected)
    {
        socket.close();
        connected = false;
        errno = 0;
    }
}

void ForwardingConnection::send(KeeperStore::RequestForSession request_for_session)
{
    if (!connected)
    {
        connect(operation_timeout.totalMicroseconds() / 3);
    }

    if (!connected)
    {
        throw Exception("ForwardingConnection connect failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }

    LOG_TRACE(log, "Forwarding session {}, xid {} to endpoint {}", toHexString(request_for_session.session_id), request_for_session.request->xid, endpoint);

    try
    {
        Coordination::write(PkgType::Data, *out);
        WriteBufferFromOwnString buf;
        Coordination::write(request_for_session.session_id, buf);
        Coordination::write(request_for_session.request->xid, buf);
        Coordination::write(request_for_session.request->getOpNum(), buf);
        request_for_session.request->writeImpl(buf);
        Coordination::write(buf.str(), *out);
        out->next();
    }
    catch(...)
    {
        LOG_ERROR(log, "Got exception while forwarding to {}, {}", endpoint, getCurrentExceptionMessage(true));
        disconnect();
        throw Exception("ForwardingConnection send failed", ErrorCodes::NETWORK_ERROR);
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
    /// 1. Feedback not accepted.
    /// 2. Receiving network packets failed, which cannot determine whether the opposite end is accepted.
    try
    {
        int8_t type;
        Coordination::read(type, *in);
        response.protocol = static_cast<PkgType>(type);

        Coordination::read(response.accepted, *in);

        int32_t code;
        Coordination::read(code, *in);
        response.error_code = code;

        Coordination::read(response.session_id, *in);
        Coordination::read(response.xid, *in);
        Coordination::read(response.opnum, *in);

        return true;
    }
    catch(...)
    {
        LOG_ERROR(log, "Got exception while receiving forward result {}, {}", endpoint, getCurrentExceptionMessage(true));
        /// TODO If it is a network exception, we receive the request by default. To be discussed.
        disconnect();
        return false;
    }
}

void ForwardingConnection::sendSession(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time)
{
    if (!connected)
    {
        connect(operation_timeout.totalMicroseconds() / 3);
    }

    if (!connected)
    {
        throw Exception("ForwardingConnection connect failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }

    LOG_TRACE(log, "Send ping to endpoint {}", endpoint);

    try
    {
        Coordination::write(PkgType::Session, *out);
        Coordination::write(static_cast<int32_t>(session_to_expiration_time.size()), *out);
        for (const auto & session_expiration_time : session_to_expiration_time)
        {
            LOG_TRACE(log, "Send session {}, expiration time {}", session_expiration_time.first, session_expiration_time.second);
            Coordination::write(session_expiration_time.first, *out);
            Coordination::write(session_expiration_time.second, *out);
        }

        out->next();
    }
    catch(...)
    {
        LOG_ERROR(log, "Got exception while send ping to {}, {}", endpoint, getCurrentExceptionMessage(true));
        disconnect();
        throw Exception("ForwardingConnection send failed", ErrorCodes::NETWORK_ERROR);
    }
}

void ForwardingConnection::sendHandshake()
{
    Coordination::write(PkgType::Handshake, *out);
    Coordination::write(my_server_id, *out);
    // TODO log
    Coordination::write(thread_id, *out);
    out->next();
}


void ForwardingConnection::receiveHandshake()
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
