
#include <Service/ForwardingConnection.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
}

void ForwardingConnection::connect(Poco::Net::SocketAddress & address, Poco::Timespan connection_timeout)
{
    static constexpr size_t num_tries = 3;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        try
        {
            LOG_TRACE(log, "try connect {}", endpoint);
            /// Reset the state of previous attempt.

            socket = Poco::Net::StreamSocket();

            socket.connect(address, connection_timeout);

            socket.setReceiveTimeout(operation_timeout);
            socket.setSendTimeout(operation_timeout);
            socket.setNoDelay(true);

            in.emplace(socket);
            out.emplace(socket);

            connected = true;

            sendHandshake();
            LOG_TRACE(log, "sent handshake {}", endpoint);

            receiveHandshake();
            LOG_TRACE(log, "received handshake {}", endpoint);

            LOG_TRACE(log, "connect succ {}", endpoint);
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
    }
}

void ForwardingConnection::send(SvsKeeperStorage::RequestForSession request_for_session)
{
    if (!connected)
    {
        Poco::Net::SocketAddress add{endpoint};
        connect(add, operation_timeout.totalMilliseconds() / 3);
    }

    if (!connected)
    {
        throw Exception("ForwardingConnection connect failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }

    LOG_TRACE(log, "forwarding endpoint {}, session {}, xid {}", endpoint, request_for_session.session_id, request_for_session.request->xid);

    try
    {
        Coordination::write(ForwardProtocol::Data, *out);
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
        LOG_ERROR(log, "Got exception send {}, {}", endpoint, getCurrentExceptionMessage(true));
        disconnect();
        throw Exception("ForwardingConnection send failed", ErrorCodes::NETWORK_ERROR);
    }
}

void ForwardingConnection::sendPing()
{
    if (!connected)
    {
        Poco::Net::SocketAddress add{endpoint};
        connect(add, operation_timeout.totalMilliseconds() / 3);
    }

    if (!connected)
    {
        throw Exception("ForwardingConnection connect failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }

    LOG_TRACE(log, "Send ping to endpoint {}", endpoint);

    try
    {
        Coordination::write(ForwardProtocol::Ping, *out);
        out->next();
    }
    catch(...)
    {
        LOG_ERROR(log, "Got exception send {}, {}", endpoint, getCurrentExceptionMessage(true));
        disconnect();
        throw Exception("ForwardingConnection send failed", ErrorCodes::NETWORK_ERROR);
    }
}

void ForwardingConnection::receivePing()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == ForwardProtocol::Ping);
}

void ForwardingConnection::sendHandshake()
{
    Coordination::write(ForwardProtocol::Hello, *out);
    out->next();
}


void ForwardingConnection::receiveHandshake()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == ForwardProtocol::Hello);
}



}
