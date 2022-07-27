
#include <Service/ForwardingClient.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
}

void ForwardingClient::connect(Poco::Net::SocketAddress & address, Poco::Timespan connection_timeout)
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

            LOG_TRACE(log, "connect succ {}", endpoint);
            break;
        }
        catch (...)
        {
            LOG_ERROR(log, "Got exception connection {}, {}: {}", endpoint, address.toString(), getCurrentExceptionMessage(true));
        }
    }
}

void ForwardingClient::disconnect()
{
    if (connected)
    {
        socket.close();
        connected = false;
    }
}

void ForwardingClient::send(SvsKeeperStorage::RequestForSession request_for_session)
{
    if (!connected)
    {
        Poco::Net::SocketAddress add{endpoint};
        connect(add, operation_timeout.totalMilliseconds() / 3);
    }

    if (!connected)
    {
        throw Exception("ForwardingClient connect failed", ErrorCodes::ALL_CONNECTION_TRIES_FAILED);
    }

    LOG_TRACE(log, "forwarding endpoint {}, session {}, xid {}", endpoint, request_for_session.session_id, request_for_session.request->xid);

    try
    {
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
        throw Exception("ForwardingClient send failed", ErrorCodes::NETWORK_ERROR);
    }
}


}
