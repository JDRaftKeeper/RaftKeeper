
#include <Service/ForwardingClient.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeperCommon.h>

namespace DB
{

void ForwardingClient::connect(Poco::Net::SocketAddress & address, Poco::Timespan connection_timeout)
{
    static constexpr size_t num_tries = 3;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        try
        {
            /// Reset the state of previous attempt.

            socket = Poco::Net::StreamSocket();

            socket.connect(address, connection_timeout);

            socket.setReceiveTimeout(operation_timeout);
            socket.setSendTimeout(operation_timeout);
            socket.setNoDelay(true);

            in.emplace(socket);
            out.emplace(socket);

            connected = true;

            LOG_TRACE(log, "connected succ {}", endpoint);
            break;
        }
        catch (...)
        {
            LOG_ERROR(log, "Got exception connection {}, {}: {}", endpoint, address.toString(), getCurrentExceptionMessage(true));
        }
    }
}

void ForwardingClient::send(SvsKeeperStorage::RequestForSession request_for_session)
{
    if (!connected)
    {
        Poco::Net::SocketAddress add{endpoint};
        connect(add, operation_timeout);
    }

    LOG_TRACE(log, "forwarding endpoint {}, session {}, xid {}", endpoint, request_for_session.session_id, request_for_session.request->xid);

//    request_for_session.request->write(*out);
    /// Excessive copy to calculate length.
    WriteBufferFromOwnString buf;
    Coordination::write(request_for_session.session_id, buf);
    Coordination::write(xid, buf);
    Coordination::write(getOpNum(), buf);
    writeImpl(buf);
    Coordination::write(buf.str(), out);
    out.next();
}


}
