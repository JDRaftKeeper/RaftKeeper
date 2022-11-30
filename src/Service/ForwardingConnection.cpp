
#include <Service/ForwardingConnection.h>
#include <IO/WriteHelpers.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int NETWORK_ERROR;
    extern const int RAFT_ERROR;
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

            sendHandshake();
            LOG_TRACE(log, "sent handshake {}", endpoint);

//            receiveHandshake();
//            LOG_TRACE(log, "received handshake {}", endpoint);

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

bool ForwardingConnection::recive(ForwardResponse & response)
{
    if (!connected)
        return false;

    /// There are two situations,
    /// 1. Feedback not accepted.
    /// 2. Receiving network packets failed, which cannot determine whether the opposite end is accepted.
    try
    {

        auto func = [this](char* buf, int32_t nSize)
        {
            String str;
            int32_t i, j, nResult;
            for ( i = 0; i < nSize; ++i )
            {
                for ( j = 7; j >= 0; j-- )
                {
                    nResult = (buf)[i] & int32_t(pow( 2, j ));
                    nResult = ( 0 != nResult );
                    str += std::to_string(nResult);
                }
            }
            LOG_TRACE(log, "in_buf {}", str);
        };

        func(in->position(), 1);
        int8_t type;
        Coordination::read(type, *in);
        response.protocol = ForwardProtocol(type);
        LOG_TRACE(log, "Recived forward response type {}", type);

        func(in->position(), 1);
        Coordination::read(response.accepted, *in);
        LOG_TRACE(log, "Recived forward response accepted {}", response.accepted);

        func(in->position(), 4);
        int32_t code;
        Coordination::read(code, *in);
        response.error_code = code;
        LOG_TRACE(log, "Recived forward response error_code {}", code);

        func(in->position(), 8);
        Coordination::read(response.session_id, *in);
        LOG_TRACE(log, "Recived forward response session_id {}", response.session_id);

        func(in->position(), 8);
        Coordination::read(response.xid, *in);
        LOG_TRACE(log, "Recived forward response xid {}", response.xid);

        func(in->position(), 4);
        Coordination::read(response.opnum, *in);
        LOG_TRACE(log, "Recived forward response opnum {}", response.opnum);

        LOG_TRACE(log, "Recived forward response {}", response.toString());

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

void ForwardingConnection::sendPing(const std::unordered_map<int64_t, int64_t> & session_to_expiration_time)
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
        Coordination::write(int32_t(session_to_expiration_time.size()), *out);
        for (auto & session_expiration_time : session_to_expiration_time)
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
    Coordination::write(ForwardProtocol::Hello, *out);
    Coordination::write(my_server_id, *out);
    Coordination::write(thread_id, *out);
    out->next();
}


void ForwardingConnection::receiveHandshake()
{
    int8_t type;
    Coordination::read(type, *in);
    assert(type == ForwardProtocol::Hello);

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
