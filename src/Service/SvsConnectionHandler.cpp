#define USE_NIO_FOR_KEEPER
#ifdef USE_NIO_FOR_KEEPER
#include "SvsConnectionHandler.h"

#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Poco/Net/NetException.h>
#include <Service/FourLetterCommand.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int READONLY;
    extern const int RAFT_ERROR;
}

using Poco::NObserver;

std::mutex SvsConnectionHandler::conns_mutex;
std::unordered_set<SvsConnectionHandler *> SvsConnectionHandler::connections;


void SvsConnectionHandler::registerConnection(SvsConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.insert(conn);
}

void SvsConnectionHandler::unregisterConnection(SvsConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.erase(conn);
}

void SvsConnectionHandler::dumpConnections(WriteBufferFromOwnString & buf, bool brief)
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->dumpStats(buf, brief);
    }
}

void SvsConnectionHandler::resetConnsStats()
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->resetStats();
    }
}

SvsConnectionHandler::SvsConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor)
    : log(&Logger::get("SvsConnectionHandler")), socket_(socket), reactor_(reactor)
    , global_context(global_context_)
    , service_keeper_storage_dispatcher(global_context.getSvsKeeperStorageDispatcher())
    , operation_timeout(
          0, global_context.getConfigRef().getUInt("service.coordination_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(
          0, global_context.getConfigRef().getUInt("service.coordination_settings.session_timeout_ms", Coordination::DEFAULT_SESSION_TIMEOUT_MS) * 1000)
    , responses(std::make_unique<ThreadSafeResponseQueue>())
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    LOG_INFO(log, "Connection from " + socket_.peerAddress().toString());
    registerConnection(this);

    reactor_.addEventHandler(socket_, NObserver<SvsConnectionHandler, ReadableNotification>(*this, &SvsConnectionHandler::onSocketReadable));
    reactor_.addEventHandler(socket_, NObserver<SvsConnectionHandler, ShutdownNotification>(*this, &SvsConnectionHandler::onSocketShutdown));
}
SvsConnectionHandler::~SvsConnectionHandler()
{
    unregisterConnection(this);
    try
    {
        LOG_INFO(log, "Disconnecting {}", socket_.peerAddress().toString());
    }
    catch (...)
    {
    }
    reactor_.removeEventHandler(socket_, NObserver<SvsConnectionHandler, ReadableNotification>(*this, &SvsConnectionHandler::onSocketReadable));
    reactor_.removeEventHandler(socket_, NObserver<SvsConnectionHandler, WritableNotification>(*this, &SvsConnectionHandler::onSocketWritable));
    reactor_.removeEventHandler(socket_, NObserver<SvsConnectionHandler, ShutdownNotification>(*this, &SvsConnectionHandler::onSocketShutdown));
}

void SvsConnectionHandler::onSocketReadable(const AutoPtr<ReadableNotification> & /*pNf*/)
{
    LOG_TRACE(log, "socket readable {}", socket_.peerAddress().toString());
    try
    {
        if (!socket_.available())
        {
            LOG_TRACE(log, "Client {} close connection!", socket_.peerAddress().toString());
            destroyMe();
            return;
        }

        while(socket_.available())
        {
            /// request body length
            int32_t body_len{};

            /// 1. Request header
            if (!next_req_header_read_done)
            {
                if (!req_header_buf.isFull())
                {
                    socket_.receiveBytes(req_header_buf);
                    if (!req_header_buf.isFull())
                        continue;
                }

                /// header read completed
                int32_t header{};
                ReadBufferFromMemory read_buf(req_header_buf.begin(), req_header_buf.used());
                Coordination::read(header, read_buf);

                /// All four letter word command code is larger than 2^24 or lower than 0.
                /// Hand shake package length must be lower than 2^24 and larger than 0.
                /// So collision never happens.
                if (!isHandShake(header) && !handshake_done)
                {
                    int32_t four_letter_cmd = header;
                    tryExecuteFourLetterWordCmd(four_letter_cmd);
                    /// Handler need to delete self
                    /// As to four letter command just close connection.
                    delete this;
                    return;
                }

                body_len = header;
                LOG_TRACE(log, "read request length : {}", body_len);

                /// clear len_buf
                req_header_buf.drain(req_header_buf.used());
                next_req_header_read_done = true;
            }

            /// 2. Read body

            if (previous_req_body_read_done)
                /// create a buffer
                req_body_buf = std::make_shared<FIFOBuffer>(body_len);

            socket_.receiveBytes(*req_body_buf);

            if (!req_body_buf->isFull())
                continue;

            /// Request reading done, set flags
            next_req_header_read_done = false;
            previous_req_body_read_done = true;

            packageReceived();

            LOG_TRACE(log, "Read request done, body length : {}", body_len);
            poco_assert_msg(int32_t (req_body_buf->used()) == body_len, "Request body length is not consistent.");

            /// 3. handshake
            if (!handshake_done)
            {
                HandShakeResult handshake_result;
                ConnectRequest connect_req;
                try
                {
                    int32_t handshake_req_len = body_len;
                    connect_req = receiveHandshake(handshake_req_len);

                    handshake_result = handleHandshake(connect_req);
                    sendHandshake(handshake_result);
                }
                catch (...)
                {
                    /// Typical for an incorrect username, password
                    /// and bad protocol version, bad las zxid, rw connection to a read only server
                    /// Close the connection directly.
                    tryLogCurrentException(log, "Cannot receive handshake");
                    destroyMe();
                    return;
                }

                if (!handshake_result.connect_success)
                {
                    destroyMe();
                    return;
                }

                /// register session response callback
                auto response_callback = [this](const Coordination::ZooKeeperResponsePtr & response) {
                    sendResponse(response);
                };
                service_keeper_storage_dispatcher->registerSession(session_id, response_callback);

                /// start session timeout timer
                session_stopwatch.start();
                handshake_done = true;
            }
            /// 4. handle request
            else
            {
                session_stopwatch.start();

                try
                {
                    auto [received_op, received_xid] = receiveRequest(body_len);

                    if (received_op == Coordination::OpNum::Close)
                    {
                        LOG_DEBUG(log, "Received close event with xid {} for session id #{}", received_xid, session_id);
                        close_xid = received_xid;
                    }
                    else if (received_op == Coordination::OpNum::Heartbeat)
                    {
                        LOG_TRACE(log, "Received heartbeat for session #{}", session_id);
                    }
                    else
                        operations[received_xid] = Poco::Timestamp();

                    /// Each request restarts session stopwatch
                    session_stopwatch.restart();
                }
                catch (const Exception & e)
                {
                    tryLogCurrentException(log, fmt::format("Error processing session {} request.", session_id));

                    if (e.code() == ErrorCodes::TIMEOUT_EXCEEDED)
                    {
                        destroyMe();
                        return;
                    }
                }
            }
        }
    }
    catch (Poco::Net::NetException &)
    {
        tryLogCurrentException(log, "Network error when receiving request, will close connection.");
        destroyMe();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fatal error when handling request, will close connection.");
        destroyMe();
    }
}

void SvsConnectionHandler::onSocketWritable(const AutoPtr<WritableNotification> &)
{
    LOG_TRACE(log, "socket writable {}", socket_.peerAddress().toString());
    try
    {
        if (responses->size() == 0 && send_buf.used() == 0)
            return;

        /// TODO use zero copy buffer
        size_t size_to_sent = 0;

        /// 1. accumulate data into tmp_buf
        responses->forEach([&size_to_sent, this] (const auto & resp) -> bool {
            if (size_to_sent + resp->used() < SENT_BUFFER_SIZE)
            {
                /// add whole resp to send_buf
                send_buf.write(resp->begin(), resp->used());
                size_to_sent += resp->used();
            }
            else if (size_to_sent + resp->used() == SENT_BUFFER_SIZE)
            {
                /// add whole resp to send_buf
                send_buf.write(resp->begin(), resp->used());
                size_to_sent += resp->used();
            }
            else
            {
                /// add part of resp to send_buf
                send_buf.write(resp->begin(), SENT_BUFFER_SIZE - size_to_sent);
            }
            return size_to_sent < SENT_BUFFER_SIZE;
        });

        /// 2. send data
        size_t sent = socket_.sendBytes(send_buf);

        /// 3. remove sent responses

        ptr<FIFOBuffer> resp;
        while(responses->peek(resp) && sent > 0)
        {
            if (sent >= resp->used())
            {
                sent -= resp->used();
                responses->remove();
                /// package sent
                packageSent();
                LOG_TRACE(log, "sent response to {}", socket_.peerAddress().toString());
            }
            else
            {
                resp->drain(sent);
                /// move data to begin
                resp->begin();
                sent = 0;
            }
        }

        /// If all sent unregister writable event.
        if (responses->size() == 0 && send_buf.used() == 0)
        {
            LOG_TRACE(log, "Remove socket writable event handler - socket {}", socket_.peerAddress().toString());
            reactor_.removeEventHandler(
                socket_, NObserver<SvsConnectionHandler, WritableNotification>(*this, &SvsConnectionHandler::onSocketWritable));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fatal error when sending data to client, will close connection.");
        destroyMe();
    }
}

void SvsConnectionHandler::onSocketShutdown(const AutoPtr<ShutdownNotification> & pNf)
{
    LOG_INFO(log, "Socket {} shutdown!", pNf->socket().peerAddress().toString());
    destroyMe();
}

KeeperConnectionStats SvsConnectionHandler::getConnectionStats() const
{
    std::lock_guard lock(conn_stats_mutex);
    return conn_stats;
}

void SvsConnectionHandler::dumpStats(WriteBufferFromOwnString & buf, bool brief)
{
    KeeperConnectionStats stats = getConnectionStats();

    writeText(" ", buf);
    writeText(socket_.peerAddress().toString(), buf);
    writeText("(recved=", buf);
    writeIntText(stats.getPacketsReceived(), buf);
    writeText(",sent=", buf);
    writeIntText(stats.getPacketsSent(), buf);
    if (!brief)
    {
        if (session_id != 0)
        {
            writeText(",sid=0x", buf);
            writeText(getHexUIntLowercase(session_id), buf);

            writeText(",lop=", buf);
            LastOpPtr op = last_op.get();
            writeText(op->name, buf);
            writeText(",est=", buf);
            writeIntText(established.epochMicroseconds() / 1000, buf);
            writeText(",to=", buf);
            writeIntText(session_timeout.totalMilliseconds(), buf);
            int64_t last_cxid = op->last_cxid;
            if (last_cxid >= 0)
            {
                writeText(",lcxid=0x", buf);
                writeText(getHexUIntLowercase(last_cxid), buf);
            }
            writeText(",lzxid=0x", buf);
            writeText(getHexUIntLowercase(op->last_zxid), buf);
            writeText(",lresp=", buf);
            writeIntText(op->last_response_time, buf);

            writeText(",llat=", buf);
            writeIntText(stats.getLastLatency(), buf);
            writeText(",minlat=", buf);
            writeIntText(stats.getMinLatency(), buf);
            writeText(",avglat=", buf);
            writeIntText(stats.getAvgLatency(), buf);
            writeText(",maxlat=", buf);
            writeIntText(stats.getMaxLatency(), buf);
        }
    }
    writeText(")", buf);
    writeText("\n", buf);
}

void SvsConnectionHandler::resetStats()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.reset();
    }
    last_op.set(std::make_unique<LastOp>(EMPTY_LAST_OP));
}

ConnectRequest SvsConnectionHandler::receiveHandshake(int32_t handshake_req_len)
{

    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout_ms;
    int64_t previous_session_id = 0;
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    if (!isHandShake(handshake_req_len))
        throw Exception("Unexpected handshake length received: " + toString(handshake_req_len), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    ReadBufferFromMemory in(req_body_buf->begin(), req_body_buf->used());
    Coordination::read(protocol_version, in);

    if (protocol_version != Coordination::ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + toString(protocol_version), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);

    Coordination::read(last_zxid_seen, in);
    Coordination::read(timeout_ms, in);

    int64_t last_zxid = service_keeper_storage_dispatcher->getStateMachine().getLastProcessedZxid();
    if (last_zxid_seen > last_zxid)
    {
        String msg = "Refusing session request  as it has seen zxid 0x" + getHexUIntLowercase(last_zxid_seen) + " our last zxid is 0x"
            + getHexUIntLowercase(last_zxid) + " client must try another server";

        throw Exception(msg, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    Coordination::read(previous_session_id, in);
    Coordination::read(passwd, in);

    bool readonly{false};

    if (handshake_req_len == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
        Coordination::read(readonly, in);

    return {protocol_version, last_zxid_seen, timeout_ms, previous_session_id, passwd, readonly};
}

SvsConnectionHandler::HandShakeResult SvsConnectionHandler::handleHandshake(ConnectRequest & connect_req)
{
    if (connect_req.timeout_ms != 0)
        session_timeout = std::min(Poco::Timespan(connect_req.timeout_ms * 1000), session_timeout);

    LOG_TRACE(log, "Negotiated session_timeout : {}", session_timeout.totalMilliseconds());

    bool session_expired = false;
    bool connect_success = service_keeper_storage_dispatcher->hasLeader();

    if (connect_success)
    {
        try
        {
            if (connect_req.previous_session_id != 0)
            {
                LOG_INFO(log, "Requesting reconnecting with session {}", getHexUIntLowercase(connect_req.previous_session_id));
                session_id = connect_req.previous_session_id;
                /// existed session
                if (!service_keeper_storage_dispatcher->getStateMachine().containsSession(connect_req.previous_session_id))
                {
                    /// session expired, set timeout <=0
                    LOG_WARNING(
                        log,
                        "Client try to reconnects but session 0x{} is already expired",
                        getHexUIntLowercase(connect_req.previous_session_id));
                    session_expired = true;
                    connect_success = false;
                }
                else
                {
                    /// update session timeout
                    if (!service_keeper_storage_dispatcher->updateSessionTimeout(session_id, session_timeout.totalMilliseconds()))
                    {
                        /// update failed
                        /// session was expired when updating
                        /// session expired, set timeout <=0
                        LOG_WARNING(log, "Session 0x{} was expired when updating", getHexUIntLowercase(connect_req.previous_session_id));
                        session_expired = true;
                        connect_success = false;
                    }
                    else
                        LOG_INFO(log, "Client reconnected with session 0x{}", getHexUIntLowercase(connect_req.previous_session_id));
                }
            }
            else
            {
                /// new session
                LOG_INFO(log, "Requesting session ID for new client");
                session_id = service_keeper_storage_dispatcher->getSessionID(session_timeout.totalMilliseconds());
                LOG_INFO(log, "Received session ID {}", session_id);
            }
        }
        catch (const Exception & e)
        {
            LOG_WARNING(log, "Cannot receive session id {}", e.displayText());
            connect_success = false;
        }
    }

    return {connect_success, session_expired};
}

void SvsConnectionHandler::sendHandshake(HandShakeResult & result)
{
    WriteBufferFromFiFoBuffer out;
    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, out);
    if (result.connect_success)
        Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, out);
    else
        Coordination::write(42, out);

    /// Session timout -1 represent session expired in Zookeeper
    int32_t negotiated_session_timeout = result.session_expired ? -1 : session_timeout.totalMilliseconds();
    Coordination::write(negotiated_session_timeout, out);

    Coordination::write(session_id, out);
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    Coordination::write(passwd, out);

    /// Set socket to blocking mode to simplify sending.
    socket_.setBlocking(true);
    socket_.sendBytes(*out.getBuffer());
    socket_.setBlocking(false);
}

bool SvsConnectionHandler::isHandShake(Int32 & handshake_length)
{
    return handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH
        || handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY;
}

bool SvsConnectionHandler::tryExecuteFourLetterWordCmd(int32_t command)
{
    if (!FourLetterCommandFactory::instance().isKnown(command))
    {
        LOG_WARNING(log, "invalid four letter command {}", IFourLetterCommand::toName(command));
        return false;
    }
    else if (!FourLetterCommandFactory::instance().isEnabled(command))
    {
        LOG_WARNING(log, "Not enabled four letter command {}", IFourLetterCommand::toName(command));
        return false;
    }
    else
    {
        auto command_ptr = FourLetterCommandFactory::instance().get(command);
        LOG_DEBUG(log, "Receive four letter command {}", command_ptr->name());

        try
        {
            String res = command_ptr->run();
            WriteBufferFromFiFoBuffer buf(res.size());
            buf.write(res.data(), res.size());

            /// Set socket to blocking mode to simplify sending.
            socket_.setBlocking(true);
            socket_.sendBytes(*buf.getBuffer());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when executing four letter command " + command_ptr->name());
        }
        return true;
    }
}

std::pair<Coordination::OpNum, Coordination::XID> SvsConnectionHandler::receiveRequest(int32_t length)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
    int32_t xid;
    Coordination::read(xid, body);

    Coordination::OpNum opnum;
    Coordination::read(opnum, body);

    LOG_TRACE(log, "Receive request: session {}, xid {}, length {}, opnum {}", session_id, xid, length, Coordination::toString(opnum));

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(body);

    if (!service_keeper_storage_dispatcher->putRequest(request, session_id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", session_id);
    return std::make_pair(opnum, xid);
}

void SvsConnectionHandler::sendResponse(const Coordination::ZooKeeperResponsePtr& response)
{
    WriteBufferFromFiFoBuffer buf;
    response->write(buf);

    /// TODO should invoked after response sent to client.
    updateStats(response);

    /// TODO handle timeout
    responses->push(buf.getBuffer());

    LOG_TRACE(log, "Add socket writable event handler - socket {}", socket_.peerAddress().toString());
    /// Trigger socket writable event
    reactor_.addEventHandler(
        socket_, NObserver<SvsConnectionHandler, WritableNotification>(*this, &SvsConnectionHandler::onSocketWritable));
    /// We must wake up reactor to interrupt it's sleeping.
    reactor_.wakeUp();
}

void SvsConnectionHandler::packageSent()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.incrementPacketsSent();
    }
    service_keeper_storage_dispatcher->incrementPacketsSent();
}

void SvsConnectionHandler::packageReceived()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.incrementPacketsReceived();
    }
    service_keeper_storage_dispatcher->incrementPacketsReceived();
}

void SvsConnectionHandler::updateStats(const Coordination::ZooKeeperResponsePtr & response)
{
    /// update statistics ignoring watch response and heartbeat.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() != Coordination::OpNum::Heartbeat)
    {
        Int64 elapsed = (Poco::Timestamp() - operations[response->xid]) / 1000;
        {
            std::lock_guard lock(conn_stats_mutex);
            conn_stats.updateLatency(elapsed);
        }
        operations.erase(response->xid);
        service_keeper_storage_dispatcher->updateKeeperStatLatency(elapsed);

        last_op.set(std::make_unique<LastOp>(LastOp{
            .name = Coordination::toString(response->getOpNum()),
            .last_cxid = response->xid,
            .last_zxid = response->zxid,
            .last_response_time = Poco::Timestamp().epochMicroseconds() / 1000,
        }));
    }
}

void SvsConnectionHandler::destroyMe()
{
    service_keeper_storage_dispatcher->finishSession(session_id);
    delete this;
}

}

#endif
