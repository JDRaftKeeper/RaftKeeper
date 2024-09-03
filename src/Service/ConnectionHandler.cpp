#include "ConnectionHandler.h"

#include <Poco/Net/NetException.h>

#include <Common/Stopwatch.h>

#include <Service/FourLetterCommand.h>
#include <Service/formatHex.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

std::mutex ConnectionHandler::conns_mutex;
std::unordered_set<ConnectionHandler *> ConnectionHandler::connections;


void ConnectionHandler::registerConnection(ConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.insert(conn);
}

void ConnectionHandler::unregisterConnection(ConnectionHandler * conn)
{
    std::lock_guard lock(conns_mutex);
    connections.erase(conn);
}

void ConnectionHandler::dumpConnections(WriteBufferFromOwnString & buf, bool brief)
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->dumpStats(buf, brief);
    }
}

void ConnectionHandler::resetConnsStats()
{
    std::lock_guard lock(conns_mutex);
    for (auto * conn : connections)
    {
        conn->resetStats();
    }
}

ConnectionHandler::ConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_)
    : log(&Logger::get("ConnectionHandler"))
    , sock(socket_)
    , peer(socket_.peerAddress().toString())
    , reactor(reactor_)
    , global_context(global_context_)
    , keeper_dispatcher(global_context.getDispatcher())
    , operation_timeout(
          0,
          Context::getConfigRef().getUInt("keeper.raft_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(0, Coordination::DEFAULT_SESSION_TIMEOUT_MS * 1000)
    , min_session_timeout(
          0,
          Context::getConfigRef().getUInt("keeper.raft_settings.min_session_timeout_ms", Coordination::DEFAULT_MIN_SESSION_TIMEOUT_MS)
              * 1000)
    , max_session_timeout(
          0,
          Context::getConfigRef().getUInt("keeper.raft_settings.max_session_timeout_ms", Coordination::DEFAULT_MAX_SESSION_TIMEOUT_MS)
              * 1000)
    , responses(std::make_unique<ThreadSafeResponseQueue>())
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    LOG_INFO(log, "New connection from {}", peer);
    registerConnection(this);

    auto read_handler = Observer<ConnectionHandler, ReadableNotification>(*this, &ConnectionHandler::onSocketReadable);
    auto error_handler = Observer<ConnectionHandler, ErrorNotification>(*this, &ConnectionHandler::onSocketError);
    auto shutdown_handler = Observer<ConnectionHandler, ShutdownNotification>(*this, &ConnectionHandler::onReactorShutdown);

    std::vector<AbstractObserver *> handlers;
    handlers.push_back(&read_handler);
    handlers.push_back(&error_handler);
    handlers.push_back(&shutdown_handler);
    reactor.addEventHandlers(sock, handlers);
}

ConnectionHandler::~ConnectionHandler()
{
    try
    {
        /// session_id==0 indicates that this is not a normal connection, it may be a 4lw connection.
        if (session_id)
            LOG_INFO(log, "Disconnecting peer {}, session #{}", peer, toHexString(session_id.load()));

        unregisterConnection(this);

        reactor.removeEventHandler(sock, Observer<ConnectionHandler, ReadableNotification>(*this, &ConnectionHandler::onSocketReadable));
        reactor.removeEventHandler(sock, Observer<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
        reactor.removeEventHandler(sock, Observer<ConnectionHandler, ErrorNotification>(*this, &ConnectionHandler::onSocketError));
        reactor.removeEventHandler(sock, Observer<ConnectionHandler, ShutdownNotification>(*this, &ConnectionHandler::onReactorShutdown));
    }
    catch (...)
    {
    }
}

void ConnectionHandler::onSocketReadable(const Notification &)
{
    try
    {
        LOG_TRACE(log, "Peer {}#{} is readable", peer, toHexString(session_id.load()));

        if (!sock.available())
        {
            destroyMe();
            return;
        }

        while (sock.available())
        {
            /// 1. Request header
            if (!next_req_header_read_done)
            {
                if (!req_header_buf.isFull())
                {
                    sock.receiveBytes(req_header_buf);
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

                    /// Handler no need delete self
                    /// As to four letter command just wait client close connection.
                    req_header_buf.drain(req_header_buf.used());
                    return;
                }

                body_len = header;
                LOG_TRACE(log, "Peer {}#{} read request length : {}", peer, toHexString(session_id.load()), body_len);

                /// clear len_buf
                req_header_buf.drain(req_header_buf.used());
                next_req_header_read_done = true;
            }

            /// 2. Read body

            if (previous_req_body_read_done)
            {
                /// create a buffer
                req_body_buf = std::make_shared<FIFOBuffer>(body_len);
                previous_req_body_read_done = false;
            }

            sock.receiveBytes(*req_body_buf);

            if (!req_body_buf->isFull())
                continue;

            /// Request reading done, set flags
            next_req_header_read_done = false;
            previous_req_body_read_done = true;

            packageReceived();

            LOG_TRACE(
                log,
                "Peer {}#{} read request done, body length : {}, req_body_buf used {}",
                peer,
                toHexString(session_id.load()),
                body_len,
                req_body_buf->used());
            poco_assert_msg(int32_t(req_body_buf->used()) == body_len, "Request body length is not consistent.");

            /// 3. handshake
            if (unlikely(!handshake_done)) /// TODO in handshaking
            {
                try
                {
                    int32_t handshake_req_len = body_len;
                    receiveHandshake(handshake_req_len);
                }
                catch (...)
                {
                    /// Typical for an incorrect username, password
                    /// and bad protocol version, bad las zxid, rw connection to a read only server
                    /// Close the connection directly.
                    tryLogCurrentException(log, "Failed to connect me");
                    destroyMe();
                    return;
                }
            }
            /// 4. handle request
            else
            {
                session_stopwatch.start();

                try
                {
                    auto [opnum, xid] = receiveRequest(body_len);
                    if (opnum == Coordination::OpNum::Close)
                        LOG_DEBUG(log, "Received close request #{}#{}#Close", toHexString(session_id.load()), xid);

                    /// Each request restarts session stopwatch
                    session_stopwatch.restart();
                }
                catch (const Exception & e)
                {
                    tryLogCurrentException(log, fmt::format("Error processing session {} request.", toHexString(session_id.load())));

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
        tryLogCurrentException(
            log, fmt::format("Network error when receiving request, will close connection session {}.", toHexString(session_id.load())));
        destroyMe();
    }
    catch (...)
    {
        tryLogCurrentException(
            log, fmt::format("Fatal error when handling request, will close connection session {}.", toHexString(session_id.load())));
        destroyMe();
    }
}


void ConnectionHandler::onSocketWritable(const Notification &)
{
    LOG_TRACE(log, "Peer {}#{} is writable", peer, toHexString(session_id.load()));

    auto remove_event_handler_if_needed = [this]
    {
        /// Double check to avoid dead lock
        if (responses->empty() && send_buf.isEmpty() && !out_buffer)
        {
            std::lock_guard lock(send_response_mutex);
            {
                /// If all sent, unregister writable event.
                if (responses->empty() && send_buf.isEmpty() && !out_buffer)
                {
                    LOG_TRACE(log, "Remove socket writable event handler for peer {}", peer);
                    socket_writable_event_registered = false;
                    reactor.removeEventHandler(
                        sock, Observer<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
                }
            }
        }
    };

    auto copy_buffer_to_send = [this]
    {
        auto used = send_buf.used();
        if (used + out_buffer->available() <= SENT_BUFFER_SIZE)
        {
            send_buf.write(out_buffer->position(), out_buffer->available());
            out_buffer.reset();
        }
        else
        {
            send_buf.write(out_buffer->position(), SENT_BUFFER_SIZE - used);
            out_buffer->seek(SENT_BUFFER_SIZE - used, SEEK_CUR);
        }
    };

    try
    {
        /// If the buffer was not completely sent last time, continue sending.
        if (out_buffer)
            copy_buffer_to_send();

        while (!responses->empty() && send_buf.available())
        {
            Coordination::ZooKeeperResponsePtr response;

            if (!responses->tryPop(response))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "We must have ready response, but queue is empty. It's a bug.");

            if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
            {
                LOG_DEBUG(log, "Received close event for session_id {}, internal_id {}", toHexString(session_id.load()), toHexString(internal_id.load()));
                destroyMe();
                return;
            }

            if (response->getOpNum() == OpNum::NewSession || response->getOpNum() == OpNum::UpdateSession)
            {
                if (!sendHandshake(response))
                {
                    LOG_ERROR(log, "Failed to establish session, close connection.");
                    sock.setBlocking(true);
                    sock.sendBytes(send_buf);
                    sock.sendBytes(out_buffer->position(), out_buffer->available());

                    destroyMe();
                    return;
                }
                copy_buffer_to_send();
            }
            else
            {
                WriteBufferFromOwnString buf;
                response->writeNoCopy(buf);
                out_buffer = std::make_shared<ReadBufferFromOwnString>(std::move(buf.str()));
                copy_buffer_to_send();
            }
            packageSent();
        }

        size_t sent = sock.sendBytes(send_buf);
        Metrics::getMetrics().response_socket_send_size->add(sent);

        remove_event_handler_if_needed();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fatal error when sending data to client, will disconnect peer " + peer);
        destroyMe();
    }
}

void ConnectionHandler::onReactorShutdown(const Notification &)
{
    LOG_INFO(log, "Reactor of peer {} shutdown!", peer);
    destroyMe();
}

void ConnectionHandler::onSocketError(const Notification &)
{
    LOG_WARNING(log, "Socket error for peer {}#{}, errno {} !", peer, toHexString(session_id.load()), errno);
    destroyMe();
}

void ConnectionHandler::dumpStats(WriteBufferFromOwnString & buf, bool brief)
{
    writeText(" ", buf);
    writeText(peer, buf);
    writeText("(recved=", buf);
    writeIntText(conn_stats.getPacketsReceived(), buf);
    writeText(",sent=", buf);
    writeIntText(conn_stats.getPacketsSent(), buf);
    if (!brief)
    {
        if (session_id != 0)
        {
            writeText(",sid=", buf);
            writeText(toHexString(session_id.load()), buf);

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
                writeText(",lcxid=", buf);
                writeText(toHexString(last_cxid), buf);
            }
            writeText(",lzxid=", buf);
            writeText(toHexString(op->last_zxid), buf);
            writeText(",lresp=", buf);
            writeIntText(op->last_response_time, buf);

            writeText(",llat=", buf);
            writeIntText(conn_stats.getLastLatency(), buf);
            writeText(",minlat=", buf);
            writeIntText(conn_stats.getMinLatency(), buf);
            writeText(",avglat=", buf);
            writeIntText(conn_stats.getAvgLatency(), buf);
            writeText(",maxlat=", buf);
            writeIntText(conn_stats.getMaxLatency(), buf);
        }
    }
    writeText(")", buf);
    writeText("\n", buf);
}

void ConnectionHandler::resetStats()
{
    conn_stats.reset();
    last_op.set(std::make_unique<LastOp>(EMPTY_LAST_OP));
}

Coordination::OpNum ConnectionHandler::receiveHandshake(int32_t handshake_req_len)
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

    /// Update session timeout
    if (timeout_ms != 0)
    {
        session_timeout = std::max(min_session_timeout, std::min(Poco::Timespan(0, timeout_ms * 1000), max_session_timeout));
        timeout_ms = session_timeout.totalMilliseconds();
    }

    int64_t last_zxid = keeper_dispatcher->getStateMachine().getLastProcessedZxid();
    if (last_zxid_seen > last_zxid)
    {
        String msg = "Refusing session request  as it has seen zxid " + toHexString(last_zxid_seen) + " our last zxid is "
            + toHexString(last_zxid) + " client must try another server";

        throw Exception(msg, ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
    }

    Coordination::read(previous_session_id, in);
    Coordination::read(passwd, in);

    bool readonly{false};

    if (handshake_req_len == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY)
        Coordination::read(readonly, in);

    auto opnum = previous_session_id == 0 ? OpNum::NewSession : OpNum::UpdateSession;
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);

    /// Generate id for new session request and use session_id for update session request
    auto id = opnum == OpNum::NewSession ? keeper_dispatcher->getAndAddInternalId() : previous_session_id;

    if (opnum == OpNum::NewSession)
    {
        LOG_DEBUG(log, "Received new session request with internal id {}", toHexString(id));
        ZooKeeperNewSessionRequest * new_session_req = dynamic_cast<ZooKeeperNewSessionRequest *>(request.get());
        new_session_req->session_timeout_ms = timeout_ms;
        new_session_req->server_id = keeper_dispatcher->myId();
        new_session_req->internal_id = id;
        new_session_req->xid = Coordination::NEW_SESSION_XID;
    }
    else
    {
        LOG_DEBUG(log, "Received update session request with session {}", toHexString(previous_session_id));
        ZooKeeperUpdateSessionRequest * update_session_req = dynamic_cast<ZooKeeperUpdateSessionRequest *>(request.get());
        update_session_req->session_id = id;
        update_session_req->session_timeout_ms = timeout_ms;
        update_session_req->server_id = keeper_dispatcher->myId();
        update_session_req->xid = Coordination::UPDATE_SESSION_XID;
    }

    auto response_callback = [this](const Coordination::ZooKeeperResponsePtr & response) { sendSessionResponseToClient(response); };
    keeper_dispatcher->registerSessionResponseCallback(id, response_callback);
    internal_id = id;

    if (!keeper_dispatcher->pushSessionRequest(request, id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", toHexString(session_id.load()));

    return opnum;
}

bool ConnectionHandler::sendHandshake(const Coordination::ZooKeeperResponsePtr & response)
{
    bool success;
    uint64_t sid;
    WriteBufferFromOwnString buf;

    if (const auto * new_session_resp = dynamic_cast<const ZooKeeperNewSessionResponse *>(response.get()))
    {
        success = new_session_resp->success;
        sid = new_session_resp->session_id;
    }
    else if (const auto * update_session_resp = dynamic_cast<const ZooKeeperUpdateSessionResponse *>(response.get()))
    {
        success = update_session_resp->success;
        sid = update_session_resp->session_id;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad session response {}", response->toString());
    }

    Coordination::write(Coordination::SERVER_HANDSHAKE_LENGTH, buf);
    if (success)
        Coordination::write(Coordination::ZOOKEEPER_PROTOCOL_VERSION, buf);
    else
        Coordination::write(42, buf);

    /// Session timeout -1 represent session expired in Zookeeper
    int32_t negotiated_session_timeout
        = !success && response->error == Coordination::Error::ZSESSIONEXPIRED ? -1 : session_timeout.totalMilliseconds();
    Coordination::write(negotiated_session_timeout, buf);

    Coordination::write(sid, buf);
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    Coordination::write(passwd, buf);

    out_buffer = std::make_shared<ReadBufferFromOwnString>(std::move(buf.str()));

    return success;
}


bool ConnectionHandler::isHandShake(Int32 & handshake_length)
{
    return handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH
        || handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY;
}

void ConnectionHandler::tryExecuteFourLetterWordCmd(int32_t command)
{
    if (!FourLetterCommandFactory::instance().isKnown(command))
    {
        LOG_WARNING(log, "Invalid four letter command {}", IFourLetterCommand::toName(command));
    }
    else if (!FourLetterCommandFactory::instance().isEnabled(command))
    {
        LOG_WARNING(log, "Not enabled four letter command {}", IFourLetterCommand::toName(command));
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
            sock.setBlocking(true);
            sock.sendBytes(*buf.getBuffer());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when executing four letter command " + command_ptr->name());
        }
    }

    /// Implements a graceful shutdown protocol.
    /// Closes the sending channel and sends a FIN (finish) signal to the client.
    /// After the client receives all pending data and the FIN from the server, it sends a FIN packet back to the server.
    /// Once the server acknowledges this with an ACK (acknowledgment), the connection is fully closed.
    sock.shutdownSend();
}

std::pair<Coordination::OpNum, Coordination::XID> ConnectionHandler::receiveRequest(int32_t)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
    int32_t xid;
    Coordination::read(xid, body);

    Coordination::OpNum opnum;
    Coordination::read(opnum, body);

    LOG_DEBUG(log, "Receive request #{}#{}#{}", toHexString(session_id.load()), xid, Coordination::toString(opnum));

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(body);

    if (!keeper_dispatcher->pushRequest(request, session_id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", toHexString(session_id.load()));
    return std::make_pair(opnum, xid);
}

void ConnectionHandler::sendSessionResponseToClient(const Coordination::ZooKeeperResponsePtr & response)
{
    /// 1. Push to sending queue.
    LOG_DEBUG(log, "Sending session response to client. {}", response->toString());

    uint64_t id;
    uint64_t sid;
    bool success;

    if (const auto * new_session_resp = dynamic_cast<const ZooKeeperNewSessionResponse *>(response.get()))
    {
        id = new_session_resp->internal_id;
        sid = new_session_resp->session_id;
        success = new_session_resp->success;
    }
    else if (const auto * update_session_resp = dynamic_cast<const ZooKeeperUpdateSessionResponse *>(response.get()))
    {
        id = update_session_resp->session_id;
        sid = update_session_resp->session_id;
        success = update_session_resp->success;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Bad session response {}", response->toString());
    }

    /// Register callback
    if (success)
    {
        session_id = sid;
        handshake_done = true;

        keeper_dispatcher->unRegisterSessionResponseCallbackWithoutLock(id);
        auto response_callback = [this](const Coordination::ZooKeeperResponsePtr & response_) { pushUserResponseToSendingQueue(response_); };

        bool is_reconnected = response->getOpNum() == Coordination::OpNum::UpdateSession;
        keeper_dispatcher->registerUserResponseCallBackWithoutLock(sid, response_callback, is_reconnected);
    }

    // Send response to client
    {
        std::lock_guard lock(send_response_mutex);
        responses->push(response);

        /// We should register write events.
        if (!socket_writable_event_registered)
        {
            socket_writable_event_registered = true;
            reactor.addEventHandler(sock, Observer<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
            /// We must wake up getWorkerReactor to interrupt it's sleeping.
            reactor.wakeUp();
        }
    }
}

void ConnectionHandler::pushUserResponseToSendingQueue(const Coordination::ZooKeeperResponsePtr & response)
{
    LOG_DEBUG(log, "Push a response of session {} to IO sending queue. {}", toHexString(session_id.load()), response->toString());
    updateStats(response);

    /// Lock to avoid data condition which will lead response leak
    {
        std::lock_guard lock(send_response_mutex);
        responses->push(response);

        /// We should register write events.
        if (!socket_writable_event_registered)
        {
            socket_writable_event_registered = true;
            reactor.addEventHandler(sock, Observer<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
            /// We must wake up getWorkerReactor to interrupt it's sleeping.
            reactor.wakeUp();
        }
    }
}

void ConnectionHandler::packageSent()
{
    conn_stats.incrementPacketsSent();
    keeper_dispatcher->incrementPacketsSent();
}

void ConnectionHandler::packageReceived()
{
    conn_stats.incrementPacketsReceived();
    keeper_dispatcher->incrementPacketsReceived();
}

void ConnectionHandler::updateStats(const Coordination::ZooKeeperResponsePtr & response)
{
    /// update statistics ignoring watch, close and heartbeat response.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() != Coordination::OpNum::Heartbeat
        && response->getOpNum() != Coordination::OpNum::SetWatches && response->getOpNum() != Coordination::OpNum::Close)
    {
        auto current_time = getCurrentTimeMilliseconds();
        Int64 elapsed = current_time - response->request_created_time_ms;
        if (elapsed < 0)
        {
            LOG_WARNING(
                log,
                "Request #{}#{}#{} finish time {} is before than created time {}, please take care.",
                toHexString(session_id.load()),
                response->xid,
                Coordination::toString(response->getOpNum()),
                current_time,
                response->request_created_time_ms);
            elapsed = 0;
        }

        conn_stats.updateLatency(elapsed);
        if (unlikely(elapsed > 10000))
            LOG_WARNING(
                log,
                "Slow request detected #{}#{}#{}, time costs {}ms, please take care.",
                toHexString(session_id.load()),
                response->xid,
                Coordination::toString(response->getOpNum()),
                elapsed);

        keeper_dispatcher->updateKeeperStatLatency(elapsed);


        last_op.set(std::make_unique<LastOp>(LastOp{
            .name = Coordination::toString(response->getOpNum()),
            .last_cxid = response->xid,
            .last_zxid = response->zxid,
            .last_response_time = Poco::Timestamp().epochMicroseconds() / 1000,
        }));
    }
}

void ConnectionHandler::destroyMe()
{
    if (session_id)
        keeper_dispatcher->unregisterUserResponseCallBack(session_id);
    if (!handshake_done)
        keeper_dispatcher->unRegisterSessionResponseCallback(internal_id);
    else
        keeper_dispatcher->unRegisterSessionResponseCallback(session_id);

    delete this;
}

}
