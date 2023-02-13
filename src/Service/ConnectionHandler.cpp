/**
 * Copyright 2021-2023 JD.com, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#    include "ConnectionHandler.h"

#    include <Service/FourLetterCommand.h>
#    include <Service/formatHex.h>
#    include <Poco/Net/NetException.h>
#    include <Common/Stopwatch.h>
#    include <Common/ZooKeeper/ZooKeeperCommon.h>
#    include <Common/ZooKeeper/ZooKeeperIO.h>
#    include <Common/setThreadName.h>

namespace RK
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

ConnectionHandler::ConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor)
    : log(&Logger::get("ConnectionHandler"))
    , socket_(socket)
    , reactor_(reactor)
    , global_context(global_context_)
    , keeper_dispatcher(global_context.getDispatcher())
    , operation_timeout(
          0,
          global_context.getConfigRef().getUInt(
              "keeper.raft_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS)
              * 1000)
    , session_timeout(
          0,
          global_context.getConfigRef().getUInt(
              "keeper.raft_settings.session_timeout_ms", Coordination::DEFAULT_SESSION_TIMEOUT_MS)
              * 1000)
    , responses(std::make_unique<ThreadSafeResponseQueue>())
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    LOG_DEBUG(log, "New connection from {}", socket_.peerAddress().toString());
    registerConnection(this);

    reactor_.addEventHandler(
        socket_, NObserver<ConnectionHandler, ReadableNotification>(*this, &ConnectionHandler::onSocketReadable));
    reactor_.addEventHandler(socket_, NObserver<ConnectionHandler, ErrorNotification>(*this, &ConnectionHandler::onSocketError));
    reactor_.addEventHandler(
        socket_, NObserver<ConnectionHandler, ShutdownNotification>(*this, &ConnectionHandler::onReactorShutdown));
}

ConnectionHandler::~ConnectionHandler()
{
    try
    {
        /// 4lw cmd connection will not init session_id
        if (session_id != -1)
            LOG_INFO(log, "Disconnecting session {}", toHexString(session_id));

        unregisterConnection(this);

        reactor_.removeEventHandler(
            socket_, NObserver<ConnectionHandler, ReadableNotification>(*this, &ConnectionHandler::onSocketReadable));
        reactor_.removeEventHandler(
            socket_, NObserver<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
        reactor_.removeEventHandler(
            socket_, NObserver<ConnectionHandler, ErrorNotification>(*this, &ConnectionHandler::onSocketError));
        reactor_.removeEventHandler(
            socket_, NObserver<ConnectionHandler, ShutdownNotification>(*this, &ConnectionHandler::onReactorShutdown));
    }
    catch (...)
    {
    }
}

void ConnectionHandler::onSocketReadable(const AutoPtr<ReadableNotification> & /*pNf*/)
{
    try
    {
        LOG_TRACE(log, "session {} socket readable", toHexString(session_id));
        if (!socket_.available())
        {
            LOG_INFO(log, "Client of session {} close connection! errno {}", toHexString(session_id), errno);
            destroyMe();
            return;
        }

        while (socket_.available())
        {
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
                LOG_TRACE(log, "session {} read request length : {}", toHexString(session_id), body_len);

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

            socket_.receiveBytes(*req_body_buf);

            if (!req_body_buf->isFull())
                continue;

            /// Request reading done, set flags
            next_req_header_read_done = false;
            previous_req_body_read_done = true;

            packageReceived();

            LOG_TRACE(
                log,
                "session {} read request done, body length : {}, req_body_buf used {}",
                toHexString(session_id),
                body_len,
                req_body_buf->used());
            poco_assert_msg(int32_t(req_body_buf->used()) == body_len, "Request body length is not consistent.");

            /// 3. handshake
            if (unlikely(!handshake_done))
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
                auto response_callback = [this](const Coordination::ZooKeeperResponsePtr & response) { sendResponse(response); };
                keeper_dispatcher->registerSession(session_id, response_callback, handshake_result.is_reconnected);

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
                        LOG_DEBUG(log, "Received close event with xid {} for session {}", received_xid, toHexString(session_id));
                        close_xid = received_xid;
                    }
                    else if (received_op == Coordination::OpNum::Heartbeat)
                    {
                        LOG_TRACE(log, "Received heartbeat for session {}", toHexString(session_id));
                    }

                    /// Each request restarts session stopwatch
                    session_stopwatch.restart();
                }
                catch (const Exception & e)
                {
                    tryLogCurrentException(log, fmt::format("Error processing session {} request.", toHexString(session_id)));

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
            log, fmt::format("Network error when receiving request, will close connection session {}.", toHexString(session_id)));
        destroyMe();
    }
    catch (...)
    {
        tryLogCurrentException(
            log, fmt::format("Fatal error when handling request, will close connection session {}.", toHexString(session_id)));
        destroyMe();
    }
}

void ConnectionHandler::onSocketWritable(const AutoPtr<WritableNotification> &)
{
    try
    {
        LOG_TRACE(log, "session {} socket writable", toHexString(session_id));

        if (responses->size() == 0 && send_buf.used() == 0)
            return;

        /// TODO use zero copy buffer
        size_t size_to_sent = 0;

        /// 1. accumulate data into tmp_buf
        responses->forEach([&size_to_sent, this](const auto & resp) -> bool {
            if (resp == is_close)
                return false;

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
        while (responses->peek(resp) && sent > 0)
        {
            if (sent >= resp->used())
            {
                sent -= resp->used();
                responses->remove();
                /// package sent
                packageSent();
                LOG_TRACE(log, "sent response to {}", toHexString(session_id));
            }
            else
            {
                resp->drain(sent);
                /// move data to begin
                resp->begin();
                sent = 0;
            }
        }

        if (responses->peek(resp) && resp == is_close)
        {
            destroyMe();
            return;
        }

        /// If all sent unregister writable event.
        if (responses->size() == 0 && send_buf.used() == 0)
        {
            LOG_DEBUG(log, "Remove socket writable event handler - session {}", socket_.peerAddress().toString());
            reactor_.removeEventHandler(
                socket_, NObserver<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fatal error when sending data to client, will close connection.");
        destroyMe();
    }
}

void ConnectionHandler::onReactorShutdown(const AutoPtr<ShutdownNotification> & /*pNf*/)
{
    LOG_INFO(log, "reactor shutdown!");
    destroyMe();
}

void ConnectionHandler::onSocketError(const AutoPtr<ErrorNotification> & /*pNf*/)
{
    LOG_WARNING(log, "Socket of session {} error, errno {} !", toHexString(session_id), errno);
    destroyMe();
}

ConnectionStats ConnectionHandler::getConnectionStats() const
{
    std::lock_guard lock(conn_stats_mutex);
    return conn_stats;
}

void ConnectionHandler::dumpStats(WriteBufferFromOwnString & buf, bool brief)
{
    ConnectionStats stats = getConnectionStats();

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
            writeText(",sid=", buf);
            writeText(toHexString(session_id), buf);

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

void ConnectionHandler::resetStats()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.reset();
    }
    last_op.set(std::make_unique<LastOp>(EMPTY_LAST_OP));
}

ConnectRequest ConnectionHandler::receiveHandshake(int32_t handshake_req_len)
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

    return {protocol_version, last_zxid_seen, timeout_ms, previous_session_id, passwd, readonly};
}

ConnectionHandler::HandShakeResult ConnectionHandler::handleHandshake(ConnectRequest & connect_req)
{
    if (connect_req.timeout_ms != 0)
        session_timeout = std::min(Poco::Timespan(connect_req.timeout_ms * 1000), session_timeout);

    LOG_INFO(log, "Negotiated session_timeout : {}", session_timeout.totalMilliseconds());

    bool is_reconnected = false;
    bool session_expired = false;
    bool connect_success = keeper_dispatcher->hasLeader();

    if (!connect_success)
    {
        LOG_WARNING(log, "Has no leader!");
        return {connect_success, true, is_reconnected};
    }

    try
    {
        if (connect_req.previous_session_id != 0)
        {
            LOG_INFO(log, "Requesting reconnecting with session {}", connect_req.previous_session_id);
            session_id = connect_req.previous_session_id;
            /// existed session
            if (!keeper_dispatcher->getStateMachine().containsSession(connect_req.previous_session_id))
            {
                /// session expired, set timeout <=0
                LOG_WARNING(
                    log, "Client try to reconnects but session {} is already expired", toHexString(connect_req.previous_session_id));
                session_expired = true;
                connect_success = false;
            }
            else
            {
                /// update session timeout
                if (!keeper_dispatcher->updateSessionTimeout(session_id, session_timeout.totalMilliseconds()))
                {
                    /// update failed
                    /// session was expired when updating
                    /// session expired, set timeout <=0
                    LOG_WARNING(log, "Session {} was expired when updating", toHexString(connect_req.previous_session_id));
                    session_expired = true;
                    connect_success = false;
                }
                else
                {
                    is_reconnected = true;
                    LOG_INFO(log, "Client reconnected with session {}", toHexString(connect_req.previous_session_id));
                }
            }
        }
        else
        {
            /// new session
            session_id = keeper_dispatcher->getSessionID(session_timeout.totalMilliseconds());
            LOG_INFO(log, "New session with ID {}", toHexString(session_id));
        }
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Cannot receive session {}", e.displayText());
        connect_success = false;
    }

    return {connect_success, session_expired, is_reconnected};
}

void ConnectionHandler::sendHandshake(HandShakeResult & result)
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

bool ConnectionHandler::isHandShake(Int32 & handshake_length)
{
    return handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH
        || handshake_length == Coordination::CLIENT_HANDSHAKE_LENGTH_WITH_READONLY;
}

bool ConnectionHandler::tryExecuteFourLetterWordCmd(int32_t command)
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

std::pair<Coordination::OpNum, Coordination::XID> ConnectionHandler::receiveRequest(int32_t length)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
    int32_t xid;
    Coordination::read(xid, body);

    Coordination::OpNum opnum;
    Coordination::read(opnum, body);

    if (opnum != Coordination::OpNum::Heartbeat)
        LOG_DEBUG(
            log,
            "Receive request: session {}, xid {}, length {}, opnum {}",
            toHexString(session_id),
            xid,
            length,
            Coordination::toString(opnum));

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(body);

    if (!keeper_dispatcher->putRequest(request, session_id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", toHexString(session_id));
    return std::make_pair(opnum, xid);
}

void ConnectionHandler::sendResponse(const Coordination::ZooKeeperResponsePtr & response)
{
    LOG_TRACE(log, "Dispatch response to conn handler session {}", toHexString(session_id));

    /// TODO should invoked after response sent to client.
    updateStats(response);

    if (response->xid != Coordination::WATCH_XID && response->getOpNum() == Coordination::OpNum::Close)
    {
        responses->push(ptr<FIFOBuffer>());
    }
    else
    {
        WriteBufferFromFiFoBuffer buf;
        response->write(buf);

        /// TODO handle timeout
        responses->push(buf.getBuffer());
    }

    LOG_TRACE(log, "Add socket writable event handler - session {}", toHexString(session_id));
    /// Trigger socket writable event
    reactor_.addEventHandler(
        socket_, NObserver<ConnectionHandler, WritableNotification>(*this, &ConnectionHandler::onSocketWritable));
    /// We must wake up reactor to interrupt it's sleeping.
    LOG_TRACE(
        log,
        "Poll trigger wakeup-- poco thread name {}, actually thread name {}",
        Poco::Thread::current() ? Poco::Thread::current()->name() : "main",
        getThreadName());

    reactor_.wakeUp();
}

void ConnectionHandler::packageSent()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.incrementPacketsSent();
    }
    keeper_dispatcher->incrementPacketsSent();
}

void ConnectionHandler::packageReceived()
{
    {
        std::lock_guard lock(conn_stats_mutex);
        conn_stats.incrementPacketsReceived();
    }
    keeper_dispatcher->incrementPacketsReceived();
}

void ConnectionHandler::updateStats(const Coordination::ZooKeeperResponsePtr & response)
{
    /// update statistics ignoring watch, close and heartbeat response.
    if (response->xid != Coordination::WATCH_XID && response->getOpNum() != Coordination::OpNum::Heartbeat
        && response->getOpNum() != Coordination::OpNum::SetWatches && response->getOpNum() != Coordination::OpNum::Close)
    {
        Int64 elapsed = Poco::Timestamp().epochMicroseconds() / 1000 - response->request_created_time_ms;
        {
            std::lock_guard lock(conn_stats_mutex);
            conn_stats.updateLatency(elapsed);
            if (elapsed > 1000)
                LOG_WARNING(
                    log,
                    "Request process time {}ms, session {} xid {} req type {}",
                    elapsed,
                    toHexString(session_id),
                    response->xid,
                    Coordination::toString(response->getOpNum()));
        }
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
    keeper_dispatcher->finishSession(session_id);
    delete this;
}

}
