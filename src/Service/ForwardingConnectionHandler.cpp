#define USE_NIO_FOR_KEEPER
#ifdef USE_NIO_FOR_KEEPER
#include <Service/ForwardingConnectionHandler.h>

#include <Service/FourLetterCommand.h>
#include <Service/formatHex.h>
#include <Poco/Net/NetException.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/setThreadName.h>

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


ForwardingConnectionHandler::ForwardingConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor)
    : log(&Logger::get("ForwardingConnectionHandler")), socket_(socket), reactor_(reactor)
    , global_context(global_context_)
    , service_keeper_storage_dispatcher(global_context.getSvsKeeperStorageDispatcher())
    , operation_timeout(
          0, global_context.getConfigRef().getUInt("service.coordination_settings.operation_timeout_ms", Coordination::DEFAULT_OPERATION_TIMEOUT_MS) * 1000)
    , session_timeout(
          0, global_context.getConfigRef().getUInt("service.coordination_settings.session_timeout_ms", Coordination::DEFAULT_SESSION_TIMEOUT_MS) * 1000)
    , responses(std::make_unique<ThreadSafeResponseQueue>())
    , last_op(std::make_unique<LastOp>(EMPTY_LAST_OP))
{
    LOG_DEBUG(log, "New connection from {}", socket_.peerAddress().toString());

    reactor_.addEventHandler(socket_, NObserver<ForwardingConnectionHandler, ReadableNotification>(*this, &ForwardingConnectionHandler::onSocketReadable));
    reactor_.addEventHandler(socket_, NObserver<ForwardingConnectionHandler, ErrorNotification>(*this, &ForwardingConnectionHandler::onSocketError));
    reactor_.addEventHandler(socket_, NObserver<ForwardingConnectionHandler, ShutdownNotification>(*this, &ForwardingConnectionHandler::onReactorShutdown));
}

ForwardingConnectionHandler::~ForwardingConnectionHandler()
{
    try
    {
        reactor_.removeEventHandler(socket_, NObserver<ForwardingConnectionHandler, ReadableNotification>(*this, &ForwardingConnectionHandler::onSocketReadable));
        reactor_.removeEventHandler(socket_, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
        reactor_.removeEventHandler(socket_, NObserver<ForwardingConnectionHandler, ErrorNotification>(*this, &ForwardingConnectionHandler::onSocketError));
        reactor_.removeEventHandler(socket_, NObserver<ForwardingConnectionHandler, ShutdownNotification>(*this, &ForwardingConnectionHandler::onReactorShutdown));
    }
    catch (...)
    {
    }
}

void ForwardingConnectionHandler::onSocketReadable(const AutoPtr<ReadableNotification> & /*pNf*/)
{
    try
    {
        LOG_TRACE(log, "forwarding handler socket readable");
        if (!socket_.available())
        {
//            LOG_INFO(log, "Client of session {} close connection! errno {}", toHexString(session_id), errno);
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

                body_len = header + 8; /// tail session_id
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

            LOG_TRACE(log, "Read request done, body length : {}", body_len);
            poco_assert_msg(int32_t (req_body_buf->used()) == body_len, "Request body length is not consistent.");

            {
                session_stopwatch.start();

                try
                {
                    auto [received_op, received_xid] = receiveRequest(body_len);

                    if (received_op == Coordination::OpNum::Close)
                    {
//                        LOG_DEBUG(log, "Received close event with xid {} for session id #{}", received_xid, session_id);
                        close_xid = received_xid;
                    }
                    else if (received_op == Coordination::OpNum::Heartbeat)
                    {
//                        LOG_TRACE(log, "Received heartbeat for session #{}", session_id);
                    }

                    /// Each request restarts session stopwatch
                    session_stopwatch.restart();
                }
                catch (const Exception & e)
                {
                    tryLogCurrentException(log);

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

void ForwardingConnectionHandler::onSocketWritable(const AutoPtr<WritableNotification> &)
{
    try
    {
//        LOG_TRACE(log, "session {} socket writable", toHexString(session_id));

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
//                LOG_TRACE(log, "sent response to {}", toHexString(session_id));
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
            LOG_TRACE(log, "Remove socket writable event handler - session {}", socket_.peerAddress().toString());
            reactor_.removeEventHandler(
                socket_, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Fatal error when sending data to client, will close connection.");
        destroyMe();
    }
}

void ForwardingConnectionHandler::onReactorShutdown(const AutoPtr<ShutdownNotification> & /*pNf*/)
{
    LOG_INFO(log, "reactor shutdown!");
    destroyMe();
}

void ForwardingConnectionHandler::onSocketError(const AutoPtr<ErrorNotification> & /*pNf*/)
{
//    LOG_WARNING(log, "Socket of session {} error, errno {} !", toHexString(session_id), errno);
    destroyMe();
}


std::pair<Coordination::OpNum, Coordination::XID> ForwardingConnectionHandler::receiveRequest(int32_t length)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());

    int32_t xid;
    Coordination::read(xid, body);

    Coordination::OpNum opnum;
    Coordination::read(opnum, body);

    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request->xid = xid;
    request->readImpl(body);

    int64_t session_id;
    Coordination::read(session_id, body);

    request->request_created_time_us = Poco::Timestamp().epochMicroseconds();

    LOG_TRACE(log, "Receive request: session {}, xid {}, length {}, opnum {}", toHexString(session_id), xid, length, Coordination::toString(opnum));

    if (!service_keeper_storage_dispatcher->putRequest(request, session_id))
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Session {} already disconnected", session_id);
    return std::make_pair(opnum, xid);
}

void ForwardingConnectionHandler::sendResponse(const Coordination::ZooKeeperResponsePtr& response)
{
//    LOG_TRACE(log, "Dispatch response to conn handler session {}", toHexString(session_id));

    WriteBufferFromFiFoBuffer buf;
    response->write(buf);

    /// TODO handle timeout
    responses->push(buf.getBuffer());

//    LOG_TRACE(log, "Add socket writable event handler - session {}", toHexString(session_id));
    /// Trigger socket writable event
    reactor_.addEventHandler(
        socket_, NObserver<ForwardingConnectionHandler, WritableNotification>(*this, &ForwardingConnectionHandler::onSocketWritable));
    /// We must wake up reactor to interrupt it's sleeping.
    LOG_TRACE(log, "Poll trigger wakeup-- poco thread name {}, actually thread name {}", Poco::Thread::current() ? Poco::Thread::current()->name() : "main", getThreadName());

    reactor_.wakeUp();
}

void ForwardingConnectionHandler::destroyMe()
{
    delete this;
}

}

#endif
