#include <Poco/Net/NetException.h>

#include <Common/Stopwatch.h>
#include <Common/setThreadName.h>

#include <Service/ForwardConnection.h>
#include <Service/ForwardConnectionHandler.h>
#include <Service/FourLetterCommand.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperIO.h>

namespace RK
{


ForwardConnectionHandler::ForwardConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_)
    : log(&Logger::get("ForwardConnectionHandler"))
    , sock(socket_)
    , reactor(reactor_)
    , global_context(global_context_)
    , keeper_dispatcher(global_context.getDispatcher())
    , responses(std::make_unique<ThreadSafeResponseQueue>())
{
    LOG_INFO(log, "New forwarding connection from {}", sock.peerAddress().toString());

    auto read_handler = Observer<ForwardConnectionHandler, ReadableNotification>(*this, &ForwardConnectionHandler::onSocketReadable);
    auto error_handler = Observer<ForwardConnectionHandler, ErrorNotification>(*this, &ForwardConnectionHandler::onSocketError);
    auto shutdown_handler
        = Observer<ForwardConnectionHandler, ShutdownNotification>(*this, &ForwardConnectionHandler::onReactorShutdown);

    std::vector<AbstractObserver *> handlers;
    handlers.push_back(&read_handler);
    handlers.push_back(&error_handler);
    handlers.push_back(&shutdown_handler);
    reactor.addEventHandlers(sock, handlers);

    need_destroy = false;
}

ForwardConnectionHandler::~ForwardConnectionHandler()
{
    try
    {
        reactor.removeEventHandler(
            sock, Observer<ForwardConnectionHandler, ReadableNotification>(*this, &ForwardConnectionHandler::onSocketReadable));
        reactor.removeEventHandler(
            sock, Observer<ForwardConnectionHandler, WritableNotification>(*this, &ForwardConnectionHandler::onSocketWritable));
        reactor.removeEventHandler(
            sock, Observer<ForwardConnectionHandler, ErrorNotification>(*this, &ForwardConnectionHandler::onSocketError));
        reactor.removeEventHandler(
            sock, Observer<ForwardConnectionHandler, ShutdownNotification>(*this, &ForwardConnectionHandler::onReactorShutdown));
    }
    catch (...)
    {
    }
}

void ForwardConnectionHandler::onSocketReadable(const Notification &)
{
    try
    {
        LOG_TRACE(log, "Forwarding handler socket readable");
        if (!sock.available())
        {
            LOG_INFO(log, "Client close connection!");
            destroyMe();
            return;
        }

        while (sock.available())
        {
            LOG_TRACE(log, "forwarding handler socket available");

            if (current_package.is_done)
            {
                LOG_TRACE(log, "Try handle new package");

                if (!req_header_buf.isFull())
                {
                    sock.receiveBytes(req_header_buf);
                    if (!req_header_buf.isFull())
                        continue;
                }

                /// read forward_protocol
                int8_t forward_type{};
                ReadBufferFromMemory read_buf(req_header_buf.begin(), req_header_buf.used());
                Coordination::read(forward_type, read_buf);
                req_header_buf.drain(req_header_buf.used());
                current_package.type = static_cast<ForwardType>(forward_type);

                LOG_TRACE(log, "Receive {}", toString(current_package.type));

                WriteBufferFromFiFoBuffer out;

                switch (static_cast<ForwardType>(forward_type))
                {
                    case ForwardType::Handshake:
                    case ForwardType::SyncSessions:
                    case ForwardType::NewSession:
                    case ForwardType::UpdateSession:
                    case ForwardType::User:
                        current_package.is_done = false;
                        break;
                    case ForwardType::Destroy:
                        LOG_WARNING(log, "Got destroy forward package");
                        destroyMe();
                        return;
                    default:
                        LOG_ERROR(log, "Got unexpected forward package");
                        destroyMe();
                        return;
                }
            }
            else
            {
                if (unlikely(current_package.type == ForwardType::Handshake))
                {
                    if (!req_body_buf)
                    {
                        /// server client
                        req_body_buf = std::make_shared<FIFOBuffer>(8);
                    }
                    sock.receiveBytes(*req_body_buf);
                    if (!req_body_buf->isFull())
                        continue;

                    processHandshake();

                    req_body_buf.reset();
                    current_package.is_done = true;
                }
                else
                {
                    ForwardRequestPtr request;
                    try
                    {
                        if (!req_body_buf) /// new data package
                        {
                            if (!req_body_len_buf.isFull())
                            {
                                sock.receiveBytes(req_body_len_buf);
                                if (!req_body_len_buf.isFull())
                                    continue;
                            }

                            /// request body length, for sessions request is session count
                            int32_t body_len{};
                            ReadBufferFromMemory read_buf(req_body_len_buf.begin(), req_body_len_buf.used());

                            Coordination::read(body_len, read_buf);
                            req_body_len_buf.drain(req_body_len_buf.used());

                            LOG_TRACE(log, "Read request done, body length {}", body_len);
                            req_body_buf = std::make_shared<FIFOBuffer>(body_len);
                        }

                        sock.receiveBytes(*req_body_buf);
                        if (!req_body_buf->isFull())
                            continue;

                        request = ForwardRequestFactory::instance().get(current_package.type);

                        if (likely(isUserOrSessionRequest(current_package.type)))
                        {
                            processUserOrSessionRequest(request);
                        }
                        else
                        {
                            processSyncSessionsRequest(request);
                        }

                        req_body_buf.reset();
                        current_package.is_done = true;
                    }
                    catch (Exception & e)
                    {
                        if (request)
                        {
                            auto response = request->makeResponse();
                            response->setAppendEntryResult(false, nuraft::cmd_result_code::FAILED);
                            keeper_dispatcher->invokeForwardResponseCallBack({server_id, client_id}, response);
                            tryLogCurrentException(log, "Error when forwarding request " + request->toString());
                        }
                        else
                        {
                            tryLogCurrentException(log, __PRETTY_FUNCTION__);
                            throw e;
                        }
                    }
                    catch (Poco::Exception & e)
                    {
                        tryLogCurrentException(log, __PRETTY_FUNCTION__);
                        throw e;
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
        tryLogCurrentException(log, "Error when handling request, will close connection.");
        destroyMe();
    }
}

bool ForwardConnectionHandler::isUserOrSessionRequest(ForwardType type)
{
    return type == ForwardType::User || type == ForwardType::NewSession || type == ForwardType::UpdateSession;
}

void ForwardConnectionHandler::processSyncSessionsRequest(ForwardRequestPtr request)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
    request->readImpl(body);

    auto * sync_sessions_req = dynamic_cast<ForwardSyncSessionsRequest *>(request.get());
    LOG_TRACE(log, "Receive {} remote sessions", sync_sessions_req->session_expiration_time.size());

    for (auto [session_id, expiration_time] : sync_sessions_req->session_expiration_time)
    {
        LOG_TRACE(log, "Receive remote session {}, expiration time {}", toHexString(session_id), expiration_time);
        keeper_dispatcher->handleRemoteSession(session_id, expiration_time);
    }

    auto response = request->makeResponse();
    keeper_dispatcher->invokeForwardResponseCallBack({server_id, client_id}, response);
}

void ForwardConnectionHandler::processUserOrSessionRequest(ForwardRequestPtr request)
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());
    request->readImpl(body);
    keeper_dispatcher->pushForwardingRequest(server_id, client_id, request);
}

void ForwardConnectionHandler::processHandshake()
{
    ReadBufferFromMemory body(req_body_buf->begin(), req_body_buf->used());

    read(server_id, body);
    read(client_id, body);

    /// register session response callback
    auto response_callback = [this](ForwardResponsePtr response) { sendResponse(response); };
    keeper_dispatcher->registerForwarderResponseCallBack({server_id, client_id}, response_callback);

    LOG_INFO(log, "Register forward from server {} client {}", server_id, client_id);

    std::shared_ptr<ForwardHandshakeResponse> response = std::make_shared<ForwardHandshakeResponse>();
    response->accepted = true;
    response->error_code = nuraft::OK;

    keeper_dispatcher->invokeForwardResponseCallBack({server_id, client_id}, response);
}

void ForwardConnectionHandler::onSocketWritable(const Notification &)
{
    LOG_TRACE(log, "Forwarder socket writable");

    auto remove_event_handler_if_needed = [this]
    {
        /// Double check to avoid dead lock
        if (responses->empty() && send_buf.used() == 0)
        {
            std::lock_guard lock(send_response_mutex);
            {
                /// If all sent unregister writable event.
                if (responses->empty() && send_buf.used() == 0)
                {
                    LOG_TRACE(log, "Remove socket writable event handler");
                    reactor.removeEventHandler(
                        sock,
                        Observer<ForwardConnectionHandler, WritableNotification>(
                            *this, &ForwardConnectionHandler::onSocketWritable));
                }
            }
        }
    };

    try
    {
        if (need_destroy)
        {
            LOG_WARNING(log, "The connection for server {} client {} is stale, will close it", server_id, client_id);
            delete this;
            return;
        }

        if (responses->empty() && send_buf.used() == 0)
        {
            remove_event_handler_if_needed();
            return;
        }

        /// TODO use zero copy buffer
        size_t size_to_sent = 0;

        /// 1. accumulate data into tmp_buf
        responses->forEach(
            [&size_to_sent, this](const auto & resp) -> bool
            {
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
        size_t sent = sock.sendBytes(send_buf);

        /// 3. remove sent responses

        ptr<FIFOBuffer> resp;
        while (responses->peek(resp) && sent > 0)
        {
            if (sent >= resp->used())
            {
                sent -= resp->used();
                responses->remove();
            }
            else
            {
                resp->drain(sent);
                /// move data to begin
                resp->begin();
                sent = 0;
            }
        }

        remove_event_handler_if_needed();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error when sending data to follower, will close connection.");
        destroyMe();
    }
}

void ForwardConnectionHandler::onReactorShutdown(const Notification &)
{
    LOG_INFO(log, "Reactor shutdown!");
    destroyMe();
}

void ForwardConnectionHandler::onSocketError(const Notification &)
{
    destroyMe();
}

void ForwardConnectionHandler::sendResponse(ForwardResponsePtr response)
{
    LOG_TRACE(log, "Send response {}", response->toString());

    if (response->forwardType() == ForwardType::Destroy)
    {
        need_destroy = true;
    }

    WriteBufferFromFiFoBuffer buf;
    response->write(buf);

    {
        /// Lock to avoid data condition which will lead response leak
        std::lock_guard lock(send_response_mutex);
        /// TODO handle timeout
        responses->push(buf.getBuffer());
        /// Trigger socket writable event
        reactor.addEventHandler(
            sock, Observer<ForwardConnectionHandler, WritableNotification>(*this, &ForwardConnectionHandler::onSocketWritable));
    }

    /// We must wake up getWorkerReactor to interrupt it's sleeping.
    reactor.wakeUp();
}

void ForwardConnectionHandler::destroyMe()
{
    keeper_dispatcher->unRegisterForwarderResponseCallBack({server_id, client_id});
    delete this;
}

}
