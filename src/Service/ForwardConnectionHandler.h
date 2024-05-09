#pragma once

#include <Poco/FIFOBuffer.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Thread.h>

#include <Network/SocketAcceptor.h>
#include <Network/SocketNotification.h>
#include <Network/SocketReactor.h>

#include <Service/ConnCommon.h>
#include <Service/ForwardConnection.h>


namespace RK
{
using Poco::Net::StreamSocket;

using Poco::FIFOBuffer;
using Poco::Logger;
using Poco::Thread;

/**
 * Server endpoint for forward request.
 */
class ForwardConnectionHandler
{
public:
    ForwardConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_);
    ~ForwardConnectionHandler();

    void onSocketReadable(const Notification &);
    void onSocketWritable(const Notification &);
    void onReactorShutdown(const Notification &);
    void onSocketError(const Notification &);

private:
    void sendResponse(ForwardResponsePtr response);

    /// destroy connection
    void destroyMe();

    static constexpr size_t SENT_BUFFER_SIZE = 1024;
    std::optional<WriteBufferFromPocoSocket> send_buf;
//    FIFOBuffer send_buf = FIFOBuffer(SENT_BUFFER_SIZE);

    Logger * log;

    StreamSocket sock;
    SocketReactor & reactor;

    std::shared_ptr<FIFOBuffer> req_body_buf;
    FIFOBuffer req_header_buf = FIFOBuffer(1);

    FIFOBuffer req_body_len_buf = FIFOBuffer(4);

    /// Represent one read from socket.
    struct CurrentPackage
    {
        ForwardType type;
        /// whether a request read completes
        bool is_done;
    };
    CurrentPackage current_package{ForwardType::Unknown, true};

    Context & global_context;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    ThreadSafeForwardResponseQueuePtr responses;

    std::mutex send_response_mutex;

    /// server id in client endpoint which actually is my_id
    int32_t server_id{-1};
    /// client id in client endpoint
    int32_t client_id{-1};

    bool isUserOrSessionRequest(ForwardType type);
    void processHandshake();
    void processUserOrSessionRequest(ForwardRequestPtr request);
    void processSyncSessionsRequest(ForwardRequestPtr request);
};

}
