#pragma once

#include <unordered_set>
#include <Service/ConnCommon.h>
#include <Service/ForwardingConnection.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <Poco/Delegate.h>
#include <Poco/Exception.h>
#include <Poco/FIFOBuffer.h>
#include <Poco/NObserver.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Util/Option.h>
#include <Poco/Util/OptionSet.h>
#include <Poco/Util/ServerApplication.h>
#include <Common/NIO/SocketNotification.h>
#include <Common/NIO/SocketReactor.h>
#include <Common/NIO/SvsSocketAcceptor.h>
#include <Common/NIO/SvsSocketReactor.h>


namespace RK
{
using Poco::Net::StreamSocket;

using Poco::AutoPtr;
using Poco::FIFOBuffer;
using Poco::Logger;
using Poco::Thread;

/**
 * Server endpoint for forwarding request.
 */
class ForwardingConnectionHandler
{
public:
    ForwardingConnectionHandler(Context & global_context_, StreamSocket & socket_, SocketReactor & reactor_);
    ~ForwardingConnectionHandler();

    void onSocketReadable(const AutoPtr<ReadableNotification> & pNf);
    void onSocketWritable(const AutoPtr<WritableNotification> & pNf);
    void onReactorShutdown(const AutoPtr<ShutdownNotification> & pNf);
    void onSocketError(const AutoPtr<ErrorNotification> & pNf);

private:
    std::tuple<int64_t, int64_t, Coordination::OpNum> receiveRequest(int32_t length);

    void sendResponse(const ForwardResponse & response);

    /// destroy connection
    void destroyMe();

    static constexpr size_t SENT_BUFFER_SIZE = 1024;
    FIFOBuffer send_buf = FIFOBuffer(SENT_BUFFER_SIZE);

    Logger * log;

    StreamSocket sock;
    SocketReactor & reactor;

    std::shared_ptr<FIFOBuffer> req_body_buf;
    FIFOBuffer req_header_buf = FIFOBuffer(1);

    FIFOBuffer req_body_len_buf = FIFOBuffer(4);

    /// Represent one read from socket.
    struct CurrentPackage
    {
        /// request type
        PkgType pkg_type;
        /// whether a request read completes
        bool is_done;
    };
    CurrentPackage current_package{Unknown, true};

    Context & global_context;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    Stopwatch session_stopwatch;
    ThreadSafeResponseQueuePtr responses;

    /// server id in client endpoint which actually is Raft ID
    int32_t server_id;
    /// client id in client endpoint
    int32_t client_id;
};

}
