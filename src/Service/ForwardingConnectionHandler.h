#pragma once

#include <Poco/Delegate.h>
#include <Poco/Exception.h>
#include <Poco/FIFOBuffer.h>
#include <Poco/NObserver.h>
#include <Service/SocketNotification.h>
#include <Service/SocketReactor.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Thread.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Util/Option.h>
#include <Poco/Util/OptionSet.h>
#include <Poco/Util/ServerApplication.h>

#include <Service/ConnCommon.h>
#include <Service/SvsSocketAcceptor.h>
#include <Service/SvsSocketReactor.h>
#include <Service/WriteBufferFromFiFoBuffer.h>


namespace DB
{
using Poco::Net::StreamSocket;

using Poco::AutoPtr;
using Poco::Thread;
using Poco::FIFOBuffer;
using Poco::Logger;

class ForwardingConnectionHandler
{
private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<ForwardingConnectionHandler *> connections;

public:
    ForwardingConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor);
    ~ForwardingConnectionHandler();

    void onSocketReadable(const AutoPtr<ReadableNotification> & pNf);
    void onSocketWritable(const AutoPtr<WritableNotification> & pNf);
    void onReactorShutdown(const AutoPtr<ShutdownNotification> & pNf);
    void onSocketError(const AutoPtr<ErrorNotification> & pNf);


private:

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest(int32_t length);

    void sendResponse(const Coordination::ZooKeeperResponsePtr& resp);

    /// destroy connection
    void destroyMe();

    static constexpr size_t SENT_BUFFER_SIZE = 1024;
    FIFOBuffer send_buf = FIFOBuffer(SENT_BUFFER_SIZE);

    Logger * log;

    StreamSocket socket_;
    SocketReactor & reactor_;

    ptr<FIFOBuffer> req_body_buf;
    FIFOBuffer req_header_buf = FIFOBuffer(1);

    FIFOBuffer req_body_len_buf = FIFOBuffer(4);

    bool current_package_done = false;

    Context & global_context;
    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;

    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;

    Stopwatch session_stopwatch;
    ThreadSafeResponseQueuePtr responses;
};

}
