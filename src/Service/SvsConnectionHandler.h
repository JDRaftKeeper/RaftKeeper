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

class SvsConnectionHandler
{
public:
    static void registerConnection(SvsConnectionHandler * conn);
    static void unregisterConnection(SvsConnectionHandler * conn);
    /// dump all connections statistics
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    static void resetConnsStats();
private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<SvsConnectionHandler *> connections;

public:
    SvsConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor);
    ~SvsConnectionHandler();

    void onSocketReadable(const AutoPtr<ReadableNotification> & pNf);
    void onSocketWritable(const AutoPtr<WritableNotification> & pNf);
    void onSocketShutdown(const AutoPtr<ShutdownNotification> & pNf);

    KeeperConnectionStats getConnectionStats() const;
    void dumpStats(WriteBufferFromOwnString & buf, bool brief);
    void resetStats();

private:
    struct HandShakeResult
    {
        bool connect_success{};
        bool session_expired{};
    };

    ConnectRequest receiveHandshake(int32_t handshake_length);
    HandShakeResult handleHandshake(ConnectRequest & connect_req);
    void sendHandshake(HandShakeResult & result);

    static bool isHandShake(Int32 & handshake_length) ;
    bool tryExecuteFourLetterWordCmd(int32_t four_letter_cmd);

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest(int32_t length);

    void sendResponse(const Coordination::ZooKeeperResponsePtr& resp);

    void packageSent();
    void packageReceived();

    void updateStats(const Coordination::ZooKeeperResponsePtr & response);

    /// destroy connection
    void destroyMe();

    static constexpr size_t SENT_BUFFER_SIZE = 1024;
    FIFOBuffer send_buf = FIFOBuffer(SENT_BUFFER_SIZE);

    Logger * log;

    StreamSocket socket_;
    SocketReactor & reactor_;

    ptr<FIFOBuffer> req_body_buf;
    FIFOBuffer req_header_buf = FIFOBuffer(4);

    bool next_req_header_read_done = false;
    bool previous_req_body_read_done = true;
    bool handshake_done = false;

    Context & global_context;
    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;

    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id{-1};

    Stopwatch session_stopwatch;
    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;
    Poco::Timestamp established;

    using Operations = std::unordered_map<Coordination::XID, Poco::Timestamp>;
    Operations operations;

    LastOpMultiVersion last_op;

    mutable std::mutex conn_stats_mutex;
    KeeperConnectionStats conn_stats;
};

}
