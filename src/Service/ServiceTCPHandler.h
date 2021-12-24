#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif


#include <unordered_map>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Server/IServer.h>
#include <Service/SvsKeeperDispatcher.h>
#include <Service/SvsKeeperThreadSafeQueue.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/Stopwatch.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/MultiVersion.h>


namespace DB
{

struct ConnectRequest
{
    int32_t protocol_version;
    int64_t last_zxid_seen;
    int32_t timeout_ms;
    int64_t previous_session_id = 0;
    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};
    bool readonly;
};

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = SvsKeeperThreadSafeQueue<Coordination::ZooKeeperResponsePtr>;

using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

struct LastOp;
using LastOpMultiVersion = MultiVersion<LastOp>;
using LastOpPtr = LastOpMultiVersion::Version;

class ServiceTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    static void registerConnection(ServiceTCPHandler * conn);
    static void unregisterConnection(ServiceTCPHandler * conn);
    /// dump all connections statistics
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    static void resetConnsStats();

private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<ServiceTCPHandler *> connections;

public:
    ServiceTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
    void run() override;

    KeeperConnectionStats getConnectionStats() const;
    void dumpStats(WriteBufferFromOwnString & buf, bool brief);
    void resetStats();

    ~ServiceTCPHandler() override;
private:
    IServer & server;
    Poco::Logger * log;
    Context global_context;
    std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher;
    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id{-1};
    Stopwatch session_stopwatch;
    SocketInterruptablePollWrapperPtr poll_wrapper;

    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;

    /// Streams for reading/writing from/to client connection socket.
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    void runImpl();

    void sendHandshake(bool has_leader);
    ConnectRequest receiveHandshake(int32_t handshake_length);

    static bool isHandShake(Int32 & handshake_length) ;
    bool tryExecuteFourLetterWordCmd(int32_t four_letter_cmd);

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();

    void packageSent();
    void packageReceived();

    void updateStats(Coordination::ZooKeeperResponsePtr & response);

    Poco::Timestamp established;

    using Operations = std::unordered_map<Coordination::XID, Poco::Timestamp>;
    Operations operations;

    LastOpMultiVersion last_op;

    mutable std::mutex conn_stats_mutex;
    KeeperConnectionStats conn_stats;
};

}
