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

namespace DB
{

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ThreadSafeResponseQueue = SvsKeeperThreadSafeQueue<Coordination::ZooKeeperResponsePtr>;

using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

class ServiceTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    ServiceTCPHandler(IServer & server_, const Poco::Net::StreamSocket & socket_);
    void run() override;
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
    Poco::Timespan receiveHandshake(int32_t handshake_length);

    static bool isHandShake(Int32 & handshake_length) ;
    bool tryExecuteFourLetterWordCmd(Int32 & four_letter_cmd);

    std::pair<Coordination::OpNum, Coordination::XID> receiveRequest();
};

}
