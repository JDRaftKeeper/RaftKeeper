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

#include <unordered_set>
#include <Service/ConnCommon.h>
#include <Service/SvsSocketAcceptor.h>
#include <Service/SvsSocketReactor.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include "ConnectionStats.h"
#include "IO/WriteBufferFromString.h"


namespace RK
{
using Poco::Net::StreamSocket;

using Poco::AutoPtr;
using Poco::Thread;
using Poco::FIFOBuffer;
using Poco::Logger;

class ConnectionHandler
{
public:
    static void registerConnection(ConnectionHandler * conn);
    static void unregisterConnection(ConnectionHandler * conn);
    /// dump all connections statistics
    static void dumpConnections(WriteBufferFromOwnString & buf, bool brief);
    static void resetConnsStats();
private:
    static std::mutex conns_mutex;
    /// all connections
    static std::unordered_set<ConnectionHandler *> connections;

public:
    ConnectionHandler(Context & global_context_, StreamSocket & socket, SocketReactor & reactor);
    ~ConnectionHandler();

    void onSocketReadable(const AutoPtr<ReadableNotification> & pNf);
    void onSocketWritable(const AutoPtr<WritableNotification> & pNf);
    void onReactorShutdown(const AutoPtr<ShutdownNotification> & pNf);
    void onSocketError(const AutoPtr<ErrorNotification> & pNf);

    ConnectionStats getConnectionStats() const;
    void dumpStats(WriteBufferFromOwnString & buf, bool brief);
    void resetStats();

private:
    struct HandShakeResult
    {
        bool connect_success{};
        bool session_expired{};
        bool is_reconnected{};
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

    std::shared_ptr<FIFOBuffer> is_close = nullptr;

    Logger * log;

    StreamSocket socket_;
    SocketReactor & reactor_;

    std::shared_ptr<FIFOBuffer> req_body_buf;
    FIFOBuffer req_header_buf = FIFOBuffer(4);

    /// request body length
    int32_t body_len{};

    bool next_req_header_read_done = false;
    bool previous_req_body_read_done = true;
    bool handshake_done = false;

    Context & global_context;
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    Poco::Timespan operation_timeout;
    Poco::Timespan session_timeout;
    int64_t session_id{-1};

    Stopwatch session_stopwatch;
    ThreadSafeResponseQueuePtr responses;

    Coordination::XID close_xid = Coordination::CLOSE_XID;
    Poco::Timestamp established;

    LastOpMultiVersion last_op;

    mutable std::mutex conn_stats_mutex;
    ConnectionStats conn_stats;
};

}
