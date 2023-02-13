/**
 * Copyright 2016-2023 ClickHouse, Inc.
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
#include <IO/Operators.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ServiceZooKeeperImpl.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif

#include <array>


namespace ProfileEvents
{
extern const Event ZooKeeperInit;
extern const Event ZooKeeperTransactions;
extern const Event ZooKeeperCreate;
extern const Event ZooKeeperRemove;
extern const Event ZooKeeperExists;
extern const Event ZooKeeperMulti;
extern const Event ZooKeeperGet;
extern const Event ZooKeeperSet;
extern const Event ZooKeeperList;
extern const Event ZooKeeperCheck;
extern const Event ZooKeeperClose;
extern const Event ZooKeeperWaitMicroseconds;
extern const Event ZooKeeperBytesSent;
extern const Event ZooKeeperBytesReceived;
extern const Event ZooKeeperWatchResponse;

extern const Event ServiceKeeperWaitMicroseconds;
extern const Event ServiceKeeperBytesSent;
extern const Event ServiceKeeperBytesReceived;
}

namespace CurrentMetrics
{
extern const Metric ZooKeeperRequest;
extern const Metric ZooKeeperWatch;
}


/** ZooKeeper wire protocol.

Debugging example:
strace -t -f -e trace=network -s1000 -x ./clickhouse-zookeeper-cli localhost:2181

All numbers are in network byte order (big endian). Sizes are 32 bit. Numbers are signed.

zxid - incremental transaction number at server side.
xid - unique request number at client side.

Client connects to one of the specified hosts.
Client sends:

int32_t sizeof_connect_req;   \x00\x00\x00\x2c (44 bytes)

struct connect_req
{
    int32_t protocolVersion;  \x00\x00\x00\x00 (Currently zero)
    int64_t lastZxidSeen;     \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t timeOut;          \x00\x00\x75\x30 (Session timeout in milliseconds: 30000)
    int64_t sessionId;        \x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
    int32_t passwd_len;       \x00\x00\x00\x10 (16)
    char passwd[16];          \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 (Zero at first connect)
};

Server replies:

struct prime_struct
{
    int32_t len;              \x00\x00\x00\x24 (36 bytes)
    int32_t protocolVersion;  \x00\x00\x00\x00
    int32_t timeOut;          \x00\x00\x75\x30
    int64_t sessionId;        \x01\x62\x2c\x3d\x82\x43\x00\x27
    int32_t passwd_len;       \x00\x00\x00\x10
    char passwd[16];          \x3b\x8c\xe0\xd4\x1f\x34\xbc\x88\x9c\xa7\x68\x69\x78\x64\x98\xe9
};

Client remembers session id and session password.


Client may send authentication request (optional).


Each one third of timeout, client sends heartbeat:

int32_t length_of_heartbeat_request \x00\x00\x00\x08 (8)
int32_t ping_xid                    \xff\xff\xff\xfe (-2, constant)
int32_t ping_op                     \x00\x00\x00\x0b ZOO_PING_OP 11

Server replies:

int32_t length_of_heartbeat_response \x00\x00\x00\x10
int32_t ping_xid                     \xff\xff\xff\xfe
int64 zxid                           \x00\x00\x00\x00\x00\x01\x87\x98 (incremental server generated number)
int32_t err                          \x00\x00\x00\x00


Client sends requests. For example, create persistent node '/hello' with value 'world'.

int32_t request_length \x00\x00\x00\x3a
int32_t xid            \x5a\xad\x72\x3f      Arbitrary number. Used for identification of requests/responses.
                                         libzookeeper uses unix timestamp for first xid and then autoincrement to that value.
int32_t op_num         \x00\x00\x00\x01      ZOO_CREATE_OP 1
int32_t path_length    \x00\x00\x00\x06
path                   \x2f\x68\x65\x6c\x6c\x6f  /hello
int32_t data_length    \x00\x00\x00\x05
data                   \x77\x6f\x72\x6c\x64      world
ACLs:
    int32_t num_acls   \x00\x00\x00\x01
    ACL:
        int32_t permissions \x00\x00\x00\x1f
        string scheme   \x00\x00\x00\x05
                        \x77\x6f\x72\x6c\x64      world
        string id       \x00\x00\x00\x06
                        \x61\x6e\x79\x6f\x6e\x65  anyone
int32_t flags           \x00\x00\x00\x00

Server replies:

int32_t response_length \x00\x00\x00\x1a
int32_t xid             \x5a\xad\x72\x3f
int64 zxid              \x00\x00\x00\x00\x00\x01\x87\x99
int32_t err             \x00\x00\x00\x00
string path_created     \x00\x00\x00\x06
                        \x2f\x68\x65\x6c\x6c\x6f  /hello - may differ to original path in case of sequential nodes.


Client may place a watch in their request.

For example, client sends "exists" request with watch:

request length \x00\x00\x00\x12
xid            \x5a\xae\xb2\x0d
op_num         \x00\x00\x00\x03
path           \x00\x00\x00\x05
               \x2f\x74\x65\x73\x74     /test
bool watch     \x01

Server will send response as usual.
And later, server may send special watch event.

struct WatcherEvent
{
    int32_t type;
    int32_t state;
    char * path;
};

response length    \x00\x00\x00\x21
special watch xid  \xff\xff\xff\xff
special watch zxid \xff\xff\xff\xff\xff\xff\xff\xff
err                \x00\x00\x00\x00
type               \x00\x00\x00\x02     DELETED_EVENT_DEF 2
state              \x00\x00\x00\x03     CONNECTED_STATE_DEF 3
path               \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74  /test


Example of multi request:

request length     \x00\x00\x00\x82 130
xid                \x5a\xae\xd6\x16
op_num             \x00\x00\x00\x0e 14

for every command:

    int32_t type;  \x00\x00\x00\x01 create
    bool done;     \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74 /test
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x31 multi1
    acl            \x00\x00\x00\x01
                   \x00\x00\x00\x1f
                   \x00\x00\x00\x05
                   \x77\x6f\x72\x6c\x64     world
                   \x00\x00\x00\x06
                   \x61\x6e\x79\x6f\x6e\x65 anyone
    flags          \x00\x00\x00\x00

    int32_t type;  \x00\x00\x00\x05 set
    bool done      \x00 false
    int32_t err;   \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    data           \x00\x00\x00\x06
                   \x6d\x75\x6c\x74\x69\x32 multi2
    version        \xff\xff\xff\xff

    int32_t type   \x00\x00\x00\x02 remove
    bool done      \x00
    int32_t err    \xff\xff\xff\xff -1

    path           \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74
    version        \xff\xff\xff\xff

after commands:

    int32_t type   \xff\xff\xff\xff -1
    bool done      \x01 true
    int32_t err    \xff\xff\xff\xff

Example of multi response:

response length    \x00\x00\x00\x81 129
xid                \x5a\xae\xd6\x16
zxid               \x00\x00\x00\x00\x00\x01\x87\xe1
err                \x00\x00\x00\x00

in a loop:

    type           \x00\x00\x00\x01 create
    done           \x00
    err            \x00\x00\x00\x00

    path_created   \x00\x00\x00\x05
                   \x2f\x74\x65\x73\x74

    type           \x00\x00\x00\x05 set
    done           \x00
    err            \x00\x00\x00\x00

    stat           \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x00\x00\x00\x01\x87\xe1
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x01\x62\x3a\xf4\x35\x0c
                   \x00\x00\x00\x01
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x00\x00\x00
                   \x00\x00\x00\x06
                   \x00\x00\x00\x00
                   \x00\x00\x00\x00\x00\x01\x87\xe1

    type           \x00\x00\x00\x02 remove
    done           \x00
    err            \x00\x00\x00\x00

after:

    type           \xff\xff\xff\xff
    done           \x01
    err            \xff\xff\xff\xff

  */


namespace Coordination
{

using namespace RK;

template <typename T>
void ServiceZooKeeper::zkWrite(const T & x)
{
    Coordination::write(x, *zk_out);
}

template <typename T>
void ServiceZooKeeper::zkRead(T & x)
{
    Coordination::read(x, *zk_in);
}

template <typename T>
void ServiceZooKeeper::serWrite(const T & x)
{
    Coordination::write(x, *ser_out);
}

template <typename T>
void ServiceZooKeeper::serRead(T & x)
{
    Coordination::read(x, *ser_in);
}

static void removeRootPath(String & path, const String & root_path)
{
    if (root_path.empty())
        return;

    if (path.size() <= root_path.size())
        throw Exception("Received path is not longer than root_path", Error::ZDATAINCONSISTENCY);

    path = path.substr(root_path.size());
}

ServiceZooKeeper::~ServiceZooKeeper()
{
    try
    {
        finalize();

        if (zk_send_thread.joinable())
            zk_send_thread.join();

        if (zk_receive_thread.joinable())
            zk_receive_thread.join();

        if (ser_send_thread.joinable())
            ser_send_thread.join();

        if (ser_receive_thread.joinable())
            ser_receive_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


ServiceZooKeeper::ServiceZooKeeper(
    const Nodes & nodes,
    const Nodes & service_nodes,
    bool use_ch_service_,
    const String & root_path_,
    const String & auth_scheme,
    const String & auth_data,
    Poco::Timespan session_timeout_,
    Poco::Timespan connection_timeout,
    Poco::Timespan operation_timeout_)
    : log(&(Poco::Logger::get("ServiceZooKeeper"))),
      root_path(root_path_),
      session_timeout(session_timeout_),
      operation_timeout(std::min(operation_timeout_, session_timeout_))
{
    if (!root_path.empty())
    {
        if (root_path.back() == '/')
            root_path.pop_back();
    }

    if (auth_scheme.empty())
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        default_acls.emplace_back(std::move(acl));
    }
    else
    {
        ACL acl;
        acl.permissions = ACL::All;
        acl.scheme = "auth";
        acl.id = "";
        default_acls.emplace_back(std::move(acl));
    }

    use_ch_service = use_ch_service_;

    zkConnect(nodes, connection_timeout);
    serConnect(service_nodes, connection_timeout);

    if (!auth_scheme.empty())
        sendAuth(auth_scheme, auth_data);

    zk_send_thread = ThreadFromGlobalPool([this] { zkSendThread(); });
    zk_receive_thread = ThreadFromGlobalPool([this] { zkReceiveThread(); });

    ser_send_thread = ThreadFromGlobalPool([this] { serSendThread(); });
    ser_receive_thread = ThreadFromGlobalPool([this] { serReceiveThread(); });

    ProfileEvents::increment(ProfileEvents::ZooKeeperInit);
}


void ServiceZooKeeper::zkConnect(
    const Nodes & nodes,
    Poco::Timespan connection_timeout)
{
    if (nodes.empty())
        throw Exception("No nodes passed to ZooKeeper constructor", Error::ZBADARGUMENTS);

    static constexpr size_t num_tries = 3;
    bool connected = false;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & node : nodes)
        {
            try
            {
                /// Reset the state of previous attempt.
                if (node.secure)
                {
#if USE_SSL
                    zk_socket = Poco::Net::SecureStreamSocket();
#else
                    throw Poco::Exception(
                        "Communication with ZooKeeper over SSL is disabled because poco library was built without NetSSL support.");
#endif
                }
                else
                {
                    zk_socket = Poco::Net::StreamSocket();
                }

                zk_socket.connect(node.address, connection_timeout);

                zk_socket.setReceiveTimeout(operation_timeout);
                zk_socket.setSendTimeout(operation_timeout);
                zk_socket.setNoDelay(true);

                zk_in.emplace(zk_socket);
                zk_out.emplace(zk_socket);

                try
                {
                    zkSendHandshake();
                }
                catch (RK::Exception & e)
                {
                    e.addMessage("while sending handshake to ZooKeeper");
                    throw;
                }

                try
                {
                    zkReceiveHandshake();
                }
                catch (RK::Exception & e)
                {
                    e.addMessage("while receiving handshake from ZooKeeper");
                    throw;
                }

                connected = true;
                break;
            }
            catch (...)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false) << ", " << node.address.toString();
            }
        }

        if (connected)
            break;
    }

    if (!connected)
    {
        WriteBufferFromOwnString message;
        message << "All connection tries failed while connecting to ZooKeeper. nodes: ";
        bool first = true;
        for (const auto & node : nodes)
        {
            if (first)
                first = false;
            else
                message << ", ";

            if (node.secure)
                message << "secure://";

            message << node.address.toString();
        }

        message << fail_reasons.str() << "\n";
        throw Exception(message.str(), Error::ZCONNECTIONLOSS);
    }
}


void ServiceZooKeeper::serConnect(
    const Nodes & nodes,
    Poco::Timespan connection_timeout)
{
    if (nodes.empty())
        throw Exception("No nodes passed to ZooKeeper constructor", Error::ZBADARGUMENTS);

    static constexpr size_t num_tries = 3;
    bool connected = false;

    WriteBufferFromOwnString fail_reasons;
    for (size_t try_no = 0; try_no < num_tries; ++try_no)
    {
        for (const auto & node : nodes)
        {
            try
            {
                /// Reset the state of previous attempt.
                if (node.secure)
                {
#if USE_SSL
                    ser_socket = Poco::Net::SecureStreamSocket();
#else
                    throw Poco::Exception(
                        "Communication with ZooKeeper over SSL is disabled because poco library was built without NetSSL support.");
#endif
                }
                else
                {
                    ser_socket = Poco::Net::StreamSocket();
                }

                ser_socket.connect(node.address, connection_timeout);

                ser_socket.setReceiveTimeout(operation_timeout);
                ser_socket.setSendTimeout(operation_timeout);
                ser_socket.setNoDelay(true);

                ser_in.emplace(ser_socket);
                ser_out.emplace(ser_socket);

                try
                {
                    serSendHandshake();
                }
                catch (RK::Exception & e)
                {
                    e.addMessage("while sending handshake to ZooKeeper");
                    throw;
                }

                try
                {
                    serReceiveHandshake();
                }
                catch (RK::Exception & e)
                {
                    e.addMessage("while receiving handshake from ZooKeeper");
                    throw;
                }

                connected = true;
                break;
            }
            catch (...)
            {
                fail_reasons << "\n" << getCurrentExceptionMessage(false) << ", " << node.address.toString();
            }
        }

        if (connected)
            break;
    }

    if (!connected)
    {
        WriteBufferFromOwnString message;
        message << "All connection tries failed while connecting to ZooKeeper. nodes: ";
        bool first = true;
        for (const auto & node : nodes)
        {
            if (first)
                first = false;
            else
                message << ", ";

            if (node.secure)
                message << "secure://";

            message << node.address.toString();
        }

        message << fail_reasons.str() << "\n";
        throw Exception(message.str(), Error::ZCONNECTIONLOSS);
    }
}

void ServiceZooKeeper::zkSendHandshake()
{
    int32_t handshake_length = 44;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    zkWrite(handshake_length);
    zkWrite(ZOOKEEPER_PROTOCOL_VERSION);
    zkWrite(last_zxid_seen);
    zkWrite(timeout);
    zkWrite(previous_session_id);
    zkWrite(passwd);

    zk_out->next();
}


void ServiceZooKeeper::zkReceiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    std::array<char, PASSWORD_LENGTH> passwd;

    zkRead(handshake_length);
    if (handshake_length != SERVER_HANDSHAKE_LENGTH)
        throw Exception("Unexpected handshake length received: " + RK::toString(handshake_length), Error::ZMARSHALLINGERROR);

    zkRead(protocol_version_read);
    if (protocol_version_read != ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + RK::toString(protocol_version_read), Error::ZMARSHALLINGERROR);

    zkRead(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        /// Use timeout from server.
        session_timeout = timeout * Poco::Timespan::MILLISECONDS;

    zkRead(zk_session_id);
    zkRead(passwd);
}

void ServiceZooKeeper::serSendHandshake()
{
    int32_t handshake_length = 44;
    int64_t last_zxid_seen = 0;
    int32_t timeout = session_timeout.totalMilliseconds();
    int64_t previous_session_id = 0;    /// We don't support session restore. So previous session_id is always zero.
    constexpr int32_t passwd_len = 16;
    std::array<char, passwd_len> passwd {};

    serWrite(handshake_length);
    serWrite(ZOOKEEPER_PROTOCOL_VERSION);
    serWrite(last_zxid_seen);
    serWrite(timeout);
    serWrite(previous_session_id);
    serWrite(passwd);

    ser_out->next();
}


void ServiceZooKeeper::serReceiveHandshake()
{
    int32_t handshake_length;
    int32_t protocol_version_read;
    int32_t timeout;
    std::array<char, PASSWORD_LENGTH> passwd;

    serRead(handshake_length);
    if (handshake_length != SERVER_HANDSHAKE_LENGTH)
        throw Exception("Unexpected handshake length received: " + RK::toString(handshake_length), Error::ZMARSHALLINGERROR);

    serRead(protocol_version_read);
    if (protocol_version_read != ZOOKEEPER_PROTOCOL_VERSION)
        throw Exception("Unexpected protocol version: " + RK::toString(protocol_version_read), Error::ZMARSHALLINGERROR);

    serRead(timeout);
    if (timeout != session_timeout.totalMilliseconds())
        /// Use timeout from server.
        session_timeout = timeout * Poco::Timespan::MILLISECONDS;

    serRead(ser_session_id);
    serRead(passwd);
}


void ServiceZooKeeper::sendAuth(const String & scheme, const String & data)
{
    ZooKeeperAuthRequest request;
    request.scheme = scheme;
    request.data = data;
    request.xid = AUTH_XID;
    request.write(*zk_out);

    int32_t length;
    XID read_xid;
    int64_t zxid;
    Error err;

    zkRead(length);
    size_t count_before_event = zk_in->count();
    zkRead(read_xid);
    zkRead(zxid);
    zkRead(err);

    if (read_xid != AUTH_XID)
        throw Exception("Unexpected event received in reply to auth request: " + RK::toString(read_xid),
                        Error::ZMARSHALLINGERROR);

    int32_t actual_length = zk_in->count() - count_before_event;
    if (length != actual_length)
        throw Exception("Response length doesn't match. Expected: " + RK::toString(length) + ", actual: " + RK::toString(actual_length),
                        Error::ZMARSHALLINGERROR);

    if (err != Error::ZOK)
        throw Exception("Error received in reply to auth request. Code: " + RK::toString(int32_t(err)) + ". Message: " + String(errorMessage(err)),
                        Error::ZMARSHALLINGERROR);
}


void ServiceZooKeeper::zkSendThread()
{
    setThreadName("ZooKeeperSend");

    auto prev_heartbeat_time = clock::now();

    try
    {
        while (!zk_expired)
        {
            auto prev_bytes_sent = zk_out->count();

            auto now = clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    UInt64(std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count()),
                    UInt64(operation_timeout.totalMilliseconds()));

                RequestInfo info;
                if (zk_requests_queue.tryPop(info, max_wait))
                {
                    /// After we popped element from the queue, we must register callbacks (even in the case when expired == true right now),
                    ///  because they must not be lost (callbacks must be called because the user will wait for them).

                    if (info.request->xid != CLOSE_XID)
                    {
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperRequest);
                        std::lock_guard lock(zk_operations_mutex);
                        zk_operations[info.request->xid] = info;
                    }

                    if (info.watch)
                    {
                        info.request->has_watch = true;
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperWatch);
                    }

                    if (zk_expired)
                    {
                        break;
                    }

                    info.request->addRootPath(root_path);

                    info.request->probably_sent = true;
                    info.request->write(*zk_out);

                    /// We sent close request, exit
                    if (info.request->xid == CLOSE_XID)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                ZooKeeperHeartbeatRequest request;
                request.xid = PING_XID;
                request.write(*zk_out);
            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesSent, zk_out->count() - prev_bytes_sent);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        zkFinalize(true, false);
    }
}


void ServiceZooKeeper::serSendThread()
{
    setThreadName("SerKeeperSend");

    auto prev_heartbeat_time = clock::now();

    try
    {
        while (!ser_expired)
        {
            auto prev_bytes_sent = ser_out->count();

            auto now = clock::now();
            auto next_heartbeat_time = prev_heartbeat_time + std::chrono::milliseconds(session_timeout.totalMilliseconds() / 3);

            if (next_heartbeat_time > now)
            {
                /// Wait for the next request in queue. No more than operation timeout. No more than until next heartbeat time.
                UInt64 max_wait = std::min(
                    UInt64(std::chrono::duration_cast<std::chrono::milliseconds>(next_heartbeat_time - now).count()),
                    UInt64(operation_timeout.totalMilliseconds()));

                RequestInfo info;
                if (ser_requests_queue.tryPop(info, max_wait))
                {
                    /// After we popped element from the queue, we must register callbacks (even in the case when expired == true right now),
                    ///  because they must not be lost (callbacks must be called because the user will wait for them).

                    if (info.request->xid != CLOSE_XID)
                    {
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperRequest);
                        std::lock_guard lock(ser_operations_mutex);
                        ser_operations[info.request->xid] = info;
                    }

                    if (info.watch)
                    {
                        info.request->has_watch = true;
                        CurrentMetrics::add(CurrentMetrics::ZooKeeperWatch);
                    }

                    if (ser_expired)
                    {
                        break;
                    }

                    info.request->addRootPath(root_path);

                    info.request->probably_sent = true;
                    info.request->write(*ser_out);

                    /// We sent close request, exit
                    if (info.request->xid == CLOSE_XID)
                        break;
                }
            }
            else
            {
                /// Send heartbeat.
                prev_heartbeat_time = clock::now();

                ZooKeeperHeartbeatRequest request;
                request.xid = PING_XID;
                request.write(*ser_out);
            }

            ProfileEvents::increment(ProfileEvents::ServiceKeeperBytesSent, ser_out->count() - prev_bytes_sent);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        serFinalize(true, false);
    }
}


void ServiceZooKeeper::zkReceiveThread()
{
    setThreadName("ZooKeeperRecv");

    try
    {
        Int64 waited = 0;
        while (!zk_expired)
        {
            auto prev_bytes_received = zk_in->count();

            clock::time_point now = clock::now();
            UInt64 max_wait = operation_timeout.totalMicroseconds();
            std::optional<RequestInfo> earliest_operation;

            {
                std::lock_guard lock(zk_operations_mutex);
                if (!zk_operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    earliest_operation = zk_operations.begin()->second;
                    auto earliest_operation_deadline = earliest_operation->time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                    if (now > earliest_operation_deadline)
                        throw Exception("Operation timeout (deadline already expired) for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                    max_wait = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (zk_in->poll(max_wait))
            {
                if (zk_expired)
                    break;

                zkReceiveEvent();
                waited = 0;
            }
            else
            {
                if (earliest_operation)
                {
                    throw Exception("Operation timeout (no response) for request " + toString(earliest_operation->request->getOpNum()) + " for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                }
                waited += max_wait;
                if (waited >= session_timeout.totalMicroseconds())
                    throw Exception("Nothing is received in session timeout", Error::ZOPERATIONTIMEOUT);

            }

            ProfileEvents::increment(ProfileEvents::ZooKeeperBytesReceived, zk_in->count() - prev_bytes_received);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        zkFinalize(false, true);
    }
}


void ServiceZooKeeper::serReceiveThread()
{
    setThreadName("SerKeeperRecv");

    try
    {
        Int64 waited = 0;
        while (!ser_expired)
        {
            auto prev_bytes_received = zk_in->count();

            clock::time_point now = clock::now();
            UInt64 max_wait = operation_timeout.totalMicroseconds();
            std::optional<RequestInfo> earliest_operation;

            {
                std::lock_guard lock(ser_operations_mutex);
                if (!ser_operations.empty())
                {
                    /// Operations are ordered by xid (and consequently, by time).
                    earliest_operation = ser_operations.begin()->second;
                    auto earliest_operation_deadline = earliest_operation->time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                    if (now > earliest_operation_deadline)
                        throw Exception("Operation timeout (deadline already expired) for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                    max_wait = std::chrono::duration_cast<std::chrono::microseconds>(earliest_operation_deadline - now).count();
                }
            }

            if (ser_in->poll(max_wait))
            {
                if (ser_expired)
                    break;

                serReceiveEvent();
                waited = 0;
            }
            else
            {
                if (earliest_operation)
                {
                    throw Exception("Operation timeout (no response) for request " + toString(earliest_operation->request->getOpNum()) + " for path: " + earliest_operation->request->getPath(), Error::ZOPERATIONTIMEOUT);
                }
                waited += max_wait;
                if (waited >= session_timeout.totalMicroseconds())
                    throw Exception("Nothing is received in session timeout", Error::ZOPERATIONTIMEOUT);

            }

            ProfileEvents::increment(ProfileEvents::ServiceKeeperBytesReceived, ser_in->count() - prev_bytes_received);
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        serFinalize(false, true);
    }
}


void ServiceZooKeeper::zkReceiveEvent()
{
    int32_t length;
    XID xid;
    int64_t zxid;
    Error err;

    zkRead(length);
    size_t count_before_event = zk_in->count();
    zkRead(xid);
    zkRead(zxid);
    zkRead(err);

    RequestInfo request_info;
    ZooKeeperResponsePtr response;

    if (xid == CLOSE_XID)
    {
        throw; // finalize
    }
    if (xid == PING_XID)
    {
        if (err != Error::ZOK)
            throw Exception("Received error in heartbeat response: " + String(errorMessage(err)), Error::ZRUNTIMEINCONSISTENCY);

        response = std::make_shared<ZooKeeperHeartbeatResponse>();
    }
    else if (xid == WATCH_XID)
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperWatchResponse);
        response = std::make_shared<ZooKeeperWatchResponse>();

        request_info.callback = [this](const Response & response_)
        {
            const WatchResponse & watch_response = dynamic_cast<const WatchResponse &>(response_);

            std::lock_guard lock(zk_watches_mutex);

            auto it = zk_watches.find(watch_response.path);
            if (it == zk_watches.end())
            {
                /// This is Ok.
                /// Because watches are identified by path.
                /// And there may exist many watches for single path.
                /// And watch is added to the list of watches on client side
                ///  slightly before than it is registered by the server.
                /// And that's why new watch may be already fired by old event,
                ///  but then the server will actually register new watch
                ///  and will send event again later.
            }
            else
            {
                for (auto & callback : it->second)
                    if (callback && !use_ch_service)
                        callback(watch_response);   /// NOTE We may process callbacks not under mutex.

                CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, it->second.size());
                zk_watches.erase(it);
            }
        };
    }
    else
    {
        {
            std::lock_guard lock(zk_operations_mutex);

            auto it = zk_operations.find(xid);
            if (it == zk_operations.end())
                throw Exception("Received response for unknown xid " + RK::toString(xid), Error::ZRUNTIMEINCONSISTENCY);

            /// After this point, we must invoke callback, that we've grabbed from 'operations'.
            /// Invariant: all callbacks are invoked either in case of success or in case of error.
            /// (all callbacks in 'operations' are guaranteed to be invoked)

            request_info = std::move(it->second);
            zk_operations.erase(it);
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest);
        }

        auto elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();
        ProfileEvents::increment(ProfileEvents::ZooKeeperWaitMicroseconds, elapsed_microseconds);
    }

    auto request_info_callback = [&, this]()
    {
        if (request_info.callback)
        {
            if (xid == WATCH_XID || !use_ch_service)
            {
                request_info.callback(*response);
            }
        }
    };

    try
    {
        if (!response)
            response = request_info.request->makeResponse();

        if (err != Error::ZOK)
        {
            response->error = err;
        }
        else
        {
            response->readImpl(*zk_in);
            response->removeRootPath(root_path);
        }
        /// Instead of setting the watch in sendEvent, set it in receiveEvent because need to check the response.
        /// The watch shouldn't be set if the node does not exist and it will never exist like sequential ephemeral nodes.
        /// By using getData() instead of exists(), a watch won't be set if the node doesn't exist.
        if (request_info.watch)
        {
            bool add_watch = false;
            /// 3 indicates the ZooKeeperExistsRequest.
            // For exists, we set the watch on both node exist and nonexist case.
            // For other case like getData, we only set the watch when node exists.
            if (request_info.request->getOpNum() == OpNum::Exists)
                add_watch = (response->error == Error::ZOK || response->error == Error::ZNONODE);
            else
                add_watch = response->error == Error::ZOK;

            if (add_watch)
            {
                /// The key of wathces should exclude the root_path
                String req_path = request_info.request->getPath();
                removeRootPath(req_path, root_path);
                std::lock_guard lock(zk_watches_mutex);
                zk_watches[req_path].emplace_back(std::move(request_info.watch));
            }
        }

        int32_t actual_length = zk_in->count() - count_before_event;
        if (length != actual_length)
            throw Exception("Response length doesn't match. Expected: " + RK::toString(length) + ", actual: " + RK::toString(actual_length), Error::ZMARSHALLINGERROR);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// Unrecoverable. Don't leave incorrect state in memory.
        if (!response)
            std::terminate();

        /// In case we cannot read the response, we should indicate it as the error of that type
        ///  when the user cannot assume whether the request was processed or not.
        response->error = Error::ZCONNECTIONLOSS;
        request_info_callback();

        throw;
    }

    /// Exception in callback will propagate to receiveThread and will lead to session expiration. This is Ok.

    request_info_callback();
//
//    if (request_info.callback && use_ch_service)
//        request_info.callback(*response);

//    compResponse(xid, response);
    if (xid == WATCH_XID || xid == PING_XID || xid == AUTH_XID || xid == CLOSE_XID)
        return;

    std::lock_guard lock(responses_mutex);
    auto it = ser_zk_responses.find(xid);
    if (it != ser_zk_responses.end())
    {
        if (it->second)
        {
            if (*ser_zk_responses.find(xid)->second != *response)
            {
                LOG_ERROR(
                    log,
                    "XID {}, OpNum {}, request {}, zk response is not same service response.",
                    xid,
                    Coordination::toString(request_info.request->getOpNum()),
                    request_info.request->toString());
                LOG_ERROR(log, "XID {}, ser response is {}.", xid, ser_zk_responses.find(xid)->second->toString());
                LOG_ERROR(log, "XID {}, zk response is {}.", xid, response->toString());
            }
            ser_zk_responses.erase(xid);
        }
        else
        {
            ser_zk_responses.find(xid)->second = response;
        }
    }
    else
    {
        LOG_ERROR(log, "XID {}, has been erase from ser_zk_responses or it has not been push to ser_zk_responses.", xid);
    }
}


void ServiceZooKeeper::serReceiveEvent()
{
    int32_t length;
    XID xid;
    int64_t zxid;
    Error err;

    serRead(length);
    size_t count_before_event = ser_in->count();
    serRead(xid);
    serRead(zxid);
    serRead(err);

    RequestInfo request_info;
    ZooKeeperResponsePtr response;

    if (xid == PING_XID)
    {
        if (err != Error::ZOK)
            throw Exception("Received error in heartbeat response: " + String(errorMessage(err)), Error::ZRUNTIMEINCONSISTENCY);

        response = std::make_shared<ZooKeeperHeartbeatResponse>();
    }
    else if (xid == WATCH_XID)
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperWatchResponse);
        response = std::make_shared<ZooKeeperWatchResponse>();

        request_info.callback = [this](const Response & response_)
        {
            const WatchResponse & watch_response = dynamic_cast<const WatchResponse &>(response_);

            std::lock_guard lock(ser_watches_mutex);

            auto it = ser_watches.find(watch_response.path);
            if (it == ser_watches.end())
            {
                /// This is Ok.
                /// Because watches are identified by path.
                /// And there may exist many watches for single path.
                /// And watch is added to the list of watches on client side
                ///  slightly before than it is registered by the server.
                /// And that's why new watch may be already fired by old event,
                ///  but then the server will actually register new watch
                ///  and will send event again later.
            }
            else
            {
                for (auto & callback : it->second)
                    if (callback && use_ch_service)
                        callback(watch_response);   /// NOTE We may process callbacks not under mutex.

                CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, it->second.size());
                ser_watches.erase(it);
            }
        };
    }
    else
    {
        {
            std::lock_guard lock(ser_operations_mutex);

            auto it = ser_operations.find(xid);
            if (it == ser_operations.end())
                throw Exception("Received response for unknown xid " + RK::toString(xid), Error::ZRUNTIMEINCONSISTENCY);

            /// After this point, we must invoke callback, that we've grabbed from 'operations'.
            /// Invariant: all callbacks are invoked either in case of success or in case of error.
            /// (all callbacks in 'operations' are guaranteed to be invoked)

            request_info = std::move(it->second);
            ser_operations.erase(it);
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest);
        }

        auto elapsed_microseconds = std::chrono::duration_cast<std::chrono::microseconds>(clock::now() - request_info.time).count();
        ProfileEvents::increment(ProfileEvents::ServiceKeeperWaitMicroseconds, elapsed_microseconds);
    }

    auto request_info_callback = [&, this]()
    {
        if (request_info.callback)
        {
            if (xid == WATCH_XID || use_ch_service)
            {
                request_info.callback(*response);
            }
        }
    };

    try
    {
        if (!response)
            response = request_info.request->makeResponse();

        if (err != Error::ZOK)
        {
            response->error = err;
        }
        else
        {
            response->readImpl(*ser_in);
            response->removeRootPath(root_path);
        }
        /// Instead of setting the watch in sendEvent, set it in receiveEvent because need to check the response.
        /// The watch shouldn't be set if the node does not exist and it will never exist like sequential ephemeral nodes.
        /// By using getData() instead of exists(), a watch won't be set if the node doesn't exist.
        if (request_info.watch)
        {
            bool add_watch = false;
            /// 3 indicates the ZooKeeperExistsRequest.
            // For exists, we set the watch on both node exist and nonexist case.
            // For other case like getData, we only set the watch when node exists.
            if (request_info.request->getOpNum() == OpNum::Exists)
                add_watch = (response->error == Error::ZOK || response->error == Error::ZNONODE);
            else
                add_watch = response->error == Error::ZOK;

            if (add_watch)
            {
                /// The key of wathces should exclude the root_path
                String req_path = request_info.request->getPath();
                removeRootPath(req_path, root_path);
                std::lock_guard lock(ser_watches_mutex);
                ser_watches[req_path].emplace_back(std::move(request_info.watch));
            }
        }

        int32_t actual_length = ser_in->count() - count_before_event;
        if (length != actual_length)
            throw Exception("Response length doesn't match. Expected: " + RK::toString(length) + ", actual: " + RK::toString(actual_length), Error::ZMARSHALLINGERROR);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);

        /// Unrecoverable. Don't leave incorrect state in memory.
        if (!response)
            std::terminate();

        /// In case we cannot read the response, we should indicate it as the error of that type
        ///  when the user cannot assume whether the request was processed or not.
        response->error = Error::ZCONNECTIONLOSS;
        request_info_callback();

        throw;
    }

    /// Exception in callback will propagate to receiveThread and will lead to session expiration. This is Ok.
    request_info_callback();
//
//    if (request_info.callback && use_ch_service)
//        request_info.callback(*response);

//    compResponse(xid, request_info.request, response);
    /// lock
    // static constexpr XID WATCH_XID = -1;
    //static constexpr XID PING_XID  = -2;
    //static constexpr XID AUTH_XID  = -4;
    //static constexpr XID CLOSE_XID = 0x7FFFFFFF;
    if (xid == WATCH_XID || xid == PING_XID || xid == AUTH_XID || xid == CLOSE_XID)
        return;

    std::lock_guard lock(responses_mutex);
    auto it = ser_zk_responses.find(xid);
    if (it != ser_zk_responses.end())
    {
        if (it->second)
        {
            if (*ser_zk_responses.find(xid)->second != *response)
            {
                LOG_ERROR(
                    log,
                    "XID {}, OpNum {}, request {}, zk response is not same service response.",
                    xid,
                    Coordination::toString(request_info.request->getOpNum()),
                    request_info.request->toString());
                LOG_ERROR(log, "XID {} zk response is {}.", xid, ser_zk_responses.find(xid)->second->toString());
                LOG_ERROR(log, "XID {} ser response is {}.", xid, response->toString());
            }
            ser_zk_responses.erase(xid);
        }
        else
        {
            ser_zk_responses.find(xid)->second = response;
        }
    }
    else
    {
        LOG_ERROR(log, "XID {}, has been erase from ser_zk_responses or it has not been push to ser_zk_responses.", xid);
    }
}


void ServiceZooKeeper::finalize()
{
    zkFinalize(false, false);
    serFinalize(false, false);
    ser_zk_responses.clear();
}



void ServiceZooKeeper::zkFinalize(bool error_send, bool error_receive)
{
    /// If some thread (send/receive) already finalizing session don't try to do it
    if (zk_finalization_started.exchange(true))
        return;

    auto expire_session_if_not_expired = [&]
    {
        std::lock_guard lock(zk_push_request_mutex);
        if (!zk_expired)
        {
            zk_expired = true;
            if (ser_expired)
                active_session_metric_increment.destroy();
        }
    };

    try
    {
        if (!error_send)
        {
            /// Send close event. This also signals sending thread to stop.
            try
            {
                zkClose();
            }
            catch (...)
            {
                /// This happens for example, when "Cannot push request to queue within operation timeout".
                /// Just mark session expired in case of error on close request, otherwise sendThread may not stop.
                expire_session_if_not_expired();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            /// Send thread will exit after sending close request or on expired flag
            zk_send_thread.join();
        }

        /// Set expired flag after we sent close event
        expire_session_if_not_expired();

        try
        {
            /// This will also wakeup the receiving thread.
            zk_socket.shutdown();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (!error_receive)
        {
            zk_receive_thread.join();
        }

        {
            std::lock_guard lock(zk_operations_mutex);
            if (!use_ch_service)
            {
                for (auto & op : zk_operations)
                {
                    RequestInfo & request_info = op.second;
                    ResponsePtr response = request_info.request->makeResponse();

                    response->error = request_info.request->probably_sent
                                      ? Error::ZCONNECTIONLOSS
                                      : Error::ZSESSIONEXPIRED;

                    if (request_info.callback)
                    {
                        try
                        {
                            request_info.callback(*response);
                        }
                        catch (...)
                        {
                            /// We must continue to all other callbacks, because the user is waiting for them.
                            tryLogCurrentException(__PRETTY_FUNCTION__);
                        }
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest, zk_operations.size());
            zk_operations.clear();
        }

        {
            std::lock_guard lock(zk_watches_mutex);
            if (!use_ch_service)
            {
                for (auto & path_watches : zk_watches)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = Error::ZSESSIONEXPIRED;

                    for (auto & callback : path_watches.second)
                    {
                        if (callback)
                        {
                            try
                            {
                                callback(response);
                            }
                            catch (...)
                            {
                                tryLogCurrentException(__PRETTY_FUNCTION__);
                            }
                        }
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, zk_watches.size());
            zk_watches.clear();
        }

        /// Drain queue
        RequestInfo info;
        while (zk_requests_queue.tryPop(info))
        {
            if (use_ch_service)
                continue;

            if (info.callback)
            {
                ResponsePtr response = info.request->makeResponse();
                if (response)
                {
                    response->error = Error::ZSESSIONEXPIRED;
                    try
                    {
                        info.callback(*response);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            }
            if (info.watch)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = Error::ZSESSIONEXPIRED;
                try
                {
                    info.watch(response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void ServiceZooKeeper::serFinalize(bool error_send, bool error_receive)
{
    /// If some thread (send/receive) already finalizing session don't try to do it
    if (ser_finalization_started.exchange(true))
        return;

    auto expire_session_if_not_expired = [&]
    {
        std::lock_guard lock(ser_push_request_mutex);
        if (!ser_expired)
        {
            ser_expired = true;
            if (zk_expired)
                active_session_metric_increment.destroy();
        }
    };

    try
    {
        if (!error_send)
        {
            /// Send close event. This also signals sending thread to stop.
            try
            {
                serClose();
            }
            catch (...)
            {
                /// This happens for example, when "Cannot push request to queue within operation timeout".
                /// Just mark session expired in case of error on close request, otherwise sendThread may not stop.
                expire_session_if_not_expired();
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }

            /// Send thread will exit after sending close request or on expired flag
            ser_send_thread.join();
        }

        /// Set expired flag after we sent close event
        expire_session_if_not_expired();

        try
        {
            /// This will also wakeup the receiving thread.
            ser_socket.shutdown();
        }
        catch (...)
        {
            /// We must continue to execute all callbacks, because the user is waiting for them.
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }

        if (!error_receive)
        {
            ser_receive_thread.join();
        }

        {
            std::lock_guard lock(ser_operations_mutex);
            if (use_ch_service)
            {
                for (auto & op : ser_operations)
                {
                    RequestInfo & request_info = op.second;
                    ResponsePtr response = request_info.request->makeResponse();

                    response->error = request_info.request->probably_sent
                                      ? Error::ZCONNECTIONLOSS
                                      : Error::ZSESSIONEXPIRED;

                    if (request_info.callback)
                    {
                        try
                        {
                            request_info.callback(*response);
                        }
                        catch (...)
                        {
                            /// We must continue to all other callbacks, because the user is waiting for them.
                            tryLogCurrentException(__PRETTY_FUNCTION__);
                        }
                    }
                }
            }

            CurrentMetrics::sub(CurrentMetrics::ZooKeeperRequest, ser_operations.size());
            ser_operations.clear();
        }

        {
            std::lock_guard lock(ser_watches_mutex);
            if (use_ch_service)
            {
                for (auto & path_watches : ser_watches)
                {
                    WatchResponse response;
                    response.type = SESSION;
                    response.state = EXPIRED_SESSION;
                    response.error = Error::ZSESSIONEXPIRED;

                    for (auto & callback : path_watches.second)
                    {
                        if (callback)
                        {
                            try
                            {
                                callback(response);
                            }
                            catch (...)
                            {
                                tryLogCurrentException(__PRETTY_FUNCTION__);
                            }
                        }
                    }
                }
            }
            CurrentMetrics::sub(CurrentMetrics::ZooKeeperWatch, ser_watches.size());
            ser_watches.clear();
        }

        /// Drain queue
        RequestInfo info;
        while (ser_requests_queue.tryPop(info))
        {
            if (!use_ch_service)
                continue;

            if (info.callback)
            {
                ResponsePtr response = info.request->makeResponse();
                if (response)
                {
                    response->error = Error::ZSESSIONEXPIRED;
                    try
                    {
                        info.callback(*response);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(__PRETTY_FUNCTION__);
                    }
                }
            }
            if (info.watch)
            {
                WatchResponse response;
                response.type = SESSION;
                response.state = EXPIRED_SESSION;
                response.error = Error::ZSESSIONEXPIRED;
                try
                {
                    info.watch(response);
                }
                catch (...)
                {
                    tryLogCurrentException(__PRETTY_FUNCTION__);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void ServiceZooKeeper::pushRequest(RequestInfo && info)
{
    auto undo = [this](XID xid)
    {
        std::lock_guard lock(responses_mutex);
        if (ser_zk_responses.find(xid) != ser_zk_responses.end())
            ser_zk_responses.erase(xid);
    };

    try
    {
        info.time = clock::now();

        if (!info.request->xid)
        {
            info.request->xid = next_xid.fetch_add(1);
            if (info.request->xid == CLOSE_XID)
                throw Exception("xid equal to close_xid", Error::ZSESSIONEXPIRED);
            if (info.request->xid < 0)
                throw Exception("XID overflow", Error::ZSESSIONEXPIRED);
        }

        {
            std::lock_guard lock(responses_mutex);
            ser_zk_responses.emplace(info.request->xid, nullptr);
        }

        bool zk_push_result = zkPushRequest(std::forward<RequestInfo>(info));
        bool ser_push_result = serPushRequest(std::forward<RequestInfo>(info));
        if (!zk_push_result || !ser_push_result)
        {
            undo(info.request->xid);
        }
    }
    catch (...)
    {
        undo(info.request->xid);
        finalize();
    }
}


bool ServiceZooKeeper::zkPushRequest(RequestInfo && info)
{
    try
    {
        /// We must serialize 'pushRequest' and 'finalize' (from sendThread, receiveThread) calls
        ///  to avoid forgotten operations in the queue when session is expired.
        /// Invariant: when expired, no new operations will be pushed to the queue in 'pushRequest'
        ///  and the queue will be drained in 'finalize'.
        std::lock_guard lock(zk_push_request_mutex);

        if (zk_expired)
            throw Exception("Session expired", Error::ZSESSIONEXPIRED);

        if (!zk_requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
            throw Exception("Cannot push request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);
    }
    catch (...)
    {
        if (!use_ch_service)
            throw;
        zkFinalize(false, false);
        return false;
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return true;
}


bool ServiceZooKeeper::serPushRequest(RequestInfo && info)
{
    try
    {
        /// We must serialize 'pushRequest' and 'finalize' (from sendThread, receiveThread) calls
        ///  to avoid forgotten operations in the queue when session is expired.
        /// Invariant: when expired, no new operations will be pushed to the queue in 'pushRequest'
        ///  and the queue will be drained in 'finalize'.
        std::lock_guard lock(ser_push_request_mutex);

        if (ser_expired)
            throw Exception("Session expired", Error::ZSESSIONEXPIRED);

        if (!ser_requests_queue.tryPush(std::move(info), operation_timeout.totalMilliseconds()))
            throw Exception("Cannot push request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);
    }
    catch (...)
    {
        if (use_ch_service)
            throw;
        serFinalize(false, false);
        return false;
    }

    ProfileEvents::increment(ProfileEvents::ZooKeeperTransactions);
    return true;
}


void ServiceZooKeeper::create(
    const String & path,
    const String & data,
    bool is_ephemeral,
    bool is_sequential,
    const ACLs & acls,
    CreateCallback callback)
{
    ZooKeeperCreateRequest request;
    request.path = path;
    request.data = data;
    request.is_ephemeral = is_ephemeral;
    request.is_sequential = is_sequential;
    request.acls = acls.empty() ? default_acls : acls;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCreateRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CreateResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCreate);
}


void ServiceZooKeeper::remove(
    const String & path,
    int32_t version,
    RemoveCallback callback)
{
    ZooKeeperRemoveRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperRemoveRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const RemoveResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperRemove);
}


void ServiceZooKeeper::exists(
    const String & path,
    ExistsCallback callback,
    WatchCallback watch)
{
    ZooKeeperExistsRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperExistsRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ExistsResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperExists);
}


void ServiceZooKeeper::get(
    const String & path,
    GetCallback callback,
    WatchCallback watch)
{
    ZooKeeperGetRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperGetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const GetResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperGet);
}


void ServiceZooKeeper::set(
    const String & path,
    const String & data,
    int32_t version,
    SetCallback callback)
{
    ZooKeeperSetRequest request;
    request.path = path;
    request.data = data;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperSetRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const SetResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperSet);
}


void ServiceZooKeeper::list(
    const String & path,
    ListCallback callback,
    WatchCallback watch)
{
    ZooKeeperListRequest request;
    request.path = path;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperListRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const ListResponse &>(response)); };
    request_info.watch = watch;

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperList);
}


void ServiceZooKeeper::check(
    const String & path,
    int32_t version,
    CheckCallback callback)
{
    ZooKeeperCheckRequest request;
    request.path = path;
    request.version = version;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCheckRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const CheckResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperCheck);
}


void ServiceZooKeeper::multi(
    const Requests & requests,
    MultiCallback callback)
{
    ZooKeeperMultiRequest request(requests, default_acls);

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperMultiRequest>(std::move(request));
    request_info.callback = [callback](const Response & response) { callback(dynamic_cast<const MultiResponse &>(response)); };

    pushRequest(std::move(request_info));
    ProfileEvents::increment(ProfileEvents::ZooKeeperMulti);
}


void ServiceZooKeeper::close()
{
    ZooKeeperCloseRequest request;
    request.xid = CLOSE_XID;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!zk_requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds())
        || !ser_requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push close request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}

void ServiceZooKeeper::zkClose()
{
    ZooKeeperCloseRequest request;
    request.xid = CLOSE_XID;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!zk_requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push close request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}

void ServiceZooKeeper::serClose()
{
    ZooKeeperCloseRequest request;
    request.xid = CLOSE_XID;

    RequestInfo request_info;
    request_info.request = std::make_shared<ZooKeeperCloseRequest>(std::move(request));

    if (!ser_requests_queue.tryPush(std::move(request_info), operation_timeout.totalMilliseconds()))
        throw Exception("Cannot push close request to queue within operation timeout", Error::ZOPERATIONTIMEOUT);

    ProfileEvents::increment(ProfileEvents::ZooKeeperClose);
}

}
