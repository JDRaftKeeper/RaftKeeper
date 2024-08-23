#pragma once

#include <unordered_map>
#include <Service/Context.h>
#include <Service/LockFreeConcurrentBoundedQueue.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <Service/ForwardResponse.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperConstants.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/IO/ReadBufferFromFileDescriptor.h>
#include <Common/IO/ReadBufferFromPocoSocket.h>
#include <Common/IO/WriteBufferFromPocoSocket.h>
#include <Common/MultiVersion.h>
#include <Common/PipeFDs.h>
#include <Common/Stopwatch.h>
#include <common/types.h>

#if defined(POCO_HAVE_FD_EPOLL)
#    include <sys/epoll.h>
#else
#    include <poll.h>
#endif

namespace RK
{

/**
 * Client connection request.
 */
struct ConnectRequest
{
    /// network protocol version should be 0.
    int32_t protocol_version;

    /// should be 0, if is new connection,
    /// or else the last zxid client seen,
    /// in which case means session is not timeout.
    int64_t last_zxid_seen;

    /// session timeout client send,
    /// the real timeout should be negotiated with server.
    int32_t session_timeout_ms;

    /// should be 0, if is new connection,
    /// or else means session is not timeout.
    int64_t previous_session_id = 0;

    std::array<char, Coordination::PASSWORD_LENGTH> passwd{};

    /// Whether the connection is readonly.
    /// It means no write requests is permitted.
    /// TODO implementation
    bool readonly;
};

struct SocketInterruptablePollWrapper;
using SocketInterruptablePollWrapperPtr = std::unique_ptr<SocketInterruptablePollWrapper>;

using ResponseQueue = LockFreeConcurrentBoundedQueue<Coordination::ZooKeeperResponsePtr>;
using ResponseQueuePtr = std::unique_ptr<ResponseQueue>;

using ForwardResponseQueue = LockFreeConcurrentBoundedQueue<ForwardResponsePtr>;
using ForwardResponseQueuePtr = std::unique_ptr<ForwardResponseQueue>;

struct LastOp;
using LastOpMultiVersion = MultiVersion<LastOp>;
using LastOpPtr = LastOpMultiVersion::Version;

/**
 * Last operation for a session or whole server.
 */
struct LastOp
{
    /// Short name for operation.
    String name{"NA"};

    /// cxid for the operation
    int64_t last_cxid{-1};

    /// zxid for the operation
    int64_t last_zxid{-1};

    /// response timestamp for the operation in millisecond
    int64_t last_response_time{0};
};

static const LastOp EMPTY_LAST_OP{"NA", -1, -1, 0};

}
