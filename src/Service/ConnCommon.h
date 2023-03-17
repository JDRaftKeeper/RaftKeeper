#pragma once

#include <unordered_map>
#include <Core/Context.h>
#include <Core/Types.h>
#include <Common/IO/ReadBufferFromFileDescriptor.h>
#include <Common/IO/ReadBufferFromPocoSocket.h>
#include <Common/IO/WriteBufferFromPocoSocket.h>
#include <Service/ThreadSafeQueue.h>
#include <Service/WriteBufferFromFiFoBuffer.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/MultiVersion.h>
#include <Common/PipeFDs.h>
#include <Common/Stopwatch.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperConstants.h>

#if defined(POCO_HAVE_FD_EPOLL)
#include <sys/epoll.h>
#else
#include <poll.h>
#endif

namespace RK
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int TIMEOUT_EXCEEDED;
    extern const int READONLY;
    extern const int RAFT_ERROR;
}

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

using ThreadSafeResponseQueue = ThreadSafeQueue<std::shared_ptr<FIFOBuffer>>;

using ThreadSafeResponseQueuePtr = std::unique_ptr<ThreadSafeResponseQueue>;

struct LastOp;
using LastOpMultiVersion = MultiVersion<LastOp>;
using LastOpPtr = LastOpMultiVersion::Version;

struct LastOp
{
    String name{"NA"};
    int64_t last_cxid{-1};
    int64_t last_zxid{-1};
    int64_t last_response_time{0};
};

static const LastOp EMPTY_LAST_OP{"NA", -1, -1, 0};

}
