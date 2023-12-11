#pragma once

#include "ZooKeeper/ZooKeeperConstants.h"
#include <Service/KeeperServer.h>
#include <Service/RequestsQueue.h>
#include <Service/Types.h>
#include <ZooKeeper/ZooKeeperConstants.h>

namespace RK
{

class KeeperDispatcher;

/** Handle user read request and Raft committed write request.
 */
class RequestProcessor
{
public:
    explicit RequestProcessor(KeeperResponsesQueue & responses_queue_)
        : responses_queue(responses_queue_), log(&Poco::Logger::get("RequestProcessor"))
    {
    }

    void push(const RequestForSession & request_for_session);

    void shutdown();

    void commit(RequestForSession request);

    /// Invoked when fail to forward request to leader or append entry.
    void onError(bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid, Coordination::OpNum opnum);

    void initialize(
        size_t thread_count_,
        std::shared_ptr<KeeperServer> server_,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        UInt64 operation_timeout_ms_);

    size_t commitQueueSize() { return committed_queue.size(); }

private:
    void run();
    /// Exist system for fatal error.
    [[noreturn]] static void systemExist() ;

    void moveRequestToPendingQueue(RunnerId runner_id);

    void processReadRequests(RunnerId runner_id);
    void processErrorRequest(size_t count);
    void processCommittedRequest(size_t count);

    /// Apply request to state machine
    void applyRequest(const RequestForSession & request) const;
    size_t getRunnerId(int64_t session_id) const { return session_id % runner_count; }

    /// Find error request in pending request queue
    std::optional<RequestForSession> findErrorRequest(const ErrorRequest & error_request);

    /// We can handle zxid as a continuous stream of committed(write) requests at once.
    /// However, if we encounter a read request or an erroneous request,
    /// we need to interrupt the processing.
    bool shouldProcessCommittedRequest(const RequestForSession & committed_request, bool & found_in_pending_queue);

    using RequestForSessions = std::vector<RequestForSession>;

    ThreadFromGlobalPool main_thread;

    std::atomic<bool> shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    KeeperResponsesQueue & responses_queue;

    /// Local requests
    ptr<RequestsQueue> requests_queue;

    /// <runner_id, <session_id, requests>>
    /// Requests from `requests_queue` grouped by session
    std::unordered_map<size_t, std::unordered_map<int64_t, RequestForSessions>> pending_requests;

    /// Raft committed write requests which can be local or from other nodes.
    ConcurrentBoundedQueue<RequestForSession> committed_queue{1000};

    size_t runner_count;

    ThreadPoolPtr request_thread;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    mutable std::mutex mutex;
    std::condition_variable cv;

    /// Error requests when append entry or forward to leader.
    ErrorRequests error_requests;
    /// Used as index for error_requests
    std::unordered_set<RequestId, RequestId::RequestIdHash> error_request_ids;

    Poco::Logger * log;

    UInt64 operation_timeout_ms = 10000;
};

}
