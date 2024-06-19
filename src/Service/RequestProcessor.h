#pragma once

#include <Service/KeeperCommon.h>
#include <Service/KeeperServer.h>
#include <Service/RequestsQueue.h>
#include <ZooKeeper/ZooKeeperConstants.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <unordered_set>

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
    [[noreturn]] static void systemExist();

    void moveRequestToPendingQueue(size_t);

    void readRequestProcessor(RunnerId id);

    void sendToProcessor(const RequestForSession & request);
    void processErrorRequest(size_t);
    void processCommittedRequest(size_t);
    void drainQueues();

    /// Apply request to state machine
    void applyRequest(const RequestForSession & request);
    size_t getRunnerId(int64_t session_id) const { return session_id % runner_count; }

    /// Find error request in pending request queue
    std::optional<RequestForSession> findErrorRequest(const ErrorRequest & error_request);

    /// We can handle zxid as a continuous stream of committed(write) requests at once.
    /// However, if we encounter a read request or an erroneous request,
    /// we need to interrupt the processing.
    bool shouldProcessCommittedRequest(const RequestForSession & committed_request, bool & found_in_pending_queue);

    using RequestForSessions = std::queue<RequestForSession>;

    ThreadFromGlobalPool main_thread;

    std::atomic<bool> shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    KeeperResponsesQueue & responses_queue;

    /// Local requests
    ptr<ConcurrentBoundedQueue<RequestForSession>> requests_queue;

    /// <session_id, requests>
    /// Requests from `requests_queue` grouped by session
    std::unordered_map<int64_t, RequestForSessions> pending_requests;

    /// <session_id, requests>
    /// sortedErrorRequests grouped by session
    using sortedErrorRequests = std::map<Coordination::XID, ErrorRequest>;
    std::unordered_map<int64_t, sortedErrorRequests> pending_error_requests;

    /// Raft committed write requests which can be local or from other nodes.
    ConcurrentBoundedQueue<RequestForSession> committed_queue{1000};

    size_t runner_count;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    std::unordered_set<int64_t> queues_to_drain;

    mutable std::mutex mutex;
    std::condition_variable cv;

    /// Error requests when append entry or forward to leader.
    ConcurrentBoundedQueue<ErrorRequest> error_requests{1000};
    // ErrorRequests error_requests;

    Poco::Logger * log;

    UInt64 operation_timeout_ms = 10000;

    ThreadPoolPtr read_thread_pool;
    std::shared_ptr<RequestsQueue> read_request_process_queues ;

    std::atomic<uint32_t> numRequestsProcessing{0};

    std::mutex empty_pool_lock;
    std::condition_variable empty_pool_cv;

    // size_t max_read_batch_size = 32;
};

}
