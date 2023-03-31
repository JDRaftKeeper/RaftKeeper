#pragma once

#include <Service/KeeperServer.h>
#include <Service/RequestsQueue.h>
#include <Service/Types.h>
#include <ZooKeeper/ZooKeeperConstants.h>

namespace RK
{

using RequestForSession = KeeperStore::RequestForSession;

struct ErrorRequest
{
    bool accepted;
    nuraft::cmd_result_code error_code;
    int64_t session_id;
    Coordination::XID xid;
    Coordination::OpNum opnum;
};

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

    void push(RequestForSession request_for_session);
    void run();

    void moveRequestToPendingQueue(RunnerId runner_id);

    void processReadRequests(RunnerId runner_id);
    void processErrorRequest();
    void processCommittedRequest(size_t count);

    /// Apply request to state machine
    void applyRequest(const RequestForSession & request) const;

    void shutdown();

    void commit(RequestForSession request);

    void onError(bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid, Coordination::OpNum opnum);

    void initialize(
        size_t thread_count_,
        std::shared_ptr<KeeperServer> server_,
        std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
        UInt64 operation_timeout_ms_);

    size_t commitQueueSize() { return committed_queue.size(); }

private:
    size_t getRunnerId(int64_t session_id) const { return session_id % runner_count; }


    using RequestForSessions = std::vector<KeeperStore::RequestForSession>;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

    std::shared_ptr<KeeperServer> server;

    KeeperResponsesQueue & responses_queue;

    /// Local requests
    ptr<RequestsQueue> requests_queue;

    /// <runner_id, <session_id, requests>>
    /// Requests from `requests_queue` grouped by session
    std::unordered_map<size_t, std::unordered_map<int64_t, RequestForSessions>> pending_requests;

    /// Raft committed write requests which can be local or from other nodes.
    ConcurrentBoundedQueue<KeeperStore::RequestForSession> committed_queue{1000};

    size_t runner_count;

    ThreadPoolPtr request_thread;

    std::shared_ptr<KeeperDispatcher> keeper_dispatcher;

    mutable std::mutex mutex;
    std::condition_variable cv;

    /// key : session_id xid
    /// Error requests when append entry or forward to leader
    std::unordered_map<UInt128, ErrorRequest> errors;

    Poco::Logger * log;

    UInt64 operation_timeout_ms = 10000;
};

}
