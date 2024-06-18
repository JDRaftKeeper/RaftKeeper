#include <Common/setThreadName.h>

#include <Service/KeeperCommon.h>
#include <Service/KeeperDispatcher.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <Service/Metrics.h>

namespace RK
{

void RequestProcessor::push(const RequestForSession & request_for_session)
{
    if (!shutdown_called)
    {
        requests_queue->push(request_for_session);
        {
            std::unique_lock lk(mutex);
            cv.notify_all();
        }
    }
}

void RequestProcessor::systemExist()
{
    ::abort();
}

void RequestProcessor::run()
{
    setThreadName("ReqProcessor");

    while (!shutdown_called)
    {
        try
        {
            auto need_wait = [&]() -> bool
            {
                /// We should not wait when pending queue is not empty, for function 'moveRequestToPendingQueue'
                /// moves all pending requests to pending queue.
                /// Suppose there is a sequence of write-read requests, 'moveRequestToPendingQueue' move all requests
                /// to pending queue and then the first loop handle the write request, then If we do not check the
                /// pending queue in our wait condition, it will result in meaningless waiting.
                bool pending_requests_empty = true;

                for (auto it = pending_requests.begin(); it != pending_requests.end();)
                {
                    if (it->second.empty())
                    {
                        LOG_ERROR(log, "Got empty queue in pending_requests, it's a bug");
                        it = pending_requests.erase(it);
                    }
                    else
                    {
                        pending_requests_empty = false;
                        break;
                    }
                }

                for (auto it = pending_error_requests.begin(); it != pending_error_requests.end();)
                {
                    if (it->second.empty())
                    {
                        LOG_ERROR(log, "Got empty queue in pending_requests, it's a bug");
                        it = pending_error_requests.erase(it);
                    }
                    else
                    {
                        pending_requests_empty = false;
                        break;
                    }
                }

                return requests_queue->empty() && committed_queue.empty() && error_requests.empty() && pending_requests_empty;
            };

            {
                using namespace std::chrono_literals;
                std::unique_lock lk(mutex);
                if (!cv.wait_for(lk, operation_timeout_ms * 1ms, [&] { return !need_wait() || shutdown_called; }))
                    LOG_DEBUG(
                        log,
                        "Waiting timeout errors size {}, requests_queue size {}, committed_queue size {}",
                        error_requests.size(),
                        requests_queue->size(),
                        committed_queue.size());
            }

            if (shutdown_called)
                return;

            size_t error_to_process = error_requests.size();
            size_t commits_to_process = committed_queue.size();
            size_t requests_to_process = requests_queue->size();

            /// 1. process request from request_queue
            if (requests_to_process != 0)
                moveRequestToPendingQueue(requests_to_process);

            /// 2. process error requests
            if (error_to_process !=0)
                processErrorRequest(error_to_process);

            /// 3. process committed request, single thread
            if (commits_to_process != 0)
                processCommittedRequest(commits_to_process);

            /// 4. handle need_drain sessions
            drainQueues();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void RequestProcessor::moveRequestToPendingQueue(size_t requestsToProcess)
{

    if (requestsToProcess == 0)
        return;

    size_t readsProcessed = 0;
    while (!shutdown_called
           && requestsToProcess > 0)
    {
        RequestForSession request;

        if (requests_queue->tryPop(request))
        {
            if (request.request->getOpNum() == Coordination::OpNum::Auth)
                continue;

            /// Since there are no pending write requests in the current session,
            /// we can directly process these read requests without moving them to the pending queue.
            if (request.request->isReadRequest() && !pending_requests.contains(request.session_id))
            {
                readsProcessed++;
                sendToProcessor(request);
            }
            else
            {
                LOG_DEBUG(log, "Move {} to pending queue", request.toSimpleString());
                pending_requests[request.session_id].emplace(request);
            }
        }

        requestsToProcess--;
    }

    Metrics::getMetrics().reads_issued_from_requests_queue->add(readsProcessed);
}

void RequestProcessor::processCommittedRequest(size_t commits_to_process)
{
    if (committed_queue.empty())
    {
        return;
    }

    /// Drain outstanding reads
    {
        std::unique_lock<std::mutex> lk(empty_pool_lock);
        cv.wait(lk, [this]{ return numRequestsProcessing == 0 || shutdown_called; });
    }


    auto start_time_ms = getCurrentTimeMilliseconds();
    size_t commits_processed = 0;

    for (; commits_processed < commits_to_process; ++commits_processed)
    {
        RequestForSession committed_request;
        if (!committed_queue.peek(committed_request))
            continue;

        LOG_DEBUG(log, "Process committed(write) request {}", committed_request.toSimpleString());

        /// New session and update session requests are not put into pending queue
        if (unlikely(isSessionRequest(committed_request.request)))
        {
            applyRequest(committed_request);
            committed_queue.pop();
        }
        /// Remote requests
        else if (!keeper_dispatcher->isLocalSession(committed_request.session_id))
        {
            if (pending_requests.contains(committed_request.session_id))
            {
                LOG_WARNING(
                    log,
                    "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                    "Just delete from pending queue.",
                    toHexString(committed_request.session_id));
                pending_requests.erase(committed_request.session_id);
            }

            applyRequest(committed_request);
            committed_queue.pop();
        }
        /// Local requests
        else
        {
            if (unlikely(committed_request.request->getOpNum() == Coordination::OpNum::Auth))
            {
                LOG_DEBUG(log, "Apply auth request {}", toHexString(committed_request.session_id));
                applyRequest(committed_request);
                committed_queue.pop();
            }
            else
            {
                /// Can't process this write yet.
                /// Either there are reads pending in this session, or we
                /// haven't gotten to this write yet
                if (!pending_requests.contains(committed_request.session_id)
                    || pending_requests[committed_request.session_id].empty()
                    )
                {
                    if (committed_request.request->getOpNum() == Coordination::OpNum::Close)
                    {
                        LOG_DEBUG(log, "Commit request got, but not in pending_requests,"
                                       "it's close requests from deadSessionCleanThread");

                        applyRequest(committed_request);
                        committed_queue.pop();
                        continue;
                    }


                    /// We push request to request_processor first, than do replication (forward to leader)
                    /// for every RequestProcessor::run(), wo got current commited queue size before got
                    /// request queue, an normal commit should find in pending queue.

                    /// ----------------------------------------- Timeline ------------------------------------------------->
                    /// --> request_processor->push(request)
                    /// -----------> replication
                    /// ------------------------> commit_queue->push(request)
                    /// ----------------------------------------> got request in commit_queue->size() to process
                    /// --------------------------------------------------------> got request in request_processor to process

                    LOG_ERROR(log, "Commit request got {}, but not in pending_requests, it's maybe get errors before (for example forward timeout),"
                                   "and we already handle it in before. But we should still apply it.", committed_request.toSimpleString());

                    applyRequest(committed_request);
                    committed_queue.pop();
                    continue;
                }

                // Pending reads
                if (pending_requests[committed_request.session_id].front().request->isReadRequest())
                {
                    LOG_DEBUG(log, "Commit request got, but next pending_request {}",
                              pending_requests[committed_request.session_id].front().toSimpleString());
                    break;
                }

                applyRequest(committed_request);

                committed_queue.pop();

                LOG_DEBUG(log, "Move committed(write) request {} from committed_queue and pending_requests", committed_request.toSimpleString());
                Metrics::getMetrics().update_latency->add(getCurrentTimeMilliseconds() - committed_request.create_time);

                /// remove request from pending queue
                auto & pending_requests_for_session = pending_requests[committed_request.session_id];
                pending_requests_for_session.pop();

                if (pending_requests_for_session.empty())
                    pending_requests.erase(committed_request.session_id);
                else
                    LOG_DEBUG(log, "Next pending_request {}", pending_requests_for_session.front().toSimpleString());
                    queues_to_drain.emplace(committed_request.session_id);
            }
        }
    }

    Metrics::getMetrics().apply_write_request_time_ms->add(getCurrentTimeMilliseconds() - start_time_ms);
    Metrics::getMetrics().write_commit_proc_issued->add(commits_processed);
}

void RequestProcessor::processErrorRequest(size_t error_to_process)
{
    // Process error_requests, move local error_request to pending_error_requests
    {
        LOG_WARNING(log, "There are {} error requests", error_to_process);

        /// Note that error requests may be not processed in order.
        for (size_t i = 0; i < error_to_process; i++)
        {
            auto & error_request = error_requests.front();
            auto [session_id, xid] = error_request.getRequestId();

            if (unlikely(isSessionRequest(error_request.opnum)))
            {
                ZooKeeperResponsePtr response;
                if (isNewSessionRequest(error_request.opnum))
                {
                    auto new_session_response = std::make_shared<ZooKeeperNewSessionResponse>();
                    new_session_response->xid = xid;
                    new_session_response->internal_id = session_id;
                    new_session_response->success = false;
                    response = std::move(new_session_response);
                }
                else
                {
                    auto update_session_response = std::make_shared<ZooKeeperUpdateSessionResponse>();
                    update_session_response->xid = xid;
                    update_session_response->session_id = session_id;
                    update_session_response->success = false;
                    response = std::move(update_session_response);
                }

                response->error = error_request.error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                               : Coordination::Error::ZCONNECTIONLOSS;
                /// TODO use real request creating time.
                response->request_created_time_ms = getCurrentTimeMilliseconds();

                responses_queue.push(ResponseForSession{session_id, response});

            }
            /// Remote request
            else if (!keeper_dispatcher->isLocalSession(session_id))
            {
                if (pending_requests.contains(session_id))
                {
                    LOG_WARNING(
                        log,
                        "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                        "Just delete from pending queue.",
                        toHexString(session_id));
                    pending_requests.erase(session_id);
                }

                LOG_WARNING(log, "Error request {} is not local", error_request.toString());
            }
            /// Local request, just move it to pending_error_requests
            else
            {
                if (pending_error_requests[error_request.session_id].contains(error_request.xid))
                {
                    LOG_WARNING(log, "Duplicate error requests found {} ", error_request.toString());
                }
                else
                {
                    pending_error_requests[error_request.session_id].emplace(error_request.xid, error_request);
                }
            }

            // Remove error request from error_requests
            error_requests.erase(error_requests.begin());
        }
    }

    /// Handle error request in pending_error_requests
    for (auto it = pending_error_requests.begin(); it != pending_error_requests.end();)
    {
        if (it->second.empty())
        {
            LOG_ERROR(log, "Got empty queue in pending_error_requests, it's a bug");
            it = pending_error_requests.erase(it);
        }
        else
        {
            auto & sorted_error_requests = it->second;
            while (!sorted_error_requests.empty())
            {
                auto & error_request = sorted_error_requests.rbegin()->second;
                std::optional<RequestForSession> request = findErrorRequest(error_request);

                if (request)
                {
                    LOG_DEBUG(log, "rico ga");
                    LOG_ERROR(log, "Make error response for {}", error_request.toString());

                    ZooKeeperResponsePtr response = request->request->makeResponse();
                    response->xid = request->request->xid;
                    response->zxid = 0;
                    response->request_created_time_ms = request->create_time;

                    response->error = error_request.error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                                   : Coordination::Error::ZCONNECTIONLOSS;
                    responses_queue.push(ResponseForSession{it->first, response});
                    sorted_error_requests.erase(response->xid);
                }
                else
                {
                    LOG_WARNING(
                        this->log,
                        "Not found error request {} in pending queue. Possible reason: 1. request forwarding error; 2.close requests from "
                        "deadSessionCleanThread are not put into pending queue; 3.error occurs(forward or append entries) but request is still "
                        "committed, 'processCommittedRequest' may delete request from pending request first, so here we can not find it."
                        , error_request.toString());
                    break;
                }
            }
            if (sorted_error_requests.empty())
            {
                it = pending_error_requests.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
}

std::optional<RequestForSession> RequestProcessor::findErrorRequest(const ErrorRequest & error_request)
{
    auto session_id = error_request.session_id;
    auto xid = error_request.xid;

    /// Auth request is not put in pending queue, so no need to remove it.
    if (xid == Coordination::AUTH_XID)
    {
        std::optional<RequestForSession> request;
        ZooKeeperRequestPtr auth_request = std::make_shared<ZooKeeperAuthRequest>();

        Poco::Timestamp timestamp;
        auto now = timestamp.epochMicroseconds();

        RequestForSession request_for_session{auth_request, session_id, now / 1000};
        request.emplace(request_for_session);
        return request;
    }

    std::optional<RequestForSession> request;

    if (pending_requests.contains(session_id))
    {
        auto & pending_requests_for_session = pending_requests[session_id];
        auto & request_first = pending_requests_for_session.front();

        if (request_first.request->xid == xid
            || (request_first.request->getOpNum() == Coordination::OpNum::Close && error_request.opnum == Coordination::OpNum::Close))
        {
            LOG_WARNING(log, "Matched error request {} in pending queue", request_first.toSimpleString());
            request.emplace(request_first);
            pending_requests_for_session.pop();

            if (pending_requests_for_session.empty())
                pending_requests.erase(session_id);
            else
            {
                LOG_DEBUG(log, "Next pending_request {}", pending_requests_for_session.front().toSimpleString());
                queues_to_drain.emplace(session_id);
            }
        }
    }

    return request;
}

void RequestProcessor::drainQueues()
{
    size_t read_processed = 0;
    if (queues_to_drain.empty())
    {
        return;
    }

    for (auto && sessionId : queues_to_drain)
    {
        auto & session_queue = pending_requests[sessionId];
        size_t read_after_write = 0;

        while (!shutdown_called && !session_queue.empty() && session_queue.front().request->isReadRequest())
        {
            auto & read_request = session_queue.front();
            ++ read_after_write;
            sendToProcessor(read_request);

            LOG_DEBUG(log, "Move read request {} from pending_requests", read_request.toSimpleString());
            session_queue.pop();

            if (!session_queue.empty())
            {
                LOG_DEBUG(log, "Next pending_request {}", session_queue.front().toSimpleString());
            }
        }

        Metrics::getMetrics().reads_after_write_in_session_queue->add(read_after_write);
        read_processed += read_after_write;

        // Remove empty queues
        if (session_queue.empty())
            pending_requests.erase(sessionId);
    }
    Metrics::getMetrics().reads_issued_from_session_queue->add(read_processed);
    queues_to_drain.clear();
}

void RequestProcessor::readRequestProcessor(RunnerId runner_id)
{
    setThreadName(("ReadProcess#" + std::to_string(runner_id)).c_str());
    auto max_wait_ms = std::min(static_cast<uint64_t>(1000), operation_timeout_ms);

    while (!shutdown_called)
    {
        RequestForSession request_for_session;

        if (read_request_process_queues->tryPop(runner_id, request_for_session, max_wait_ms))
        {
            if (shutdown_called)
                break;
            applyRequest(request_for_session);

            Metrics::getMetrics().read_latency->add(getCurrentTimeMilliseconds() - request_for_session.create_time);


            LOG_DEBUG(log, "NumRequestsProcessing {}", numRequestsProcessing.load());

            if (--numRequestsProcessing == 0)
            {
                LOG_DEBUG(log, "Try notify empty");
                std::unique_lock lk(empty_pool_lock);
                empty_pool_cv.notify_all();
                LOG_DEBUG(log, "Finish notify empty");
            }
        }
    }
}

void RequestProcessor::sendToProcessor(const RequestForSession & request)
{
    numRequestsProcessing++;
    LOG_DEBUG(log, "Schedule request {}", request.toSimpleString());
    read_request_process_queues->push(request);
}

void RequestProcessor::applyRequest(const RequestForSession & request)
{
    LOG_DEBUG(log, "Apply request {}", request.toSimpleString());
    try
    {
        if (request.request->isReadRequest())
        {
            if (server->isLeaderAlive())
            {
                server->getKeeperStateMachine()->getStore().processRequest(responses_queue, request);
            }
            else
            {
                auto response = request.request->makeResponse();

                response->request_created_time_ms = request.create_time;
                response->xid = request.request->xid;
                response->zxid = 0;
                response->error = Coordination::Error::ZCONNECTIONLOSS;

                responses_queue.push(ResponseForSession{request.session_id, response});
            }
        }
        else
        {
            if (!server->isLeaderAlive())
                LOG_WARNING(log, "Write request is committed, when try to apply it to store the leader is not alive.");
            server->getKeeperStateMachine()->getStore().processRequest(responses_queue, request);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Fail to apply request {}.", request.request->toString()));
        if (!request.request->isReadRequest())
        {
            LOG_FATAL(log, "Fail to apply committed(write) request which will lead state machine inconsistency, system will exist.");
            systemExist();
        }
    }
}

void RequestProcessor::shutdown()
{
    if (shutdown_called)
        return;

    LOG_INFO(log, "Shutting down request processor!");
    shutdown_called = true;

    {
        std::unique_lock lk(mutex);
        cv.notify_all();
    }

    if (main_thread.joinable())
        main_thread.join();

    RequestForSession request_for_session;
    while (requests_queue->tryPop(request_for_session))
    {
        LOG_DEBUG(log, "Make session expire response for request {}", request_for_session.toSimpleString());
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->request_created_time_ms = request_for_session.create_time;
        response->error = Coordination::Error::ZSESSIONEXPIRED;
        responses_queue.push(ResponseForSession{request_for_session.session_id, response});
    }
}

void RequestProcessor::commit(RequestForSession request)
{
    if (!shutdown_called)
    {
        committed_queue.push(request);
        {
            std::unique_lock lk(mutex);
            cv.notify_all();
        }
        LOG_DEBUG(log, "Commit {}, now committed queue size is {}", request.toSimpleString(), committed_queue.size());
    }
}

void RequestProcessor::onError(
    bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid, Coordination::OpNum opnum)
{
    if (!shutdown_called)
    {
        ErrorRequest error_request{accepted, error_code, session_id, xid, opnum};
        error_requests.push_back(error_request);

        LOG_WARNING(log, "Found error request {}", error_request.toString());
        {
            std::unique_lock lock(mutex);
            cv.notify_all();
        }
    }
}

void RequestProcessor::initialize(
    size_t thread_count_,
    std::shared_ptr<KeeperServer> server_,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
    UInt64 operation_timeout_ms_)
{
    operation_timeout_ms = operation_timeout_ms_;
    runner_count = thread_count_;
    // max_read_batch_size = thread_count_ * 4;
    server = server_;
    keeper_dispatcher = keeper_dispatcher_;
    requests_queue = std::make_shared<ConcurrentBoundedQueue<RequestForSession>>(10000);
    main_thread = ThreadFromGlobalPool([this] { run(); });

    read_request_process_queues = std::make_shared<RequestsQueue>(runner_count, 1000);

    read_thread_pool = std::make_shared<ThreadPool>(runner_count);

    for (size_t i = 0; i < runner_count; i++)
    {
        read_thread_pool->trySchedule([this, i] { readRequestProcessor(i); });
    }
}

}
