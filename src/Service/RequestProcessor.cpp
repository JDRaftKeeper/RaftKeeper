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
                        it = pending_requests.erase(it);  // erase 返回下一个迭代器
                    }
                    else
                    {
                        pending_requests_empty = false;
                        break;
                    }
                }

                return error_request_ids.empty() && requests_queue->empty() && committed_queue.empty() && pending_requests_empty;
            };

            {
                using namespace std::chrono_literals;
                std::unique_lock lk(mutex);
                if (!cv.wait_for(lk, operation_timeout_ms * 1ms, [&] { return !need_wait() || shutdown_called; }))
                    LOG_DEBUG(
                        log,
                        "Waiting timeout errors size {}, requests_queue size {}, committed_queue size {}",
                        error_request_ids.size(),
                        requests_queue->size(),
                        committed_queue.size());
            }

            if (shutdown_called)
                return;

            size_t error_request_size;
            {
                std::unique_lock lk(mutex);
                error_request_size = error_request_ids.size();
            }

            /// 1. process read request
            moveRequestToPendingQueue();

            /// 2. process committed request, single thread
            size_t committed_request_size = committed_queue.size();

            if (committed_request_size == 0)
                continue;


            auto queuesToDrain = processCommittedRequest(committed_request_size);

            /// 3. process error requests
            processErrorRequest(error_request_size);

            /// 4. handle need_drain sessions
            size_t read_processed = 0;

            for (auto && sessionId : queuesToDrain)
            {
                if (!pending_requests.contains(sessionId))
                    continue;

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

        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void RequestProcessor::moveRequestToPendingQueue()
{
    size_t requestsToProcess = requests_queue->size();
    if (requestsToProcess == 0)
        return;

    size_t readsProcessed = 0;
    while (!shutdown_called
           && requestsToProcess > 0
           && readsProcessed <= max_read_batch_size)
    {
        RequestForSession request;

        if (requests_queue->tryPop(request))
        {
            if (request.request->getOpNum() == Coordination::OpNum::Auth)
            {
                continue;
            }

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

bool RequestProcessor::shouldProcessCommittedRequest(const RequestForSession & committed_request, bool & found_in_pending_queue)
{
    bool has_read_request = false;
    bool found_error = false;


    auto & pending_requests_for_session = pending_requests[committed_request.session_id];

    auto process_not_in_pending_queue = [this, &found_in_pending_queue, &committed_request]()
    {
        found_in_pending_queue = false;
        LOG_WARNING(
            this->log,
            "Not found committed(write) request {} in pending queue. Possible reason: 1.close requests from deadSessionCleanThread are not "
            "put into pending queue; 2.error occurs(because of forward or append entries) but request is still committed, "
            "'processErrorRequest' may delete request from pending request first, so here we can not find it.",
            committed_request.toSimpleString());
    };

    if (pending_requests_for_session.empty())
    {
        process_not_in_pending_queue();
        return true;
    }

    auto & first_pending_request = pending_requests_for_session.front();
    LOG_DEBUG(
        log,
        "First pending request of session {} is {}",
        toHexString(committed_request.session_id),
        first_pending_request.toSimpleString());

    if (first_pending_request.request->xid == committed_request.request->xid)
    {
        found_in_pending_queue = true;
        std::unique_lock lk(mutex);
        if (error_request_ids.contains(first_pending_request.getRequestId()))
        {
            LOG_WARNING(log, "Request {} is in errors, but is successfully committed", committed_request.toSimpleString());
        }
        return true;
    }
    else
    {
        found_in_pending_queue = false;
        /// Session of the previous committed(write) request is not same with the current,
        /// which means a write_request(session_1) -> request(session_2) sequence.
        if (first_pending_request.request->isReadRequest())
        {
            LOG_DEBUG(log, "Found read request, We should terminate the processing of committed(write) requests.");
            has_read_request = true;
        }
        else
        {
            {
                std::unique_lock lk(mutex);
                found_error = error_request_ids.contains(first_pending_request.getRequestId());
            }

            if (found_error)
                LOG_WARNING(log, "Found error request, We should terminate the processing of committed(write) requests.");
            else
                process_not_in_pending_queue();
        }
    }

    return !has_read_request && !found_error;
}

std::unordered_set<int64_t> RequestProcessor::processCommittedRequest(size_t commits_to_process)
{
    /// Drain outstanding reads
    {
        std::unique_lock<std::mutex> lk(empty_pool_lock);
        cv.wait(lk, [this]{ return numRequestsProcessing == 0 || shutdown_called; });
    }

    auto start_time_ms = getCurrentTimeMilliseconds();

    std::unordered_set<int64_t> queues_to_drain;

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
                    if (!pending_requests.contains(committed_request.session_id))
                    {
                        LOG_DEBUG(log, "Commit request got, but not in pending_requests");
                    }
                    else if (pending_requests[committed_request.session_id].empty())
                    {
                        LOG_DEBUG(log, "Commit request got, but pending_requests is empty, it's a Bug");
                    }

                    break;
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

    return queues_to_drain;
}

void RequestProcessor::processErrorRequest(size_t count)
{
    std::lock_guard lock(mutex);

    if (error_request_ids.empty())
        return;

    LOG_INFO(log, "There are {} error requests", count);

    ///Note that error requests may be not processed in order.
    for (size_t i = 0; i < count; i++)
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

            error_request_ids.erase(error_request.getRequestId());
            error_requests.erase(error_requests.begin());
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
            error_request_ids.erase(error_request.getRequestId());
            error_requests.erase(error_requests.begin());
        }
        /// Local request
        else
        {
            /// find error request in pending queue
            std::optional<RequestForSession> request = findErrorRequest(error_request);

            /// process error request
            if (request)
            {
                LOG_ERROR(log, "Make error response for {}", error_request.toString());

                ZooKeeperResponsePtr response = request->request->makeResponse();
                response->xid = request->request->xid;
                response->zxid = 0;
                response->request_created_time_ms = request->create_time;

                response->error = error_request.error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                               : Coordination::Error::ZCONNECTIONLOSS;

                responses_queue.push(ResponseForSession{session_id, response});

                error_request_ids.erase(error_request.getRequestId());
                error_requests.erase(error_requests.begin());
            }
            else
            {
                LOG_WARNING(
                    this->log,
                    "Not found error request {} in pending queue. Possible reason: 1. request forwarding error; 2.close requests from "
                    "deadSessionCleanThread are not put into pending queue; 3.error occurs(forward or append entries) but request is still "
                    "committed, 'processCommittedRequest' may delete request from pending request first, so here we can not find it. We "
                    "also delete it from errors.",
                    error_request.toString());

                error_request_ids.erase(error_request.getRequestId());
                error_requests.erase(error_requests.begin());
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
        auto & session_requests = pending_requests[session_id];

        RequestForSessions new_session_requests;

        while (!session_requests.empty())
        {
            auto & request_it = session_requests.front();
            if (request_it.request->xid == xid
                || (request_it.request->getOpNum() == Coordination::OpNum::Close && error_request.opnum == Coordination::OpNum::Close))
            {
                LOG_WARNING(log, "Matched error request {} in pending queue", request_it.toSimpleString());
                request.emplace(request_it);
            }
            else
            {
                new_session_requests.push(request_it);
            }
            session_requests.pop();

        }

        if (new_session_requests.empty())
            pending_requests.erase(session_id);
        else
            pending_requests[session_id] = new_session_requests;
    }

    return request;
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
        RequestId id{session_id, xid};
        ErrorRequest error_request{accepted, error_code, session_id, xid, opnum};

        LOG_WARNING(log, "Found error request {}", error_request.toString());
        {
            std::unique_lock lock(mutex);
            error_requests.push_back(error_request);
            error_request_ids.emplace(id);
        }
        cv.notify_all();
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
    max_read_batch_size = thread_count_ * 4;
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
