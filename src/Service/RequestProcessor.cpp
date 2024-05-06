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
    Stopwatch watch;

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
                for (const auto & [_, runner_pending_requests] : pending_requests)
                {
                    for (const auto & [session_, session_pending_requests] : runner_pending_requests)
                        if (!session_pending_requests.empty())
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

            size_t committed_request_size = committed_queue.size();
            size_t error_request_size;
            {
                std::unique_lock lk(mutex);
                error_request_size = error_request_ids.size();
            }

            /// 1. process read request
            watch.restart();
            for (RunnerId runner_id = 0; runner_id < runner_count; runner_id++)
            {
                moveRequestToPendingQueue(runner_id);
                processReadRequests(runner_id);
            }
            Metrics::getMetrics().apply_read_request_time_ms->add(watch.elapsedMilliseconds());

            /// 2. process committed request, single thread
            watch.restart();
            processCommittedRequest(committed_request_size);
            Metrics::getMetrics().apply_write_request_time_ms->add(watch.elapsedMilliseconds());

            /// 3. process error requests
            processErrorRequest(error_request_size);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void RequestProcessor::moveRequestToPendingQueue(RunnerId runner_id)
{
    auto & thread_requests = pending_requests.find(runner_id)->second;
    size_t request_size = requests_queue->size(runner_id);

    if (request_size)
        LOG_TRACE(log, "Prepare to move {} requests to pending queue of runner {}", request_size, runner_id);

    for (size_t i = 0; i < request_size; ++i)
    {
        RequestForSession request;
        if (requests_queue->tryPop(runner_id, request))
        {
            auto op_num = request.request->getOpNum();
            if (op_num != Coordination::OpNum::Auth)
            {
                LOG_TRACE(log, "Move {} to pending queue", request.toSimpleString());
                thread_requests[request.session_id].push_back(request);
            }
        }
    }
}

bool RequestProcessor::shouldProcessCommittedRequest(const RequestForSession & committed_request, bool & found_in_pending_queue)
{
    bool has_read_request = false;
    bool found_error = false;

    auto runner_id = getRunnerId(committed_request.session_id);
    auto & my_pending_requests = pending_requests.find(runner_id)->second;

    auto & pending_requests_for_session = my_pending_requests[committed_request.session_id];

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

void RequestProcessor::processCommittedRequest(size_t count)
{
    RequestForSession committed_request;
    for (size_t i = 0; i < count; ++i)
    {
        if (!committed_queue.peek(committed_request))
            continue;

        LOG_DEBUG(log, "Process committed(write) request {}", committed_request.toSimpleString());

        auto runner_id = getRunnerId(committed_request.session_id);
        auto & my_pending_requests = pending_requests.find(runner_id)->second;

        /// New session and update session requests are not put into pending queue
        if (unlikely(isSessionRequest(committed_request.request)))
        {
            applyRequest(committed_request);
            committed_queue.pop();
        }
        /// Remote requests
        else if (!keeper_dispatcher->isLocalSession(committed_request.session_id))
        {
            if (my_pending_requests.contains(committed_request.session_id))
            {
                LOG_WARNING(
                    log,
                    "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                    "Just delete from pending queue.",
                    toHexString(committed_request.session_id));
                my_pending_requests.erase(committed_request.session_id);
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
                bool found_in_pending_queue;
                if (!shouldProcessCommittedRequest(committed_request, found_in_pending_queue))
                    break;

                /// apply request
                applyRequest(committed_request);
                committed_queue.pop();
                auto current_time = getCurrentTimeMilliseconds();
                Metrics::getMetrics().update_latency->add(current_time - committed_request.create_time);

                /// remove request from pending queue
                auto & pending_requests_for_session = my_pending_requests[committed_request.session_id];
                if (found_in_pending_queue)
                    pending_requests_for_session.erase(pending_requests_for_session.begin());

                if (pending_requests_for_session.empty())
                    my_pending_requests.erase(committed_request.session_id);
            }
        }
    }
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

        auto & my_pending_requests = pending_requests.find(getRunnerId(session_id))->second;

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
            if (my_pending_requests.contains(session_id))
            {
                LOG_WARNING(
                    log,
                    "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                    "Just delete from pending queue.",
                    toHexString(session_id));
                my_pending_requests.erase(session_id);
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

    auto & my_pending_requests = pending_requests.find(getRunnerId(session_id))->second;
    auto session_requests = my_pending_requests.find(session_id);

    if (session_requests != my_pending_requests.end())
    {
        auto & requests = session_requests->second;
        for (auto request_it = requests.begin(); request_it != requests.end();)
        {
            LOG_TRACE(log, "Try match {}", request_it->toSimpleString());

            if (request_it->request->xid == xid
                || (request_it->request->getOpNum() == Coordination::OpNum::Close && error_request.opnum == Coordination::OpNum::Close))
            {
                LOG_WARNING(log, "Matched error request {} in pending queue", request_it->toSimpleString());
                request = *request_it;
                requests.erase(request_it);
                break;
            }
            else
            {
                ++request_it;
            }
        }
    }

    return request;
}

void RequestProcessor::processReadRequests(RunnerId runner_id)
{
    auto & thread_requests = pending_requests.find(runner_id)->second;

    /// process every session, until encountered write request
    for (auto it = thread_requests.begin(); it != thread_requests.end();)
    {
        auto & session_requests = it->second;
        for (auto session_request = session_requests.begin(); session_request != session_requests.end();)
        {
            /// read request
            if (session_request->request->isReadRequest())
            {
                applyRequest(*session_request);
                auto current_time = getCurrentTimeMilliseconds();
                Metrics::getMetrics().read_latency->add(current_time - session_request->create_time);
                session_request = session_requests.erase(session_request);
            }
            else
            {
                break;
            }
        }

        if (session_requests.empty())
            it = thread_requests.erase(it);
        else
            ++it;
    }
}

void RequestProcessor::applyRequest(const RequestForSession & request) const
{
    LOG_TRACE(log, "Apply request {}", request.toSimpleString());
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
    while (requests_queue->tryPopAny(request_for_session))
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
    server = server_;
    keeper_dispatcher = keeper_dispatcher_;
    requests_queue = std::make_shared<RequestsQueue>(runner_count, 20000);
    for (size_t i = 0; i < runner_count; i++)
    {
        pending_requests[i];
    }
    main_thread = ThreadFromGlobalPool([this] { run(); });
}

}
