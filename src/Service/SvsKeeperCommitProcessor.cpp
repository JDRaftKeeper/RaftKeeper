
#include <Service/SvsKeeperDispatcher.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

void SvsKeeperCommitProcessor::processRequest(Request request_for_session)
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

void SvsKeeperCommitProcessor::run()
{
    while (!shutdown_called)
    {
        try
        {
            auto need_wait = [&]() -> bool
            {
                if (errors.empty() /*&& pending_requests.empty()*/ && requests_queue->empty() && committed_queue.empty())
                    return true;

                return false;
            };

            {
                using namespace std::chrono_literals;
                std::unique_lock lk(mutex);
                if (!cv.wait_for(lk, operation_timeout_ms * 1ms, [&] { return !need_wait() || shutdown_called; }))
                    LOG_WARNING(log, "wait time out errors size {}, requests_queue size {}, committed_queue size {}", errors.size(), requests_queue->size(), committed_queue.size());
            }

            if (shutdown_called)
                return;

            {
                std::lock_guard lock(mutex);
                if (!errors.empty())
                {
                    for (auto it = errors.begin(); it != errors.end();)
                    {
                        auto & [session_id, xid] = it->first;

                        auto & error_request = it->second;

                        LOG_WARNING(log, "error session {}, xid {}", session_id, xid);

                        if (!service_keeper_storage_dispatcher->containsSession(session_id))
                        {
                            LOG_WARNING(log, "Not my session error, session {}, xid {}", session_id, xid);
                            it = errors.erase(it);
                        }
                        else
                        {
                            using namespace Coordination;
                            std::optional<Request> request;
                            if (int32_t(xid) == Coordination::PING_XID)
                            {
                                ZooKeeperRequestPtr heartbeat_request = std::make_shared<ZooKeeperHeartbeatRequest>();
                                Request request1{ int64_t(session_id), heartbeat_request, 0, -1, -1};
                                request.emplace(request1);
                            }
                            else if (int32_t(xid) == Coordination::AUTH_XID)
                            {
                                ZooKeeperRequestPtr auth_request = std::make_shared<ZooKeeperAuthRequest>();
                                Request request1{ int64_t(session_id), auth_request, 0, -1, -1};
                                request.emplace(request1);
                            }
                            else
                            {
                                auto & pending_requests = thread_pending_requests.find(session_id % thread_count)->second;
                                auto session_requests = pending_requests.find(session_id);

                                if (session_requests != pending_requests.end())
                                {
                                    auto & requests = session_requests->second;
                                    for (auto request_it = requests.begin(); request_it != requests.end();)
                                    {
                                        if (uint64_t(request_it->request->xid) <= xid)
                                        {
                                            break;
                                        }
                                        else if (uint64_t(request_it->request->xid) == xid)
                                        {
                                            request = *request_it;
                                            request_it = requests.erase(request_it);
                                            break;
                                        }
                                        else
                                        {
                                            ++request_it;
                                        }
                                    }
                                }
                            }

                            if (request)
                            {
                                auto response = request->request->makeResponse();

                                response->xid = request->request->xid;
                                response->zxid = 0;

                                auto accepted = error_request.accepted;
                                auto error_code = error_request.error_code;

                                response->error = error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                                 : Coordination::Error::ZCONNECTIONLOSS;

                                responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request->session_id, response});

                                LOG_ERROR(log, "Make error response for session {}, xid {}, opNum {}", session_id, response->xid, Coordination::toString(request->request->getOpNum()));

                                if (!accepted)
                                    LOG_ERROR(log, "Request batch is not accepted");
                                else
                                    LOG_ERROR(log, "Request batch error, nuraft code {}", error_code);

                                it = errors.erase(it);
                                LOG_ERROR(log, "Matched error request session {}, xid {} from pending requests queue", session_id, xid);
                            }
                            else
                            {
                                LOG_WARNING(
                                    log, "Not found error request session {}, xid {} from pending queue. Maybe it is still in the request queue and will be processed next time", session_id, xid);
                                break;
                            }
                        }
                    }
                }
            }

            size_t committed_request_size = committed_queue.size();

            for (size_t i = 0; i < thread_count; i++)
            {
                request_thread->trySchedule([this, i] { processReadRequests(i); });
            }
            request_thread->wait();

            {
                /// process committed request, single thread

                LOG_DEBUG(log, "committed_request_size {}", committed_request_size);
                Request committed_request;
                for (size_t i = 0; i < committed_request_size; ++i)
                {
                    if (committed_queue.peek(committed_request))
                    {
                        auto & pending_requests = thread_pending_requests.find(committed_request.session_id % thread_count)->second;
                        auto & current_session_pending_requests = pending_requests[committed_request.session_id];

                        LOG_DEBUG(
                            log,
                            "current_session_pending_requests size {} committed_request opNum {}, session {} xid {} request {}",
                            current_session_pending_requests.size(),
                            Coordination::toString(committed_request.request->getOpNum()),
                            committed_request.session_id,
                            committed_request.request->xid,
                            committed_request.request->toString());

                        auto op_num = committed_request.request->getOpNum();

                        /// another server session request or can be out of order
                        if (!service_keeper_storage_dispatcher->containsSession(committed_request.session_id) || op_num == Coordination::OpNum::Heartbeat || op_num == Coordination::OpNum::Auth)
                        {
                            LOG_DEBUG(log, "Not contains session {}", committed_request.session_id);
                            stateMachineProcessRequest(committed_request);
                            committed_queue.pop();
                        }
                        else
                        {
                            bool has_read_request = false;
                            bool found_in_error = false;
                            if (!current_session_pending_requests.empty())
                            {
                                LOG_DEBUG(
                                    log,
                                    "Current session pending request opNum {}, session {}, xid {}",
                                    Coordination::toString(current_session_pending_requests.begin()->request->getOpNum()),
                                    current_session_pending_requests.begin()->session_id,
                                    current_session_pending_requests.begin()->request->xid);

                                while (current_session_pending_requests.begin()->request->xid != committed_request.request->xid)
                                {
                                    auto current_begin_request_session = current_session_pending_requests.begin();
                                    if (current_begin_request_session->request->isReadRequest())
                                    {
                                        LOG_DEBUG(
                                            log,
                                            "Current session {} pending head request xid {} {} is read request",
                                            committed_request.session_id,
                                            current_begin_request_session->session_id,
                                            current_begin_request_session->request->xid);

                                        has_read_request = true;
                                        break;
                                    }
                                    /// Because close's xid is not necessarily CLOSE_XID.
                                    else if (current_begin_request_session->request->getOpNum() == Coordination::OpNum::Close && committed_request.request->getOpNum() == Coordination::OpNum::Close)
                                    {
                                        break;
                                    }
                                    else
                                    {
                                        if (errors.contains(UInt128(current_begin_request_session->session_id, current_begin_request_session->request->xid)))
                                        {
                                            LOG_WARNING(
                                                log,
                                                "Current session {} pending head request xid {} {} not same committed request xid {} {}, because it is in errors",
                                                committed_request.session_id,
                                                current_begin_request_session->session_id,
                                                current_begin_request_session->request->xid,
                                                committed_request.request->xid,
                                                Coordination::toString(committed_request.request->getOpNum()));

                                            found_in_error = true;
                                            break;
                                        }
                                        else
                                        {
                                            LOG_WARNING(
                                                log,
                                                "Logic Error, maybe reconnected current session {} pending head request xid {} {} not same committed request xid {} {}, pending request size {}",
                                                committed_request.session_id,
                                                current_begin_request_session->request->xid,
                                                current_begin_request_session->request->toString(),
                                                committed_request.request->xid,
                                                committed_request.request->toString(),
                                                current_session_pending_requests.size());

                                            break;
                                        }
                                    }
                                }
                            }
                            else
                            {
                                /// close request never appear in current_session_pending_requests
                                if (committed_request.request->getOpNum() != Coordination::OpNum::Close)
                                {
                                    LOG_WARNING(log, "current_session_pending_requests is empty, maybe request in request queue. session {} committed request xid {} {}",
                                                committed_request.session_id,
                                                committed_request.request->xid,
                                                committed_request.request->toString());
                                    break;
                                }
                            }

                            if (has_read_request || found_in_error)
                                break;

                            stateMachineProcessRequest(committed_request);

                            committed_queue.pop();

                            for (auto it = current_session_pending_requests.begin(); it != current_session_pending_requests.end();)
                            {
                                auto xid = it->request->xid;
                                it = current_session_pending_requests.erase(it);
                                if (xid == committed_request.request->xid)
                                {
                                    break;
                                }
                            }

                            if (current_session_pending_requests.empty())
                                pending_requests.erase(committed_request.session_id);
                        }
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

void SvsKeeperCommitProcessor::processReadRequests(size_t thread_idx)
{
    auto & pending_requests = thread_pending_requests.find(thread_idx)->second;

    size_t request_size = requests_queue->size(thread_idx);

    LOG_TRACE(log, "thread_idx {} request_size {}", thread_idx, request_size);
    for (size_t i = 0; i < request_size; ++i)
    {
        Request request;
        if (requests_queue->tryPop(thread_idx, request))
        {
            auto op_num = request.request->getOpNum();
            if (op_num != Coordination::OpNum::Heartbeat && op_num != Coordination::OpNum::Auth)
            {
                pending_requests[request.session_id].push_back(request);
            }
        }
    }

    /// process every session, until encountered write request
    for (auto it = pending_requests.begin(); it != pending_requests.end();)
    {
        auto & requests = it->second;
        for (auto requets_it = requests.begin(); requets_it != requests.end();)
        {
            /// read request
            if (requets_it->request->isReadRequest())
            {
                stateMachineProcessRequest(*requets_it);
                requets_it = requests.erase(requets_it);
            }
            else
            {
                break;
            }
        }

        if (requests.empty())
            it = pending_requests.erase(it);
        else
            ++it;
    }
}

void SvsKeeperCommitProcessor::stateMachineProcessRequest(const RequestForSession & reuqest) const
{
    try
    {
        LOG_TRACE(
            log,
            "Process reuqest session {} xid {} request {}",
            reuqest.session_id,
            reuqest.request->xid,
            reuqest.request->toString());

        server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, reuqest.request, reuqest.session_id, reuqest.time, {}, true, false);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Got exception while process session {} read request {}.", reuqest.session_id, reuqest.request->toString()));
    }
}

void SvsKeeperCommitProcessor::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    {
        std::unique_lock lk(mutex);
        cv.notify_all();
    }

    if (main_thread.joinable())
        main_thread.join();

    SvsKeeperStorage::RequestForSession request_for_session;
    while (requests_queue->tryPopAny(request_for_session))
    {
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->error = Coordination::Error::ZSESSIONEXPIRED;
        responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request_for_session.session_id, response});
    }
}

void SvsKeeperCommitProcessor::commit(Request request)
{
    if (!shutdown_called)
    {
        committed_queue.push(request);
        {
            std::unique_lock lk(mutex);
            cv.notify_all();
        }
        LOG_DEBUG(log, "commit notify committed_queue size {}", committed_queue.size());
    }
}

void SvsKeeperCommitProcessor::onError(bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid)
{
    if (!shutdown_called)
    {
        {
            std::unique_lock lk(mutex);
            ErrorRequest error_request{accepted, error_code, session_id, xid};
            errors.emplace(UInt128(session_id, xid), error_request);
        }
        cv.notify_all();
    }
}

void SvsKeeperCommitProcessor::initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_, UInt64 operation_timeout_ms_)
{
    operation_timeout_ms = operation_timeout_ms_;
    thread_count = thread_count_;
    server = server_;
    service_keeper_storage_dispatcher = service_keeper_storage_dispatcher_;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
    request_thread = std::make_shared<ThreadPool>(thread_count_);
    for (size_t i = 0; i < thread_count; i++)
    {
        thread_pending_requests[i];
    }
    main_thread = ThreadFromGlobalPool([this] { run(); });
}

}
