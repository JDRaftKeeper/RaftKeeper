/**
 * Copyright 2021-2023 JD.com, Inc.
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

#include <Service/KeeperDispatcher.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace RK
{

void RequestProcessor::push(RequestForSession request_for_session)
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

void RequestProcessor::run()
{
    while (!shutdown_called)
    {
        try
        {
            auto need_wait = [&]() -> bool { return errors.empty() && requests_queue->empty() && committed_queue.empty(); };

            {
                using namespace std::chrono_literals;
                std::unique_lock lk(mutex);
                if (!cv.wait_for(lk, operation_timeout_ms * 1ms, [&] { return !need_wait() || shutdown_called; }))
                    LOG_DEBUG(
                        log,
                        "wait time out errors size {}, requests_queue size {}, committed_queue size {}",
                        errors.size(),
                        requests_queue->size(),
                        committed_queue.size());
            }

            if (shutdown_called)
                return;

            /// 1. process error requests
            processErrorRequest();

            size_t committed_request_size = committed_queue.size();

            /// 2. process read request, multi thread
            for (RunnerId runner_id = 0; runner_id < thread_count; runner_id++)
            {
                request_thread->trySchedule([this, runner_id] {
                    moveRequestToPendingQueue(runner_id);
                    processReadRequests(runner_id);
                });
            }
            request_thread->wait();

            /// 3. process committed request, single thread
            processCommittedRequest(committed_request_size);
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

    LOG_TRACE(log, "runner_id {} request_size {}", runner_id, request_size);
    for (size_t i = 0; i < request_size; ++i)
    {
        RequestForSession request;
        if (requests_queue->tryPop(runner_id, request))
        {
            auto op_num = request.request->getOpNum();
            if (op_num != Coordination::OpNum::Auth)
            {
                LOG_TRACE(log, "put session {} xid {} to pending requests", request.session_id, request.request->xid);
                thread_requests[request.session_id].push_back(request);
            }
        }
    }
}


void RequestProcessor::processCommittedRequest(size_t count)
{
    LOG_DEBUG(log, "committed_request_size {}", count);
    RequestForSession committed_request;
    for (size_t i = 0; i < count; ++i)
    {
        if (committed_queue.peek(committed_request))
        {
            auto & pending_requests_for_thread = pending_requests.find(getThreadIndex(committed_request.session_id))->second;

            LOG_DEBUG(
                log,
                "current_session_pending_requests size {} committed_request opNum {}, session {} xid {} request {}",
                pending_requests_for_thread.contains(committed_request.session_id)
                    ? pending_requests_for_thread[committed_request.session_id].size()
                    : 0,
                Coordination::toString(committed_request.request->getOpNum()),
                toHexString(committed_request.session_id),
                committed_request.request->xid,
                committed_request.request->toString());

            auto op_num = committed_request.request->getOpNum();

            /// Remote requests
            if (!keeper_dispatcher->isLocalSession(committed_request.session_id)
                || op_num == Coordination::OpNum::Auth)
            {
                LOG_DEBUG(log, "Not contains session {}", committed_request.session_id);
                if (pending_requests_for_thread.contains(committed_request.session_id))
                {
                    LOG_WARNING(
                        log,
                        "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                        "Just delete from pending queue",
                        toHexString(committed_request.session_id));
                    pending_requests_for_thread.erase(committed_request.session_id);
                }
                applyRequest(committed_request);
                committed_queue.pop();
            }
            /// Local requests
            else
            {
                bool has_read_request = false;
                bool found_error = false;
                auto & pending_requests_for_session = pending_requests_for_thread[committed_request.session_id];
                if (!pending_requests_for_session.empty())
                {
                    LOG_DEBUG(
                        log,
                        "Current session pending request opNum {}, session {}, xid {}",
                        Coordination::toString(pending_requests_for_session.begin()->request->getOpNum()),
                        pending_requests_for_session.begin()->session_id,
                        pending_requests_for_session.begin()->request->xid);

                    while (pending_requests_for_session.begin()->request->xid != committed_request.request->xid)
                    {
                        auto current_begin_request_session = pending_requests_for_session.begin();
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
                        else if (
                            current_begin_request_session->request->getOpNum() == Coordination::OpNum::Close
                            && committed_request.request->getOpNum() == Coordination::OpNum::Close)
                        {
                            break;
                        }
                        else
                        {
                            std::unique_lock lk(mutex);
                            if (errors.contains(UInt128(
                                    current_begin_request_session->session_id, current_begin_request_session->request->xid)))
                            {
                                LOG_WARNING(
                                    log,
                                    "Current session {} pending head request xid {} not same committed request xid {} opnum "
                                    "{}, "
                                    "because it is in errors",
                                    toHexString(committed_request.session_id),
                                    current_begin_request_session->request->xid,
                                    committed_request.request->xid,
                                    Coordination::toString(committed_request.request->getOpNum()));

                                found_error = true;
                                break;
                            }
                            else
                            {
                                /// TODO should exit？
                                LOG_WARNING(
                                    log,
                                    "Logic Error, maybe reconnected current session {} pending head request xid {} {} not same "
                                    "committed request xid {} {}, pending request size {}",
                                    committed_request.session_id,
                                    current_begin_request_session->request->xid,
                                    current_begin_request_session->request->toString(),
                                    committed_request.request->xid,
                                    committed_request.request->toString(),
                                    pending_requests_for_session.size());
                                break;
                            }
                        }
                    }
                }
                else
                {
                    LOG_WARNING(log, "Logic error, pending request for session {} is empty", toHexString(committed_request.session_id));
                }

                if (has_read_request || found_error)
                    break;

                applyRequest(committed_request);
                committed_queue.pop();

                for (auto it = pending_requests_for_session.begin(); it != pending_requests_for_session.end();)
                {
                    auto xid = it->request->xid;
                    auto opnum = it->request->getOpNum();
                    it = pending_requests_for_session.erase(it);
                    if (xid == committed_request.request->xid
                        || (opnum == Coordination::OpNum::Close
                            && committed_request.request->getOpNum() == Coordination::OpNum::Close))
                    {
                        break;
                    }
                }

                if (pending_requests_for_session.empty())
                    pending_requests_for_thread.erase(committed_request.session_id);
            }
        }
    }
}

void RequestProcessor::processErrorRequest()
{
    /// 1. handle error requests
    std::lock_guard lock(mutex);
    if (!errors.empty())
    {
        for (auto it = errors.begin(); it != errors.end();)
        {
            const auto & [session_id, xid] = it->first;
            auto & error_request = it->second;
            
            LOG_WARNING(log, "error {} session {}, xid {}", error_request.error_code, toHexString(session_id), xid);

            auto & pending_requests_for_thread = pending_requests.find(getThreadIndex(session_id))->second;

            if (!keeper_dispatcher->isLocalSession(session_id))
            {
                if (pending_requests_for_thread.contains(session_id))
                {
                    LOG_WARNING(
                        log,
                        "Found session {} in pending_queue while it is not local, maybe because of connection disconnected. "
                        "Just delete from pending queue",
                        toHexString(session_id));
                    pending_requests_for_thread.erase(session_id);
                }

                LOG_WARNING(log, "Not my session error, session {}, xid {}", toHexString(session_id), xid);
                it = errors.erase(it);
            }
            else
            {
                using namespace Coordination;
                std::optional<RequestForSession> request;
                if (static_cast<int32_t>(xid) == Coordination::AUTH_XID)
                {
                    ZooKeeperRequestPtr auth_request = std::make_shared<ZooKeeperAuthRequest>();
                    using namespace std::chrono;
                    int64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    RequestForSession request1{static_cast<int64_t>(session_id), auth_request, now, -1, -1};
                    request.emplace(request1);
                }
                else
                {
                    auto session_requests = pending_requests_for_thread.find(session_id);

                    if (session_requests != pending_requests_for_thread.end())
                    {
                        auto & requests = session_requests->second;
                        for (auto request_it = requests.begin(); request_it != requests.end();)
                        {
                            LOG_TRACE(
                                log,
                                "session {} pending request xid {}, target error xid {}",
                                session_id,
                                request_it->request->xid,
                                xid);
                            if (static_cast<uint64_t>(request_it->request->xid) < xid)
                            {
                                break;
                            }
                            else if (
                                static_cast<uint64_t>(request_it->request->xid) == xid
                                || (request_it->request->getOpNum() == Coordination::OpNum::Close
                                    && error_request.opnum == Coordination::OpNum::Close))
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
                    else
                    {
                        LOG_WARNING(log, "session {}, no pending requests", session_id);
                    }
                }

                if (request)
                {
                    ZooKeeperResponsePtr response = request->request->makeResponse();
                    response->xid = request->request->xid;
                    response->zxid = 0;
                    response->request_created_time_ms = request->create_time;

                    auto accepted = error_request.accepted;
                    auto error_code = error_request.error_code;

                    response->error = error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                     : Coordination::Error::ZCONNECTIONLOSS;

                    responses_queue.push(RK::KeeperStore::ResponseForSession{static_cast<int64_t>(session_id), response});

                    LOG_ERROR(
                        log,
                        "Make error response for session {}, xid {}, opNum {}",
                        session_id,
                        response->xid,
                        error_request.opnum);

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
                        log,
                        "Not found error request session {}, xid {} from pending queue. Maybe it is still in the request queue "
                        "and will be processed next time",
                        session_id,
                        xid);
                    break;
                }
            }
        }
    }
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
    try
    {
        LOG_TRACE(
            log,
            "Apply request session {} xid {} request {}",
            toHexString(request.session_id),
            request.request->xid,
            request.request->toString());

        if (!server->isLeaderAlive() && request.request->isReadRequest())
        {
            auto response = request.request->makeResponse();

            response->request_created_time_ms = request.create_time;
            response->xid = request.request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZCONNECTIONLOSS;

            responses_queue.push(RK::KeeperStore::ResponseForSession{request.session_id, response});
        }
        /// Raft already committed the request, we must apply it/
        else
        {
            if (!server->isLeaderAlive())
                LOG_WARNING(log, "Apply write request but leader not alive.");
            server->getKeeperStateMachine()->getStore().processRequest(
                responses_queue, request.request, request.session_id, request.create_time, {}, true, false);
        }
    }
    catch (...)
    {
        tryLogCurrentException(
            log,
            fmt::format(
                "Got exception while process session {} read request {}.", toHexString(request.session_id), request.request->toString()));
    }
}

void RequestProcessor::shutdown()
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

    KeeperStore::RequestForSession request_for_session;
    while (requests_queue->tryPopAny(request_for_session))
    {
        auto response = request_for_session.request->makeResponse();
        response->xid = request_for_session.request->xid;
        response->zxid = 0;
        response->request_created_time_ms = request_for_session.create_time;
        response->error = Coordination::Error::ZSESSIONEXPIRED;
        responses_queue.push(RK::KeeperStore::ResponseForSession{request_for_session.session_id, response});
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
        LOG_DEBUG(log, "commit notify committed_queue size {}", committed_queue.size());
    }
}

void RequestProcessor::onError(
    bool accepted, nuraft::cmd_result_code error_code, int64_t session_id, Coordination::XID xid, Coordination::OpNum opnum)
{
    if (!shutdown_called)
    {
        LOG_WARNING(log, "on error session {}, xid {}", session_id, xid);
        {
            std::unique_lock lk(mutex);
            ErrorRequest error_request{accepted, error_code, session_id, xid, opnum};
            errors.emplace(UInt128(session_id, xid), error_request);
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
    thread_count = thread_count_;
    server = server_;
    keeper_dispatcher = keeper_dispatcher_;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
    request_thread = std::make_shared<ThreadPool>(thread_count_);
    for (size_t i = 0; i < thread_count; i++)
    {
        pending_requests[i];
    }
    main_thread = ThreadFromGlobalPool([this] { run(); });
}

}
