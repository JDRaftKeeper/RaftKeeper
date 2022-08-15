
#include <Service/SvsKeeperCommitProcessor.h>

namespace DB
{

void SvsKeeperCommitProcessor::processRequest(Request request_for_session)
{
    if (!shutdown_called) {
        requests_queue->push(request_for_session);
        cv.notify_all();
    }
}

void SvsKeeperCommitProcessor::run()
{
    while (!shutdown_called)
    {
        try
        {
            auto need_wait = [&]() -> bool {
                if (errors.empty() /*&& pending_requests.empty()*/ && requests_queue->empty() && committed_queue.empty())
                    return true;

                return false;
            };

            {
                using namespace std::chrono_literals;
                std::unique_lock lk(mutex);
                cv.wait_for(lk, operation_timeout_ms * 1ms, [&] { return !need_wait() || shutdown_called; });
            }

            if (shutdown_called)
                return;

            {
                std::lock_guard lock(errors_mutex);
                if (!errors.empty())
                {
                    for (auto it = errors.begin(); it != errors.end();)
                    {
                        auto & [session_id, xid] = it->first;

                        LOG_TRACE(log, "error session {}, xid {}", session_id, xid);

                        auto & error_request = it->second;
                        auto op_num = error_request.request.request->getOpNum();

                        std::optional<Request> request;
                        if (op_num == Coordination::OpNum::Close || op_num == Coordination::OpNum::Heartbeat || op_num == Coordination::OpNum::Auth)
                        {
                            request = error_request.request;
                        }
                        else
                        {
                            auto & pending_requests = thread_pending_requests.find(session_id % thread_count)->second;
                            auto & requests = pending_requests.find(session_id)->second;


                            for (auto request_it = requests.begin(); request_it != requests.end();)
                            {
                                if (uint64_t(request_it->request->xid) == xid)
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

                            auto & pending_write_requests = thread_pending_write_requests.find(session_id % thread_count)->second;
                            auto & w_requests = pending_write_requests.find(session_id)->second;
                            for (auto w_request_it = w_requests.begin(); w_request_it != w_requests.end();)
                            {
                                if (uint64_t(w_request_it->request->xid) == xid)
                                {
                                    w_request_it = w_requests.erase(w_request_it);
                                    break;
                                }
                                else
                                {
                                    ++w_request_it;
                                }
                            }
                        }

                        if (request)
                        {
                            auto response = request->request->makeResponse();

                            response->xid = request->request->xid;
                            response->zxid = 0;

                            auto accepted = it->second.accepted;
                            auto error_code = it->second.error_code;

                            response->error = error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                             : Coordination::Error::ZCONNECTIONLOSS;

                            responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request->session_id, response});

                            it = errors.erase(it);

                            if (!accepted)
                                throw Exception(ErrorCodes::RAFT_ERROR, "Request batch is not accepted.");
                            else
                                throw Exception(ErrorCodes::RAFT_ERROR, "Request batch error, nuraft code {}", error_code);
                        }
                        else
                        {
                            LOG_WARNING(
                                log, "Not found error request. Maybe it is still in the request queue and will be processed next time");
                            break;
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

                LOG_TRACE(log, "committed_request_size {}", committed_request_size);
                Request committed_request;
                for (size_t i = 0; i < committed_request_size; ++i)
                {
                    if (committed_queue.peek(committed_request))
                    {
                        auto & pending_requests = thread_pending_requests.find(committed_request.session_id % thread_count)->second;
                        auto & pending_write_requests
                            = thread_pending_write_requests.find(committed_request.session_id % thread_count)->second;

                        LOG_TRACE(
                            log,
                            "committed_request opNum {}, session {}, xid {}",
                            Coordination::toString(committed_request.request->getOpNum()),
                            committed_request.session_id,
                            committed_request.request->xid);

                        auto op_num = committed_request.request->getOpNum();
                        auto & current_session_pending_w_requests = pending_write_requests[committed_request.session_id];
                        /// another server session request or can be out of order
                        if (current_session_pending_w_requests.empty() || op_num == Coordination::OpNum::Close || op_num == Coordination::OpNum::Heartbeat || op_num == Coordination::OpNum::Auth)
                        {
                            server->getKeeperStateMachine()->getStorage().processRequest(
                                responses_queue,
                                committed_request.request,
                                committed_request.session_id,
                                committed_request.time,
                                {},
                                true,
                                false);
                            committed_queue.pop();
                        }
                        else
                        {
                            LOG_TRACE(
                                log,
                                "current_session_pending_w_request opNum {}, session {}, xid {}",
                                Coordination::toString(current_session_pending_w_requests.begin()->request->getOpNum()),
                                current_session_pending_w_requests.begin()->session_id,
                                current_session_pending_w_requests.begin()->request->xid);

                            if (current_session_pending_w_requests.begin()->request->xid != committed_request.request->xid)
                                throw Exception(
                                    ErrorCodes::RAFT_ERROR,
                                    "Logic Error, current session {} pending head write request xid {} {} not same committed request "
                                    "xid {} {}",
                                    committed_request.session_id,
                                    current_session_pending_w_requests.begin()->request->xid,
                                    current_session_pending_w_requests.begin()->request->getOpNum(),
                                    committed_request.request->xid,
                                    committed_request.request->getOpNum());

                            auto & current_session_pending_requests = pending_requests[committed_request.session_id];
                            //                                while (current_session_pending_requests.begin()->request->xid < committed_request.request->xid)
                            //                                {
                            //                                    server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, current_session_pending_requests[0].request, current_session_pending_requests[0].session_id, {}, true, false);
                            //                                    current_session_pending_requests.erase(current_session_pending_requests.begin());
                            //                                }
                            LOG_TRACE(
                                log,
                                "Current session pending request opNum {}, session {}, xid {}",
                                Coordination::toString(current_session_pending_requests.begin()->request->getOpNum()),
                                current_session_pending_requests.begin()->session_id,
                                current_session_pending_requests.begin()->request->xid);

                            if (current_session_pending_requests.begin()->request->xid != committed_request.request->xid) /// read request
                                break;

                            server->getKeeperStateMachine()->getStorage().processRequest(
                                responses_queue,
                                committed_request.request,
                                committed_request.session_id,
                                committed_request.time,
                                {},
                                true,
                                false);
                            committed_queue.pop();

                            current_session_pending_w_requests.erase(current_session_pending_w_requests.begin());
                            current_session_pending_requests.erase(current_session_pending_requests.begin());

                            if (current_session_pending_w_requests.empty())
                                pending_write_requests.erase(committed_request.session_id);

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
    auto & pending_write_requests = thread_pending_write_requests.find(thread_idx)->second;
    auto & pending_requests = thread_pending_requests.find(thread_idx)->second;

    size_t request_size = requests_queue->size(thread_idx);

    LOG_TRACE(log, "thread_idx {} request_size {}", thread_idx, request_size);
    for (size_t i = 0; i < request_size; ++i)
    {
        Request request;
        if (requests_queue->tryPop(thread_idx, request))
        {
            auto op_num = request.request->getOpNum();
            if (op_num != Coordination::OpNum::Close && op_num != Coordination::OpNum::Heartbeat && op_num != Coordination::OpNum::Auth)
            {
                pending_requests[request.session_id].push_back(request);
                if (!request.request->isReadRequest())
                {
                    pending_write_requests[request.session_id].push_back(request);
                }
            }
        }
    }

    /// process every session, until encountered write request
    for (auto it = pending_requests.begin(); it != pending_requests.end();)
    {
        auto current_session_id = it->first;

        auto & requests = it->second;
        for (auto requets_it = requests.begin(); requets_it != requests.end();)
        {
            if (pending_write_requests[current_session_id].empty() || requets_it->request->xid != pending_write_requests[current_session_id].begin()->request->xid)
            {
                /// read request
                if (!requets_it->request->isReadRequest())
                    throw Exception(ErrorCodes::RAFT_ERROR, "Logic Error, request requried read request, session {}, xid {}", requets_it->session_id, requets_it->request->xid);

                server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, requets_it->request, requets_it->session_id, requets_it->time, {}, true, false);
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

void SvsKeeperCommitProcessor::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;
    cv.notify_all();

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
    if (!shutdown_called) {
        committed_queue.push(request);
        cv.notify_all();
    }
}

void SvsKeeperCommitProcessor::onError(bool accepted, nuraft::cmd_result_code error_code, Request request)
{
    if (!shutdown_called) {
        std::lock_guard lock(errors_mutex);
        ErrorRequest error_request{accepted, error_code, request};
        errors.emplace(UInt128(request.session_id, request.request->xid), error_request);
        cv.notify_all();
    }
}

void SvsKeeperCommitProcessor::initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, UInt64 operation_timeout_ms_)
{
    operation_timeout_ms = operation_timeout_ms_;
    thread_count = thread_count_;
    server = server_;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
    request_thread = std::make_shared<ThreadPool>(thread_count_);
    for (size_t i = 0; i < thread_count; i++)
    {
        thread_pending_write_requests[i];
        thread_pending_requests[i];
    }
    main_thread = ThreadFromGlobalPool([this] { run(); });
}

}
