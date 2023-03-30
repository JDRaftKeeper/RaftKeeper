
#include <Service/KeeperDispatcher.h>
#include <Service/RequestForwarder.h>
#include <Common/setThreadName.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int RAFT_FORWARDING_ERROR;
    extern const int RAFT_ERROR;
}

void RequestForwarder::push(RequestForSession request_for_session)
{
    requests_queue->push(request_for_session);
}

void RequestForwarder::runSend(RunnerId runner_id)
{
    setThreadName(("ReqFwdSend-" + toString(runner_id)).c_str());

    LOG_DEBUG(log, "Starting forwarding request sending thread.");
    while (!shutdown_called)
    {
        UInt64 max_wait = session_sync_period_ms;
        if (session_sync_idx == runner_id)
        {
            auto elapsed_milliseconds = session_sync_time_watch.elapsedMilliseconds();
            max_wait = elapsed_milliseconds >= session_sync_period_ms ? 0 : session_sync_period_ms - elapsed_milliseconds;
        }

        KeeperStore::RequestForSession request_for_session;

        if (requests_queue->tryPop(runner_id, request_for_session, max_wait))
        {
            try
            {
                if (server->isLeader())
                {
                    LOG_WARNING(log, "A leader switch may have occurred suddenly");
                    throw Exception("Can't forward request", ErrorCodes::RAFT_ERROR);
                }

                if (!server->isLeaderAlive())
                    throw Exception("Raft no leader", ErrorCodes::RAFT_ERROR);

                auto client = server->getLeaderClient(runner_id);

                if (!client)
                    throw Exception("Not found client for runner " + std::to_string(runner_id), ErrorCodes::RAFT_FORWARDING_ERROR);

                ForwardRequestPtr forward_request = ForwardRequestFactory::instance().convertFromRequest(request_for_session);
                forward_request->send_time = clock::now();
                client->send(forward_request);

                forwarding_queues[runner_id]->push(forward_request);
            }
            catch (...)
            {
                tryLogCurrentException(log, "error forward request to leader for runner " + std::to_string(runner_id));
                request_processor->onError(
                    false,
                    nuraft::cmd_result_code::FAILED,
                    request_for_session.session_id,
                    request_for_session.request->xid,
                    request_for_session.request->getOpNum());
            }
        }

        if (session_sync_idx == runner_id && session_sync_time_watch.elapsedMilliseconds() >= session_sync_period_ms)
        {
            if (!server->isLeader() && server->isLeaderAlive())
            {
                /// send sessions
                try
                {
                    auto client = server->getLeaderClient(runner_id);
                    if (client)
                    {
                        /// TODO if keeper nodes time has large gap something will be wrong.
                        auto session_to_expiration_time = server->getKeeperStateMachine()->getStore().sessionToExpirationTime();
                        keeper_dispatcher->filterLocalSessions(session_to_expiration_time);
                        LOG_DEBUG(log, "Has {} local sessions to send", session_to_expiration_time.size());
                        if (!session_to_expiration_time.empty())
                        {
                            ForwardRequestPtr forward_request = std::make_shared<ForwardSessionRequest>(std::move(session_to_expiration_time));
                            client->send(forward_request);
                            forwarding_queues[runner_id]->push(forward_request);
                        }
                    }
                    else
                    {
                        throw Exception(
                            "Not found client when sending sessions for runner " + std::to_string(runner_id),
                            ErrorCodes::RAFT_FORWARDING_ERROR);
                    }
                }
                catch (...)
                {
                    tryLogCurrentException(log, "error forward session to leader for runner " + std::to_string(runner_id));
                }
            }

            session_sync_time_watch.restart();
            session_sync_idx++;
            session_sync_idx = session_sync_idx % thread_count;
        }
    }
}

void RequestForwarder::runReceive(RunnerId runner_id)
{
    setThreadName(("ReqFwdRecv-" + toString(runner_id)).c_str());

    LOG_DEBUG(log, "Starting forwarding response receiving thread.");
    while (!shutdown_called)
    {
        try
        {
            UInt64 max_wait = session_sync_period_ms;
            clock::time_point now = clock::now();

            ForwardRequestPtr earliest_request;
            if (forwarding_queues[runner_id]->peek(earliest_request))
            {
                auto earliest_request_deadline = earliest_request->send_time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                if (now > earliest_request_deadline)
                {
                    if (processTimeoutRequest(runner_id, earliest_request))
                        earliest_request_deadline = earliest_request->send_time + std::chrono::microseconds(operation_timeout.totalMicroseconds());
                }

                max_wait = std::min(max_wait, static_cast<UInt64>(std::chrono::duration_cast<std::chrono::microseconds>(earliest_request_deadline - now).count()));
            }

            if (!server->isLeader() && server->isLeaderAlive())
            {
                auto client = server->getLeaderClient(runner_id);
                if (client && client->isConnected())
                {
                    if (!client->poll(max_wait * 1000))
                    {
                        continue;
                    }

                    ForwardResponsePtr response;
                    client->receive(response);
                    processResponse(runner_id, response);
                }
                else
                {
                    if (!client)
                        LOG_DEBUG(log, "Not found client for runner {}, maybe no session attached to me", runner_id);
                    else if (!client->isConnected())
                        LOG_TRACE(log, "Client not connected for runner {}, maybe no session attached to me", runner_id);
                    std::this_thread::sleep_for(std::chrono::milliseconds(session_sync_period_ms));
                }
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(session_sync_period_ms));
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "Error when receiving forwarding response, runner " + std::to_string(runner_id));
            std::this_thread::sleep_for(std::chrono::milliseconds(session_sync_period_ms));
        }
    }
}

bool RequestForwarder::processTimeoutRequest(RunnerId runner_id, ForwardRequestPtr newFront)
{
    clock::time_point now = clock::now();

    auto func = [this, now](const ForwardRequestPtr & request) -> bool
    {
        clock::time_point earliest_send_time = request->send_time;
        auto earliest_operation_deadline
            = clock::time_point(earliest_send_time) + std::chrono::microseconds(operation_timeout.totalMicroseconds());
        if (now > earliest_operation_deadline)
        {
            ForwardResponsePtr response = request->makeResponse();
            response->onError(*this); /// timeout
            return true;
        }
        else
        {
            return false;
        }
    };

    return forwarding_queues[runner_id]->removeFrontIf(func, newFront);
}


void RequestForwarder::removeFromQueue(RunnerId runner_id, ForwardResponsePtr forward_response_ptr)
{
    forwarding_queues[runner_id]->findAndRemove([forward_response_ptr](const ForwardRequestPtr & reuqest) -> bool
    {
        if (reuqest->forwardType() != forward_response_ptr->forwardType())
            return false;

        return forward_response_ptr->match(reuqest);
    });
}


void RequestForwarder::processResponse(RunnerId runner_id, ForwardResponsePtr forward_response_ptr)
{
    removeFromQueue(runner_id, forward_response_ptr);

    if (forward_response_ptr->accepted)
        return;

    /// common request
    LOG_ERROR(log, "Receive failed forward response {}", forward_response_ptr->toString());

    forward_response_ptr->onError(*this); /// for GetSession UpdateSession Op, maybe peer not accepted or raft not accepted
}

void RequestForwarder::shutdown()
{
    LOG_INFO(log, "Shutting down request forwarder!");
    if (shutdown_called)
        return;

    shutdown_called = true;

    request_thread->wait();
    response_thread->wait();

    for (auto & forwarding_queue : forwarding_queues)
    {
        forwarding_queue->forEach([this](const ForwardRequestPtr & request) -> bool
        {
            ForwardResponsePtr response = request->makeResponse();
            response->onError(*this); /// shutdown
            return true;
        });
    }

    KeeperStore::RequestForSession request_for_session;
    while (requests_queue->tryPopAny(request_for_session))
    {
        request_processor->onError(
            false,
            nuraft::cmd_result_code::CANCELLED,
            request_for_session.session_id,
            request_for_session.request->xid,
            request_for_session.request->getOpNum());
    }
}

void RequestForwarder::initialize(
    size_t thread_count_,
    std::shared_ptr<KeeperServer> server_,
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
    UInt64 session_sync_period_ms_)
{
    thread_count = thread_count_;
    session_sync_period_ms = session_sync_period_ms_;
    server = server_;
    keeper_dispatcher = keeper_dispatcher_;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);

    for (RunnerId runner_id = 0; runner_id < thread_count; runner_id++)
    {
        forwarding_queues.push_back(std::make_unique<ForwardingQueue>());
    }

//    session_sync_thread = ThreadFromGlobalPool(&RequestForwarder::runSessionSync, this);
    //connections.

    request_thread = std::make_shared<ThreadPool>(thread_count);
    for (RunnerId runner_id = 0; runner_id < thread_count; runner_id++)
    {
        request_thread->trySchedule([this, runner_id] { runSend(runner_id); });
    }

    response_thread = std::make_shared<ThreadPool>(thread_count);
    for (RunnerId runner_id = 0; runner_id < thread_count; runner_id++)
    {
        response_thread->trySchedule([this, runner_id] { runReceive(runner_id); });
    }
}

}
