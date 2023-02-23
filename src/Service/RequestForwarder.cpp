
#include <Service/KeeperDispatcher.h>
#include <Service/RequestForwarder.h>
#include <Common/setThreadName.h>

namespace RK
{

void RequestForwarder::push(RequestForSession request_for_session)
{
    requests_queue->push(request_for_session);
}

void RequestForwarder::run(RunnerId runner_id)
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
                if (!server->isLeader() && server->isLeaderAlive())
                {
                    auto client = server->getLeaderClient(runner_id);
                    if (client)
                    {
                        client->send(request_for_session);
                    }
                    else
                    {
                        throw Exception("Not found client for runner " + std::to_string(runner_id), ErrorCodes::RAFT_FORWARDING_ERROR);
                    }
                }
                else
                    throw Exception("Raft no leader", ErrorCodes::RAFT_ERROR);
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
                            client->sendSession(session_to_expiration_time);
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
            ForwardResponse response;
            if (!server->isLeader() && server->isLeaderAlive())
            {
                auto client = server->getLeaderClient(runner_id);
                if (client && client->isConnected())
                {
                    if (!client->poll(max_wait * 1000))
                        continue;

                    client->receive(response);

                    if (!response.accepted)
                    {
                        /// common request
                        if (response.protocol == Result && response.session_id != ForwardResponse::non_session_id)
                        {
                            LOG_ERROR(
                                log,
                                "Receive failed forward response with type(Result), session {}, xid {}, error code {}",
                                response.session_id,
                                response.xid,
                                response.error_code);
                            request_processor->onError(
                                response.accepted,
                                static_cast<nuraft::cmd_result_code>(response.error_code),
                                response.session_id,
                                response.xid,
                                response.opnum);
                        }
                        else if (response.protocol == Session)
                        {
                            LOG_ERROR(
                                log,
                                "Receive failed forward response with type(Session), session {}, xid {}, error code {}",
                                response.session_id,
                                response.xid,
                                response.error_code);
                        }
                        else if (response.protocol == Handshake)
                        {
                            LOG_ERROR(
                                log,
                                "Receive failed forward response with type(Handshake), session {}, xid {}, error code {}",
                                response.session_id,
                                response.xid,
                                response.error_code);
                        }
                    }
                }
                else
                {
                    if (!client)
                        LOG_DEBUG(log, "Not found client for runner {}, maybe no session attached to me", runner_id);
                    else if (!client->isConnected())
                        LOG_DEBUG(log, "Client not connected for runner {}, maybe no session attached to me", runner_id);
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

void RequestForwarder::shutdown()
{
    LOG_INFO(log, "Shutting down request forwarder!");
    if (shutdown_called)
        return;

    shutdown_called = true;

    request_thread->wait();
    response_thread->wait();

    KeeperStore::RequestForSession request_for_session;
    while (requests_queue->tryPopAny(request_for_session))
    {
        try
        {
            auto client = server->getLeaderClient(0);
            if (client)
            {
                client->send(request_for_session);
            }
            else
            {
                LOG_WARNING(log, "Not found client for {} {}", server->getLeader(), 0);
            }
        }
        catch (...)
        {
            request_processor->onError(
                false,
                nuraft::cmd_result_code::CANCELLED,
                request_for_session.session_id,
                request_for_session.request->xid,
                request_for_session.request->getOpNum());
        }
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
    request_thread = std::make_shared<ThreadPool>(thread_count);

    for (size_t i = 0; i < thread_count; i++)
    {
        request_thread->trySchedule([this, i] { run(i); });
    }

    response_thread = std::make_shared<ThreadPool>(thread_count);
    for (RunnerId runner_id = 0; runner_id < thread_count; runner_id++)
    {
        response_thread->trySchedule([this, runner_id] { runReceive(runner_id); });
    }
}

}
