
#include <Service/SvsKeeperDispatcher.h>
#include <Service/SvsKeeperFollowerProcessor.h>

namespace DB
{

void SvsKeeperFollowerProcessor::processRequest(Request request_for_session)
{
    requests_queue->push(request_for_session);
}

void SvsKeeperFollowerProcessor::run(size_t thread_idx)
{
    while (!shutdown_called)
    {
        UInt64 max_wait = session_sync_period_ms;
        if (session_sync_idx == thread_idx)
        {
            auto elapsed_milliseconds = session_sync_time_watch.elapsedMilliseconds();
            max_wait = elapsed_milliseconds >= session_sync_period_ms ? 0 : session_sync_period_ms - elapsed_milliseconds;
        }

        SvsKeeperStorage::RequestForSession request_for_session;

        if (requests_queue->tryPop(thread_idx, request_for_session, max_wait))
        {
            try
            {
                if (!server->isLeader() && server->isLeaderAlive())
                {
                    auto client = server->getLeaderClient(thread_idx);
                    if (client)
                    {
                        {
                            std::lock_guard<std::mutex> lock(*mutexes[thread_idx]);
                            thread_requests.find(thread_idx)->second[request_for_session.session_id].emplace(request_for_session.request->xid, request_for_session);
                        }
                        client->send(request_for_session);
                    }
                    else
                    {
                        LOG_WARNING(log, "Not found client for {} {}", server->getLeader(), thread_idx);
                    }
                }
                else
                    throw Exception("Raft no leader", ErrorCodes::RAFT_ERROR);
            }
            catch (...)
            {
                svskeeper_commit_processor->onError(false, nuraft::cmd_result_code::CANCELLED, request_for_session.session_id, request_for_session.request->xid, request_for_session.request->getOpNum());
            }
        }

        if (session_sync_idx == thread_idx && session_sync_time_watch.elapsedMilliseconds() >= session_sync_period_ms)
        {
            if (!server->isLeader() && server->isLeaderAlive())
            {
                /// sned ping
                try
                {
                    auto client = server->getLeaderClient(thread_idx);
                    if (client)
                    {
                        auto session_to_expiration_time = server->getKeeperStateMachine()->getStorage().sessionToExpirationTime();
                        service_keeper_storage_dispatcher->localSessions(session_to_expiration_time);
                        LOG_DEBUG(log, "Has {} session expiration time to send", session_to_expiration_time.size());
                        if (!session_to_expiration_time.empty())
                            client->sendPing(session_to_expiration_time);
                    }
                    else
                    {
                        LOG_WARNING(log, "Not found client for {} {}", server->getLeader(), thread_idx);
                    }
                }
                catch (...)
                {
                    ///
                }
            }

            session_sync_time_watch.restart();

            if (thread_idx + 1 == thread_count)
                session_sync_idx = 0;
            else
                session_sync_idx++;
        }
    }
}

void SvsKeeperFollowerProcessor::runRecive(size_t thread_idx)
{
    while (!shutdown_called)
    {
        try
        {
            /// timeout?
            {
                auto session_xid_request = thread_requests.find(thread_idx)->second;
                std::lock_guard<std::mutex> lock(*mutexes[thread_idx]);
                for (auto it = session_xid_request.begin(); it != session_xid_request.end();)
                {
                    for (auto requests_it = it->second.begin(); requests_it != it->second.end();)
                    {
                        using namespace std::chrono;
                        int64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                        int64_t timeout = server->getSessionTimeout(it->first);
                        if ((requests_it->second.time + timeout) > now) /// timeout
                        {
                            svskeeper_commit_processor->onError(
                                false,
                                nuraft::cmd_result_code::TIMEOUT,
                                it->first,
                                requests_it->first,
                                requests_it->second.request->getOpNum());
                            it->second.erase(requests_it);
                        }
                        else
                        {
                            ++requests_it;
                        }
                    }

                    if (it->second.empty())
                        session_xid_request.erase(it);
                    else
                        ++it;
                }
            }

            UInt64 max_wait = session_sync_period_ms;
            ForwardResponse response;
            if (!server->isLeader() && server->isLeaderAlive())
            {
                auto client = server->getLeaderClient(thread_idx);
                if (client)
                {
                    if (!client->poll(max_wait))
                        continue;

                    client->recive(response);

                    if (response.protocol == Result && !response.accepted && response.session_id != ForwardResponse::non_session_id)
                    {
                        LOG_WARNING(log, "Recive forward response session {}, xid {}, error code {}", response.session_id, response.xid, response.error_code);
                        svskeeper_commit_processor->onError(response.accepted, nuraft::cmd_result_code(response.error_code), response.session_id, response.xid, response.opnum);
                    }

                    auto session_xid_request = thread_requests.find(thread_idx)->second;
                    if (response.protocol == Result && session_xid_request.contains(response.session_id))
                    {
                        {
                            std::lock_guard<std::mutex> lock(mutexes[thread_idx]);
                            session_xid_request.find(response.session_id)->second.erase(response.xid);
                        }
                    }
                }
                else
                {
                    LOG_WARNING(log, "Not found client for {} {}", server->getLeader(), thread_idx);
                }
            }
            else
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }
        catch (...)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
}

void SvsKeeperFollowerProcessor::shutdown()
{
    if (shutdown_called)
        return;

    shutdown_called = true;

    request_thread->wait();
    response_thread->wait();

    SvsKeeperStorage::RequestForSession request_for_session;
    while (requests_queue->tryPopAny(request_for_session))
    {
        /// TODO ?
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
            svskeeper_commit_processor->onError(false, nuraft::cmd_result_code::CANCELLED, request_for_session.session_id, request_for_session.request->xid, request_for_session.request->getOpNum());
        }
    }
}

void SvsKeeperFollowerProcessor::initialize(size_t thread_count_, std::shared_ptr<SvsKeeperServer> server_, std::shared_ptr<SvsKeeperDispatcher> service_keeper_storage_dispatcher_, UInt64 session_sync_period_ms_)
{
    thread_count = thread_count_;
    mutexes.resize(thread_count);

    for (size_t i = 0; i < thread_count; i++)
    {
        mutexes.emplace_back(std::make_shared<std::mutex>());
    }

    session_sync_period_ms = session_sync_period_ms_;
    server = server_;
    service_keeper_storage_dispatcher = service_keeper_storage_dispatcher_;
    requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
    request_thread = std::make_shared<ThreadPool>(thread_count);
    for (size_t i = 0; i < thread_count; i++)
    {
        request_thread->trySchedule([this, i] { run(i); });
    }

    response_thread = std::make_shared<ThreadPool>(thread_count);
    for (size_t i = 0; i < thread_count; i++)
    {
        response_thread->trySchedule([this, i] { runRecive(i); });
    }

    for (size_t i = 0; i < thread_count; i++)
    {
        thread_requests[i];
    }
}

}
