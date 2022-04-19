#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

class SvsKeeperCommitProcessor
{
using Request = SvsKeeperStorage::RequestForSession;

public:
    SvsKeeperCommitProcessor(
        RequestsCommitEvent & requests_commit_event_, SvsKeeperResponsesQueue & responses_queue_)
        : requests_queue(std::make_shared<RequestsQueue>(1, 20000)), requests_commit_event(requests_commit_event_), responses_queue(responses_queue_)
    {
        main_thread = ThreadFromGlobalPool([this] { run2(); });
    }

    void processRequest(Request request_for_session)
    {
        if (!shutdown_called) {
            requests_queue->push(request_for_session);
            cv.notify_all();
        }
    }

    void run()
    {
        while (!shutdown_called)
        {
//            UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());
            try
            {
                UInt64 max_wait = 10000;

                SvsKeeperStorage::RequestForSession request_for_session;
                if (!requests_queue->tryPop(0, request_for_session, max_wait))
                {
                    continue;
                }

                if (!request_for_session.request->isReadRequest())
                {
                    LOG_TRACE(log, "wait commit session {}, xid {}", request_for_session.session_id, request_for_session.request->xid);
                    requests_commit_event.waitForCommit(request_for_session.session_id, request_for_session.request->xid);
                    LOG_TRACE(log, "wait commit done session {}, xid {}", request_for_session.session_id, request_for_session.request->xid);
                }

                if (requests_commit_event.isError(request_for_session.session_id, request_for_session.request->xid))
                {
                    auto response = request_for_session.request->makeResponse();

                    response->xid = request_for_session.request->xid;
                    response->zxid = 0;

                    auto [accepted, error_code] = requests_commit_event.getError(request_for_session.session_id, request_for_session.request->xid);
                    response->error = error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                     : Coordination::Error::ZCONNECTIONLOSS;

                    responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request_for_session.session_id, response});

                    requests_commit_event.eraseError(request_for_session.session_id, request_for_session.request->xid);

                    if (!accepted)
                        throw Exception(ErrorCodes::RAFT_ERROR,
                                        "Request batch is not accepted.");
                    else
                        throw Exception(ErrorCodes::RAFT_ERROR,
                                        "Request batch error, nuraft code {}", error_code);

                }
                else /// TODO leader alive
                {
                    server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, request_for_session.request, request_for_session.session_id, {}, true, false);
                }
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    void run1()
    {
        while (!shutdown_called)
        {
            //            UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());
            std::optional<Request> nextPending;
            SvsKeeperStorage::RequestsForSessions toProcess;
            try
            {
                int len = toProcess.size();
                for (int i = 0; i < len; i++)
                {
                    server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, toProcess[i].request, toProcess[i].session_id, {}, true, false);
                }
                toProcess.clear();

                {
                    std::unique_lock lk(mutex);

                    cv.wait(lk, [&]{return !((requests_queue->size() == 0 || nextPending)
                                               && committed_queue.size() == 0);});

                    // First check and see if the commit came in for the pending
                    // request
                    if ((requests_queue->size() == 0 || nextPending)
                        && committed_queue.size() > 0) {
                        Request r;
                        committed_queue.tryPop(r);
                        /*
                     * We match with nextPending so that we can move to the
                     * next request when it is committed. We also want to
                     * use nextPending because it has the cnxn member set
                     * properly.
                     */
                        if (nextPending
                            && nextPending->session_id == r.session_id
                            && nextPending->request->xid == r.request->xid) {
                            // we want to send our version of the request.
                            // the pointer to the connection in the request
                            //                            nextPending.hdr = r.hdr;
                            //                            nextPending.txn = r.txn;
                            //                            nextPending.zxid = r.zxid;
                            toProcess.push_back(*nextPending);
                            nextPending.reset();
                        } else {
                            // this request came from someone else so just
                            // send the commit packet
                            toProcess.push_back(r);
                        }
                    }
                }


                // We haven't matched the pending requests, so go back to
                // waiting
                if (nextPending) {
                    continue;
                }

                {
                    std::unique_lock lk(mutex);
                    // Process the next requests in the queuedRequests
                    while (!nextPending && requests_queue->size() > 0)
                    {
                        //                        Request request = queuedRequests.remove();
                        Request request;
                        requests_queue->tryPop(0, request);
                        if (!request.request->isReadRequest())
                        {
                            nextPending = request;
                        }
                        else
                        {
                            toProcess.push_back(request);
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

    /// for each requests_queue and committed_queue, if requests_queue pop a write request need wait it appear in the committed_queue.
    /// So for each requests_queue pop a read request process it, wait write request commit, for each committed_queue find current wait write request and process other request.
    void run2()
    {
        while (!shutdown_called)
        {
            //            UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());
            std::optional<Request> pending_write_request;

            try
            {
                auto need_wait = [&]()-> bool
                {
                    bool wait = false;
                    if (pending_write_request)
                    {
                        if (requests_commit_event.isError(pending_write_request->session_id, pending_write_request->request->xid))
                            wait = false;

                        if (committed_queue.empty())
                        {
                            wait = true;
                        }
                    }
                    else
                    {
                        if (requests_queue->empty() || committed_queue.empty())
                            wait = true;
                    }
                    return wait;
                };

                {
                    std::unique_lock lk(mutex);

                    cv.wait(lk, [&]{ return !need_wait() || shutdown_called; });
                }

                if (shutdown_called)
                    return;

                if (pending_write_request)
                {
                    if (requests_commit_event.isError(pending_write_request->session_id, pending_write_request->request->xid))
                    {
                        auto response = pending_write_request->request->makeResponse();

                        response->xid = pending_write_request->request->xid;
                        response->zxid = 0;

                        auto [accepted, error_code] = requests_commit_event.getError(pending_write_request->session_id, pending_write_request->request->xid);
                        response->error = error_code == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
                                                                                         : Coordination::Error::ZCONNECTIONLOSS;

                        responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{pending_write_request->session_id, response});

                        requests_commit_event.eraseError(pending_write_request->session_id, pending_write_request->request->xid);

                        if (!accepted)
                            throw Exception(ErrorCodes::RAFT_ERROR,
                                            "Request batch is not accepted.");
                        else
                            throw Exception(ErrorCodes::RAFT_ERROR,
                                            "Request batch error, nuraft code {}", error_code);

                    }
                }


                size_t committed_request_size = committed_queue.size();
                size_t request_size = requests_queue->size();

                Request request;
                if (!pending_write_request)
                {
                    for (size_t i = 0; i < request_size; ++i)
                    {
                        if (requests_queue->tryPop(0, request))
                        {
                            if (request.request->isReadRequest())
                            {
                                server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, request.request, request.session_id, {}, true, false);
                            }
                            else
                            {
                                pending_write_request = request;
                                break;
                            }
                        }
                    }
                }

                Request committed_request;
                for (size_t i = 0; i < committed_request_size; ++i)
                {
                    if (committed_queue.tryPop(committed_request))
                    {
                        if (pending_write_request && committed_request.request->xid == request.request->xid
                            && committed_request.session_id == request.session_id)
                        {
                            server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, request.request, request.session_id, {}, true, false);
                            pending_write_request.reset();
                            break;
                        }
                        else
                        {
                            server->getKeeperStateMachine()->getStorage().processRequest(responses_queue, request.request, request.session_id, {}, true, false);
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

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;
        cv.notify_all();

        if (main_thread.joinable())
            main_thread.join();

        SvsKeeperStorage::RequestForSession request_for_session;
        while (requests_queue->tryPop(0,request_for_session))
        {
            auto response = request_for_session.request->makeResponse();
            response->xid = request_for_session.request->xid;
            response->zxid = 0;
            response->error = Coordination::Error::ZSESSIONEXPIRED;
            responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request_for_session.session_id, response});
        }
    }

    void setRaftServer(std::shared_ptr<SvsKeeperServer> server_)
    {
        server = server_;
    }

    void commit(Request request) {
        if (!shutdown_called) {
            committed_queue.push(request);
            cv.notify_all();
        }
    }

    void notifyOnError() {
        if (!shutdown_called) {
            cv.notify_all();
        }
    }

private:
    ptr<RequestsQueue> requests_queue;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

//    size_t thread_index;

    RequestsCommitEvent & requests_commit_event;

    std::shared_ptr<SvsKeeperServer> server;

    SvsKeeperResponsesQueue & responses_queue;

    SvsKeeperThreadSafeQueue<SvsKeeperStorage::RequestForSession> committed_queue;

    std::mutex mutex;

    std::condition_variable cv;

    Poco::Logger * log;
};

}
