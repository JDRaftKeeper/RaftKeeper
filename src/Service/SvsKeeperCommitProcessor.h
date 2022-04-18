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
        : requests_queue(std::make_shared<RequestsQueue>(1, 20000)), requests_commit_event(requests_commit_event_), responses_queue(responses_queue_), log(&Poco::Logger::get("SvsKeeperCommitProcessor"))
    {
        main_thread = ThreadFromGlobalPool([this] { run(); });
    }

    void processRequest(Request request_for_session)
    {
        requests_queue->push(request_for_session);
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

    void shutdown()
    {
        if (shutdown_called)
            return;

        shutdown_called = true;

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

private:
    ptr<RequestsQueue> requests_queue;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

//    size_t thread_index;

    RequestsCommitEvent & requests_commit_event;

    std::shared_ptr<SvsKeeperServer> server;

    SvsKeeperResponsesQueue & responses_queue;

    Poco::Logger * log;
};
}
