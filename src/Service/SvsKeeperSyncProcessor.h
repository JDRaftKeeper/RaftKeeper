#pragma once

#include <Service/RequestsQueue.h>
#include <Service/SvsKeeperServer.h>
#include <Service/SvsKeeperCommitProcessor.h>

namespace DB
{

class SvsKeeperSyncProcessor
{
using Request = SvsKeeperStorage::RequestForSession;
using ThreadPoolPtr = std::shared_ptr<ThreadPool>;

public:

    SvsKeeperSyncProcessor(/*RequestsCommitEvent & requests_commit_event_,*/ std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor_)
        : /*requests_commit_event(requests_commit_event_),*/ svskeeper_commit_processor(svskeeper_commit_processor_)
    {
//        main_thread = ThreadFromGlobalPool([this] { run(); });
    }

    void processRequest(Request request_for_session)
    {
        requests_queue->push(request_for_session);
    }

    bool waitResultAndHandleError(nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> prev_result, const SvsKeeperStorage::RequestsForSessions & prev_batch)
    {
        /// Forcefully process all previous pending requests

        if (!prev_result->has_result())
            prev_result->get();

        bool result_accepted = prev_result->get_accepted();

        if (result_accepted && prev_result->get_result_code() == nuraft::cmd_result_code::OK)
        {
            return true;
        }
        else
        {

            for (auto & request_session : prev_batch)
            {
//                requests_commit_event.addError(request_session.session_id, request_session.request->xid, result_accepted, prev_result->get_result_code());
//                requests_commit_event.notifiy(request_session.session_id, request_session.request->xid);
                svskeeper_commit_processor->onError(request_session.session_id, request_session.request->xid, result_accepted, prev_result->get_result_code());
//                auto response = request_session.request->makeResponse();
//
//                response->xid = request_session.request->xid;
//                response->zxid = 0;
//
//                response->error = prev_result->get_result_code() == nuraft::cmd_result_code::TIMEOUT ? Coordination::Error::ZOPERATIONTIMEOUT
//                                                                                                     : Coordination::Error::ZCONNECTIONLOSS;
//
//                responses_queue.push(DB::SvsKeeperStorage::ResponseForSession{request_session.session_id, response});
            }

//            if (!result_accepted)
//                throw Exception(ErrorCodes::RAFT_ERROR,
//                                "Request batch is not accepted.");
//            else
//                throw Exception(ErrorCodes::RAFT_ERROR,
//                                "Request batch error, nuraft code {} and message: '{}'",
//                                prev_result->get_result_code(),
//                                prev_result->get_result_str());
            return false;
        }
    }

    void run(size_t thread_idx)
    {
        nuraft::ptr<nuraft::cmd_result<nuraft::ptr<nuraft::buffer>>> result = nullptr;
        /// Requests from previous iteration. We store them to be able
        /// to send errors to the client.
        //    SvsKeeperStorage::RequestsForSessions prev_batch;

        SvsKeeperStorage::RequestsForSessions to_append_batch;

        while (!shutdown_called)
        {
            //            UInt64 max_wait = UInt64(configuration_and_settings->coordination_settings->operation_timeout_ms.totalMilliseconds());
            UInt64 max_wait = 10000;

            SvsKeeperStorage::RequestForSession request_for_session;

            bool pop_succ = false;
            if (to_append_batch.empty())
            {
                pop_succ = requests_queue->tryPop(thread_idx, request_for_session, max_wait);
            }
            else
            {
                if (!requests_queue->tryPop(thread_idx, request_for_session))
                {
                    result = server->putRequestBatch(to_append_batch);
                    waitResultAndHandleError(result, to_append_batch);
                    result.reset();
                    to_append_batch.clear();

                    continue;
                }
                pop_succ = true;
            }

            if (pop_succ)
            {
                to_append_batch.emplace_back(request_for_session);

                if (to_append_batch.size() > 1000)
                {
                    result = server->putRequestBatch(to_append_batch);
                    waitResultAndHandleError(result, to_append_batch);
                    result.reset();
                    to_append_batch.clear();

                }
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
        while (requests_queue->tryPopAny(request_for_session))
        {
//            requests_commit_event.addError(request_for_session.session_id, request_for_session.request->xid, false, nuraft::cmd_result_code::CANCELLED);
//            requests_commit_event.notifiy(request_for_session.session_id, request_for_session.request->xid);
            svskeeper_commit_processor->onError(request_for_session.session_id, request_for_session.request->xid, false, nuraft::cmd_result_code::CANCELLED);
        }
    }

    void setRaftServer(std::shared_ptr<SvsKeeperServer> server_)
    {
        server = server_;
    }


    void initialize(size_t thread_count, std::shared_ptr<SvsKeeperServer> server_)
    {
        server = server_;
        requests_queue = std::make_shared<RequestsQueue>(thread_count, 20000);
        request_thread = std::make_shared<ThreadPool>(thread_count);
        for (size_t i = 0; i < thread_count; i++)
        {
            request_thread->trySchedule([this, i] { run(i); });
        }
    }


private:
    ptr<RequestsQueue> requests_queue;

    ThreadPoolPtr request_thread;

    ThreadFromGlobalPool main_thread;

    bool shutdown_called{false};

//    RequestsCommitEvent & requests_commit_event;

    std::shared_ptr<SvsKeeperServer> server;

    std::shared_ptr<SvsKeeperCommitProcessor> svskeeper_commit_processor;

//    SvsKeeperResponsesQueue & responses_queue;
};

}
