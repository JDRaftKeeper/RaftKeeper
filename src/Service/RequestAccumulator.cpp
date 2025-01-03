#include <Common/setThreadName.h>

#include <Service/KeeperDispatcher.h>
#include <Service/RequestAccumulator.h>
#include <Service/Metrics.h>

namespace RK
{

void RequestAccumulator::push(const RequestForSession & request_for_session)
{
    requests_queue->push(request_for_session);
}


void RequestAccumulator::run()
{
    setThreadName("ReqAccumulator");

    NuRaftResult result;

    RequestsForSessions to_append_batch;
    UInt64 max_wait = std::min(static_cast<uint64_t>(1000), operation_timeout_ms);

    while (!shutdown_called)
    {
        RequestForSession request_for_session;

        bool pop_success;
        if (to_append_batch.empty())
        {
            pop_success = requests_queue->tryPop(request_for_session, max_wait);
        }
        else
        {
            if (!requests_queue->tryPop(request_for_session))
            {
                Metrics::getMetrics().log_replication_batch_size->add(to_append_batch.size());
                result = server->pushRequestBatch(to_append_batch);
                waitResultAndHandleError(result, to_append_batch);
                result.reset();
                to_append_batch.clear();
                continue;
            }
            pop_success = true;
        }

        if (pop_success)
        {
            request_for_session.process_time = getCurrentWallTimeMilliseconds();
            to_append_batch.emplace_back(request_for_session);

            if (to_append_batch.size() >= max_batch_size)
            {
                Metrics::getMetrics().log_replication_batch_size->add(to_append_batch.size());
                result = server->pushRequestBatch(to_append_batch);
                waitResultAndHandleError(result, to_append_batch);
                result.reset();
                to_append_batch.clear();
            }
        }
    }
}

bool RequestAccumulator::waitResultAndHandleError(NuRaftResult prev_result, const RequestsForSessions & prev_batch)
{
    /// Forcefully process all previous pending requests

    if (!prev_result->has_result())
        prev_result->get();

    bool result_accepted = prev_result->get_accepted();

    for (const auto & request_session : prev_batch)
    {
        if (request_session.isForwardRequest())
        {
            auto request = ForwardRequestFactory::instance().convertFromRequest(request_session);
            ForwardResponsePtr response = request->makeResponse();
            response->setAppendEntryResult(result_accepted, prev_result->get_result_code());

            keeper_dispatcher->invokeForwardResponseCallBack({request_session.server_id, request_session.client_id}, response);
        }
        else if (!result_accepted || prev_result->get_result_code() != nuraft::cmd_result_code::OK)
        {
            request_processor->onError(
                result_accepted,
                prev_result->get_result_code(),
                request_session.session_id,
                request_session.request->xid,
                request_session.request->getOpNum());
        }
    }

    return result_accepted && prev_result->get_result_code() == nuraft::cmd_result_code::OK;
}

void RequestAccumulator::shutdown()
{
    if (shutdown_called)
        return;

    LOG_INFO(log, "Shutting down request accumulator!");
    shutdown_called = true;

    RequestForSession request_for_session;
    while (requests_queue->tryPop(request_for_session))
    {
        request_processor->onError(
            false,
            nuraft::cmd_result_code::CANCELLED,
            request_for_session.session_id,
            request_for_session.request->xid,
            request_for_session.request->getOpNum());
    }
}

void RequestAccumulator::initialize(
    std::shared_ptr<KeeperDispatcher> keeper_dispatcher_,
    std::shared_ptr<KeeperServer> server_,
    UInt64 operation_timeout_ms_,
    UInt64 max_batch_size_)
{
    keeper_dispatcher = keeper_dispatcher_;
    operation_timeout_ms = operation_timeout_ms_;
    max_batch_size = max_batch_size_;
    server = server_;
    requests_queue = std::make_shared<ConcurrentBoundedQueue<RequestForSession>>(20000);
    request_thread = ThreadFromGlobalPool([this] { run(); });
}

}
