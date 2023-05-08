#pragma once

#include <Service/NuRaftStateMachine.h>
#include <boost/lockfree/queue.hpp>
#include <Common/ConcurrentBoundedQueue.h>

namespace RK
{

/**
 * User requests queue who is a compound queue. It is used as cache in execution
 * pipeline components.
 *
 *      1 KeeperDispatcher
 *      2 RequestForwarder
 *      3 RequestAccumulator
 *      4 Raft data replication (not use)
 *      5 RequestProcessor
 *
 * For high throughput, we setup n execution pipe in whole pipeline, execution
 * pipes can execute request in parallel.
 *
 * The whole requests execution pipeline looks like:
 *
 *        1        2        3         4         5
 *      -----    -----    -----               -----
 *      -----    -----    -----     -----     -----
 *      -----    -----    -----               -----
 * 1 KeeperDispatcher: receive user requests and dispatcher to RequestForwarder(write request)
 *   or RequestProcessor(read request).
 * 2 RequestForwarder: redirect request to leader.
 * 3 RequestAccumulator: accumulate request and send to Raft in batch.
 * 4 Raft data replication
 * 5 RequestProcessor: process user requests, for read requests
 */
struct RequestsQueue
{
    using Queue = ConcurrentBoundedQueue<KeeperStore::RequestForSession>;

    std::vector<ptr<Queue>> queues;

    explicit RequestsQueue(size_t child_queue_size, size_t capacity = 20000)
    {
        assert(child_queue_size > 0);
        assert(capacity > 0);

        queues.resize(child_queue_size);
        for (size_t i = 0; i < child_queue_size; i++)
        {
            queues[i] = std::make_shared<Queue>(std::max(1ul, capacity / child_queue_size));
        }
    }

    template <typename Request>
    bool push(Request && request)
    {
        return queues[request.session_id % queues.size()]->push(std::forward<Request>(request));
    }

    template <typename Request>
    bool tryPush(Request && request, UInt64 wait_ms = 0)
    {
        return queues[request.session_id % queues.size()]->tryPush(std::forward<Request>(request), wait_ms);
    }

    bool pop(size_t queue_id, KeeperStore::RequestForSession & request)
    {
        assert(queue_id != 0 && queue_id <= queues.size());
        return queues[queue_id]->pop(request);
    }

    bool tryPop(size_t queue_id, KeeperStore::RequestForSession & request, UInt64 wait_ms = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPop(request, wait_ms);
    }

    [[maybe_unused]] bool tryPopMicro(size_t queue_id, KeeperStore::RequestForSession & request, UInt64 wait_micro = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPopMicro(request, wait_micro);
    }


    bool tryPopAny(KeeperStore::RequestForSession & request, UInt64 wait_ms = 0)
    {
        for (const auto & queue : queues)
        {
            if (queue->tryPop(request, wait_ms * 1000))
                return true;
        }
        return false;
    }

    size_t size() const
    {
        size_t size{};
        for (const auto & queue : queues)
            size += queue->size();
        return size;
    }

    size_t size(size_t queue_id) const
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->size();
    }

    bool empty() const { return size() == 0; }
};

}
