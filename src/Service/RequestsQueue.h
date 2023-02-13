#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Service/NuRaftStateMachine.h>
#include <boost/lockfree/queue.hpp>

namespace RK
{

struct RequestsQueue
{
    using Queue = ConcurrentBoundedQueue<KeeperStore::RequestForSession>;

//    using Queue = boost::lockfree::queue<KeeperStore::RequestForSession>;

//    using Queue = BlockingConcurrentQueue<KeeperStore::RequestForSession>;

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

    bool push(const KeeperStore::RequestForSession & request)
    {
        return queues[request.session_id % queues.size()]->push(std::forward<const KeeperStore::RequestForSession>(request));
//        return queues[request.session_id % queues.size()]->enqueue(std::forward<const KeeperStore::RequestForSession>(request));
    }

    bool tryPush(const KeeperStore::RequestForSession & request, UInt64 wait_ms = 0)
    {
        return queues[request.session_id % queues.size()]->tryPush(
            std::forward<const KeeperStore::RequestForSession>(request), wait_ms);
//        return queues[request.session_id % queues.size()]->try_enqueue(
//            std::forward<const KeeperStore::RequestForSession>(request));
    }

    bool pop(size_t queue_id, KeeperStore::RequestForSession & request)
    {
        assert(queue_id != 0 && queue_id <= queues.size());
        return queues[queue_id]->pop(request);
//        return queues[queue_id]->try_dequeue<KeeperStore::RequestForSession>(request);
    }

    bool tryPop(size_t queue_id, KeeperStore::RequestForSession & request, UInt64 wait_ms = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPop(request, wait_ms);
//        return queues[queue_id]->wait_dequeue_timed(request, wait_ms * 1000);
    }

    bool tryPopMicro(size_t queue_id, KeeperStore::RequestForSession & request, UInt64 wait_micro = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPopMicro(request, wait_micro);
//        return queues[queue_id]->wait_dequeue_timed(request, wait_micro);
    }


    bool tryPopAny(KeeperStore::RequestForSession & request, UInt64 wait_ms = 0)
    {
        for (const auto & queue : queues)
        {
//            if (queue->wait_dequeue_timed(request, wait_ms * 1000))
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
//        size_t size{};
//        for (const auto & queue : queues)
//            size += queue->size_approx();
//        return size;
    }

    size_t size(size_t queue_id) const
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->size();
//        assert(queue_id < queues.size());
//        return queues[queue_id]->size_approx();
    }

    bool empty() const
    {
        return size() == 0;
    }
};

}
