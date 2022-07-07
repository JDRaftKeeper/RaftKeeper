#pragma once

#include <Common/ConcurrentBoundedQueue.h>
#include <Service/NuRaftStateMachine.h>

namespace DB
{

struct RequestsQueue
{
    using Queue = ConcurrentBoundedQueue<SvsKeeperStorage::RequestForSession>;
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

    bool push(const SvsKeeperStorage::RequestForSession & request)
    {
        return queues[request.session_id % queues.size()]->push(std::forward<const SvsKeeperStorage::RequestForSession>(request));
    }

    bool tryPush(const SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
    {
        return queues[request.session_id % queues.size()]->tryPush(
            std::forward<const SvsKeeperStorage::RequestForSession>(request), wait_ms);
    }

    bool pop(size_t queue_id, SvsKeeperStorage::RequestForSession & request)
    {
        assert(queue_id != 0 && queue_id <= queues.size());
        return queues[queue_id]->pop(request);
    }

    bool tryPop(size_t queue_id, SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPop(request, wait_ms);
    }

    bool tryPopMicro(size_t queue_id, SvsKeeperStorage::RequestForSession & request, UInt64 wait_micro = 0)
    {
        assert(queue_id < queues.size());
        return queues[queue_id]->tryPopMicro(request, wait_micro);
    }


    bool tryPopAny(SvsKeeperStorage::RequestForSession & request, UInt64 wait_ms = 0)
    {
        for (const auto & queue : queues)
        {
            if (queue->tryPop(request, wait_ms))
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

    bool empty() const
    {
        return size() == 0;
    }
};

}
