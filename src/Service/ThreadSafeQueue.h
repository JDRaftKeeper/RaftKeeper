#pragma once

#include <deque>
#include <mutex>

namespace RK
{

/// Queue with mutex and condvar. As simple as possible.
template <typename T>
class ThreadSafeQueue
{
private:
    mutable std::mutex queue_mutex;
    std::condition_variable cv;
    std::deque<T> queue;
public:


    using Func = std::function<bool(const T & e)>;

    void push(const T & response)
    {
        std::lock_guard lock(queue_mutex);
        queue.push_back(response);
        cv.notify_one();
    }

    bool peek(T & response, int64_t timeout_ms = 0)
    {
        std::unique_lock lock(queue_mutex);
        if (!cv.wait_for(lock,
                         std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); }))
            return false;

        response = queue.front();
        return true;
    }

    void pop()
    {
        std::unique_lock lock(queue_mutex);
        queue.pop();
    }

    bool tryPop(T & response, int64_t timeout_ms = 0)
    {
        std::unique_lock lock(queue_mutex);
        if (!cv.wait_for(lock,
                         std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); }))
            return false;

        response = queue.front();
        queue.pop_front();
        return true;
    }

    bool peek(T & response)
    {
        std::unique_lock lock(queue_mutex);
        if (queue.empty())
            return false;
        response = queue.front();
        return true;
    }

    bool remove()
    {
        std::unique_lock lock(queue_mutex);
        if (queue.empty())
            return false;
        queue.pop_front();
        return true;
    }

    void forEach(Func func)
    {
        std::unique_lock lock(queue_mutex);

        for (const auto & e : queue)
        {
            if (!func(e))
                break;
        }
    }

    size_t size() const
    {
        std::lock_guard lock(queue_mutex);
        return queue.size();
    }

    bool empty() const
    {
        return size() == 0;
    }
};

}
