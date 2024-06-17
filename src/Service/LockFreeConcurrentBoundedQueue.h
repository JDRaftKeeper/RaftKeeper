#pragma once

#include <atomic>
#include <chrono>
#include <vector>


/**
 * A lock free concurrent bounded queue which can only serve one consumer and one producer.
 */
template <typename T>
class LockFreeConcurrentBoundedQueue
{
public:
    explicit LockFreeConcurrentBoundedQueue(size_t capacity = 10000);

    void push(const T & item);
    void pop(T & item);

    bool tryPush(const T & item, uint64_t timeout_ms);
    bool tryPop(T & item, uint64_t timeout_ms);

    size_t size() const;
    bool empty() const;

    void stop();

private:
    std::vector<T> buffer;
    std::atomic<size_t> head;
    std::atomic<size_t> tail;

    std::atomic_flag pop_lock = ATOMIC_FLAG_INIT;
    std::atomic_flag push_lock = ATOMIC_FLAG_INIT;

    size_t capacity;
    std::atomic<size_t> count;

    std::atomic_bool stopped{false};
};

template <typename T>
LockFreeConcurrentBoundedQueue<T>::LockFreeConcurrentBoundedQueue(size_t capacity_)
    : buffer(capacity_), head(0), tail(0), count(0), capacity(capacity_)
{
}

template <typename T>
void LockFreeConcurrentBoundedQueue<T>::push(const T & item)
{
    if (stopped.load())
        return;

    size_t current_tail = tail.load();
    size_t next_tail = (current_tail + 1) % capacity;

    // Queue is full
    if (next_tail == head.load())
    {
        push_lock.wait(false);
    }

    if (stopped.load())
        return;

    buffer[current_tail] = item;
    tail.store(next_tail);

    count++;

    /// notify pop thread
    pop_lock.test_and_set();
    pop_lock.notify_one();
}

template <typename T>
bool LockFreeConcurrentBoundedQueue<T>::tryPush(const T & item, uint64_t timeout_ms)
{
    using namespace std::chrono;

    auto start_time = steady_clock::now();
    milliseconds timeout(timeout_ms);

    size_t current_tail = tail.load();
    size_t next_tail = (current_tail + 1) % capacity;

    // Queue is full
    while (next_tail == head.load())
    {
        if (steady_clock::now() - start_time > timeout || stopped.load())
            return false;
        /// Sleep a shot period to avoid high CPU usage when spin waiting.
        /// For Macbook Pro 10us takes 10%, 100us 2%, 1000us 0.2%
        std::this_thread::sleep_for(std::chrono::microseconds (1000));
    }

    buffer[current_tail] = item;
    tail.store(next_tail);

    count++;

    /// notify pop thread
    pop_lock.test_and_set();
    pop_lock.notify_one();

    return true;
}

template <typename T>
void LockFreeConcurrentBoundedQueue<T>::pop(T & item)
{
    if (stopped.load())
        return;

    size_t current_head = head.load();

    // Queue is empty
    if (current_head == tail.load())
    {
        pop_lock.wait(false);
    }

    if (stopped.load())
        return;

    item = std::move(buffer[current_head]);
    head.store((current_head + 1) % capacity);

    count--;

    // notify push thread
    push_lock.test_and_set();
    push_lock.notify_one();
}


template <typename T>
bool LockFreeConcurrentBoundedQueue<T>::tryPop(T & item, uint64_t timeout_ms)
{
    using namespace std::chrono;

    auto start_time = steady_clock::now();
    milliseconds timeout(timeout_ms);

    size_t current_head = head.load();

    // Queue is empty
    while (current_head == tail.load())
    {
        if (steady_clock::now() - start_time > timeout || stopped.load())
            return false;
        /// Sleep a shot period to avoid high CPU usage when spin waiting.
        /// For Macbook Pro 10us takes 10%, 100us 2%, 1000us 0.2%
        std::this_thread::sleep_for(std::chrono::microseconds (1000));
    }

    item = buffer[current_head];
    head.store((current_head + 1) % capacity);

    count--;

    // notify push thread
    push_lock.test_and_set();
    push_lock.notify_one();

    return true;
}

template <typename T>
size_t LockFreeConcurrentBoundedQueue<T>::size() const
{
    return count;
}

template <typename T>
bool LockFreeConcurrentBoundedQueue<T>::empty() const
{
    return count == 0;
}

template <typename T>
void LockFreeConcurrentBoundedQueue<T>::stop()
{
    stopped.store(true);
}

