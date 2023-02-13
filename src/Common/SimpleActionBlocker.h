/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <atomic>


namespace RK
{

class SimpleActionLock;


/// Similar to ActionBlocker, but without weak_ptr magic
class SimpleActionBlocker
{
    using Counter = std::atomic<int>;
    Counter counter = 0;

public:

    SimpleActionBlocker() = default;

    bool isCancelled() const { return counter > 0; }

    /// Temporarily blocks corresponding actions (while the returned object is alive)
    friend class SimpleActionLock;
    inline SimpleActionLock cancel();

    /// Cancel the actions forever.
    void cancelForever() { ++counter; }
};


/// Blocks related action while a SimpleActionLock instance exists
class SimpleActionLock
{
    SimpleActionBlocker * block = nullptr;

public:

    SimpleActionLock() = default;

    explicit SimpleActionLock(SimpleActionBlocker & block_) : block(&block_)
    {
        ++block->counter;
    }

    SimpleActionLock(const SimpleActionLock &) = delete;

    SimpleActionLock(SimpleActionLock && rhs) noexcept
    {
        *this = std::move(rhs);
    }

    SimpleActionLock & operator=(const SimpleActionLock &) = delete;

    SimpleActionLock & operator=(SimpleActionLock && rhs) noexcept
    {
        if (block)
            --block->counter;

        block = rhs.block;
        rhs.block = nullptr;

        return *this;
    }

    ~SimpleActionLock()
    {
        if (block)
            --block->counter;
    }
};


SimpleActionLock SimpleActionBlocker::cancel()
{
    return SimpleActionLock(*this);
}

}
