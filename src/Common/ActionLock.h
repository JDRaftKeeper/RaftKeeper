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
#include <memory>
#include <atomic>


namespace RK
{

class ActionBlocker;
using StorageActionBlockType = size_t;

/// Blocks related action while a ActionLock instance exists
/// ActionBlocker could be destroyed before the lock, in this case ActionLock will safely do nothing in its destructor
class ActionLock
{
public:

    ActionLock() = default;

    explicit ActionLock(const ActionBlocker & blocker);

    ActionLock(ActionLock && other);
    ActionLock & operator=(ActionLock && other);

    ActionLock(const ActionLock & other) = delete;
    ActionLock & operator=(const ActionLock & other) = delete;

    bool expired() const
    {
        return counter_ptr.expired();
    }

    ~ActionLock()
    {
        if (auto counter = counter_ptr.lock())
            --(*counter);
    }

private:
    using Counter = std::atomic<int>;
    using CounterWeakPtr = std::weak_ptr<Counter>;

    CounterWeakPtr counter_ptr;
};

}
