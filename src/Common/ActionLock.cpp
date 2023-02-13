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
#include "ActionLock.h"
#include <Common/ActionBlocker.h>


namespace RK
{

ActionLock::ActionLock(const ActionBlocker & blocker) : counter_ptr(blocker.counter)
{
    if (auto counter = counter_ptr.lock())
        ++(*counter);
}

ActionLock::ActionLock(ActionLock && other)
{
    *this = std::move(other);
}

ActionLock & ActionLock::operator=(ActionLock && other)
{
    auto lock_lhs = this->counter_ptr.lock();

    counter_ptr = std::move(other.counter_ptr);
    /// After move other.counter_ptr still points to counter, reset it explicitly
    other.counter_ptr.reset();

    if (lock_lhs)
        --(*lock_lhs);

    return *this;
}

}
