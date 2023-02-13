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

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <atomic>
#include <common/types.h>
#include <string_view>

/** Allows to count number of simultaneously happening error codes.
  * See also Exception.cpp for incrementing part.
  */

namespace RK
{

namespace ErrorCodes
{
    /// ErrorCode identifier (index in array).
    using ErrorCode = size_t;
    using Value = int;

    /// Get name of error_code by identifier.
    /// Returns statically allocated string.
    std::string_view getName(ErrorCode error_code);

    /// ErrorCode identifier -> current value of error_code.
    extern std::atomic<Value> values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Add value for specified error_code.
    inline void increment(ErrorCode error_code)
    {
        if (error_code >= end())
        {
            /// For everything outside the range, use END.
            /// (end() is the pointer pass the end, while END is the last value that has an element in values array).
            error_code = end() - 1;
        }
        values[error_code].fetch_add(1, std::memory_order_relaxed);
    }
}

}
