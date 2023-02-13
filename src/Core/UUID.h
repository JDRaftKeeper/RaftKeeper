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

#include <Common/UInt128.h>
#include <common/strong_typedef.h>
#include <Common/thread_local_rng.h>

namespace RK
{

STRONG_TYPEDEF(UInt128, UUID)

namespace UUIDHelpers
{
    inline UUID generateV4()
    {
        UInt128 res{thread_local_rng(), thread_local_rng()};
        res.low = (res.low & 0xffffffffffff0fffull) | 0x0000000000004000ull;
        res.high = (res.high & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        return UUID{res};
    }

    const UUID Nil = UUID(UInt128(0, 0));
}

}
