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

#include <Core/Defines.h>
#include <IO/WriteBuffer.h>
#include <common/itoa.h>

/// 40 digits or 39 digits and a sign
#define WRITE_HELPERS_MAX_INT_WIDTH 40U

namespace RK
{

namespace detail
{
    template <typename T>
    void NO_INLINE writeUIntTextFallback(T x, WriteBuffer & buf)
    {
        char tmp[WRITE_HELPERS_MAX_INT_WIDTH];
        int len = itoa(x, tmp) - tmp;
        buf.write(tmp, len);
    }
}

template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
    if (likely(reinterpret_cast<intptr_t>(buf.position()) + WRITE_HELPERS_MAX_INT_WIDTH < reinterpret_cast<intptr_t>(buf.buffer().end())))
        buf.position() = itoa(x, buf.position());
    else
        detail::writeUIntTextFallback(x, buf);
}

}
