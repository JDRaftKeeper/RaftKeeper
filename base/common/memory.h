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

#include <new>
#include "defines.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC || JEMALLOC_VERSION_MAJOR < 4
#    include <cstdlib>
#endif


namespace Memory
{

inline ALWAYS_INLINE void * newImpl(std::size_t size)
{
    auto * ptr = malloc(size);
    if (likely(ptr != nullptr))
        return ptr;

    /// @note no std::get_new_handler logic implemented
    throw std::bad_alloc{};
}

inline ALWAYS_INLINE void * newNoExept(std::size_t size) noexcept
{
    return malloc(size);
}

inline ALWAYS_INLINE void deleteImpl(void * ptr) noexcept
{
    free(ptr);
}

#if USE_JEMALLOC && JEMALLOC_VERSION_MAJOR >= 4

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size) noexcept
{
    if (unlikely(ptr == nullptr))
        return;

    sdallocx(ptr, size, 0);
}

#else

inline ALWAYS_INLINE void deleteSized(void * ptr, std::size_t size [[maybe_unused]]) noexcept
{
    free(ptr);
}

#endif

}
