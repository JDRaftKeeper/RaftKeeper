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

#include <Common/Arena.h>
#include <common/unaligned.h>


namespace RK
{

/** Can allocate memory objects of fixed size with deletion support.
  * For small `object_size`s allocated no less than pointer size.
  */
class SmallObjectPool
{
private:
    const size_t object_size;
    Arena pool;
    char * free_list = nullptr;

public:
    SmallObjectPool(size_t object_size_)
        : object_size{std::max(object_size_, sizeof(char *))}
    {
    }

    char * alloc()
    {
        if (free_list)
        {
            char * res = free_list;
            free_list = unalignedLoad<char *>(free_list);
            return res;
        }

        return pool.alloc(object_size);
    }

    void free(char * ptr)
    {
        unalignedStore<char *>(ptr, free_list);
        free_list = ptr;
    }

    /// The size of the allocated pool in bytes
    size_t size() const
    {
        return pool.size();
    }

};

}
