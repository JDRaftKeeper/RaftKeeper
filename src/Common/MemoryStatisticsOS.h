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
#if defined(OS_LINUX)
#include <cstdint>


namespace RK
{

/** Opens a file /proc/self/mstat. Keeps it open and reads memory statistics via 'pread'.
  * This is Linux specific.
  * See: man procfs
  *
  * Note: a class is used instead of a single function to avoid excessive file open/close on every use.
  * pread is used to avoid lseek.
  *
  * Actual performance is from 1 to 5 million iterations per second.
  */
class MemoryStatisticsOS
{
public:
    /// In number of bytes.
    struct Data
    {
        uint64_t virt;
        uint64_t resident;
        uint64_t shared;
        uint64_t code;
        uint64_t data_and_stack;
    };

    MemoryStatisticsOS();
    ~MemoryStatisticsOS();

    /// Thread-safe.
    Data get() const;

private:
    int fd;
};

}

#endif
