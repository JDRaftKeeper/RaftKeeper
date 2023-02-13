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
#include <cstdlib>
#include <cstring>
#include <vector>
#include <thread>


void thread_func()
{
    for (size_t i = 0; i < 100; ++i)
    {
        size_t size = 4096;

        void * buf = malloc(size);
        if (!buf)
            abort();
        memset(buf, 0, size);

        while (size < 1048576)
        {
            size_t next_size = size * 4;

            void * new_buf = realloc(buf, next_size);
            if (!new_buf)
                abort();
            buf = new_buf;

            memset(reinterpret_cast<char*>(buf) + size, 0, next_size - size);
            size = next_size;
        }

        free(buf);
    }
}


int main(int, char **)
{
    std::vector<std::thread> threads(16);
    for (size_t i = 0; i < 1000; ++i)
    {
        for (auto & thread : threads)
            thread = std::thread(thread_func);
        for (auto & thread : threads)
            thread.join();
    }
    return 0;
}
