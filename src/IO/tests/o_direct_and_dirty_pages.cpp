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
#include <string>

#include <iostream>

#include <common/types.h>
#include <common/getPageSize.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>

int main(int, char **)
{
    using namespace RK;

    try
    {
        static const size_t N = 100000;
        static const size_t BUF_SIZE = 1048576;
        size_t page_size = static_cast<size_t>(::getPageSize());

        ReadBufferFromFile rand_in("/dev/urandom");
        unsigned rand = 0;
        readBinary(rand, rand_in);

        String test = "Hello, world! " + toString(rand);

        /// Write to file as usual, read with O_DIRECT.

        {
            WriteBufferFromFile wb("test1", BUF_SIZE);
            for (size_t i = 0; i < N; ++i)
                writeStringBinary(test, wb);
            wb.next();
        }

        {
            ReadBufferFromFile rb("test1", BUF_SIZE, O_RDONLY | O_DIRECT, nullptr, page_size);
            String res;
            for (size_t i = 0; i < N; ++i)
                readStringBinary(res, rb);

            std::cerr << "test: " << test << ", res: " << res << ", bytes: " << rb.count() << std::endl;
        }

        /// Write to file with O_DIRECT, read as usual.

        {
            WriteBufferFromFile wb("test2", BUF_SIZE, O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT, 0666, nullptr, page_size);

            for (size_t i = 0; i < N; ++i)
                writeStringBinary(test, wb);

            if (wb.offset() % page_size != 0)
            {
                size_t pad = page_size - wb.offset() % page_size;
                memset(wb.position(), 0, pad);
                wb.position() += pad;
            }

            wb.next();
        }

        {
            ReadBufferFromFile rb("test2", BUF_SIZE);
            String res;
            for (size_t i = 0; i < N; ++i)
                readStringBinary(res, rb);

            std::cerr << "test: " << test << ", res: " << res << ", bytes: " << rb.count() << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.what() << ", " << e.displayText() << std::endl;
        return 1;
    }

    return 0;
}
