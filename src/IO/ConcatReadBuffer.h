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

#include <vector>

#include <IO/ReadBuffer.h>


namespace RK
{

/** Reads from the concatenation of multiple ReadBuffers
  */
class ConcatReadBuffer : public ReadBuffer
{
public:
    using ReadBuffers = std::vector<ReadBuffer *>;

protected:
    ReadBuffers buffers;
    ReadBuffers::iterator current;

    bool nextImpl() override
    {
        if (buffers.end() == current)
            return false;

        /// First reading
        if (working_buffer.empty())
        {
            if ((*current)->hasPendingData())
            {
                working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
                return true;
            }
        }
        else
            (*current)->position() = position();

        if (!(*current)->next())
        {
            ++current;
            if (buffers.end() == current)
                return false;

            /// We skip the filled up buffers; if the buffer is not filled in, but the cursor is at the end, then read the next piece of data.
            while ((*current)->eof())
            {
                ++current;
                if (buffers.end() == current)
                    return false;
            }
        }

        working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
        return true;
    }

public:
    explicit ConcatReadBuffer(const ReadBuffers & buffers_) : ReadBuffer(nullptr, 0), buffers(buffers_), current(buffers.begin())
    {
        assert(!buffers.empty());
    }

    ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2) : ConcatReadBuffer({&buf1, &buf2}) {}
};

}
