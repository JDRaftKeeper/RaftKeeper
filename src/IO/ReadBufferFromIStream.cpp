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
#include <IO/ReadBufferFromIStream.h>
#include <Common/Exception.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_ISTREAM;
}

bool ReadBufferFromIStream::nextImpl()
{
    istr.read(internal_buffer.begin(), internal_buffer.size());
    size_t gcount = istr.gcount();

    if (!gcount)
    {
        if (istr.eof())
            return false;

        if (istr.fail())
            throw Exception("Cannot read from istream at offset " + std::to_string(count()), ErrorCodes::CANNOT_READ_FROM_ISTREAM);

        throw Exception("Unexpected state of istream at offset " + std::to_string(count()), ErrorCodes::CANNOT_READ_FROM_ISTREAM);
    }
    else
        working_buffer.resize(gcount);

    return true;
}

ReadBufferFromIStream::ReadBufferFromIStream(std::istream & istr_, size_t size)
    : BufferWithOwnMemory<ReadBuffer>(size), istr(istr_)
{
}

}
