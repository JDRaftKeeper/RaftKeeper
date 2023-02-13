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
#include <IO/LimitReadBuffer.h>

#include <Common/Exception.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}


bool LimitReadBuffer::nextImpl()
{
    assert(position() >= in->position());

    /// Let underlying buffer calculate read bytes in `next()` call.
    in->position() = position();

    if (bytes >= limit)
    {
        if (throw_exception)
            throw Exception("Limit for LimitReadBuffer exceeded: " + exception_message, ErrorCodes::LIMIT_EXCEEDED);
        else
            return false;
    }

    if (!in->next())
    {
        working_buffer = in->buffer();
        return false;
    }

    working_buffer = in->buffer();

    if (limit - bytes < working_buffer.size())
        working_buffer.resize(limit - bytes);

    return true;
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer * in_, bool owns, UInt64 limit_, bool throw_exception_, std::string exception_message_)
    : ReadBuffer(in_ ? in_->position() : nullptr, 0)
    , in(in_)
    , owns_in(owns)
    , limit(limit_)
    , throw_exception(throw_exception_)
    , exception_message(std::move(exception_message_))
{
    assert(in);

    size_t remaining_bytes_in_buffer = in->buffer().end() - in->position();
    if (remaining_bytes_in_buffer > limit)
        remaining_bytes_in_buffer = limit;

    working_buffer = Buffer(in->position(), in->position() + remaining_bytes_in_buffer);
}


LimitReadBuffer::LimitReadBuffer(ReadBuffer & in_, UInt64 limit_, bool throw_exception_, std::string exception_message_)
    : LimitReadBuffer(&in_, false, limit_, throw_exception_, exception_message_)
{
}


LimitReadBuffer::LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, UInt64 limit_, bool throw_exception_, std::string exception_message_)
    : LimitReadBuffer(in_.release(), true, limit_, throw_exception_, exception_message_)
{
}


LimitReadBuffer::~LimitReadBuffer()
{
    /// Update underlying buffer's position in case when limit wasn't reached.
    if (!working_buffer.empty())
        in->position() = position();

    if (owns_in)
        delete in;
}

}
