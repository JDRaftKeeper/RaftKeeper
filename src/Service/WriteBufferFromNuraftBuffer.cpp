/**
 * Copyright 2016-2026 ClickHouse, Inc..
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
#include <Service/WriteBufferFromNuraftBuffer.h>
#include <common/logger_useful.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

void WriteBufferFromNuraftBuffer::nextImpl()
{
    if (is_finished)
        throw Exception("WriteBufferFromNuraftBuffer is finished", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

    /// pos may not be equal to vector.data() + old_size, because WriteBuffer::next() can be used to flush data
    size_t pos_offset = pos - reinterpret_cast<Position>(buffer->data_begin());
    size_t old_size = buffer->size();
    if (pos_offset == old_size)
    {
        nuraft::ptr<nuraft::buffer> new_buffer = nuraft::buffer::alloc(old_size * size_multiplier);
        memcpy(new_buffer->data_begin(), buffer->data_begin(), buffer->size());
        buffer = new_buffer;
    }
    internal_buffer = Buffer(
        reinterpret_cast<Position>(buffer->data_begin() + pos_offset), reinterpret_cast<Position>(buffer->data_begin() + buffer->size()));
    working_buffer = internal_buffer;
}

WriteBufferFromNuraftBuffer::WriteBufferFromNuraftBuffer() : WriteBuffer(nullptr, 0)
{
    buffer = nuraft::buffer::alloc(initial_size);
    set(reinterpret_cast<Position>(buffer->data_begin()), buffer->size());
}

void WriteBufferFromNuraftBuffer::finalize()
{
    if (is_finished)
        return;

    is_finished = true;
    size_t real_size = pos - reinterpret_cast<Position>(buffer->data_begin());
    nuraft::ptr<nuraft::buffer> new_buffer = nuraft::buffer::alloc(real_size);
    memcpy(new_buffer->data_begin(), buffer->data_begin(), real_size);
    buffer = new_buffer;

    /// Prevent further writes.
    set(nullptr, 0);
}

nuraft::ptr<nuraft::buffer> WriteBufferFromNuraftBuffer::getBuffer()
{
    finalize();
    return buffer;
}

WriteBufferFromNuraftBuffer::~WriteBufferFromNuraftBuffer()
{
    try
    {
        finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
