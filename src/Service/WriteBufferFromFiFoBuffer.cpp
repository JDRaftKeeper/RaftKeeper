/**
 * Copyright 2021-2023 JD.com, Inc.
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
#include "WriteBufferFromFiFoBuffer.h"


namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

void WriteBufferFromFiFoBuffer::nextImpl()
{
    if (is_finished)
        throw Exception("WriteBufferFromFiFoBuffer is finished", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

    /// pos may not be equal to vector.data() + old_size,
    /// because WriteBuffer::next() can be used to flush data
    size_t pos_offset = pos - reinterpret_cast<Position>(buffer->begin());
    size_t old_size = buffer->size();
    if (pos_offset == old_size)
    {
        /// Buffer need to expand
        auto new_buffer = std::make_shared<FIFOBuffer>(old_size * size_multiplier);
        memcpy(new_buffer->begin(), buffer->begin(), old_size);
        buffer = new_buffer;
    }
    internal_buffer = Buffer(
        reinterpret_cast<Position>(buffer->begin() + pos_offset),
        reinterpret_cast<Position>(buffer->begin() + buffer->size()));
    working_buffer = internal_buffer;
}

//WriteBufferFromFiFoBuffer::WriteBufferFromFiFoBuffer() : WriteBuffer(nullptr, 0)
//{
//    buffer = std::make_shared<FIFOBuffer>(initial_size);
//    set(reinterpret_cast<Position>(buffer->begin()), buffer->size());
//}

WriteBufferFromFiFoBuffer::WriteBufferFromFiFoBuffer(size_t size) : WriteBuffer(nullptr, 0)
{
    buffer = std::make_shared<FIFOBuffer>(size);
    set(reinterpret_cast<Position>(buffer->begin()), buffer->size());
}

void WriteBufferFromFiFoBuffer::finalize()
{
    if (is_finished)
        return;

    is_finished = true;
    size_t real_size = pos - reinterpret_cast<Position>(buffer->begin());
    auto new_buffer = std::make_shared<FIFOBuffer>(real_size);
    memcpy(new_buffer->begin(), buffer->begin(), real_size);
    if (real_size > 0)
        new_buffer->advance(real_size);
    buffer = new_buffer;
    set(nullptr, 0);
}

std::shared_ptr<FIFOBuffer> WriteBufferFromFiFoBuffer::getBuffer()
{
    finalize();
    return buffer;
}

WriteBufferFromFiFoBuffer::~WriteBufferFromFiFoBuffer()
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
