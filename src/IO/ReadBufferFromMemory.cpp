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
#include "ReadBufferFromMemory.h"

namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

off_t ReadBufferFromMemory::seek(off_t offset, int whence)
{
    if (whence == SEEK_SET)
    {
        if (offset >= 0 && working_buffer.begin() + offset < working_buffer.end())
        {
            pos = working_buffer.begin() + offset;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else if (whence == SEEK_CUR)
    {
        Position new_pos = pos + offset;
        if (new_pos >= working_buffer.begin() && new_pos < working_buffer.end())
        {
            pos = new_pos;
            return size_t(pos - working_buffer.begin());
        }
        else
            throw Exception(
                "Seek position is out of bounds. "
                "Offset: "
                    + std::to_string(offset) + ", Max: " + std::to_string(size_t(working_buffer.end() - working_buffer.begin())),
                ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
    else
        throw Exception("Only SEEK_SET and SEEK_CUR seek modes allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

off_t ReadBufferFromMemory::getPosition()
{
    return pos - working_buffer.begin();
}

}
