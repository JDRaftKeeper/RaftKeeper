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
#include <IO/WriteBufferFromFileDescriptorDiscardOnFailure.h>

namespace ProfileEvents
{
    extern const Event CannotWriteToWriteBufferDiscard;
}

namespace RK
{

void WriteBufferFromFileDescriptorDiscardOnFailure::nextImpl()
{
    size_t bytes_written = 0;
    while (bytes_written != offset())
    {
        ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::CannotWriteToWriteBufferDiscard);
            break;  /// Discard
        }

        if (res > 0)
            bytes_written += res;
    }
}

}
