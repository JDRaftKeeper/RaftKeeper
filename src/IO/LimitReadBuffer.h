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

#include <common/types.h>
#include <IO/ReadBuffer.h>


namespace RK
{

/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  * Note that the nested ReadBuffer may read slightly more data internally to fill its buffer.
  */
class LimitReadBuffer : public ReadBuffer
{
public:
    LimitReadBuffer(ReadBuffer & in_, UInt64 limit_, bool throw_exception_, std::string exception_message_ = {});
    LimitReadBuffer(std::unique_ptr<ReadBuffer> in_, UInt64 limit_, bool throw_exception_, std::string exception_message_ = {});
    ~LimitReadBuffer() override;

private:
    ReadBuffer * in;
    bool owns_in;

    UInt64 limit;
    bool throw_exception;
    std::string exception_message;

    LimitReadBuffer(ReadBuffer * in_, bool owns, UInt64 limit_, bool throw_exception_, std::string exception_message_);

    bool nextImpl() override;
};

}
