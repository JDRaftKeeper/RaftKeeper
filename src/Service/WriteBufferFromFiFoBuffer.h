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
#pragma once

#include <Core/Types.h>
#include <IO/WriteBuffer.h>
#include <Poco/FIFOBuffer.h>

namespace RK
{

using Poco::FIFOBuffer;

class WriteBufferFromFiFoBuffer : public WriteBuffer
{
private:
    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    std::shared_ptr<FIFOBuffer> buffer;
    bool is_finished = false;

    void nextImpl() override;

public:
    explicit WriteBufferFromFiFoBuffer(size_t size = initial_size);

    void finalize() override final;
    std::shared_ptr<FIFOBuffer> getBuffer();
    bool isFinished() const { return is_finished; }

    ~WriteBufferFromFiFoBuffer() override;
};

using WriteBufferFromFiFoBufferPtr = std::shared_ptr<WriteBufferFromFiFoBuffer>;

}
