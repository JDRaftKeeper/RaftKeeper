/**
 * Copyright 2016-2026 ClickHouse, Inc.
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

#include <IO/WriteBuffer.h>
#include <libnuraft/nuraft.hxx> // Y_IGNORE

namespace RK
{

class WriteBufferFromNuraftBuffer : public WriteBuffer
{
private:
    nuraft::ptr<nuraft::buffer> buffer;
    bool is_finished = false;

    static constexpr size_t initial_size = 32;
    static constexpr size_t size_multiplier = 2;

    void nextImpl() override;

public:
    WriteBufferFromNuraftBuffer();

    void finalize() override final;
    nuraft::ptr<nuraft::buffer> getBuffer();
    bool isFinished() const { return is_finished; }

    ~WriteBufferFromNuraftBuffer() override;
};

}
