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

#include <IO/WriteBufferFromFileDescriptor.h>


namespace RK
{

/** Write to file descriptor but drop the data if write would block or fail.
  * To use within signal handler. Motivating example: a signal handler invoked during execution of malloc
  *  should not block because some mutex (or even worse - a spinlock) may be held.
  */
class WriteBufferFromFileDescriptorDiscardOnFailure : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    using WriteBufferFromFileDescriptor::WriteBufferFromFileDescriptor;
    ~WriteBufferFromFileDescriptorDiscardOnFailure() override {}
};

}
