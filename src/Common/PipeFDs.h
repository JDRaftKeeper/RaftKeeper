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

#include <cstddef>


namespace RK
{

/** Struct containing a pipe with lazy initialization.
  * Use `open` and `close` methods to manipulate pipe and `fds_rw` field to access
  * pipe's file descriptors.
  */
struct LazyPipeFDs
{
    int fds_rw[2] = {-1, -1};

    void open();
    void close();

    /// Set O_NONBLOCK to different ends of pipe preserving existing flags.
    /// Throws an exception if fcntl was not successful.
    void setNonBlockingWrite();
    void setNonBlockingRead();
    void setNonBlockingReadWrite();

    void tryIncreaseSize(int desired_size);

    ~LazyPipeFDs();
};


/** Struct which opens new pipe on creation and closes it on destruction.
  * Use `fds_rw` field to access pipe's file descriptors.
  */
struct PipeFDs : public LazyPipeFDs
{
    PipeFDs();
};

}
