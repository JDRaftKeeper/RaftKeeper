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

#include <string>
#include <functional>
#include <boost/noncopyable.hpp>


namespace RK
{

class WriteBuffer;


/** Provides that no more than one server works with one data directory.
  */
class StatusFile : private boost::noncopyable
{
public:
    using FillFunction = std::function<void(WriteBuffer&)>;

    StatusFile(std::string path_, FillFunction fill_);
    ~StatusFile();

    /// You can use one of these functions to fill the file or provide your own.
    static FillFunction write_pid;
    static FillFunction write_full_info;

private:
    const std::string path;
    FillFunction fill;
    int fd = -1;
};


}
