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

#include <Common/CurrentMetrics.h>
#include <IO/MMapReadBufferFromFileDescriptor.h>


namespace CurrentMetrics
{
    extern const Metric OpenFileForRead;
}


namespace RK
{

class MMapReadBufferFromFile : public MMapReadBufferFromFileDescriptor
{
public:
    MMapReadBufferFromFile(const std::string & file_name_, size_t offset, size_t length_);

    /// Map till end of file.
    MMapReadBufferFromFile(const std::string & file_name_, size_t offset);

    ~MMapReadBufferFromFile() override;

    void close();

    std::string getFileName() const override;

private:
    int fd = -1;
    std::string file_name;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

    void open();
};

}

