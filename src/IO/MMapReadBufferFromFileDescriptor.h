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

#include <IO/ReadBufferFromFileBase.h>


namespace RK
{

/** MMap range in a file and represent it as a ReadBuffer.
  * Please note that mmap is not always the optimal way to read file.
  * Also you cannot control whether and how long actual IO take place,
  *  so this method is not manageable and not recommended for anything except benchmarks.
  */
class MMapReadBufferFromFileDescriptor : public ReadBufferFromFileBase
{
public:
    off_t seek(off_t off, int whence) override;

protected:
    MMapReadBufferFromFileDescriptor() {}
    void init(int fd_, size_t offset, size_t length_);
    void init(int fd_, size_t offset);

public:
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_, size_t length_);

    /// Map till end of file.
    MMapReadBufferFromFileDescriptor(int fd_, size_t offset_);

    ~MMapReadBufferFromFileDescriptor() override;

    /// unmap memory before call to destructor
    void finish();

    off_t getPosition() override;
    std::string getFileName() const override;
    int getFD() const;

private:
    size_t length = 0;
    int fd = -1;
};

}

