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
#include <unistd.h>
#include <fcntl.h>

#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <IO/MMapReadBufferFromFile.h>


namespace ProfileEvents
{
    extern const Event FileOpen;
}

namespace RK
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_CLOSE_FILE;
}


void MMapReadBufferFromFile::open()
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    fd = ::open(file_name.c_str(), O_RDONLY | O_CLOEXEC);

    if (-1 == fd)
        throwFromErrnoWithPath("Cannot open file " + file_name, file_name,
                               errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}


std::string MMapReadBufferFromFile::getFileName() const
{
    return file_name;
}


MMapReadBufferFromFile::MMapReadBufferFromFile(const std::string & file_name_, size_t offset, size_t length_)
    : file_name(file_name_)
{
    open();
    init(fd, offset, length_);
}


MMapReadBufferFromFile::MMapReadBufferFromFile(const std::string & file_name_, size_t offset)
    : file_name(file_name_)
{
    open();
    init(fd, offset);
}


MMapReadBufferFromFile::~MMapReadBufferFromFile()
{
    if (fd != -1)
        close();    /// Exceptions will lead to std::terminate and that's Ok.
}


void MMapReadBufferFromFile::close()
{
    finish();

    if (0 != ::close(fd))
        throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

    fd = -1;
    metric_increment.destroy();
}

}
