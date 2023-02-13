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
#include <Common/createHardLink.h>
#include <Common/Exception.h>
#include <errno.h>
#include <unistd.h>
#include <sys/stat.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int CANNOT_STAT;
    extern const int CANNOT_LINK;
}

void createHardLink(const String & source_path, const String & destination_path)
{
    if (0 != link(source_path.c_str(), destination_path.c_str()))
    {
        if (errno == EEXIST)
        {
            auto link_errno = errno;

            struct stat source_descr;
            struct stat destination_descr;

            if (0 != lstat(source_path.c_str(), &source_descr))
                throwFromErrnoWithPath("Cannot stat " + source_path, source_path, ErrorCodes::CANNOT_STAT);

            if (0 != lstat(destination_path.c_str(), &destination_descr))
                throwFromErrnoWithPath("Cannot stat " + destination_path, destination_path, ErrorCodes::CANNOT_STAT);

            if (source_descr.st_ino != destination_descr.st_ino)
                throwFromErrnoWithPath(
                        "Destination file " + destination_path + " is already exist and have different inode.",
                        destination_path, ErrorCodes::CANNOT_LINK, link_errno);
        }
        else
            throwFromErrnoWithPath("Cannot link " + source_path + " to " + destination_path, destination_path,
                                   ErrorCodes::CANNOT_LINK);
    }
}

}
