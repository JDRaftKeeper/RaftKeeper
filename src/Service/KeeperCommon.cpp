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
#include <iostream>
#include <Service/KeeperCommon.h>
#include <libnuraft/nuraft.hxx>
#include <sys/stat.h>
#include <sys/types.h>
#include <Poco/File.h>

using namespace nuraft;

namespace RK
{
int Directory::createDir(const std::string & dir)
{
    Poco::File(dir).createDirectories();
    return 0;
}
}
