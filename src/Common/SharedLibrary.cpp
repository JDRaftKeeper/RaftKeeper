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
#include "SharedLibrary.h"
#include <string>
#include <boost/core/noncopyable.hpp>
#include <common/phdr_cache.h>
#include "Exception.h"


namespace RK
{
namespace ErrorCodes
{
    extern const int CANNOT_DLOPEN;
    extern const int CANNOT_DLSYM;
}

SharedLibrary::SharedLibrary(std::string_view path, int flags)
{
    handle = dlopen(path.data(), flags);
    if (!handle)
        throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot dlopen: ({})", dlerror());

    updatePHDRCache();

    /// NOTE: race condition exists when loading multiple shared libraries concurrently.
    /// We don't care (or add global mutex for this method).
}

SharedLibrary::~SharedLibrary()
{
    if (handle && dlclose(handle))
        std::terminate();
}

void * SharedLibrary::getImpl(std::string_view name, bool no_throw)
{
    dlerror();

    auto * res = dlsym(handle, name.data());

    if (char * error = dlerror())
    {
        if (no_throw)
            return nullptr;

        throw Exception(ErrorCodes::CANNOT_DLSYM, "Cannot dlsym: ({})", error);
    }

    return res;
}
}
