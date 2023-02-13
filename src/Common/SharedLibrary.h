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

#include <dlfcn.h>
#include <memory>
#include <string>
#include <boost/noncopyable.hpp>


namespace RK
{

/** Allows you to open a dynamic library and get a pointer to a function from it.
  */
class SharedLibrary : private boost::noncopyable
{
public:
    explicit SharedLibrary(std::string_view path, int flags = RTLD_LAZY);

    ~SharedLibrary();

    template <typename Func>
    Func get(std::string_view name)
    {
        return reinterpret_cast<Func>(getImpl(name));
    }

    template <typename Func>
    Func tryGet(std::string_view name)
    {
        return reinterpret_cast<Func>(getImpl(name, true));
    }

private:
    void * getImpl(std::string_view name, bool no_throw = false);

    void * handle = nullptr;
};

using SharedLibraryPtr = std::shared_ptr<SharedLibrary>;

}
