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
#include <pthread.h>

#if defined(__APPLE__)
#elif defined(__FreeBSD__)
    #include <pthread_np.h>
#else
    #include <sys/prctl.h>
#endif

#include <cstring>

#include <Common/Exception.h>
#include <Common/setThreadName.h>


namespace RK
{
namespace ErrorCodes
{
    extern const int PTHREAD_ERROR;
}
}


void setThreadName(const char * name)
{
#ifndef NDEBUG
    if (strlen(name) > 15)
        throw RK::Exception("Thread name cannot be longer than 15 bytes", RK::ErrorCodes::PTHREAD_ERROR);
#endif

#if defined(OS_FREEBSD)
    pthread_set_name_np(pthread_self(), name);
    if ((false))
#elif defined(OS_DARWIN)
    if (0 != pthread_setname_np(name))
#else
    if (0 != prctl(PR_SET_NAME, name, 0, 0, 0))
#endif
        RK::throwFromErrno("Cannot set thread name with prctl(PR_SET_NAME, ...)", RK::ErrorCodes::PTHREAD_ERROR);
}

std::string getThreadName()
{
    std::string name(16, '\0');

#if defined(__APPLE__)
    if (pthread_getname_np(pthread_self(), name.data(), name.size()))
        throw RK::Exception("Cannot get thread name with pthread_getname_np()", RK::ErrorCodes::PTHREAD_ERROR);
#elif defined(__FreeBSD__)
// TODO: make test. freebsd will have this function soon https://freshbsd.org/commit/freebsd/r337983
//    if (pthread_get_name_np(pthread_self(), name.data(), name.size()))
//        throw RK::Exception("Cannot get thread name with pthread_get_name_np()", RK::ErrorCodes::PTHREAD_ERROR);
#else
    if (0 != prctl(PR_GET_NAME, name.data(), 0, 0, 0))
        RK::throwFromErrno("Cannot get thread name with prctl(PR_GET_NAME)", RK::ErrorCodes::PTHREAD_ERROR);
#endif

    name.resize(std::strlen(name.data()));
    return name;
}
