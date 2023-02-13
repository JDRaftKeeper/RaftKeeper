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
#if defined(__linux__)

#include "hasLinuxCapability.h"

#include <syscall.h>
#include <unistd.h>
#include <linux/capability.h>
#include <Common/Exception.h>


namespace RK
{

namespace ErrorCodes
{
    extern const int NETLINK_ERROR;
}

static __user_cap_data_struct getCapabilities()
{
    /// See man getcap.
    __user_cap_header_struct request{};
    request.version = _LINUX_CAPABILITY_VERSION_1; /// It's enough to check just single CAP_NET_ADMIN capability we are interested.
    request.pid = getpid();

    __user_cap_data_struct response{};

    /// Avoid dependency on 'libcap'.
    if (0 != syscall(SYS_capget, &request, &response))
        throwFromErrno("Cannot do 'capget' syscall", ErrorCodes::NETLINK_ERROR);

    return response;
}

bool hasLinuxCapability(int cap)
{
    static __user_cap_data_struct capabilities = getCapabilities();
    return (1 << cap) & capabilities.effective;
}

}

#endif
