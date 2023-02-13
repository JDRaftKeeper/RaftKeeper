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

#include <sys/types.h>
#include <common/types.h>
#include <boost/noncopyable.hpp>

struct taskstats;

namespace RK
{

/// Get taskstat info from OS kernel via Netlink protocol.
class TaskStatsInfoGetter : private boost::noncopyable
{
public:
    TaskStatsInfoGetter();
    ~TaskStatsInfoGetter();

    void getStat(::taskstats & out_stats, pid_t tid) const;

    /// Whether the current process has permissions (sudo or cap_net_admin capabilities) to get taskstats info
    static bool checkPermissions();

#if defined(OS_LINUX)
private:
    int netlink_socket_fd = -1;
    UInt16 taskstats_family_id = 0;
#endif
};

}
