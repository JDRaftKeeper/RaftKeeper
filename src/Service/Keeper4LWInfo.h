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
#pragma once

#include <string>
#include <common/types.h>
#include <Common/Exception.h>

namespace RK
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Keeper server related information for different 4lw commands
struct Keeper4LWInfo
{
    bool is_leader;
    bool is_observer;
    bool is_follower;
    bool is_standalone;

    bool has_leader;

    uint64_t alive_connections_count;
    uint64_t outstanding_requests_count;

    uint64_t follower_count;
    uint64_t synced_follower_count;

    uint64_t total_nodes_count;
    int64_t last_zxid;

    String getRole() const
    {
        if (is_standalone)
            return "standalone";
        if (is_leader)
            return "leader";
        if (is_observer)
            return "observer";
        if (is_follower)
            return "follower";

        throw Exception(ErrorCodes::LOGICAL_ERROR, "RAFT server has undefined state state, it's a bug");
    }
};

/// Keeper log information for 4lw commands
struct KeeperLogInfo
{
    /// My first log index in log store.
    uint64_t first_log_idx;

    /// My first log term.
    uint64_t first_log_term;

    /// My last log index in log store.
    uint64_t last_log_idx;

    /// My last log term.
    uint64_t last_log_term;

    /// My last committed log index in state machine.
    uint64_t last_committed_log_idx;

    /// Leader's committed log index from my perspective.
    uint64_t leader_committed_log_idx;

    /// Target log index should be committed to.
    uint64_t target_committed_log_idx;

    /// The largest committed log index in last snapshot.
    uint64_t last_snapshot_idx;
};


}
