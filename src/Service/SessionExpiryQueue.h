/**
 * Copyright 2016-2026 ClickHouse, Inc.
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
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <chrono>

namespace RK
{

/// Simple class for checking expired sessions. Main idea -- to round sessions
/// timeouts and place all sessions into buckets rounded by their expired time.
/// So we will have not too many different buckets and can check expired
/// sessions quite fast.
/// So buckets looks like this:
/// [1630580418000] -> {1, 5, 6}
/// [1630580418500] -> {2, 3}
/// ...
/// When new session appear it's added to the existing bucket or create new bucket.
class SessionExpiryQueue
{
private:
    /// Session -> timeout ms
    std::unordered_map<int64_t, int64_t> session_to_expiration_time;

    /// Expire time -> session expire near this time
    std::map<int64_t, std::unordered_set<int64_t>> expiry_to_sessions;

    int64_t expiration_interval;

    static int64_t getNowMilliseconds()
    {
        using namespace std::chrono;
        return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    }

    /// Round time to the next expiration interval. The result used as a key for
    /// expiry_to_sessions map.
    int64_t roundToNextInterval(int64_t time) const
    {
        return (time / expiration_interval + 1) * expiration_interval;
    }

public:
    /// expiration_interval -- how often we will check new sessions and how small
    /// buckets we will have. In ZooKeeper normal session timeout is around 30 seconds
    /// and expiration_interval is about 500ms.
    explicit SessionExpiryQueue(int64_t expiration_interval_)
        : expiration_interval(expiration_interval_)
    {
    }

    /// Session was actually removed
    bool remove(int64_t session_id);

    /// Update session expiry time (must be called on hearbeats)
    void addNewSessionOrUpdate(int64_t session_id, int64_t timeout_ms);

    /// Get all expired sessions
    std::vector<int64_t> getExpiredSessions() const;

    const std::unordered_map<int64_t, int64_t> & sessionToExpirationTime();

    void setSessionExpirationTime(int64_t session_id, int64_t expiration_time);

    void clear();
};

}
