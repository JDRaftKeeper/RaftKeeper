#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Poco/Logger.h>

#include <Common/IO/Operators.h>
#include <Common/IO/WriteBufferFromString.h>
#include <common/logger_useful.h>

#include <Service/SessionExpiryQueue.h>
#include <Service/formatHex.h>
#include <ZooKeeper/ZooKeeperCommon.h>

namespace RK
{

class SessionManager
{
public:
    using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;
    using SessionIDs = std::vector<int64_t>;

    explicit SessionManager(int64_t dead_session_check_period_ms)
        : session_expiry_queue(dead_session_check_period_ms), log(&Poco::Logger::get("WatchManager"))
    {
    }

    /// Allocate a new session id with initialized expiry timeout session_timeout_ms.
    /// Will increase session_id_counter and zxid.
    int64_t getSessionID(int64_t session_timeout_ms);

    /// Update session timeout for session_id, invoked when client reconnect to keeper.
    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    void updateSessionExpirationTime(int64_t session_id)
    {
        std::lock_guard lock(session_mutex);
        if (session_and_timeout.contains(session_id))
            session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
    }

    bool contains(int64_t session_id) const
    {
        std::lock_guard lock(session_mutex);
        return session_and_timeout.contains(session_id);
    }

    void expireSession(int64_t session_id)
    {
        std::lock_guard lock(session_mutex);
        session_expiry_queue.remove(session_id);
        session_and_timeout.erase(session_id);
    }

    int64_t getSessionIDCounter() const
    {
        std::lock_guard lock(session_mutex);
        return session_id_counter;
    }

    void setSessionIDCounter(int64_t counter)
    {
        std::lock_guard lock(session_mutex);
        session_id_counter = counter;
    }

    int64_t getSessionCount() const
    {
        std::lock_guard lock(session_mutex);
        return session_and_timeout.size();
    }

    SessionAndTimeout getSessionAndTimeOut() const
    {
        std::lock_guard lock(session_mutex);
        return session_and_timeout;
    }

    /// Add session id. Used when restoring KeeperStore from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms)
    {
        std::lock_guard lock(session_mutex);
        session_and_timeout.emplace(session_id, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
    }

    std::vector<int64_t> getDeadSessions() const
    {
        std::lock_guard lock(session_mutex);
        auto ret = session_expiry_queue.getExpiredSessions();
        return ret;
    }

    std::unordered_map<int64_t, int64_t> sessionToExpirationTime() const
    {
        std::lock_guard lock(session_mutex);
        const auto & ret = session_expiry_queue.sessionToExpirationTime();
        return ret;
    }

    void handleRemoteSession(int64_t session_id, int64_t expiration_time)
    {
        std::lock_guard lock(session_mutex);
        session_expiry_queue.setSessionExpirationTime(session_id, expiration_time);
    }

    void dumpSessionIDs(WriteBuffer & buf, const String & delimiter = "\n") const
    {
        std::lock_guard lock(session_mutex);
        buf << "Sessions dump (" << session_and_timeout.size() << "):\n";
        for (const auto & [session_id, _] : session_and_timeout)
        {
            buf << toHexString(session_id) << delimiter;
        }
    }

    void reset();

private:
    /// Hold session and initialized expiry timeout, only local sessions.
    SessionAndTimeout session_and_timeout;

    /// Hold session and expiry time
    /// For leader, holds all sessions in cluster.
    /// For follower/leaner, holds only local sessions
    SessionExpiryQueue session_expiry_queue;

    mutable std::mutex session_mutex;

    int64_t session_id_counter{1};

    Poco::Logger * log;

};

}
