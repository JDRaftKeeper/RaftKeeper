#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <Poco/Logger.h>

#include <Service/KeeperCommon.h>
#include <Service/formatHex.h>
#include <ZooKeeper/ZooKeeperCommon.h>

namespace RK
{

// Enum values must be 1, 2, 4, 8, 16 to support bitwise operations
enum class WatchType : UInt8
{
    List = 1,
    Data = 2,
};

class WatchManager
{
public:
    using Watches = std::unordered_map<String, std::unordered_set<int64_t>>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_map<String, UInt8>>;

    explicit WatchManager() : log(&Poco::Logger::get("WatchManager")) { }

    void registerWatches(const String & path, int64_t session_id, Coordination::OpNum opnum);

    ResponsesForSessions processWatches(const String & path, Coordination::OpNum opnum);
    ResponsesForSessions processWatches(const String & path, Coordination::Event event_type);

    /// Process request SetWatch from client
    ResponsesForSessions processRequestSetWatch(
        const RequestForSession & request_for_session, std::unordered_map<String, std::pair<int64_t, int64_t>> & watch_nodes_info);

    void cleanDeadWatches(int64_t session_id);

    uint64_t getWatchedPathsCount() const
    {
        std::lock_guard lock(watch_mutex);
        return watches.size() + list_watches.size();
    }

    uint64_t getTotalWatchesCount() const;
    uint64_t getSessionsWithWatchesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;

    void reset();

private:
    /// Session id -> node path
    SessionAndWatcher sessions_and_watchers;
    /// Node path -> session id. Watches for 'get' and 'exist' requests
    Watches watches;
    /// Node path -> session id. Watches for 'list' request (watches on children).
    Watches list_watches;

    mutable std::mutex watch_mutex;

    Poco::Logger * log;
};

}
