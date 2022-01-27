#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Service/SvsKeeperSessionExpiryQueue.h>
#include <Poco/Logger.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Service/SvsKeeperThreadSafeQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <functional>

namespace DB
{
using namespace DB;
struct SvsKeeperStorageRequest;
using SvsKeeperStorageRequestPtr = std::shared_ptr<SvsKeeperStorageRequest>;
using ResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr &)>;
using ChildrenSet = std::unordered_set<std::string>;

#ifndef USE_CONCURRENTMAP
#    define USE_CONCURRENTMAP
#endif

struct KeeperNode
{
    String data;
    Coordination::ACLs acls{};
    bool is_ephemeral = false;
    bool is_sequental = false;
    Coordination::Stat stat{};
    ChildrenSet children{};
    std::shared_mutex mutex;
    std::shared_ptr<KeeperNode> clone()
    {
        auto node = std::make_shared<KeeperNode>();
        node->data = data;
        node->acls = acls;
        node->is_ephemeral = is_ephemeral;
        node->is_sequental = is_sequental;
        node->stat = stat;
        node->children = children;
        return node;
    }

    Coordination::Stat statView()
    {
        Coordination::Stat stat_view;
        stat_view = stat;
        stat_view.cversion = stat.cversion * 2 - stat.numChildren;
        return stat_view;
    }
    /// Object memory size
    uint64_t sizeInBytes() const;
};

#ifdef USE_CONCURRENTMAP
template <typename Element, unsigned NumBlocks>
class ConcurrentMap
{
public:
    using SharedElement = std::shared_ptr<Element>;
    using ElementMap = std::unordered_map<std::string, SharedElement>;
    using Action = std::function<void(const String &, const SharedElement &)>;

    class InnerMap
    {
    public:
        SharedElement get(std::string const & key)
        {
            std::shared_lock rlock(mut_);
            auto i = map_.find(key);
            return (i != map_.end()) ? i->second : nullptr;
        }
        bool emplace(std::string const & key, SharedElement && value)
        {
            std::unique_lock wlock(mut_);
            auto [_, created] = map_.insert_or_assign(key, std::forward<SharedElement>(value));
            return created;
        }
        bool emplace(std::string const & key, const SharedElement & value)
        {
            std::unique_lock wlock(mut_);
            auto [_, created] = map_.insert_or_assign(key, value);
            return created;
        }
        bool erase(std::string const & key)
        {
            std::unique_lock wlock(mut_);
            return map_.erase(key);
        }

        size_t size() const { return map_.size(); }

        void forEach(Action & fn)
        {
            std::shared_lock rlock(mut_);
            for (auto [key, value] : map_)
                fn(key, value);
        }

        std::shared_mutex & getMutex() { return mut_; }

        /// This method will destroy InnerMap thread safety property.
        /// deprecated use forEach instead.
        ElementMap & getMap() { return map_; }

    private:
        std::shared_mutex mut_;
        ElementMap map_;
    };

private:
    std::array<InnerMap, NumBlocks> maps_;
    std::hash<std::string> hash_;

public:
    SharedElement get(const std::string & key) { return map_for(key).get(key); }
    SharedElement at(const std::string & key) { return map_for(key).get(key); }

    bool emplace(const std::string & key, SharedElement && value) { return map_for(key).emplace(key, std::forward<SharedElement>(value)); }
    bool emplace(const std::string & key, const SharedElement & value) { return map_for(key).emplace(key, value); }
    size_t count(const std::string & key) { return get(key) != nullptr ? 1 : 0; }
    bool erase(std::string const & key) { return map_for(key).erase(key); }

    InnerMap & map_for(std::string const & key) { return maps_[hash_(key) % NumBlocks]; }
    UInt32 getBlockNum() const { return NumBlocks; }
    InnerMap & getMap(const UInt32 & index) { return maps_[index]; }

    size_t size() const
    {
        size_t s(0);
        for (auto it = maps_.begin(); it != maps_.end(); it++)
        {
            s += it->size();
        }
        return s;
    }
};

#endif

class SvsKeeperStorage
{
public:
    static constexpr int MAP_BLOCK_NUM = 16;

    int64_t session_id_counter{1};

    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
    };

    using ResponsesForSessions = std::vector<ResponseForSession>;
    using SvsKeeperResponsesQueue = SvsKeeperThreadSafeQueue<SvsKeeperStorage::ResponseForSession>;

    struct RequestForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperRequestPtr request;
    };

    using RequestsForSessions = std::vector<RequestForSession>;

#ifdef USE_CONCURRENTMAP
    using Container = ConcurrentMap<KeeperNode, MAP_BLOCK_NUM>;
#else
    using Container = std::unordered_map<std::string, KeeperNode>;
#endif

    using Ephemerals = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using EphemeralsPtr = std::shared_ptr<Ephemerals>;
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndWatcherPtr = std::shared_ptr<SessionAndWatcher>;
    using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;
    using SessionIDs = std::vector<int64_t>;

    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    Container container;

    Ephemerals ephemerals;
    mutable std::mutex ephemerals_mutex;

    SvsKeeperSessionExpiryQueue session_expiry_queue;
    SessionAndTimeout session_and_timeout;
    /// pending close sessions
    std::unordered_set<int64_t> closing_sessions;
    mutable std::mutex session_mutex;

    /// Session id -> patch
    SessionAndWatcher sessions_and_watchers;
    /// Path -> session id. Watches for 'get' and 'exist' requests
    Watches watches;
    /// Path -> session id. Watches for 'list' request (watches on children).
    Watches list_watches;

    mutable std::mutex watch_mutex;

    std::atomic<int64_t> zxid{0};
    bool finalized{false};

    void clearDeadWatches(int64_t session_id);

    int64_t getZXID() { return zxid++; }

public:
    SvsKeeperStorage(int64_t tick_time_ms);

    int64_t getSessionID(int64_t session_timeout_ms)
    {
        /// Creating session should increase zxid
        /// TODO handle new_last_zxid
        getZXID();

        std::lock_guard lock(session_mutex);
        auto result = session_id_counter++;
        session_and_timeout.emplace(result, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(result, session_timeout_ms);
        return result;
    }

    bool updateSessionTimeout(int64_t session_id, int64_t session_timeout_ms);

    void processRequest(
        SvsKeeperThreadSafeQueue<ResponseForSession> & responses_queue,
        const Coordination::ZooKeeperRequestPtr & request,
        int64_t session_id,
        std::optional<int64_t> new_last_zxid = {},
        bool check_acl = true,
        bool ignore_response = false);

    /// build path children after load data from snapshot
    void buildPathChildren(bool from_zk_snapshot = false);

    void finalize();

    /// Add session id. Used when restoring KeeperStorage from snapshot.
    void addSessionID(int64_t session_id, int64_t session_timeout_ms)
    {
        std::lock_guard lock(session_mutex);
        session_and_timeout.emplace(session_id, session_timeout_ms);
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_timeout_ms);
    }

    std::vector<int64_t> getDeadSessions()
    {
        std::lock_guard lock(session_mutex);
        auto ret = session_expiry_queue.getExpiredSessions();
        for (auto session : ret)
            closing_sessions.emplace(session);
        return ret;
    }

    bool containsSession(int64_t session_id) const;

    /// Introspection functions mostly used in 4-letter commands
    uint64_t getNodesCount() const
    {
        return container.size();
    }

    uint64_t getApproximateDataSize() const
    {
        UInt64 node_count = container.size();
        UInt64 size_bytes = container.getBlockNum() * sizeof(Container::InnerMap) /* Inner map size */
            + node_count * 8 / 0.75 /*hash map array size*/
            + node_count * sizeof(KeeperNode) /*node size*/
            + node_count * 100; /*path and child of node size*/
        return size_bytes;
    }

    uint64_t getTotalWatchesCount() const;

    uint64_t getWatchedPathsCount() const
    {
        std::lock_guard lock(watch_mutex);
        return watches.size() + list_watches.size();
    }

    uint64_t getSessionsWithWatchesCount() const;

    uint64_t getSessionWithEphemeralNodesCount() const
    {
        std::lock_guard lock(watch_mutex);
        return ephemerals.size();
    }
    uint64_t getTotalEphemeralNodesCount() const;

    void dumpWatches(WriteBufferFromOwnString & buf) const;
    void dumpWatchesByPath(WriteBufferFromOwnString & buf) const;
    void dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const;

private:
    Poco::Logger * log;
};

using Ephemerals = SvsKeeperStorage::Ephemerals;
using EphemeralsPtr = SvsKeeperStorage::EphemeralsPtr;
using SessionAndWatcher = SvsKeeperStorage::SessionAndWatcher;
using SessionAndWatcherPtr = SvsKeeperStorage::SessionAndWatcherPtr;
using SessionIDs = SvsKeeperStorage::SessionIDs;
using Watches = SvsKeeperStorage::Watches;

}
