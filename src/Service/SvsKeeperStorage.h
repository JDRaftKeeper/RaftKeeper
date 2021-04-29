#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Service/SvsKeeperSessionExpiryQueue.h>
#include <Poco/Logger.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

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
    int32_t seq_num = 0;
    ChildrenSet children{};
    std::mutex mutex;
    std::shared_ptr<KeeperNode> clone()
    {
        auto node = std::make_shared<KeeperNode>();
        node->data = data;
        node->acls = acls;
        node->is_ephemeral = is_ephemeral;
        node->is_sequental = is_sequental;
        node->stat = stat;
        node->seq_num = seq_num;
        node->children = children;
        return node;
    }
};

#ifdef USE_CONCURRENTMAP
template <typename Element, unsigned NumBlocks>
class ConcurrentMap
{
public:
    using SharedElement = std::shared_ptr<Element>;
    using ElementMap = std::unordered_map<std::string, SharedElement>;
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

        std::shared_mutex & getMutex() { return mut_; }
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

    int64_t session_id_counter{0};

    struct ResponseForSession
    {
        int64_t session_id;
        Coordination::ZooKeeperResponsePtr response;
    };

    using ResponsesForSessions = std::vector<ResponseForSession>;

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
    using SessionAndWatcher = std::unordered_map<int64_t, std::unordered_set<std::string>>;
    using SessionAndTimeout = std::unordered_map<int64_t, long>;
    using SessionIDs = std::vector<int64_t>;

    using Watches = std::map<String /* path, relative of root_path */, SessionIDs>;

    Container container;

    Ephemerals ephemerals;
    mutable std::shared_mutex ephemerals_mutex;

    SessionAndWatcher sessions_and_watchers;
    SvsKeeperSessionExpiryQueue session_expiry_queue;
    SessionAndTimeout session_and_timeout;
    std::shared_mutex session_mutex;

    Watches watches;
    Watches list_watches; /// Watches for 'list' request (watches on children).

    std::shared_mutex watch_mutex;

    std::atomic<int64_t> zxid{0};
    bool finalized{false};

    void clearDeadWatches(int64_t session_id);

    int64_t getZXID() { return zxid++; }

public:
    SvsKeeperStorage(int64_t tick_time_ms);

    int64_t getSessionID(int64_t session_timeout_ms)
    {
        std::lock_guard lock(session_mutex);
        auto result = session_id_counter++;
        session_and_timeout.emplace(result, session_timeout_ms);
        session_expiry_queue.update(result, session_timeout_ms);
        return result;
    }

    ResponsesForSessions processRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    void finalize();

    std::unordered_set<int64_t> getDeadSessions()
    {
        std::lock_guard lock(session_mutex);
        return session_expiry_queue.getExpiredSessions();
    }

    /// no need to lock
    UInt64 getNodeNum() const
    {
        return container.size();
    }

    /// not accurate
    UInt64 getNodeSizeMB() const
    {
        UInt64 node_count = getNodeNum();
        UInt64 size_bytes = container.getBlockNum() * sizeof(Container::InnerMap) /* Inner map size */
            + node_count * 8 / 0.75             /*hash map array size*/
            + node_count * sizeof(KeeperNode)   /*node size*/
            + node_count * 100;                 /*path and child of node size*/
        return size_bytes / 1024 / 1024;
    }

    /// no need to lock
    UInt64 getSessionNum() const
    {
        return session_and_timeout.size();
    }

private:
    Poco::Logger * log;
};

}
