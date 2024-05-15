#include <functional>
#include <iomanip>
#include <Service/KeeperStore.h>
#include <Service/KeeperUtils.h>
#include <ZooKeeper/IKeeper.h>
#include <Common/StringUtils.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

static inline void set_response(
    ThreadSafeQueue<ResponseForSession> & responses_queue,
    const KeeperStore::ResponsesForSessions & responses,
    bool ignore_response)
{
    if (!ignore_response)
    {
        for (auto && response : responses)
            responses_queue.push(std::move(response));
    }
}

static inline void set_response(
    ThreadSafeQueue<ResponseForSession> & responses_queue,
    const ResponseForSession & response,
    bool ignore_response)
{
    KeeperStore::ResponsesForSessions responses;
    responses.push_back(response);
    set_response(responses_queue, responses, ignore_response);
}

static bool checkACL(int32_t permission, const Coordination::ACLs & node_acls, const std::vector<Coordination::AuthID> & session_auths)
{
    if (node_acls.empty())
        return true;

    for (const auto & session_auth : session_auths)
        if (session_auth.scheme == "super")
            return true;

    for (const auto & node_acl : node_acls)
    {
        if (node_acl.permissions & permission)
        {
            if (node_acl.scheme == "world" && node_acl.id == "anyone")
                return true;

            for (const auto & session_auth : session_auths)
            {
                if (node_acl.scheme == session_auth.scheme && node_acl.id == session_auth.id)
                    return true;
            }
        }
    }

    return false;
}

static bool fixupACL(
    const std::vector<Coordination::ACL> & request_acls,
    const std::vector<Coordination::AuthID> & current_ids,
    std::vector<Coordination::ACL> & result_acls)
{
    if (request_acls.empty())
        return true;

    bool valid_found = false;
    for (const auto & request_acl : request_acls)
    {
        if (request_acl.scheme == "auth")
        {
            for (const auto & current_id : current_ids)
            {
                valid_found = true;
                Coordination::ACL new_acl = request_acl;
                new_acl.scheme = current_id.scheme;
                new_acl.id = current_id.id;
                result_acls.push_back(new_acl);
            }
        }
        else if (request_acl.scheme == "world" && request_acl.id == "anyone")
        {
            valid_found = true;

            Coordination::ACL new_acl = request_acl;
            result_acls.push_back(new_acl);
        }
        else if (request_acl.scheme == "digest")
        {
            Coordination::ACL new_acl = request_acl;

            /// Bad auth
            if (std::count(new_acl.id.begin(), new_acl.id.end(), ':') != 1)
                return false;

            valid_found = true;

            /// Consistent with zookeeper, accept generated digest
            result_acls.push_back(new_acl);
        }
    }
    return valid_found;
}

static KeeperStore::ResponsesForSessions
processWatchesImpl(const String & path, KeeperStore::Watches & watches, KeeperStore::Watches & list_watches, Coordination::Event event_type)
{
    static auto * log = &(Poco::Logger::get("KeeperStore"));
    KeeperStore::ResponsesForSessions result;
    auto it = watches.find(path);
    if (it != watches.end())
    {
        std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_response = std::make_shared<Coordination::ZooKeeperWatchResponse>();
        watch_response->path = path;
        watch_response->xid = Coordination::WATCH_XID;
        watch_response->zxid = -1;
        watch_response->type = event_type;
        watch_response->state = Coordination::State::CONNECTED;
        for (auto watcher_session : it->second)
        {
            result.push_back(ResponseForSession{watcher_session, watch_response});
            LOG_TRACE(log, "Watch triggered path {}, watcher session {}", path, watcher_session);
        }
        watches.erase(it);
    }

    auto parent_path = getParentPath(path);

    Strings paths_to_check_for_list_watches;
    if (event_type == Coordination::Event::CREATED)
    {
        paths_to_check_for_list_watches.push_back(parent_path); /// Trigger list watches for parent
    }
    else if (event_type == Coordination::Event::DELETED)
    {
        paths_to_check_for_list_watches.push_back(path); /// Trigger both list watches for this path
        paths_to_check_for_list_watches.push_back(parent_path); /// And for parent path
    }
    /// CHANGED event never trigger list wathes

    for (const auto & path_to_check : paths_to_check_for_list_watches)
    {
        it = list_watches.find(path_to_check);
        if (it != list_watches.end())
        {
            std::shared_ptr<Coordination::ZooKeeperWatchResponse> watch_list_response
                = std::make_shared<Coordination::ZooKeeperWatchResponse>();
            watch_list_response->path = path_to_check;
            watch_list_response->xid = Coordination::WATCH_XID;
            watch_list_response->zxid = -1;
            if (path_to_check == parent_path)
                watch_list_response->type = Coordination::Event::CHILD;
            else
                watch_list_response->type = Coordination::Event::DELETED;

            watch_list_response->state = Coordination::State::CONNECTED;
            for (auto watcher_session : it->second)
                result.push_back(ResponseForSession{watcher_session, watch_list_response});

            list_watches.erase(it);
        }
    }
    return result;
}

/** only write request should increase zxid
 */
static bool shouldIncreaseZxid(const Coordination::ZooKeeperRequestPtr & zk_request)
{
    return !(dynamic_cast<Coordination::ZooKeeperGetRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperSetWatchesRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperExistsRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperAuthRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperHeartbeatRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperListRequest *>(zk_request.get())
        || dynamic_cast<Coordination::ZooKeeperSimpleListRequest *>(zk_request.get()));
}

KeeperStore::KeeperStore(int64_t tick_time_ms, const String & super_digest_)
    : session_expiry_queue(tick_time_ms), super_digest(super_digest_)
{
    log = &(Poco::Logger::get("KeeperStore"));
    container.emplace("/", std::make_shared<KeeperNode>());
}

using Undo = std::function<void()>;

struct StoreRequest
{
    Coordination::ZooKeeperRequestPtr zk_request;

    explicit StoreRequest(const Coordination::ZooKeeperRequestPtr & zk_request_) : zk_request(zk_request_) { }

    virtual std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t zxid, int64_t session_id, int64_t time) const = 0;

    virtual bool checkAuth(KeeperStore & /*storage*/, int64_t /*session_id*/) const { return true; }

    virtual KeeperStore::ResponsesForSessions
    processWatches(KeeperStore::Watches & /*watches*/, KeeperStore::Watches & /*list_watches*/) const
    {
        return {};
    }

    virtual ~StoreRequest() = default;
};

struct StoreRequestHeartbeat final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & /* store */, int64_t /* zxid */, int64_t /* session_id */, int64_t /* time */) const override
    {
        return {zk_request->makeResponse(), {}};
    }
};

struct StoreRequestSetWatches final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & /* storage */, int64_t /* zxid */, int64_t /* session_id */, int64_t /* time */) const override
    {
        return {zk_request->makeResponse(), {}};
    }

    KeeperStore::ResponsesForSessions
    processWatches(KeeperStore::Watches & /*watches*/, KeeperStore::Watches & /*list_watches*/) const override
    {
        return {};
    }
};

struct StoreRequestSync final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & /* storage */, int64_t /* zxid */, int64_t /* session_id */, int64_t /* time */) const override
    {
        auto response = zk_request->makeResponse();
        dynamic_cast<Coordination::ZooKeeperSyncResponse *>(response.get())->path
            = dynamic_cast<Coordination::ZooKeeperSyncRequest *>(zk_request.get())->path;
        return {response, {}};
    }
};

struct StoreRequestCreate final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    KeeperStore::ResponsesForSessions processWatches(KeeperStore::Watches & watches, KeeperStore::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CREATED);
    }

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto parent_path = getParentPath(zk_request->getPath());

        auto parent = container.get(parent_path);
        if (parent == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(parent->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Create, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Create, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t zxid, int64_t session_id, int64_t time) const override
    {
        Poco::Logger * log = &(Poco::Logger::get("StoreRequestCreate"));

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Undo undo;
        Coordination::ZooKeeperCreateResponse & response = dynamic_cast<Coordination::ZooKeeperCreateResponse &>(*response_ptr);
        Coordination::ZooKeeperCreateRequest & request = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*zk_request);

        auto parent = store.container.get(getParentPath(request.path));
        if (parent == nullptr)
        {
            LOG_TRACE(log, "Create no parent {}, path {}", getParentPath(request.path), request.path);
            response.error = Coordination::Error::ZNONODE;
            return {response_ptr, undo};
        }
        else if (parent->is_ephemeral)
        {
            response.error = Coordination::Error::ZNOCHILDRENFOREPHEMERALS;
            return {response_ptr, undo};
        }

        String path_created = request.path;
        if (request.is_sequential)
        {
            auto seq_num = parent->stat.cversion;

            std::stringstream seq_num_str; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
            seq_num_str.exceptions(std::ios::failbit);
            seq_num_str << std::setw(10) << std::setfill('0') << seq_num;

            path_created += seq_num_str.str();
        }
        if (store.container.count(path_created) == 1)
        {
            response.error = Coordination::Error::ZNODEEXISTS;
            return {response_ptr, undo};
        }
        String child_path = getBaseName(path_created);
        if (child_path.empty())
        {
            response.error = Coordination::Error::ZBADARGUMENTS;
            return {response_ptr, undo};
        }
        std::shared_ptr<KeeperNode> created_node = std::make_shared<KeeperNode>();

        Coordination::ACLs node_acls;
        uint64_t acl_id{};

        if (!request.acls.empty())
        {
            std::lock_guard lock(store.auth_mutex);
            auto & session_auth_ids = store.session_and_auth[session_id];

            if (!fixupACL(request.acls, session_auth_ids, node_acls))
            {
                response.error = Coordination::Error::ZINVALIDACL;
                return {response_ptr, {}};
            }

            acl_id = store.acl_map.convertACLs(node_acls);
            store.acl_map.addUsage(acl_id);
        }

        created_node->acl_id = acl_id;
        LOG_TRACE(log, "path {}, acl_id {}, node_acls {}", zk_request->getPath(), created_node->acl_id, toString(node_acls));
        created_node->stat.czxid = zxid;
        created_node->stat.mzxid = zxid;
        created_node->stat.pzxid = zxid;
        created_node->stat.ctime = time;
        created_node->stat.mtime = created_node->stat.ctime;
        created_node->stat.numChildren = 0;
        created_node->stat.dataLength = request.data.length();
        created_node->data = request.data;
        created_node->is_ephemeral = request.is_ephemeral;
        if (request.is_ephemeral)
            created_node->stat.ephemeralOwner = session_id;
        created_node->is_sequential = request.is_sequential;

        int64_t pzxid;

        {
            response.path_created = path_created;

            parent->children.insert(child_path);

            ++parent->stat.cversion;
            ++parent->stat.numChildren;

            pzxid = parent->stat.pzxid;
            parent->stat.pzxid = zxid;
        }

        store.container.emplace(path_created, std::move(created_node));

        if (request.is_ephemeral)
        {
            std::lock_guard w_lock(store.ephemerals_mutex);
            store.ephemerals[session_id].emplace(path_created);
        }

        undo = [&store,
                session_id,
                path_created,
                pzxid,
                is_ephemeral = request.is_ephemeral,
                parent_path = getParentPath(request.path),
                child_path,
                acl_id] {
            {
                store.container.erase(path_created);
                store.acl_map.removeUsage(acl_id);
            }
            if (is_ephemeral)
            {
                std::lock_guard w_lock(store.ephemerals_mutex);
                store.ephemerals[session_id].erase(path_created);
            }
            auto undo_parent = store.container.at(parent_path);
            {
                --undo_parent->stat.cversion;
                --undo_parent->stat.numChildren;
                undo_parent->stat.pzxid = pzxid;
                undo_parent->children.erase(child_path);
            }
        };

        response.error = Coordination::Error::ZOK;
        return {response_ptr, undo};
    }
};

struct StoreRequestGet final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        Poco::Logger * log = &(Poco::Logger::get("StoreRequestGet"));
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        LOG_TRACE(log, "path {}, acl_id {}, node_acls {}", zk_request->getPath(), node->acl_id, toString(node_acls));
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Read, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Read, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /* zxid */, int64_t /* session_id */, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetResponse & response = dynamic_cast<Coordination::ZooKeeperGetResponse &>(*response_ptr);
        Coordination::ZooKeeperGetRequest & request = dynamic_cast<Coordination::ZooKeeperGetRequest &>(*zk_request);

        auto node = store.container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            {
                response.stat = node->statForResponse();
                response.data = node->data;
            }
            response.error = Coordination::Error::ZOK;
        }

        return {response_ptr, {}};
    }
};

struct StoreRequestRemove final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto parent = container.get(getParentPath(zk_request->getPath()));
        if (parent == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(parent->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Delete, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Delete, node_acls, empty_auth_ids);
        }
    }


    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t zxid, int64_t /* session_id */, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperRemoveResponse & response = dynamic_cast<Coordination::ZooKeeperRemoveResponse &>(*response_ptr);
        Coordination::ZooKeeperRemoveRequest & request = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*zk_request);
        Undo undo;

        Poco::Logger * log = &(Poco::Logger::get("StoreRequestRemove"));
        auto node = store.container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != node->stat.version)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else if (!node->children.empty())
        {
            LOG_TRACE(log, "Parent children begin {}", *node->children.begin());
            response.error = Coordination::Error::ZNOTEMPTY;
        }
        else
        {
            response.error = Coordination::Error::ZOK;

            int64_t pzxid;
            auto prev_node = node->clone();
            auto child_basename = getBaseName(request.path);

            auto parent = store.container.at(getParentPath(request.path));
            {
                --parent->stat.numChildren;
                pzxid = parent->stat.pzxid;
                parent->stat.pzxid = zxid;
                parent->children.erase(child_basename);
            }

            store.acl_map.removeUsage(prev_node->acl_id);
            store.container.erase(request.path);

            int64_t ephemeral_owner{};

            if (prev_node->is_ephemeral)
            {
                ephemeral_owner = prev_node->stat.ephemeralOwner;
                std::lock_guard w_lock(store.ephemerals_mutex);
                store.ephemerals[ephemeral_owner].erase(request.path);
            }

            undo = [prev_node, &store, ephemeral_owner, path = request.path, pzxid, child_basename] {
                if (prev_node->is_ephemeral)
                {
                    std::lock_guard w_lock(store.ephemerals_mutex);
                    store.ephemerals[ephemeral_owner].emplace(path);
                }
                store.acl_map.addUsage(prev_node->acl_id);

                store.container.emplace(path, prev_node);
                auto undo_parent = store.container.at(getParentPath(path));
                {
                    ++(undo_parent->stat.numChildren);
                    undo_parent->stat.pzxid = pzxid;
                    undo_parent->children.insert(child_basename);
                }
            };
        }

        return {response_ptr, undo};
    }

    KeeperStore::ResponsesForSessions processWatches(KeeperStore::Watches & watches, KeeperStore::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::DELETED);
    }
};

struct StoreRequestExists final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /* zxid */, int64_t /* session_id */, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperExistsResponse & response = dynamic_cast<Coordination::ZooKeeperExistsResponse &>(*response_ptr);
        Coordination::ZooKeeperExistsRequest & request = dynamic_cast<Coordination::ZooKeeperExistsRequest &>(*zk_request);

        auto node = store.container.get(request.path);
        if (node != nullptr)
        {
            {
                response.stat = node->statForResponse();
            }
            response.error = Coordination::Error::ZOK;
        }
        else
        {
            response.error = Coordination::Error::ZNONODE;
        }

        return {response_ptr, {}};
    }
};

struct StoreRequestSet final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Write, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Write, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t zxid, int64_t /* session_id */, int64_t time) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetResponse & response = dynamic_cast<Coordination::ZooKeeperSetResponse &>(*response_ptr);
        Coordination::ZooKeeperSetRequest & request = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*zk_request);
        Undo undo;

        auto node = store.container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version == -1 || request.version == node->stat.version)
        {
            auto prev_node = node->clone();
            {
                ++node->stat.version;
                node->stat.mzxid = zxid;
                node->stat.mtime = time;
                node->stat.dataLength = request.data.length();
                node->data = request.data;
            }

            auto parent = store.container.at(getParentPath(request.path));
            response.stat = node->statForResponse();
            response.error = Coordination::Error::ZOK;

            undo = [prev_node, &store, path = request.path] { store.container.emplace(path, prev_node); };
        }
        else
        {
            response.error = Coordination::Error::ZBADVERSION;
        }

        return {response_ptr, undo};
    }

    KeeperStore::ResponsesForSessions processWatches(KeeperStore::Watches & watches, KeeperStore::Watches & list_watches) const override
    {
        return processWatchesImpl(zk_request->getPath(), watches, list_watches, Coordination::Event::CHANGED);
    }
};

struct StoreRequestList final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Read, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Read, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperListRequest & request = dynamic_cast<Coordination::ZooKeeperListRequest &>(*zk_request);

        auto node = store.container.get(request.path);
        if (node == nullptr)
        {
            response_ptr->error = Coordination::Error::ZNONODE;
            return {response_ptr, {}};
        }

        auto path_prefix = request.path;
        if (path_prefix.empty())
            throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: path cannot be empty");

        if (response_ptr->getOpNum() == Coordination::OpNum::List || response_ptr->getOpNum() == Coordination::OpNum::FilteredList)
        {
            using enum Coordination::ZooKeeperFilteredListRequest::ListRequestType;
            auto list_request_type = ALL;
            if (auto * filtered_list_request = dynamic_cast<Coordination::ZooKeeperFilteredListRequest *>(&request))
            {
                list_request_type = filtered_list_request->list_request_type;
            }

            Coordination::ZooKeeperListResponse & response = dynamic_cast<Coordination::ZooKeeperListResponse &>(*response_ptr);

            response.stat = node->statForResponse();

            if (list_request_type == ALL)
            {
                response.names.reserve(node->children.size());
                response.names.insert(response.names.end(), node->children.begin(), node->children.end());
                return {response_ptr, {}};
            }

            auto add_child = [&](const auto & child)
            {
                auto child_node = store.container.get(request.path + "/" + child);
                if (node == nullptr)
                {
                    LOG_ERROR(
                        &Poco::Logger::get("StoreRequestList"),
                        "Inconsistency found between uncommitted and committed data, can't get child {} for {} ."
                        "Keeper will terminate to avoid undefined behaviour.", child, request.path);
                    std::terminate();
                }

                const auto is_ephemeral = child_node->stat.ephemeralOwner != 0;
                return (is_ephemeral && list_request_type == EPHEMERAL_ONLY) || (!is_ephemeral && list_request_type == PERSISTENT_ONLY);
            };

            for (const auto & child: node->children)
            {
                if (add_child(child))
                    response.names.push_back(child);
            }
        }
        else
        {
            Coordination::ZooKeeperSimpleListResponse & response = dynamic_cast<Coordination::ZooKeeperSimpleListResponse &>(*response_ptr);
            response.names.reserve(node->children.size());
            response.names.insert(response.names.end(), node->children.begin(), node->children.end());
        }

        response_ptr->error = Coordination::Error::ZOK;

        return {response_ptr, {}};
    }
};

struct StoreRequestCheck final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Read, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Read, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperCheckResponse & response = dynamic_cast<Coordination::ZooKeeperCheckResponse &>(*response_ptr);
        Coordination::ZooKeeperCheckRequest & request = dynamic_cast<Coordination::ZooKeeperCheckRequest &>(*zk_request);

        auto node = store.container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != node->stat.version) /// don't need lock
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else
        {
            response.error = Coordination::Error::ZOK;
        }
        return {response_ptr, {}};
    }
};

struct StoreRequestSetACL final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            return checkACL(Coordination::ACL::Admin, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Admin, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /*zxid*/, int64_t session_id, int64_t /* time */) const override
    {
        auto & container = store.container;

        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperSetACLResponse & response = dynamic_cast<Coordination::ZooKeeperSetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperSetACLRequest & request = dynamic_cast<Coordination::ZooKeeperSetACLRequest &>(*zk_request);
        auto node = container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else if (request.version != -1 && request.version != node->stat.aversion)
        {
            response.error = Coordination::Error::ZBADVERSION;
        }
        else
        {
            Coordination::ACLs node_acls;
            {
                std::lock_guard lock(store.auth_mutex);
                auto & session_auth_ids = store.session_and_auth[session_id];

                if (!fixupACL(request.acls, session_auth_ids, node_acls))
                {
                    response.error = Coordination::Error::ZINVALIDACL;
                    return {response_ptr, {}};
                }
            }

            uint64_t acl_id = store.acl_map.convertACLs(node_acls);
            store.acl_map.addUsage(acl_id);

            node->acl_id = acl_id;
            ++node->stat.aversion;

            response.stat = node->stat;
            response.error = Coordination::Error::ZOK;
        }

        /// It cannot be used inside multi-transaction?
        return {response_ptr, {}};
    }
};

struct StoreRequestGetACL final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        auto & container = store.container;
        auto node = container.get(zk_request->getPath());
        if (node == nullptr)
            return true;

        const auto & node_acls = store.acl_map.convertNumber(node->acl_id);
        if (node_acls.empty())
            return true;

        std::shared_lock r_lock(store.auth_mutex);
        auto it = store.session_and_auth.find(session_id);
        if (it != store.session_and_auth.end())
        {
            const auto & session_auths = it->second;
            /// LOL, GetACL require more permissions, then SetACL...
            return checkACL(Coordination::ACL::Admin | Coordination::ACL::Read, node_acls, session_auths);
        }
        else
        {
            std::vector<Coordination::AuthID> empty_auth_ids;
            return checkACL(Coordination::ACL::Admin | Coordination::ACL::Read, node_acls, empty_auth_ids);
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /*zxid*/, int64_t /*session_id*/, int64_t /* time */) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperGetACLResponse & response = dynamic_cast<Coordination::ZooKeeperGetACLResponse &>(*response_ptr);
        Coordination::ZooKeeperGetACLRequest & request = dynamic_cast<Coordination::ZooKeeperGetACLRequest &>(*zk_request);
        auto & container = store.container;
        auto node = container.get(request.path);
        if (node == nullptr)
        {
            response.error = Coordination::Error::ZNONODE;
        }
        else
        {
            response.stat = node->stat;
            response.acl = store.acl_map.convertNumber(node->acl_id);
        }

        return {response_ptr, {}};
    }
};

struct StoreRequestAuth final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t /*zxid*/, int64_t session_id, int64_t /* time */) const override
    {
        Coordination::ZooKeeperAuthRequest & auth_request = dynamic_cast<Coordination::ZooKeeperAuthRequest &>(*zk_request);
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperAuthResponse & auth_response = dynamic_cast<Coordination::ZooKeeperAuthResponse &>(*response_ptr);
        auto & sessions_and_auth = store.session_and_auth;

        if (auth_request.scheme != "digest" || std::count(auth_request.data.begin(), auth_request.data.end(), ':') != 1)
        {
            auth_response.error = Coordination::Error::ZAUTHFAILED;
        }
        else
        {
            auto digest = generateDigest(auth_request.data);
            if (digest == store.super_digest)
            {
                Coordination::AuthID auth{"super", ""};

                std::lock_guard w_lock(store.auth_mutex);
                sessions_and_auth[session_id].emplace_back(auth);
            }
            else
            {
                Coordination::AuthID auth{auth_request.scheme, digest};

                std::lock_guard w_lock(store.auth_mutex);
                auto & session_ids = sessions_and_auth[session_id];
                if (std::find(session_ids.begin(), session_ids.end(), auth) == session_ids.end())
                    sessions_and_auth[session_id].emplace_back(auth);
            }
        }

        return {response_ptr, {}};
    }
};

struct StoreRequestMultiTxn final : public StoreRequest
{
    using OperationType = Coordination::ZooKeeperMultiRequest::OperationType;
    OperationType operation_type = OperationType::Unspecified;

    bool checkAuth(KeeperStore & store, int64_t session_id) const override
    {
        for (const auto & concrete_request : concrete_requests)
            if (!concrete_request->checkAuth(store, session_id))
                return false;
        return true;
    }

    std::vector<StoreRequestPtr> concrete_requests;
    explicit StoreRequestMultiTxn(const Coordination::ZooKeeperRequestPtr & zk_request_) : StoreRequest(zk_request_)
    {
        Coordination::ZooKeeperMultiRequest & request = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*zk_request);
        concrete_requests.reserve(request.requests.size());

        const auto check_operation_type = [&](OperationType type)
        {
            if (operation_type != OperationType::Unspecified && operation_type != type)
                throw RK::Exception(ErrorCodes::BAD_ARGUMENTS, "Illegal mixing of read and write operations in multi request");
            operation_type = type;
        };

        for (const auto & sub_request : request.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_request);
            if (sub_zk_request->getOpNum() == Coordination::OpNum::Create)
            {
                check_operation_type(OperationType::Write);
                concrete_requests.push_back(std::make_shared<StoreRequestCreate>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Remove)
            {
                check_operation_type(OperationType::Write);
                concrete_requests.push_back(std::make_shared<StoreRequestRemove>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Set)
            {
                check_operation_type(OperationType::Write);
                concrete_requests.push_back(std::make_shared<StoreRequestSet>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Check)
            {
                check_operation_type(OperationType::Write);
                concrete_requests.push_back(std::make_shared<StoreRequestCheck>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Get)
            {
                check_operation_type(OperationType::Read);
                concrete_requests.push_back(std::make_shared<StoreRequestGet>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::Exists)
            {
                check_operation_type(OperationType::Read);
                concrete_requests.push_back(std::make_shared<StoreRequestExists>(sub_zk_request));
            }
            else if (sub_zk_request->getOpNum() == Coordination::OpNum::List || sub_zk_request->getOpNum() == Coordination::OpNum::SimpleList
                     || sub_zk_request->getOpNum() == Coordination::OpNum::FilteredList)
            {
                check_operation_type(OperationType::Read);
                concrete_requests.push_back(std::make_shared<StoreRequestList>(sub_zk_request));
            }
            else
                throw RK::Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Illegal command as part of multi ZooKeeper request {}", sub_zk_request->getOpNum());
        }
    }

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & store, int64_t zxid, int64_t session_id, int64_t time) const override
    {
        Coordination::ZooKeeperResponsePtr response_ptr = zk_request->makeResponse();
        Coordination::ZooKeeperMultiResponse & response = dynamic_cast<Coordination::ZooKeeperMultiResponse &>(*response_ptr);
        std::vector<Undo> undo_actions;

        try
        {
            size_t i = 0;
            for (const auto & concrete_request : concrete_requests)
            {
                auto [cur_response, undo_action] = concrete_request->process(store, zxid, session_id, time);

                response.responses[i] = cur_response;
                if (cur_response->error != Coordination::Error::ZOK && operation_type == OperationType::Write)
                {
                    for (size_t j = 0; j <= i; ++j)
                    {
                        auto response_error = response.responses[j]->error;
                        response.responses[j] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                        response.responses[j]->error = response_error;
                    }

                    for (size_t j = i + 1; j < response.responses.size(); ++j)
                    {
                        response.responses[j] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                        response.responses[j]->error = Coordination::Error::ZRUNTIMEINCONSISTENCY;
                    }

                    for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                        if (*it)
                            (*it)();

                    return {response_ptr, {}};
                }
                else
                {
#ifdef COMPATIBLE_MODE_ZOOKEEPER
                    if (cur_response->error != Coordination::Error::ZOK)
                    {
                        auto response_error = cur_response->error;
                        response.responses[i] = std::make_shared<Coordination::ZooKeeperErrorResponse>();
                        response.responses[i]->error = response_error;
                    }
#endif
                    undo_actions.emplace_back(std::move(undo_action));
                }
                ++i;
            }

            response.error = Coordination::Error::ZOK;
            return {response_ptr, {}};
        }
        catch (...)
        {
            for (auto it = undo_actions.rbegin(); it != undo_actions.rend(); ++it)
                if (*it)
                    (*it)();
            throw;
        }
    }

    KeeperStore::ResponsesForSessions processWatches(KeeperStore::Watches & watches, KeeperStore::Watches & list_watches) const override
    {
        KeeperStore::ResponsesForSessions result;
        for (const auto & generic_request : concrete_requests)
        {
            auto responses = generic_request->processWatches(watches, list_watches);
            result.insert(result.end(), responses.begin(), responses.end());
        }
        return result;
    }
};

struct StoreRequestClose final : public StoreRequest
{
    using StoreRequest::StoreRequest;

    std::pair<Coordination::ZooKeeperResponsePtr, Undo>
    process(KeeperStore & /* store */, int64_t, int64_t, int64_t /* time */) const override
    {
        throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "Called process on close request");
    }
};

void KeeperStore::finalize()
{
    if (finalized)
        throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "keeper store already finalized");

    finalized = true;

    for (const auto & [session_id, ephemerals_paths] : ephemerals)
        for (const String & ephemeral_path : ephemerals_paths)
        {
            auto parent = container.at(getParentPath(ephemeral_path));
            {
                --parent->stat.numChildren;
                parent->children.erase(getBaseName(ephemeral_path));
            }
            container.erase(ephemeral_path);
        }

    {
        std::lock_guard lock(ephemerals_mutex);
        ephemerals.clear();
    }

    {
        std::lock_guard session_lock(session_mutex);
        std::lock_guard watch_lock(watch_mutex);
        watches.clear();
        list_watches.clear();
        sessions_and_watchers.clear();
        session_expiry_queue.clear();
        session_and_timeout.clear();
    }
    {
        std::lock_guard auth_lock(auth_mutex);
        session_and_auth.clear();
    }
}

class StoreRequestFactory final : private boost::noncopyable
{
public:
    using Creator = std::function<StoreRequestPtr(const Coordination::ZooKeeperRequestPtr &)>;
    using OpNumToRequest = std::unordered_map<Coordination::OpNum, Creator>;

    static StoreRequestFactory & instance()
    {
        static StoreRequestFactory factory;
        return factory;
    }

    StoreRequestPtr get(const Coordination::ZooKeeperRequestPtr & zk_request) const
    {
        auto it = op_num_to_request.find(zk_request->getOpNum());
        if (it == op_num_to_request.end())
            throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "Unknown operation type {}", toString(zk_request->getOpNum()));

        return it->second(zk_request);
    }

    void registerRequest(Coordination::OpNum op_num, Creator creator)
    {
        if (!op_num_to_request.try_emplace(op_num, creator).second)
            throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "Request with op num {} already registered", op_num);
    }

private:
    OpNumToRequest op_num_to_request;
    StoreRequestFactory();
};

template <Coordination::OpNum num, typename RequestT>
void registerNuKeeperRequestWrapper(StoreRequestFactory & factory)
{
    factory.registerRequest(
        num, [](const Coordination::ZooKeeperRequestPtr & zk_request) { return std::make_shared<RequestT>(zk_request); });
}


StoreRequestFactory::StoreRequestFactory()
{
    registerNuKeeperRequestWrapper<Coordination::OpNum::Heartbeat, StoreRequestHeartbeat>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::SetWatches, StoreRequestSetWatches>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Sync, StoreRequestSync>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Auth, StoreRequestAuth>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Close, StoreRequestClose>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Create, StoreRequestCreate>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Remove, StoreRequestRemove>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Exists, StoreRequestExists>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Get, StoreRequestGet>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Set, StoreRequestSet>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::List, StoreRequestList>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::SimpleList, StoreRequestList>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::FilteredList, StoreRequestList>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Check, StoreRequestCheck>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::Multi, StoreRequestMultiTxn>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::MultiRead, StoreRequestMultiTxn>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::SetACL, StoreRequestSetACL>(*this);
    registerNuKeeperRequestWrapper<Coordination::OpNum::GetACL, StoreRequestGetACL>(*this);
}


void KeeperStore::processRequest(
    ThreadSafeQueue<ResponseForSession> & responses_queue,
    const RequestForSession & request_for_session,
    std::optional<int64_t> new_last_zxid,
    bool check_acl,
    bool ignore_response)
{
    LOG_TRACE(log, "Process request {}", request_for_session.toSimpleString());

    if (new_last_zxid)
    {
        if (zxid >= *new_last_zxid)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Got new ZXID {} smaller or equal than current {}. It's a bug", *new_last_zxid, zxid);
        zxid = *new_last_zxid;
    }

    const auto & zk_request = request_for_session.request;
    const auto session_id = request_for_session.session_id;

    if (zk_request->getOpNum() == Coordination::OpNum::Close)
    {
        /// Clean ephemeral nodes and watches
        cleanEphemeralNodes(session_id, responses_queue, ignore_response);
        cleanDeadWatches(session_id);

        /// Finish connection
        auto response = std::make_shared<Coordination::ZooKeeperCloseResponse>();
        response->xid = zk_request->xid;
        response->zxid = new_last_zxid ? zxid.load() : fetchAndGetZxid();
        {
            std::lock_guard lock(session_mutex);
            session_expiry_queue.remove(session_id);
            session_and_timeout.erase(session_id);
            LOG_INFO(log, "Process close session {}, total sessions {}", toHexString(session_id), session_and_timeout.size());
        }

        {
            std::lock_guard lock(auth_mutex);
            session_and_auth.erase(session_id);
        }

        set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
        return;
    }
    else if (isNewSessionRequest(zk_request->getOpNum()))
    {
        auto * new_session_req = dynamic_cast<Coordination::ZooKeeperNewSessionRequest *>(zk_request.get());
        assert(new_session_req != nullptr);

        auto response = new_session_req->makeResponse();
        auto * new_session_resp = dynamic_cast<Coordination::ZooKeeperNewSessionResponse *>(response.get());

        /// Creating session should increase zxid
        new_session_resp->zxid = fetchAndGetZxid();

        {
            std::lock_guard lock(session_mutex);

            auto new_session_id = session_id_counter++;
            new_session_resp->session_id = new_session_id;
            new_session_resp->success = true;

            auto it = session_and_timeout.emplace(new_session_id, new_session_req->session_timeout_ms);
            if (!it.second)
            {
                LOG_DEBUG(log, "Session {} already exist, must applying a fuzzy log.", toHexString(new_session_id));
            }
            session_expiry_queue.addNewSessionOrUpdate(new_session_id, new_session_req->session_timeout_ms);
        }
        set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
        return;
    }
    else if (zk_request->getOpNum() == Coordination::OpNum::UpdateSession)
    {
        auto * update_session_req = dynamic_cast<Coordination::ZooKeeperUpdateSessionRequest *>(zk_request.get());
        assert(update_session_req != nullptr);

        auto response = update_session_req->makeResponse();
        auto * update_session_resp = dynamic_cast<Coordination::ZooKeeperUpdateSessionResponse *>(response.get());

        /// Update session should increase zxid /// TODO
        update_session_resp->zxid = fetchAndGetZxid();

        {
            std::lock_guard lock(session_mutex);
            if (!session_and_timeout.contains(session_id))
            {
                LOG_WARNING(log, "Updating session timeout for {}, but it is already expired.", toHexString(session_id));
                update_session_resp->success = false;
                update_session_resp->error = Coordination::Error::ZSESSIONEXPIRED;
            }
            else
            {
                LOG_INFO(log, "Updated session timeout for {}", toHexString(session_id));
                session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
                update_session_resp->success = true;
            }
        }
        set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
        return;
    }

    /// ZooKeeper update sessions expiry for each request, not only for heartbeats
    {
        std::lock_guard lock(session_mutex);
        if (!session_and_timeout.contains(session_id) && !new_last_zxid)
        {
            LOG_WARNING(
                log,
                "Session {} is expired, ignore op {} to path {}",
                toHexString(session_id),
                Coordination::toString(zk_request->getOpNum()),
                zk_request->getPath());
            return;
        }
        session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
    }

    if (zk_request->getOpNum() == Coordination::OpNum::Heartbeat)
    {
        StoreRequestPtr store_request = StoreRequestFactory::instance().get(zk_request);
        auto [response, _] = store_request->process(*this, zxid, session_id, request_for_session.create_time);
        response->xid = zk_request->xid;
        /// Heartbeat not increase zxid
        response->zxid = zxid;
        set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
    }
    else if (zk_request->getOpNum() == Coordination::OpNum::SetWatches)
    {
        StoreRequestPtr store_request = StoreRequestFactory::instance().get(zk_request);
        auto [response, _] = store_request->process(*this, zxid, session_id, request_for_session.create_time);
        response->xid = zk_request->xid;
        /// SetWatches not increase zxid
        response->zxid = zxid;

        auto * request = dynamic_cast<Coordination::ZooKeeperSetWatchesRequest *>(zk_request.get());

        std::lock_guard lock(watch_mutex);
        for (String & path : request->data_watches)
        {
            LOG_TRACE(log, "Register data_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
            /// register watches
            watches[path].emplace_back(session_id);
            sessions_and_watchers[session_id].emplace(path);

            /// trigger watches
            auto node = container.get(path);
            if (!node)
            {
                LOG_TRACE(
                    log, "Trigger data_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
                auto watch_responses = processWatchesImpl(path, watches, list_watches, Coordination::Event::DELETED);
                set_response(responses_queue, watch_responses, ignore_response);
            }
            else if (node->stat.mzxid > request->relative_zxid)
            {
                LOG_TRACE(
                    log, "Trigger data_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
                auto watch_responses = processWatchesImpl(path, watches, list_watches, Coordination::Event::CHANGED);
                set_response(responses_queue, watch_responses, ignore_response);
            }
        }

        for (String & path : request->exist_watches)
        {
            LOG_TRACE(log, "Register exist_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
            /// register watches
            watches[path].emplace_back(session_id);
            sessions_and_watchers[session_id].emplace(path);

            /// trigger watches
            auto node = container.get(path);
            if (node)
            {
                LOG_TRACE(
                    log, "Trigger exist_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
                auto watch_responses = processWatchesImpl(path, watches, list_watches, Coordination::Event::CREATED);
                set_response(responses_queue, watch_responses, ignore_response);
            }
        }

        for (String & path : request->list_watches)
        {
            LOG_TRACE(log, "Register list_watches for session {}, path {}, xid", toHexString(session_id), path, request->xid);
            /// register watches
            list_watches[path].emplace_back(session_id);
            sessions_and_watchers[session_id].emplace(path);

            /// trigger watches
            auto node = container.get(path);
            if (node == nullptr)
            {
                LOG_TRACE(
                    log, "Trigger list_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
                auto watch_responses = processWatchesImpl(path, watches, list_watches, Coordination::Event::DELETED);
                set_response(responses_queue, watch_responses, ignore_response);
            }
            else if (node->stat.pzxid > request->relative_zxid)
            {
                LOG_TRACE(
                    log, "Trigger list_watches when processing SetWatch operation for session {}, path {}", toHexString(session_id), path);
                auto watch_responses = processWatchesImpl(path, watches, list_watches, Coordination::Event::CHILD);
                set_response(responses_queue, watch_responses, ignore_response);
            }
        }

        /// no response for SetWatches request
        set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
    }
    else
    {
        StoreRequestPtr store_request = StoreRequestFactory::instance().get(zk_request);
        Coordination::ZooKeeperResponsePtr response;

        if (check_acl && !store_request->checkAuth(*this, session_id))
        {
            response = zk_request->makeResponse();
            /// Original ZooKeeper always throws no auth, even when user provided some credentials
            response->error = Coordination::Error::ZNOAUTH;
        }
        else
        {
            response = store_request->process(*this, zxid, session_id, request_for_session.create_time).first;
        }

        response->request_created_time_ms = request_for_session.create_time;

        response->xid = zk_request->xid;
        response->zxid = new_last_zxid ? zxid.load() : (shouldIncreaseZxid(zk_request) ? fetchAndGetZxid() : zxid.load());

        //2^19 = 524,288
        if (container.size() << 45 == 0)
        {
            LOG_INFO(log, "Container size {}, opnum {}", container.size(), Coordination::toString(zk_request->getOpNum()));
        }

        if (response->error != Coordination::Error::ZOK)
        {
            if (!(zk_request->getOpNum() == Coordination::OpNum::Remove && response->error == Coordination::Error::ZNONODE)
                && !(zk_request->getOpNum() == Coordination::OpNum::Create && response->error == Coordination::Error::ZNODEEXISTS))
            {
                LOG_TRACE(
                    log,
                    "Zxid {}, session {}, opnum {}, xid {}, error no {}, msg {}",
                    zxid,
                    toHexString(session_id),
                    Coordination::toString(zk_request->getOpNum()),
                    zk_request->xid,
                    response->error,
                    Coordination::errorMessage(response->error));
            }
        }

        if (zk_request->isReadRequest())
        {
            if (zk_request->has_watch)
            {
                std::lock_guard lock(watch_mutex);

                /// handle watch register, below 1 and 2 must be atomic
                if (response->error == Coordination::Error::ZOK
                    || (response->error == Coordination::Error::ZNONODE && zk_request->getOpNum() == Coordination::OpNum::Exists))
                {
                    /// 1. register watch
                    auto & watches_type
                        = zk_request->getOpNum() == Coordination::OpNum::List
                            || zk_request->getOpNum() == Coordination::OpNum::SimpleList
                            || zk_request->getOpNum() == Coordination::OpNum::FilteredList
                        ? list_watches
                        : watches;
                    watches_type[zk_request->getPath()].emplace_back(session_id);
                    sessions_and_watchers[session_id].emplace(zk_request->getPath());

                    LOG_TRACE(
                        log,
                        "Register watch, session {}, path {}, opnum {}, xid {}, error no {}, msg {}",
                        toHexString(session_id),
                        zk_request->getPath(),
                        Coordination::toString(zk_request->getOpNum()),
                        zk_request->xid,
                        response->error,
                        Coordination::errorMessage(response->error));
                }
                /// 2. push response to queue
                set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
            }
            else
            {
                /// push response to queue
                set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
            }
        }
        else
        {
            {
                std::lock_guard lock(watch_mutex);

                /// handle watch trigger, below 1 and 2 must be atomic
                if (zk_request->getOpNum() == Coordination::OpNum::Multi || watches.contains(zk_request->getPath())
                    || list_watches.contains(getParentPath(zk_request->getPath())))
                {
                    if (response->error == Coordination::Error::ZOK)
                    {
                        /// 1. trigger watch
                        auto watch_responses = store_request->processWatches(watches, list_watches);

                        /// 2. push watch response to queue
                        set_response(responses_queue, watch_responses, ignore_response);

                        for (auto & session_id_response : watch_responses)
                        {
                            auto * watch_response
                                = dynamic_cast<Coordination::ZooKeeperWatchResponse *>(session_id_response.response.get());
                            LOG_TRACE(
                                log,
                                "Processed watch, session {}, path {}, type {}, xid {} zxid {}",
                                toHexString(session_id_response.session_id),
                                watch_response->path,
                                watch_response->type,
                                watch_response->xid,
                                watch_response->zxid);
                        }
                    }
                }
            }

            /// push response to queue
            set_response(responses_queue, ResponseForSession{session_id, response}, ignore_response);
        }
    }
}

bool KeeperStore::updateSessionTimeout(int64_t session_id, int64_t /*session_timeout_ms*/)
{
    std::lock_guard lock(session_mutex);
    if (!session_and_timeout.contains(session_id))
    {
        LOG_WARNING(log, "Updating session timeout for {}, but it is already expired.", toHexString(session_id));
        return false;
    }
    session_expiry_queue.addNewSessionOrUpdate(session_id, session_and_timeout[session_id]);
    LOG_INFO(log, "Updated session timeout for {}", toHexString(session_id));
    return true;
}

void KeeperStore::buildChildrenSet(bool from_zk_snapshot)
{
    for (UInt32 bucket_id = 0; bucket_id < container.getBucketNum(); bucket_id++)
    {
        for (const auto & it : container.getMap(bucket_id).getMap())
        {
            if (it.first == "/")
                continue;

            auto parent_path = getParentPath(it.first);
            auto child_path = getBaseName(it.first);
            auto parent = container.get(parent_path);

            if (parent == nullptr)
                throw RK::Exception(ErrorCodes::LOGICAL_ERROR, "Error when building children set, can not find parent for node {}", it.first);

            parent->children.insert(child_path);
            if (from_zk_snapshot)
                parent->stat.numChildren++;
        }
    }
}

void KeeperStore::fillDataTreeBucket(const std::vector<BucketNodes> & all_objects_nodes, UInt32 bucket_id)
{
    for (auto && object_nodes : all_objects_nodes)
    {
        for (auto && [path, node] : object_nodes[bucket_id])
        {
            if (!container.emplace(path, std::move(node), bucket_id) && path != "/")
                throw RK::Exception(RK::ErrorCodes::LOGICAL_ERROR, "Error when filling data tree bucket {}, duplicated node {}", bucket_id, path);
        }
    }
}

void KeeperStore::buildBucketChildren(const std::vector<BucketEdges> & all_objects_edges, UInt32 bucket_id)
{
    for (const auto & object_edges : all_objects_edges)
    {
        for (const auto & [parent_path, path] : object_edges[bucket_id])
        {
            auto parent = container.get(parent_path);

            if (unlikely(parent == nullptr))
                throw RK::Exception(RK::ErrorCodes::LOGICAL_ERROR, "Can not find parent for node {}", path);

            parent->children.emplace(std::move(path));
        }
    }
}

void KeeperStore::cleanEphemeralNodes(int64_t session_id, ThreadSafeQueue<ResponseForSession> & responses_queue, bool ignore_response)
{
    LOG_DEBUG(log, "Clean ephemeral nodes for session {}", toHexString(session_id));

    std::lock_guard lock(ephemerals_mutex);
    auto it = ephemerals.find(session_id);

    if (it != ephemerals.end())
    {
        for (const auto & ephemeral_path : it->second)
        {
            LOG_TRACE(log, "Disconnect session {}, deleting its ephemeral node {}", toHexString(session_id), ephemeral_path);
            auto parent = container.at(getParentPath(ephemeral_path));
            if (!parent)
            {
                LOG_ERROR(
                    log,
                    "Logical error, disconnect session {}, ephemeral znode parent not exist {}",
                    toHexString(session_id),
                    ephemeral_path);
            }
            else
            {
                --parent->stat.numChildren;
                parent->children.erase(getBaseName(ephemeral_path));
            }
            container.erase(ephemeral_path);

            std::lock_guard watch_lock(watch_mutex);
            auto responses = processWatchesImpl(ephemeral_path, watches, list_watches, Coordination::Event::DELETED);
            set_response(responses_queue, responses, ignore_response);
        }
        ephemerals.erase(it);
    }
    else
    {
        LOG_DEBUG(log, "Session {} has no ephemeral nodes", toHexString(session_id));
    }
}

void KeeperStore::cleanDeadWatches(int64_t session_id)
{
    LOG_DEBUG(log, "Clean dead watches for session {}", toHexString(session_id));

    std::lock_guard watch_lock(watch_mutex);
    auto watches_it = sessions_and_watchers.find(session_id);

    if (watches_it != sessions_and_watchers.end())
    {
        for (const auto & watch_path : watches_it->second)
        {
            auto watch = watches.find(watch_path);
            if (watch != watches.end())
            {
                auto & watches_for_path = watch->second;
                for (auto w_it = watches_for_path.begin(); w_it != watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (watches_for_path.empty())
                    watches.erase(watch);
            }

            auto list_watch = list_watches.find(watch_path);
            if (list_watch != list_watches.end())
            {
                auto & list_watches_for_path = list_watch->second;
                for (auto w_it = list_watches_for_path.begin(); w_it != list_watches_for_path.end();)
                {
                    if (*w_it == session_id)
                        w_it = list_watches_for_path.erase(w_it);
                    else
                        ++w_it;
                }
                if (list_watches_for_path.empty())
                    list_watches.erase(list_watch);
            }
        }
        sessions_and_watchers.erase(watches_it);
    }
}

void KeeperStore::dumpWatches(WriteBufferFromOwnString & buf) const
{
    std::lock_guard lock(watch_mutex);
    for (const auto & [session_id, watches_paths] : sessions_and_watchers)
    {
        buf << toHexString(session_id) << "\n";
        for (const String & path : watches_paths)
            buf << "\t" << path << "\n";
    }
}

void KeeperStore::dumpWatchesByPath(WriteBufferFromOwnString & buf) const
{
    auto write_int_vec = [&buf](const std::vector<int64_t> & session_ids)
    {
        for (int64_t session_id : session_ids)
        {
            buf << "\t" << toHexString(session_id) << "\n";
        }
    };

    std::lock_guard lock(watch_mutex);
    for (const auto & [watch_path, sessions] : watches)
    {
        buf << watch_path << "\n";
        write_int_vec(sessions);
    }

    for (const auto & [watch_path, sessions] : list_watches)
    {
        buf << watch_path << "\n";
        write_int_vec(sessions);
    }
}

void KeeperStore::dumpSessionsAndEphemerals(WriteBufferFromOwnString & buf) const
{
    auto write_str_set = [&buf](const std::unordered_set<String> & ephemeral_paths)
    {
        for (const String & path : ephemeral_paths)
        {
            buf << "\t" << path << "\n";
        }
    };

    {
        std::lock_guard lock(session_mutex);
        buf << "Sessions dump (" << session_and_timeout.size() << "):\n";
        for (const auto & [session_id, _] : session_and_timeout)
        {
            buf << toHexString(session_id) << "\n";
        }
    }

    buf << "Sessions with Ephemerals (" << getSessionWithEphemeralNodesCount() << "):\n";
    std::lock_guard lock(ephemerals_mutex);
    for (const auto & [session_id, ephemeral_paths] : ephemerals)
    {
        buf << toHexString(session_id) << "\n";
        write_str_set(ephemeral_paths);
    }
}

uint64_t KeeperStore::getTotalWatchesCount() const
{
    std::lock_guard lock(watch_mutex);
    uint64_t ret = 0;
    for (const auto & [path, subscribed_sessions] : watches)
        ret += subscribed_sessions.size();

    for (const auto & [path, subscribed_sessions] : list_watches)
        ret += subscribed_sessions.size();

    return ret;
}

uint64_t KeeperStore::getSessionsWithWatchesCount() const
{
    std::lock_guard lock(watch_mutex);
    std::unordered_set<int64_t> counter;
    for (const auto & [path, subscribed_sessions] : watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    for (const auto & [path, subscribed_sessions] : list_watches)
        counter.insert(subscribed_sessions.begin(), subscribed_sessions.end());

    return counter.size();
}

uint64_t KeeperStore::getTotalEphemeralNodesCount() const
{
    std::lock_guard lock(ephemerals_mutex);
    uint64_t ret = 0;
    for (const auto & [session_id, nodes] : ephemerals)
        ret += nodes.size();

    return ret;
}

bool KeeperStore::containsSession(int64_t session_id) const
{
    std::lock_guard lock(session_mutex);
    return session_and_timeout.contains(session_id);
}

void KeeperStore::reset()
{
    container.clear();
    zxid = 0;

    acl_map.reset();

    /// clear session
    {
        std::lock_guard lock(session_mutex);
        session_id_counter = 1;
        session_and_auth.clear();
        session_and_timeout.clear();
        session_expiry_queue.clear();
        sessions_and_watchers.clear();
    }

    {
        std::lock_guard lock(ephemerals_mutex);
        ephemerals.clear();
    }

    {
        std::lock_guard lock(watch_mutex);
        watches.clear();
        list_watches.clear();
    }
}

std::shared_ptr<KeeperStore::BucketNodes> KeeperStore::dumpDataTree()
{
    auto result = std::make_shared<KeeperStore::BucketNodes>();
    ThreadPool object_thread_pool(MAP_BUCKET_NUM);

    for (UInt32 thread_idx = 0; thread_idx < MAP_BUCKET_NUM; thread_idx++)
    {
        object_thread_pool.trySchedule(
            [thread_idx, this, &result]
            {
                for (UInt32 bucket_idx = 0; bucket_idx < MAP_BUCKET_NUM; bucket_idx++)
                {
                    if (bucket_idx % MAP_BUCKET_NUM != thread_idx)
                        continue;

                    LOG_INFO(log, "Dump data tree for bucket {}", bucket_idx);

                    auto && bucket = this->container.getMap(bucket_idx).getMap();
                    auto & bucket_in_result = (*result)[bucket_idx];
                    bucket_in_result.reserve(bucket.size());

                    size_t key_size = 0;
                    for (auto it = bucket.begin(); it != bucket.end(); ++it)
                    {
                        auto data_size = it->first.size();
                        key_size += data_size;

                        String path_copied;
                        path_copied.resize(data_size);
                        memcopy(path_copied.data(), it->first.data(), data_size);

                        bucket_in_result.emplace_back(std::move(path_copied), it->second->cloneWithoutChildren());

                        /// Prefetch the next element, may slightly improve performance in this case.
                        auto next_it = std::next(it);
                        if (likely(next_it != bucket.end()))
                        {
                            __builtin_prefetch(next_it->first.data(), 0, 3);
                            __builtin_prefetch(next_it->second.get(), 0, 3);
                        }
                    }
                    LOG_INFO(log, "Dump data tree for bucket {} done, key_size {}, result size {}", bucket_idx, key_size, (*result)[bucket_idx].size());
                }
            });
    }

    object_thread_pool.wait();
    return result;
}

}
