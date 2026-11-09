#include <gtest/gtest.h>

#include <Service/KeeperStore.h>
#include <ZooKeeper/ZooKeeperCommon.h>

using namespace RK;
using namespace Coordination;

namespace
{

class KeeperStoreTest : public ::testing::Test
{
protected:
    KeeperStore store{500};
    int64_t session_id = store.getSessionID(30000);
    int32_t xid = 0;

    ZooKeeperResponsePtr process(const ZooKeeperRequestPtr & request)
    {
        request->xid = ++xid;
        KeeperStore::KeeperResponsesQueue responses;
        store.processRequest(responses, {request, session_id, xid});
        ResponseForSession response;
        EXPECT_TRUE(responses.tryPop(response));
        return response.response;
    }

    void create(const String & path, const String & data = "data", const ACLs & acls = {})
    {
        auto request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = path;
        request->data = data;
        request->acls = acls;
        auto response = process(request);
        ASSERT_NE(response, nullptr);
        ASSERT_EQ(response->error, Error::ZOK);
    }
};

}

TEST_F(KeeperStoreTest, FilteredListToleratesDanglingChildWithStatsAndData)
{
    create("/parent");
    create("/parent/live", "live data");
    auto parent = store.getNode("/parent");
    ASSERT_NE(parent, nullptr);
    /// Simulate the inconsistent child index tolerated by an ALL list before the ACL pre-pass.
    parent->children.insert("missing");

    for (bool with_stat : {false, true})
    {
        for (bool with_data : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "with_stat=" << with_stat << ", with_data=" << with_data);
            auto request = std::make_shared<ZooKeeperFilteredListRequest>();
            request->path = "/parent";
            request->list_with_stats_and_data = true;
            request->with_stat = with_stat;
            request->with_data = with_data;
            auto response = process(request);
            ASSERT_NE(response, nullptr);
            ASSERT_EQ(response->error, Error::ZOK);
            auto & list = dynamic_cast<ZooKeeperFilteredListWithStatsAndDataResponse &>(*response);
            ASSERT_EQ(list.names.size(), 2u);
            ASSERT_EQ(list.stats.size(), with_stat ? 2u : 0u);
            ASSERT_EQ(list.data.size(), with_data ? 2u : 0u);
            size_t index = 0;
            for (const auto name : list.names)
            {
                const bool missing = name.toString() == "missing";
                if (!missing)
                    ASSERT_EQ(name.toString(), "live");
                if (with_stat)
                {
                    auto expected = missing ? Stat{} : store.getNode("/parent/live")->statForResponse();
                    EXPECT_EQ(list.stats[index].toString(), expected.toString());
                }
                if (with_data)
                    EXPECT_EQ(list.data[index], missing ? "" : "live data");
                ++index;
            }
        }
    }
}

TEST_F(KeeperStoreTest, FilteredListChecksAllChildACLsBeforeCollecting)
{
    create("/parent");
    create("/parent/public");
    create("/parent/private", "secret", {{ACL::Read, "digest", "reader:hash"}});

    using enum ZooKeeperFilteredListRequest::ListRequestType;
    for (bool authenticated : {false, true})
    {
        if (authenticated)
            store.addSessionAuth(session_id, {{"digest", "reader:hash"}});
        for (auto list_type : {ALL, EPHEMERAL_ONLY, PERSISTENT_ONLY})
        {
            for (bool with_details : {false, true})
            {
                SCOPED_TRACE(
                    ::testing::Message() << "authenticated=" << authenticated << ", list_type=" << static_cast<int>(list_type)
                                         << ", with_details=" << with_details);
                auto request = std::make_shared<ZooKeeperFilteredListRequest>();
                request->path = "/parent";
                request->list_request_type = list_type;
                request->list_with_stats_and_data = with_details;
                request->with_stat = with_details;
                request->with_data = with_details;
                auto response = process(request);
                ASSERT_NE(response, nullptr);
                auto & list = dynamic_cast<ListResponse &>(*response);
                if (!authenticated && (list_type != ALL || with_details))
                {
                    /// Even an excluded persistent child must pass the Read ACL pre-check.
                    EXPECT_EQ(response->error, Error::ZNOAUTH);
                    EXPECT_EQ(list.names.size(), 0u);
                    EXPECT_TRUE(list.stats.empty());
                    EXPECT_TRUE(list.data.empty());
                }
                else
                {
                    EXPECT_EQ(response->error, Error::ZOK);
                    const size_t count = list_type == EPHEMERAL_ONLY ? 0 : 2;
                    EXPECT_EQ(list.names.size(), count);
                    EXPECT_EQ(list.stats.size(), with_details ? count : 0);
                    EXPECT_EQ(list.data.size(), with_details ? count : 0);
                }
            }
        }
    }
}

TEST_F(KeeperStoreTest, RemoveRecursiveChecksDescendantAuthBeforeMutation)
{
    create("/tree");
    create("/tree/public");
    create("/tree/public/private", "secret", {{ACL::Delete, "digest", "owner:hash"}});
    const auto parent_stat = store.getNode("/")->statForResponse();
    const auto node_count = store.getNodesCount();

    auto request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
    request->path = "/tree";
    for (bool wrong_auth : {false, true})
    {
        if (wrong_auth)
            store.addSessionAuth(session_id, {{"digest", "other:hash"}});
        auto response = process(request);
        ASSERT_NE(response, nullptr);
        EXPECT_EQ(response->error, Error::ZNOAUTH);
        EXPECT_EQ(store.getNodesCount(), node_count);
        EXPECT_EQ(store.getNode("/")->statForResponse().toString(), parent_stat.toString());
        EXPECT_NE(store.getNode("/tree"), nullptr);
        EXPECT_NE(store.getNode("/tree/public"), nullptr);
        EXPECT_NE(store.getNode("/tree/public/private"), nullptr);
    }

    store.addSessionAuth(session_id, {{"digest", "owner:hash"}});
    auto response = process(request);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->error, Error::ZOK);
    EXPECT_EQ(store.getNodesCount(), node_count - 3);
    EXPECT_EQ(store.getNode("/tree"), nullptr);
    EXPECT_EQ(store.getNode("/tree/public"), nullptr);
    EXPECT_EQ(store.getNode("/tree/public/private"), nullptr);
}

TEST_F(KeeperStoreTest, RemoveRecursiveAllowsWorldDeleteWithoutSessionAuth)
{
    create("/tree");
    create("/tree/child", "data", {{ACL::Read | ACL::Delete, "world", "anyone"}});
    /// A fresh session has no entry in session_and_auth, not even an empty auth vector.
    session_id = store.getSessionID(30000);
    ASSERT_FALSE(store.getSessionAndAuth().contains(session_id));
    auto request = std::make_shared<ZooKeeperRemoveRecursiveRequest>();
    request->path = "/tree";
    auto response = process(request);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->error, Error::ZOK);
    EXPECT_EQ(store.getNode("/tree"), nullptr);
    EXPECT_EQ(store.getNode("/tree/child"), nullptr);
}

TEST_F(KeeperStoreTest, SequentialSuffixUsesModeSpecificParentCversion)
{
    create("/parent");
    create("/parent/ordinary");
    auto request = std::make_shared<ZooKeeperCreateRequest>();
    request->path = "/parent/seq";
    request->is_sequential = true;
    auto first = process(request);
    ASSERT_NE(first, nullptr);
    ASSERT_EQ(first->error, Error::ZOK);
    EXPECT_EQ(dynamic_cast<ZooKeeperCreateResponse &>(*first).path_created, "/parent/seq0000000001");

    auto set = std::make_shared<ZooKeeperSetRequest>();
    set->path = "/parent/ordinary";
    set->data = "updated";
    auto set_response = process(set);
    ASSERT_NE(set_response, nullptr);
    ASSERT_EQ(set_response->error, Error::ZOK);
    auto remove = std::make_shared<ZooKeeperRemoveRequest>();
    remove->path = "/parent/ordinary";
    auto remove_response = process(remove);
    ASSERT_NE(remove_response, nullptr);
    ASSERT_EQ(remove_response->error, Error::ZOK);

    auto second = process(request);
    ASSERT_NE(second, nullptr);
    ASSERT_EQ(second->error, Error::ZOK);
#ifdef COMPATIBLE_MODE_ZOOKEEPER
    EXPECT_EQ(dynamic_cast<ZooKeeperCreateResponse &>(*second).path_created, "/parent/seq0000000002");
#else
    /// Pin the documented non-parity: RK also counts child Sets/removes in ClickHouse mode.
    EXPECT_EQ(dynamic_cast<ZooKeeperCreateResponse &>(*second).path_created, "/parent/seq0000000004");
#endif
    EXPECT_NE(store.getNode("/parent/seq0000000001"), nullptr);
}

TEST_F(KeeperStoreTest, CheckStatDefaultsToWildcards)
{
    create("/node");
    auto request = std::make_shared<ZooKeeperCheckStatRequest>();
    request->path = "/node";
    EXPECT_EQ(request->version, -1);
    const auto & stat = request->stat_to_check;
    EXPECT_EQ(stat.czxid, -1);
    EXPECT_EQ(stat.mzxid, -1);
    EXPECT_EQ(stat.ctime, -1);
    EXPECT_EQ(stat.mtime, -1);
    EXPECT_EQ(stat.version, -1);
    EXPECT_EQ(stat.cversion, -1);
    EXPECT_EQ(stat.aversion, -1);
    EXPECT_EQ(stat.ephemeralOwner, -1);
    EXPECT_EQ(stat.dataLength, -1);
    EXPECT_EQ(stat.numChildren, -1);
    EXPECT_EQ(stat.pzxid, -1);
    auto response = process(request);
    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->error, Error::ZOK);
}
