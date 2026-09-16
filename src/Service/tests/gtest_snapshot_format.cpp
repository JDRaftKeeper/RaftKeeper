#include <filesystem>
#include <fstream>
#include <iterator>

#include <Common/IO/ReadBufferFromMemory.h>
#include <gtest/gtest.h>

#include <Service/Crc32.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/Settings.h>
#include <Service/SnapshotConverter.h>
#include <Service/ZstdLogCodec.h>
#include <Service/tests/raft_test_common.h>
#include <ZooKeeper/ZooKeeperIO.h>

using namespace RK;
using namespace Coordination;
using namespace nuraft;

namespace
{

String readFile(const String & path)
{
    std::ifstream in(path, std::ios::binary);
    return String(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void writeFile(const String & path, const String & bytes)
{
    WriteBufferFromFile out(path);
    out.write(bytes.data(), bytes.size());
    out.next();
    out.close();
}

class SnapshotFormatTest : public ::testing::Test
{
protected:
    String dir;
    KeeperStore store{500};
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    void SetUp() override
    {
        dir = "./test_snapshot_format/" + String(::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::create_directories(dir);
        auto session = store.getSessionID(30000);
        setNode(store, "parent", "parent data", false, session);
        setNode(store, "parent/a", "a data", false, session);
        setNode(store, "parent/b", String(1024, 'b'), false, session);
        setNode(store, "ephemeral", "temporary data", true, session);
        store.addSessionAuth(session, {AuthID{"digest", "user:hash"}});
    }

    void TearDown() override { std::filesystem::remove_all(dir); }

    void checkRestored(KeeperStore & restored, SnapshotVersion version)
    {
        EXPECT_EQ(restored.getNodesCount(), store.getNodesCount());
        EXPECT_EQ(restored.getZxid(), store.getZxid());
        EXPECT_EQ(restored.getSessionIDCounter(), store.getSessionIDCounter());
        EXPECT_EQ(restored.getSessionAndTimeOut(), store.getSessionAndTimeOut());
        EXPECT_EQ(restored.getEphemerals(), store.getEphemerals());
        EXPECT_EQ(restored.getNode("/parent")->children, store.getNode("/parent")->children);
        EXPECT_EQ(restored.getNode("/parent/b")->data, store.getNode("/parent/b")->data);
        EXPECT_EQ(restored.getNode("/parent")->stat.cversion, store.getNode("/parent")->stat.cversion);
        if (version >= SnapshotVersion::V1)
        {
            EXPECT_EQ(restored.getSessionAndAuth(), store.getSessionAndAuth());
            EXPECT_EQ(restored.getACLMap().getMapping(), store.getACLMap().getMapping());
        }
    }

    std::map<ulong, String> writeSource(
        const String & path,
        SnapshotFormat format = {},
        UInt64 index = 77,
        UInt64 term = 7,
        const String & timestamp = "20260915010101",
        bool async = false)
    {
        auto meta = cs_new<snapshot>(index, term, config, store.getNodesCount());
        KeeperSnapshotStore writer(path, *meta, 2, 1, format);
        writer.init(timestamp);
        if (async)
        {
            async_result<bool>::handler_type done = [](bool, ptr<std::exception> &) {};
            SnapTask task(meta, store, done);
            writer.createObjectsAsync(task);
        }
        else
            writer.createObjects(store, store.getZxid(), store.getSessionIDCounter());
        return writer.getObjectPaths();
    }
};

TEST_F(SnapshotFormatTest, LegacyAndV4RoundTrip)
{
    const SnapshotFormat formats[]
        = {SnapshotVersion::V0,
           SnapshotVersion::V1,
           SnapshotVersion::V2,
           SnapshotVersion::V3,
           {SnapshotVersion::V4, SnapshotCodec::None},
           {SnapshotVersion::V4, SnapshotCodec::Zstd}};
    size_t index = 0;
    for (auto format : formats)
    {
        SCOPED_TRACE(++index);
        snapshot meta(index, 1, config, store.getNodesCount());
        KeeperSnapshotStore writer(dir + "/" + std::to_string(index), meta, 2, 1, format);
        writer.init();
        /// Five nodes, split into three data objects. Metadata uses three legacy objects or one V4 object.
        EXPECT_EQ(writer.createObjects(store, store.getZxid(), store.getSessionIDCounter()), 3 + format.metadataObjects());
        auto paths = writer.getObjectPaths();
        EXPECT_EQ(paths.size(), 3 + format.metadataObjects());

        for (const auto & [id, path] : paths)
        {
            auto bytes = readFile(path);
            ASSERT_EQ(bytes.substr(0, 8), "SnapHead");
            ReadBufferFromMemory in(bytes.data() + 8, bytes.size() - 8);
            auto decoded = readSnapshotFormat(in);
            EXPECT_EQ(decoded.version, format.version);
            EXPECT_EQ(decoded.codec, format.codec);
            EXPECT_EQ(in.count(), format.version == SnapshotVersion::V4 ? 8 : 1);
            if (id == 1)
            {
                std::vector<SnapshotBatchType> types;
                while (in.count() < bytes.size() - 8 - 12)
                {
                    UInt32 length, crc;
                    readIntBinary(length, in);
                    readIntBinary(crc, in);
                    String body(length, '\0');
                    in.readStrict(body.data(), length);
                    EXPECT_EQ(getCRC32(body.data(), body.size()), crc);
                    if (format.codec == SnapshotCodec::Zstd)
                    {
                        auto raw = ZstdLogCodec::decompress(body.data(), body.size());
                        body.assign(reinterpret_cast<const char *>(raw->data_begin()), raw->size());
                    }
                    types.push_back(SnapshotBatchBody::parse(body)->type);
                }
                EXPECT_EQ(types.front(), SnapshotBatchType::SNAPSHOT_TYPE_UINTMAP);
                if (format.version == SnapshotVersion::V4)
                {
                    EXPECT_NE(std::find(types.begin(), types.end(), SnapshotBatchType::SNAPSHOT_TYPE_SESSION), types.end());
                    EXPECT_EQ(types.back(), SnapshotBatchType::SNAPSHOT_TYPE_ACLMAP);
                }
            }
        }
        KeeperStore restored(500);
        writer.loadLatestSnapshot(restored);
        checkRestored(restored, format.version);
        if (format.version == SnapshotVersion::V0)
        {
            for (const auto & [id, path] : paths)
            {
                auto bytes = readFile(path);
                writeFile(path, bytes.substr(9, bytes.size() - 9 - 12));
            }
            KeeperStore headerless_restored(500);
            writer.loadLatestSnapshot(headerless_restored);
            checkRestored(headerless_restored, SnapshotVersion::V0);
        }
    }
}

TEST_F(SnapshotFormatTest, V4AsyncRoundTrip)
{
    for (auto codec : {SnapshotCodec::None, SnapshotCodec::Zstd})
    {
        auto meta = cs_new<snapshot>(1, 1, config, store.getNodesCount());
        async_result<bool>::handler_type done = [](bool, ptr<std::exception> &) {};
        SnapTask task(meta, store, done);
        KeeperSnapshotStore writer(dir + "/" + std::to_string(static_cast<uint8_t>(codec)), *meta, 2, 1, {SnapshotVersion::V4, codec});
        writer.init();
        EXPECT_EQ(writer.createObjectsAsync(task), 4);
        KeeperStore restored(500);
        writer.loadLatestSnapshot(restored);
        checkRestored(restored, SnapshotVersion::V4);
    }
}

TEST_F(SnapshotFormatTest, LegacyV4LegacyMigration)
{
    for (auto codec : {SnapshotCodec::None, SnapshotCodec::Zstd})
    {
        String case_dir = dir + "/" + std::to_string(static_cast<uint8_t>(codec));
        snapshot meta(1, 1, config, store.getNodesCount());
        KeeperSnapshotStore legacy(case_dir + "/legacy", meta, 100, 2, SnapshotVersion::V2);
        legacy.init();
        ASSERT_EQ(legacy.createObjects(store, store.getZxid(), store.getSessionIDCounter()), 4);
        KeeperStore legacy_restored(500);
        legacy.loadLatestSnapshot(legacy_restored);

        KeeperSnapshotStore upgraded(case_dir + "/upgraded", meta, 100, 2, {SnapshotVersion::V4, codec});
        upgraded.init();
        ASSERT_EQ(upgraded.createObjects(legacy_restored, legacy_restored.getZxid(), legacy_restored.getSessionIDCounter()), 2);
        KeeperStore upgraded_restored(500);
        upgraded.loadLatestSnapshot(upgraded_restored);
        checkRestored(upgraded_restored, SnapshotVersion::V4);

        KeeperSnapshotStore rewritten(case_dir + "/rewritten", meta, 100, 2, SnapshotVersion::V3);
        rewritten.init();
        ASSERT_EQ(rewritten.createObjects(upgraded_restored, upgraded_restored.getZxid(), upgraded_restored.getSessionIDCounter()), 4);
        KeeperStore rewritten_restored(500);
        rewritten.loadLatestSnapshot(rewritten_restored);
        checkRestored(rewritten_restored, SnapshotVersion::V3);
    }
}

TEST_F(SnapshotFormatTest, SmallSnapshotObjectCountAndBytes)
{
    snapshot meta(1, 1, config, store.getNodesCount());
    UInt64 sizes[2]{};
    size_t index = 0;
    for (auto version : {SnapshotVersion::V2, SnapshotVersion::V4})
    {
        KeeperSnapshotStore writer(dir + "/" + toString(version), meta, 100, 100, version);
        writer.init();
        EXPECT_EQ(writer.createObjects(store, store.getZxid(), store.getSessionIDCounter()), index == 0 ? 4 : 2);
        for (const auto & [id, path] : writer.getObjectPaths())
            sizes[index] += std::filesystem::file_size(path);
        ++index;
    }
    /// Two fewer legacy headers/tails save 42 bytes; the two V4 headers add 14 bytes.
    EXPECT_EQ(sizes[0] - sizes[1], 28);
    RecordProperty("legacy_objects", 4);
    RecordProperty("v4_objects", 2);
    RecordProperty("legacy_bytes", std::to_string(sizes[0]));
    RecordProperty("v4_bytes", std::to_string(sizes[1]));
}

TEST_F(SnapshotFormatTest, V4TransferAndRestart)
{
    for (auto codec : {SnapshotCodec::None, SnapshotCodec::Zstd})
    {
        snapshot meta(1, 1, config, store.getNodesCount());
        String case_dir = dir + "/" + std::to_string(static_cast<uint8_t>(codec));
        KeeperSnapshotStore writer(case_dir + "/source", meta, 2, 1, {SnapshotVersion::V4, codec});
        writer.init();
        ASSERT_EQ(writer.createObjects(store, store.getZxid(), store.getSessionIDCounter()), 4);
        KeeperSnapshotManager receiver(case_dir + "/target", 3, 2);
        ASSERT_TRUE(receiver.receiveSnapshotMeta(meta));
        for (const auto & [id, path] : writer.getObjectPaths())
        {
            ptr<buffer> data;
            writer.loadObject(id, data);
            ASSERT_TRUE(receiver.saveSnapshotObject(meta, id, *data));
        }
        KeeperSnapshotManager restarted(case_dir + "/target", 3, 2);
        ASSERT_EQ(restarted.loadSnapshotMetas(), 1);
        KeeperStore restored(500);
        ASSERT_TRUE(restarted.parseSnapshot(meta, restored));
        checkRestored(restored, SnapshotVersion::V4);
    }
}

TEST_F(SnapshotFormatTest, EmptyMetadata)
{
    KeeperStore empty(500);
    snapshot meta(1, 1, config, empty.getNodesCount());
    KeeperSnapshotStore writer(dir, meta, 100, 1, {SnapshotVersion::V4, SnapshotCodec::Zstd});
    writer.init();
    ASSERT_EQ(writer.createObjects(empty, empty.getZxid(), empty.getSessionIDCounter()), 2);
    KeeperStore restored(500);
    writer.loadLatestSnapshot(restored);
    EXPECT_EQ(restored.getNodesCount(), 1);
    EXPECT_EQ(restored.getSessionCount(), 0);
    EXPECT_TRUE(restored.getACLMap().getMapping().empty());
}

TEST_F(SnapshotFormatTest, MissingLastDataObjectIsRejected)
{
    snapshot meta(1, 1, config, store.getNodesCount());
    KeeperSnapshotStore writer(dir, meta, 2, 1, SnapshotVersion::V4);
    writer.init();
    ASSERT_EQ(writer.createObjects(store, store.getZxid(), store.getSessionIDCounter()), 4);
    KeeperSnapshotStore incomplete(dir, meta);
    auto paths = writer.getObjectPaths();
    for (auto & [id, path] : paths)
        if (id != 4)
            incomplete.addObjectPath(id, path);
    KeeperStore restored(500);
    EXPECT_THROW(incomplete.loadLatestSnapshot(restored), RK::Exception);
}

TEST_F(SnapshotFormatTest, InvalidAndTruncatedHeadersAreRejected)
{
    snapshot meta(1, 1, config, store.getNodesCount());
    KeeperSnapshotStore writer(dir, meta, 100, 1, SnapshotVersion::V4);
    writer.init();
    writer.createObjects(store, store.getZxid(), store.getSessionIDCounter());
    auto metadata_path = writer.getObjectPaths().at(1);
    auto original = readFile(metadata_path);
    /// Version, codec, flags and reserved fields are all validated before decoding batches.
    for (size_t offset : {8, 9, 10, 12})
    {
        auto corrupted = original;
        corrupted[offset] = static_cast<char>(255);
        writeFile(metadata_path, corrupted);
        KeeperStore restored(500);
        EXPECT_THROW(writer.loadLatestSnapshot(restored), RK::Exception);
    }
    for (size_t size : {8, 9, 10, 12, 15})
    {
        writeFile(metadata_path, original.substr(0, size));
        KeeperStore restored(500);
        EXPECT_THROW(writer.loadLatestSnapshot(restored), RK::Exception);
    }
}

TEST_F(SnapshotFormatTest, CorruptedBodyIsRejected)
{
    snapshot meta(1, 1, config, store.getNodesCount());
    KeeperSnapshotStore writer(dir, meta, 100, 1, {SnapshotVersion::V4, SnapshotCodec::Zstd});
    writer.init();
    writer.createObjects(store, store.getZxid(), store.getSessionIDCounter());
    auto path = writer.getObjectPaths().at(1);
    auto bytes = readFile(path);
    bytes[24] ^= 1; // first compressed batch, after the 16-byte file header and 8-byte batch header
    writeFile(path, bytes);
    KeeperStore restored(500);
    EXPECT_THROW(writer.loadLatestSnapshot(restored), RK::Exception);
}

TEST_F(SnapshotFormatTest, FormatSelectionDefaultsToV4)
{
    auto settings = RaftSettings::getDefault();
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V4);
    EXPECT_EQ(SnapshotFormat().version, SnapshotVersion::V4);
    settings->snapshot_compression = "zstd";
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V4);
    settings->snapshot_format_version = 2;
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V3);
    settings->snapshot_compression = "none";
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V2);
    settings->snapshot_compression = "zstd";
    settings->snapshot_format_version = 4;
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V4);
    EXPECT_EQ(settings->getSnapshotFormat().codec, SnapshotCodec::Zstd);
    settings->snapshot_compression = "none";
    EXPECT_EQ(settings->getSnapshotFormat().version, SnapshotVersion::V4);
    EXPECT_EQ(settings->getSnapshotFormat().codec, SnapshotCodec::None);
    settings->snapshot_format_version = 5;
    EXPECT_THROW(settings->getSnapshotFormat(), RK::Exception);
    settings->snapshot_format_version = 4;
    settings->snapshot_compression = "unknown";
    EXPECT_THROW(settings->getSnapshotFormat(), RK::Exception);
    EXPECT_THROW((SnapshotFormat{SnapshotVersion::V2, SnapshotCodec::Zstd}.validate()), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeRoutesPreserveCompleteState)
{
    const SnapshotFormat formats[]
        = {SnapshotVersion::V3, {SnapshotVersion::V4, SnapshotCodec::None}, {SnapshotVersion::V4, SnapshotCodec::Zstd}};
    size_t test_case = 0;
    auto node = store.getNode("/parent/a");
    node->stat.czxid = 101;
    node->stat.mzxid = 202;
    node->stat.ctime = 303;
    node->stat.mtime = 404;
    node->stat.version = 5;
    node->stat.aversion = 6;
    node->stat.pzxid = 707;
    for (auto format : formats)
    {
        for (auto target : {SnapshotVersion::V2, SnapshotVersion::V3})
        {
            if (target >= format.version)
                continue;
            for (bool async : {false, true})
            {
                auto case_dir = dir + "/" + std::to_string(++test_case);
                auto paths = writeSource(case_dir + "/source", format, 77, 7, "20260915010101", async);
                std::map<String, String> source_bytes;
                for (const auto & [id, path] : paths)
                    source_bytes[path] = readFile(path);
                auto result = downgradeSnapshot(case_dir + "/source", case_dir + "/output", target);
                EXPECT_EQ(result.source_version, format.version);
                EXPECT_EQ(result.target_version, target);
                EXPECT_EQ(result.term, 7);
                EXPECT_EQ(result.log_index, 77);
                EXPECT_EQ(result.prefix, "snapshot_20260915010101_7_77");
                EXPECT_EQ(result.target_objects, 4);
                KeeperSnapshotManager manager(result.output_dir, 3, 100);
                ASSERT_EQ(manager.loadSnapshotMetas(), 1);
                EXPECT_EQ(manager.lastSnapshot()->get_last_log_idx(), 77);
                EXPECT_EQ(manager.lastSnapshot()->get_last_log_term(), 7);
                KeeperStore restored(500);
                manager.parseSnapshot(*manager.lastSnapshot(), restored);
                checkRestored(restored, target);
                for (UInt32 bucket = 0; bucket < store.getDataTreeBucketNum(); ++bucket)
                    store.getDataTree().getMap(bucket).forEach(
                        [&restored](const String & path, const KeeperNodePtr & expected)
                        {
                            ASSERT_NE(restored.getNode(path), nullptr);
                            EXPECT_EQ(
                                serializeKeeperNode(path, expected, SnapshotVersion::V2),
                                serializeKeeperNode(path, restored.getNode(path), SnapshotVersion::V2));
                        });
                for (const auto & [path, bytes] : source_bytes)
                    EXPECT_EQ(readFile(path), bytes);
            }
        }
    }
}

TEST_F(SnapshotFormatTest, DowngradeSelectsLatestOrExplicitPrefix)
{
    writeSource(dir + "/source", SnapshotVersion::V4);
    writeSource(dir + "/source", SnapshotVersion::V4, 88, 8, "20260915020202");
    auto latest = downgradeSnapshot(dir + "/source", dir + "/latest", SnapshotVersion::V2);
    EXPECT_EQ(latest.log_index, 88);
    EXPECT_EQ(latest.term, 8);
    auto earlier = downgradeSnapshot(dir + "/source", dir + "/earlier", SnapshotVersion::V2, "snapshot_20260915010101_7_77");
    EXPECT_EQ(earlier.log_index, 77);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/missing", SnapshotVersion::V2, "missing"), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeNeverFallsBackToOlderSnapshot)
{
    writeSource(dir + "/source", SnapshotVersion::V4);
    auto latest = writeSource(dir + "/source", SnapshotVersion::V4, 88, 8, "20260915020202");
    std::filesystem::remove(latest.rbegin()->second);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2), RK::Exception);
    EXPECT_FALSE(std::filesystem::exists(dir + "/output"));
}

TEST_F(SnapshotFormatTest, DowngradeRequiresCompleteMetadata)
{
    auto paths = writeSource(dir + "/source");
    IntMap counters{{"ZXID", store.getZxid()}, {"SESSIONID", store.getSessionIDCounter()}};
    serializeSnapshotMetadata(
        counters,
        store.getSessionAndTimeOut(),
        store.getSessionAndAuth(),
        store.getACLMap().getMapping(),
        1,
        SnapshotVersion::V4,
        paths.at(1));
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2), RK::Exception);
    EXPECT_FALSE(std::filesystem::exists(dir + "/output"));
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/missing-parent/output", SnapshotVersion::V2), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeRejectsMissingSourceCounters)
{
    for (auto codec : {SnapshotCodec::None, SnapshotCodec::Zstd})
    {
        for (const String missing : {"ZXID", "SESSIONID", "OBJECTCOUNT"})
        {
            SCOPED_TRACE(missing);
            auto case_dir = dir + "/" + std::to_string(static_cast<uint8_t>(codec)) + missing;
            SnapshotFormat format{SnapshotVersion::V4, codec};
            auto paths = writeSource(case_dir + "/source", format);
            IntMap counters{
                {"ZXID", store.getZxid()}, {"SESSIONID", store.getSessionIDCounter()}, {"OBJECTCOUNT", static_cast<Int64>(paths.size())}};
            counters.erase(missing);
            serializeSnapshotMetadata(
                counters, store.getSessionAndTimeOut(), store.getSessionAndAuth(), store.getACLMap().getMapping(), 1, format, paths.at(1));
            for (auto target : {SnapshotVersion::V2, SnapshotVersion::V3})
            {
                EXPECT_THROW(downgradeSnapshot(case_dir + "/source", case_dir + "/output", target), RK::Exception);
                EXPECT_FALSE(std::filesystem::exists(case_dir + "/output"));
            }
        }
    }
}

TEST_F(SnapshotFormatTest, DowngradeRejectsMissingSourceRoot)
{
    store.removeNode("/");
    for (auto format :
         {SnapshotFormat{SnapshotVersion::V3},
          SnapshotFormat{SnapshotVersion::V4, SnapshotCodec::None},
          SnapshotFormat{SnapshotVersion::V4, SnapshotCodec::Zstd}})
    {
        auto case_dir = dir + "/" + toString(format.version) + std::to_string(static_cast<uint8_t>(format.codec));
        /// The async writer serializes buckets directly, so it can construct a checksummed snapshot without a root.
        writeSource(case_dir + "/source", format, 77, 7, "20260915010101", /*async=*/true);
        EXPECT_THROW(downgradeSnapshot(case_dir + "/source", case_dir + "/output", SnapshotVersion::V2), RK::Exception);
        EXPECT_FALSE(std::filesystem::exists(case_dir + "/output"));
    }
}

TEST_F(SnapshotFormatTest, DowngradeAcceptsTrailingOutputSeparators)
{
    writeSource(dir + "/source");
    for (const String suffix : {"/", "///"})
    {
        auto output = dir + "/output" + std::to_string(suffix.size());
        auto result = downgradeSnapshot(dir + "/source", output + suffix, SnapshotVersion::V2);
        EXPECT_EQ(result.output_dir, std::filesystem::canonical(output).string());
        EXPECT_THROW(downgradeSnapshot(dir + "/source", output + suffix, SnapshotVersion::V2), RK::Exception);
        EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/missing-parent/output" + suffix, SnapshotVersion::V2), RK::Exception);
        EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/source" + suffix, SnapshotVersion::V2), RK::Exception);
    }
    std::filesystem::create_directory_symlink(std::filesystem::absolute(dir + "/absent"), dir + "/symlink");
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/symlink/", SnapshotVersion::V2), RK::Exception);
    EXPECT_FALSE(std::filesystem::exists(dir + "/absent"));
    EXPECT_THROW(downgradeSnapshot(dir + "/source", "/", SnapshotVersion::V2), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeEmptySnapshot)
{
    KeeperStore empty(500);
    snapshot meta(77, 7, config, empty.getNodesCount());
    KeeperSnapshotStore writer(dir + "/source", meta);
    writer.init("20260915010101");
    ASSERT_EQ(writer.createObjects(empty, empty.getZxid(), empty.getSessionIDCounter()), 2);
    auto result = downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2);
    EXPECT_EQ(result.target_objects, 4);
    KeeperSnapshotManager manager(result.output_dir, 3, 100);
    ASSERT_EQ(manager.loadSnapshotMetas(), 1);
    KeeperStore restored(500);
    manager.parseSnapshot(*manager.lastSnapshot(), restored);
    EXPECT_EQ(restored.getNodesCount(), 1);
    EXPECT_EQ(restored.getSessionCount(), 0);
    EXPECT_TRUE(restored.getACLMap().getMapping().empty());
}

TEST_F(SnapshotFormatTest, DowngradeRejectsAmbiguousAndDuplicateObjects)
{
    auto paths = writeSource(dir + "/source", SnapshotVersion::V4);
    writeSource(dir + "/source", SnapshotVersion::V4, 77, 7, "20260915020202");
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2), RK::Exception);
    EXPECT_NO_THROW(downgradeSnapshot(dir + "/source", dir + "/explicit", SnapshotVersion::V2, "snapshot_20260915010101_7_77"));
    auto duplicate = paths.at(1).substr(0, paths.at(1).rfind('_')) + "_01";
    std::filesystem::copy_file(paths.at(1), duplicate);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/duplicate", SnapshotVersion::V2), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeRejectsInvalidTargetsAndOverlappingDirectories)
{
    writeSource(dir + "/source", SnapshotVersion::V4);
    for (auto target : {SnapshotVersion::V0, SnapshotVersion::V1, SnapshotVersion::V4, SnapshotVersion::UNKNOWN})
        EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", target), RK::Exception);
    writeSource(dir + "/v3", SnapshotVersion::V3);
    writeSource(dir + "/v2", SnapshotVersion::V2);
    EXPECT_THROW(downgradeSnapshot(dir + "/v3", dir + "/output", SnapshotVersion::V3), RK::Exception);
    EXPECT_THROW(downgradeSnapshot(dir + "/v2", dir + "/output", SnapshotVersion::V2), RK::Exception);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/source", SnapshotVersion::V2), RK::Exception);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/source/child", SnapshotVersion::V2), RK::Exception);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir, SnapshotVersion::V2), RK::Exception);
    std::filesystem::create_directory(dir + "/existing");
    writeFile(dir + "/existing/keep", "user data");
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/existing", SnapshotVersion::V2), RK::Exception);
    EXPECT_EQ(readFile(dir + "/existing/keep"), "user data");
    std::filesystem::create_directory_symlink(std::filesystem::absolute(dir + "/absent"), dir + "/symlink");
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/symlink", SnapshotVersion::V2), RK::Exception);
}

TEST_F(SnapshotFormatTest, DowngradeRejectsCorruptionAndMixedFormats)
{
    auto paths = writeSource(dir + "/source", SnapshotVersion::V4);
    auto original = readFile(paths.at(1));
    for (size_t offset : {8, 9, 10, 12, 24})
    {
        auto bytes = original;
        bytes[offset] = static_cast<char>(255);
        writeFile(paths.at(1), bytes);
        EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2), RK::Exception);
        EXPECT_FALSE(std::filesystem::exists(dir + "/output"));
    }
    writeFile(paths.at(1), original);
    auto second = readFile(paths.at(2));
    second[8] = 3;
    writeFile(paths.at(2), second);
    EXPECT_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2), RK::Exception);
    for (const auto & entry : std::filesystem::directory_iterator(dir))
        EXPECT_FALSE(entry.path().filename().string().starts_with("output.tmp."));
}

TEST_F(SnapshotFormatTest, DowngradeAcceptsReadOnlySourceAndRejectsUnwritableDestination)
{
    auto paths = writeSource(dir + "/source", SnapshotVersion::V4);
    for (const auto & [id, path] : paths)
        std::filesystem::permissions(path, std::filesystem::perms::owner_read);
    EXPECT_NO_THROW(downgradeSnapshot(dir + "/source", dir + "/output", SnapshotVersion::V2));
#ifdef __linux__
    EXPECT_THROW(downgradeSnapshot(dir + "/source", "/proc/raftkeeper-converter-output", SnapshotVersion::V2), RK::Exception);
#endif
}

}
