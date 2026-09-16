#include <filesystem>
#include <future>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <Poco/File.h>
#include <Poco/TemporaryFile.h>

#include <Common/IO/WriteBufferFromFile.h>
#include <Common/IO/WriteHelpers.h>
#include <gtest/gtest.h>

#include <Service/NuRaftFileLogStore.h>
#include <Service/tests/raft_test_common.h>

extern char ** environ;

namespace RK
{
using namespace std::chrono_literals;

#if defined(OS_LINUX)
static int runCompactionChild(const String & path, const String & stage)
{
    std::string directory_env = "RK_COMPACTION_CRASH_DIR=" + path;
    std::string stage_env = "RK_COMPACTION_CRASH_STAGE=" + stage;
    std::vector<char *> child_env;
    for (char ** item = environ; *item; ++item)
        child_env.push_back(*item);
    child_env.push_back(directory_env.data());
    child_env.push_back(stage_env.data());
    child_env.push_back(nullptr);
    std::string executable = "/proc/self/exe";
    std::string filter = "--gtest_filter=LogCompactionCrashChild.Run";
    char * args[] = {executable.data(), filter.data(), nullptr};
    pid_t pid;
    if (posix_spawn(&pid, executable.c_str(), nullptr, nullptr, args, child_env.data()) != 0)
        throw std::runtime_error("Cannot spawn compaction test child");
    int status = 0;
    if (waitpid(pid, &status, 0) != pid)
        throw std::runtime_error("Cannot wait for compaction test child");
    return status;
}
#endif

class LogCompactionTest : public ::testing::Test
{
protected:
    using Result = std::pair<bool, String>;

    void SetUp() override
    {
        log_dir = Poco::TemporaryFile::tempName();
        restart_dir = Poco::TemporaryFile::tempName();
        store = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200);
        for (UInt64 i = 1; i <= 12; ++i)
        {
            auto entry = createLogEntry(1, "/ck/table/table1", "CREATE TABLE table1;");
            ASSERT_EQ(store->append(entry), i);
        }
        ASSERT_EQ(store->segmentStore()->getClosedSegments().size(), 5);
    }

    void TearDown() override
    {
        store->shutdown();
        store->segmentStore()->close();
        store.reset();
        Poco::File(log_dir).remove(true);
        if (Poco::File(restart_dir).exists())
            Poco::File(restart_dir).remove(true);
    }

    static auto blockRemoval(const ptr<NuRaftLogSegment> & segment) { return std::unique_lock(segment->log_mutex); }

    static auto completion(const std::shared_ptr<std::promise<Result>> & promise)
    {
        return [promise](bool & result, ptr<std::exception> & error) { promise->set_value({result, error ? error->what() : ""}); };
    }

    std::future<Result> compactAsync(UInt64 index)
    {
        auto promise = std::make_shared<std::promise<Result>>();
        auto future = promise->get_future();
        store->compact_async(index, completion(promise));
        return future;
    }

    void reopen(UInt64 snapshot_index, UInt64 committed_index = 0)
    {
        store->shutdown();
        store->segmentStore()->close();
        store = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
        store->prepareRecovery({snapshot_index, committed_index, 1000});
        store->init();
    }

    static bool waitUntil(const std::function<bool()> & ready)
    {
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!ready() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(5ms);
        return ready();
    }

    String log_dir;
    String restart_dir;
    std::unique_ptr<NuRaftFileLogStore> store;
};

TEST_F(LogCompactionTest, SlowDeletionDoesNotBlockLogOperations)
{
    store->setRetentionBoundary(100);
    auto segment = store->segmentStore()->getClosedSegments().front();
    auto blocked = blockRemoval(segment);
    auto promise = std::make_shared<std::promise<Result>>();
    auto done = promise->get_future();
    auto caller = std::async(
        std::launch::async,
        [&]
        {
            auto start = std::chrono::steady_clock::now();
            store->compact_async(3, completion(promise));
            RecordProperty(
                "compact_return_us",
                std::to_string(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count()));
        });
    EXPECT_EQ(caller.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(store->start_index(), 4);
    EXPECT_EQ(done.wait_for(150ms), std::future_status::timeout);

    auto progress = std::async(
        std::launch::async,
        [&]
        {
            EXPECT_EQ(store->entry_at(3), nullptr);
            EXPECT_EQ(store->term_at(3), 0);
            EXPECT_EQ(store->entry_at(8)->get_term(), 1);
            auto entry = createLogEntry(2, "/new", "value");
            EXPECT_EQ(store->append(entry), 13);
            EXPECT_TRUE(store->flush());
            store->write_at(11, entry);
            EXPECT_EQ(store->entry_at(11)->get_term(), 2);
            EXPECT_TRUE(store->compact(3)); /// no-op must not become a barrier for the first delete
            return compactAsync(7);
        });
    EXPECT_EQ(progress.wait_for(1s), std::future_status::ready);
    blocked.unlock();
    caller.get();
    auto second_done = progress.get();
    EXPECT_TRUE(done.get().first);
    EXPECT_TRUE(second_done.get().first);
    EXPECT_EQ(store->start_index(), 8);
    EXPECT_FALSE(Poco::File(log_dir + "/" + segment->getFileName()).exists());
}

TEST_F(LogCompactionTest, SynchronousCompactionOnlyWaitsForLogicalCompletion)
{
    store->setRetentionBoundary(100);
    auto segment = store->segmentStore()->getClosedSegments().front();
    auto blocked = blockRemoval(segment);
    auto caller = std::async(std::launch::async, [&] { return store->compact(3); });
    EXPECT_EQ(caller.wait_for(1s), std::future_status::ready);
    EXPECT_EQ(store->start_index(), 4);
    EXPECT_TRUE(Poco::File(log_dir + "/" + segment->getFileName()).exists());
    blocked.unlock();
    EXPECT_TRUE(caller.get());
    store->waitForCleanup();
    EXPECT_FALSE(Poco::File(log_dir + "/" + segment->getFileName()).exists());
}

TEST_F(LogCompactionTest, NoSnapshotMeansNoPhysicalDeletion)
{
    auto segments = store->segmentStore()->getClosedSegments();
    EXPECT_TRUE(compactAsync(20).get().first);
    EXPECT_EQ(store->start_index(), 21);
    EXPECT_EQ(store->next_slot(), 21);
    EXPECT_EQ(store->last_entry()->get_term(), 0);
    for (const auto & segment : segments)
        EXPECT_TRUE(Poco::File(log_dir + "/" + segment->getFileName()).exists());
    EXPECT_EQ(store->segmentStore()->getClosedSegments().size(), 6);
    EXPECT_FALSE(Poco::File(log_dir + "/compacted_to").exists());
}

TEST_F(LogCompactionTest, SnapshotRetentionOverridesSmallerReservedLogWindow)
{
    store->setRetentionBoundary(5);
    EXPECT_TRUE(compactAsync(9).get().first);
    EXPECT_EQ(store->start_index(), 10);
    EXPECT_EQ(store->segmentStore()->getClosedSegments().front()->firstIndex(), 5);
    EXPECT_EQ(store->entry_at(8), nullptr);
    store->setRetentionBoundary(9);
    store->waitForCleanup();
    EXPECT_EQ(store->start_index(), 10);
    EXPECT_EQ(store->segmentStore()->getClosedSegments().front()->firstIndex(), 9);
#if defined(ABORT_ON_LOGICAL_ERROR) && defined(OS_LINUX)
    /// Use the same self-exec harness as crash recovery, without forking a live worker.
    const auto status = runCompactionChild(restart_dir + "/boundary", "invalid_retention");
    EXPECT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGABRT);
#elif !defined(ABORT_ON_LOGICAL_ERROR)
    EXPECT_THROW(store->setRetentionBoundary(8), Exception);
#endif
}

TEST_F(LogCompactionTest, NoOpAndPartialSegmentCompactionPreserveCache)
{
    store->setRetentionBoundary(100);
    EXPECT_TRUE(compactAsync(2).get().first);
    auto retained = store->segmentStore()->getClosedSegments().front();
    auto blocked = blockRemoval(retained);
    auto caller = std::async(
        std::launch::async,
        [&]
        {
            EXPECT_TRUE(store->compact(1));
            EXPECT_TRUE(compactAsync(1).get().first);
            EXPECT_EQ(store->entry_at(3)->get_term(), 1);
            EXPECT_TRUE(compactAsync(3).get().first);
            EXPECT_EQ(store->start_index(), 4);
            EXPECT_EQ(store->entry_at(3), nullptr);
            EXPECT_EQ(store->entry_at(4)->get_term(), 1);
        });
    EXPECT_EQ(caller.wait_for(1s), std::future_status::ready);
    blocked.unlock();
    caller.get();
}

TEST_F(LogCompactionTest, RestartCanReadLogsHiddenByPreviousLogicalBoundary)
{
    store->setRetentionBoundary(3);
    EXPECT_TRUE(compactAsync(9).get().first);
    EXPECT_EQ(store->entry_at(5), nullptr);
    reopen(4, 12);
    EXPECT_EQ(store->start_index(), 3);
    ASSERT_NE(store->entry_at(5), nullptr);
    EXPECT_EQ(store->entry_at(5)->get_term(), 1);
}

TEST_F(LogCompactionTest, RestartClassifiesMultipleOpenFilesUsingSnapshot)
{
    EXPECT_TRUE(compactAsync(20).get().first);
    auto entry = createLogEntry(2, "/new", "value");
    EXPECT_EQ(store->append(entry), 21);
    store->flush();
    reopen(20, 21);
    EXPECT_EQ(store->next_slot(), 22);
    EXPECT_EQ(store->entry_at(12), nullptr);
    ASSERT_NE(store->entry_at(21), nullptr);
    EXPECT_EQ(store->entry_at(21)->get_term(), 2);
    EXPECT_EQ(store->append(entry), 22);
}

TEST_F(LogCompactionTest, TailReplacementDoesNotReopenRetiredOpenFile)
{
    store->compact(12);
    auto entry = createLogEntry(2, "/new", "first");
    EXPECT_EQ(store->append(entry), 13);
    auto replacement = createLogEntry(3, "/new", "replacement");
    EXPECT_NO_THROW(store->write_at(13, replacement));
    EXPECT_EQ(store->entry_at(12), nullptr);
    EXPECT_EQ(store->entry_at(13)->get_term(), 3);
    reopen(12, 13);
    EXPECT_EQ(store->entry_at(13)->get_term(), 3);
}

TEST_F(LogCompactionTest, InvalidRecoveryDoesNotRepairPartialTail)
{
    String open_path;
    for (const auto & file : std::filesystem::directory_iterator(log_dir))
        if (file.path().filename().string().find("_open_") != String::npos)
            open_path = file.path().string();
    ASSERT_FALSE(open_path.empty());
    const auto valid_size = std::filesystem::file_size(open_path);
    {
        WriteBufferFromFile out(open_path, 4096, O_WRONLY | O_APPEND);
        out.write("bad", 3);
        out.finalize();
    }
    auto probe = std::make_unique<NuRaftFileLogStore>(
        log_dir, true, FsyncMode::FSYNC, 1000, LogSegmentStore::MAX_LOG_SEGMENT_FILE_SIZE, LogEntryCodec::RAW, true);
    EXPECT_THROW(probe->prepareRecovery({4, 15, 100}), Exception);
    EXPECT_EQ(std::filesystem::file_size(open_path), valid_size + 3);
    EXPECT_NO_THROW(probe->prepareRecovery({4, 12, 100}));
    EXPECT_EQ(std::filesystem::file_size(open_path), valid_size + 3);
    probe->init();
    EXPECT_EQ(std::filesystem::file_size(open_path), valid_size);
    probe->shutdown();
    probe->segmentStore()->close();
}

TEST_F(LogCompactionTest, RecoveryRejectsOverlappingPostSnapshotRanges)
{
    auto segment = store->segmentStore()->getClosedSegments().at(2);
    std::filesystem::copy_file(log_dir + "/" + segment->getFileName(), log_dir + "/log_5_6_duplicate");
    auto probe = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
    EXPECT_THROW(probe->prepareRecovery({4, 12, 100}), Exception);
    EXPECT_NO_THROW(probe->prepareRecovery({8, 12, 100}));
}

TEST_F(LogCompactionTest, RecoveryRejectsPostSnapshotGapWithoutChangingFiles)
{
    auto segments = store->segmentStore()->getClosedSegments();
    segments[2]->remove(); /// missing [5, 6]
    auto probe = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
    EXPECT_THROW(probe->prepareRecovery({4, 12, 100}), Exception);
    EXPECT_TRUE(Poco::File(log_dir + "/" + segments[0]->getFileName()).exists());
    EXPECT_TRUE(Poco::File(log_dir + "/" + segments[3]->getFileName()).exists());
    EXPECT_NO_THROW(probe->prepareRecovery({6, 12, 100}));
}

TEST_F(LogCompactionTest, NoSnapshotCannotExplainMissingPrefix)
{
    store->segmentStore()->getClosedSegments().front()->remove();
    auto probe = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
    EXPECT_THROW(probe->prepareRecovery({0, 12, 100}), Exception);
}

TEST_F(LogCompactionTest, DeletionFailureDoesNotStopBatchAndRetriesWithoutRestart)
{
    store->setRetentionBoundary(100);
    const auto segments = store->segmentStore()->getClosedSegments();
    const auto path = log_dir + "/" + segments.front()->getFileName();
    Poco::File(path).renameTo(path + ".saved");
    Poco::File(path).createDirectory();
    Poco::File(path + "/child").createFile();
    const auto result = compactAsync(6).get();
    EXPECT_FALSE(result.first);
    EXPECT_FALSE(result.second.empty());
    EXPECT_FALSE(Poco::File(log_dir + "/" + segments[1]->getFileName()).exists());
    EXPECT_FALSE(Poco::File(log_dir + "/" + segments[2]->getFileName()).exists());
    EXPECT_TRUE(compactAsync(6).get().first); /// repeating the boundary must not lose the retry
    Poco::File(path).remove(true);
    Poco::File(path + ".saved").renameTo(path);
    EXPECT_TRUE(waitUntil([&] { return !Poco::File(path).exists(); }));
}

TEST_F(LogCompactionTest, ShutdownDrainsCallbacksButDoesNotRetryForever)
{
    store->setRetentionBoundary(100);
    auto segment = store->segmentStore()->getClosedSegments().front();
    auto blocked = blockRemoval(segment);
    auto first_done = compactAsync(4);
    auto second_done = compactAsync(8);
    auto shutdown = std::async(std::launch::async, [&] { store->shutdown(); });
    EXPECT_EQ(shutdown.wait_for(20ms), std::future_status::timeout);
    blocked.unlock();
    shutdown.get();
    EXPECT_EQ(first_done.wait_for(0s), std::future_status::ready);
    EXPECT_EQ(second_done.wait_for(0s), std::future_status::ready);
    EXPECT_TRUE(first_done.get().first);
    EXPECT_TRUE(second_done.get().first);
    EXPECT_THROW(compactAsync(12), std::logic_error);
    store->shutdown();
}

TEST_F(LogCompactionTest, PersistentDeletionFailureDoesNotPreventShutdown)
{
    store->setRetentionBoundary(100);
    const auto path = log_dir + "/" + store->segmentStore()->getClosedSegments().front()->getFileName();
    Poco::File(path).renameTo(path + ".saved");
    Poco::File(path).createDirectory();
    Poco::File(path + "/child").createFile();
    EXPECT_FALSE(compactAsync(4).get().first);
    auto shutdown = std::async(std::launch::async, [&] { store->shutdown(); });
    EXPECT_EQ(shutdown.wait_for(1s), std::future_status::ready);
    shutdown.get();
}

TEST_F(LogCompactionTest, ExperimentalMarkerIsRejectedWithoutRemovingIt)
{
    for (const auto & name : {"compacted_to", "compacted_to.tmp"})
    {
        const auto path = log_dir + "/" + name;
        Poco::File(path).createFile();
        auto probe = std::make_unique<NuRaftFileLogStore>(log_dir, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
        EXPECT_THROW(probe->prepareRecovery({8, 12, 100}), Exception);
        EXPECT_TRUE(Poco::File(path).exists());
        Poco::File(path).remove();
    }
}

TEST_F(LogCompactionTest, ThrowingCallbackDoesNotStopWorker)
{
    store->setRetentionBoundary(100);
    store->compact_async(4, [](bool &, ptr<std::exception> &) { throw std::runtime_error("callback failure"); });
    EXPECT_TRUE(compactAsync(8).get().first);
}

TEST_F(LogCompactionTest, EmptyStoreRestartsFromSnapshotWithoutMarker)
{
    EXPECT_TRUE(compactAsync(20).get().first);
    reopen(20);
    EXPECT_EQ(store->start_index(), 21);
    EXPECT_EQ(store->next_slot(), 21);
    reopen(20);
    auto entry = createLogEntry(2, "/new", "value");
    EXPECT_EQ(store->append(entry), 21);
}

TEST_F(LogCompactionTest, CorruptLatestSnapshotFallsBackAcrossOldLogicalBoundary)
{
    const auto data_dir = restart_dir + "/log";
    String snap_dir = restart_dir + "/snapshot";
    auto logs = std::make_shared<NuRaftFileLogStore>(data_dir, true, FsyncMode::FSYNC);
    KeeperSnapshotManager snapshots(snap_dir, 3, 100);
    KeeperStore tree(500);
    auto config = nuraft::cs_new<nuraft::cluster_config>();
    for (int i = 1; i <= 12; ++i)
    {
        const auto name = "node" + std::to_string(i);
        setNode(tree, name, "value");
        auto entry = createLogEntry(1, "/" + name, "value");
        logs->append(entry);
        if (i == 4 || i == 10)
        {
            snapshot meta(i, 1, config, tree.getNodesCount());
            snapshots.createSnapshot(meta, tree, tree.getZxid(), tree.getSessionIDCounter());
        }
    }
    logs->flush();
    logs->setRetentionBoundary(4);
    logs->compact(9);
    EXPECT_EQ(logs->entry_at(5), nullptr);
    logs->shutdown();
    logs->segmentStore()->close();
    const auto latest = snapshots.getSnapshots().rbegin()->second;
    std::filesystem::resize_file(latest->getObjectPaths().begin()->second, 0);
    {
        WriteBufferFromFile out(data_dir + "/last_committed_index.bin");
        writeIntBinary(UInt64(12), out);
        out.finalize();
    }

    auto recovered = std::make_shared<NuRaftFileLogStore>(
        data_dir, true, FsyncMode::FSYNC, 1000, LogSegmentStore::MAX_LOG_SEGMENT_FILE_SIZE, LogEntryCodec::RAW, true);
    KeeperResponsesQueue responses;
    auto settings = RaftSettings::getDefault();
    settings->reserved_log_items = 2;
    std::mutex mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> callbacks;
    String mutable_log_dir = data_dir;
    NuRaftStateMachine machine(responses, settings, snap_dir, mutable_log_dir, 3600, 3, mutex, callbacks, recovered);
    EXPECT_EQ(machine.last_commit_index(), 12);
    EXPECT_TRUE(machine.exists("/node5"));
    EXPECT_TRUE(machine.exists("/node12"));
    machine.shutdown();
    recovered->shutdown();
    recovered->segmentStore()->close();
}

class SnapshotDurabilityTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        directory = Poco::TemporaryFile::tempName();
        auto config = nuraft::cs_new<nuraft::cluster_config>();
        KeeperStore tree(500);
        setNode(tree, "node", "value");
        snapshot meta(4, 1, config, tree.getNodesCount());
        snapshot_store = std::make_unique<KeeperSnapshotStore>(directory, meta, 100);
        snapshot_store->init();
        snapshot_store->createObjects(tree, tree.getZxid(), tree.getSessionIDCounter());
    }

    void TearDown() override
    {
        snapshot_store.reset();
        Poco::File(directory).remove(true);
    }

    void useDirectory(const String & path) { snapshot_store->snap_dir = path; }
    String directory;
    std::unique_ptr<KeeperSnapshotStore> snapshot_store;
};

TEST_F(SnapshotDurabilityTest, ObjectSyncFailureDoesNotAuthorizeReclamation)
{
    auto original_path = snapshot_store->getObjectPaths().at(1);
    String invalid_sync_target = "/dev/null";
    snapshot_store->addObjectPath(1, invalid_sync_target);
    EXPECT_THROW(snapshot_store->sync(), Exception);
    EXPECT_FALSE(snapshot_store->isDurable());
    snapshot_store->addObjectPath(1, original_path);
    EXPECT_NO_THROW(snapshot_store->sync());
    EXPECT_TRUE(snapshot_store->isDurable());
}

TEST_F(SnapshotDurabilityTest, DirectorySyncFailureDoesNotPublishSnapshot)
{
#if defined(OS_LINUX)
    /// procfs accepts opening a directory but rejects fsync, after object fsyncs succeeded.
    useDirectory("/proc");
    EXPECT_THROW(snapshot_store->sync(), Exception);
    EXPECT_FALSE(snapshot_store->isDurable());
    useDirectory(directory);
    EXPECT_NO_THROW(snapshot_store->sync());
    EXPECT_TRUE(snapshot_store->isDurable());
#else
    GTEST_SKIP() << "Directory fsync failure injection uses procfs";
#endif
}

TEST_F(SnapshotDurabilityTest, ReceivingSnapshotIsNotPublishedBeforeConfirmation)
{
    const auto target = directory + "/received";
    KeeperSnapshotManager receiver(target, 3, 100);
    auto meta = snapshot_store->getSnapshotMeta();
    receiver.receiveSnapshotMeta(*meta);
    for (const auto & [id, path] : snapshot_store->getObjectPaths())
    {
        ptr<buffer> data;
        snapshot_store->loadObject(id, data);
        receiver.saveSnapshotObject(*meta, id, *data);
    }
    EXPECT_EQ(receiver.lastSnapshot(), nullptr);
    KeeperStore restored(500);
    ASSERT_TRUE(receiver.parseSnapshot(*meta, restored));
    receiver.confirmSnapshot(*meta);
    ASSERT_NE(receiver.lastSnapshot(), nullptr);
    EXPECT_EQ(receiver.lastSnapshot()->get_last_log_idx(), 4);
}

TEST(LogCompactionCrashChild, Run)
{
    const char * path = std::getenv("RK_COMPACTION_CRASH_DIR");
    const char * stage = std::getenv("RK_COMPACTION_CRASH_STAGE");
    if (!path || !stage)
        GTEST_SKIP() << "Executed by the crash-recovery parent";
    auto child_store = std::make_unique<NuRaftFileLogStore>(path, true, FsyncMode::FSYNC, 1000, 200);
    for (int i = 0; i < 12; ++i)
    {
        auto entry = createLogEntry(1, "/ck/table/table1", "CREATE TABLE table1;");
        child_store->append(entry);
    }
    child_store->flush();
    if (String(stage) == "invalid_retention")
    {
        child_store->setRetentionBoundary(100);
        child_store->setRetentionBoundary(1);
    }
    if (String(stage) == "partial")
    {
        child_store->segmentStore()->setRetentionBoundary(100);
        auto detached = child_store->segmentStore()->detachSegments(8);
        detached.at(1)->remove();
    }
    else
    {
        child_store->compact(20);
        if (String(stage) == "replacement")
        {
            auto entry = createLogEntry(2, "/new", "value");
            child_store->append(entry);
            child_store->flush();
        }
    }
    /// Deliberately bypass destructors, close, and worker shutdown.
    ::_exit(0);
}

TEST_F(LogCompactionTest, ProcessInterruptionDuringReclamation)
{
#if defined(OS_LINUX)
    for (const auto & stage : {"detached", "partial", "replacement"})
    {
        const auto path = restart_dir + "/" + stage;
        const auto status = runCompactionChild(path, stage);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
        auto recovered = std::make_unique<NuRaftFileLogStore>(path, true, FsyncMode::FSYNC, 1000, 200, LogEntryCodec::RAW, true);
        const bool partial = String(stage) == "partial";
        recovered->prepareRecovery({partial ? 8UL : 20UL, 0, 100});
        recovered->init();
        EXPECT_EQ(recovered->next_slot(), partial ? 13 : (String(stage) == "replacement" ? 22 : 21));
        if (partial)
            EXPECT_NE(recovered->entry_at(9), nullptr);
        else if (String(stage) == "replacement")
            EXPECT_EQ(recovered->entry_at(21)->get_term(), 2);
        recovered->shutdown();
        recovered->segmentStore()->close();
    }
#else
    GTEST_SKIP() << "Self-exec crash test uses /proc/self/exe";
#endif
}
}
