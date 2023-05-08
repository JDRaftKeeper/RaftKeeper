#include <Service/KeeperCommon.h>
#include <Service/NuRaftFileLogStore.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftStateMachine.h>
#include <Service/proto/Log.pb.h>
#include <Service/tests/raft_test_common.h>
#include <gtest/gtest.h>
#include <libnuraft/nuraft.hxx>
#include <Poco/File.h>
#include <Common/Stopwatch.h>
#include <common/argsToConfig.h>

using namespace nuraft;
using namespace RK;

static const UInt32 LOG_COUNT = 100000;

TEST(RaftPerformance, appendLogPerformance)
{
    Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    std::string log_dir(LOG_DIR + "/50");
    cleanDirectory(log_dir);
    ptr<NuRaftFileLogStore> file_store = cs_new<NuRaftFileLogStore>(log_dir, true);
    int key_bytes = 256;
    int value_bytes = 1024;
    //256 byte
    std::string key;
    for (int i = 0; i < key_bytes; i++)
    {
        key.append("k");
    }
    //1024 byte
    std::string data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }
    std::vector<ptr<log_entry>> entry_vec;
    int log_count = LOG_COUNT;
    for (int i = 0; i < log_count; i++)
    {
        UInt64 term = 1;
        LogOpTypePB op = OP_TYPE_CREATE;
        createEntry(term, op, key, data, entry_vec);
    }
    Stopwatch watch;
    watch.start();
    for (auto & it : entry_vec)
    {
        file_store->append(it);
    }
    watch.stop();
    int mill_second = watch.elapsedMilliseconds();
    int log_size = ((key_bytes + value_bytes) + sizeof(UInt32) * 4 + sizeof(UInt32) * 6);
    double total_size = 1.0 * log_size * log_count / 1000 / 1000;
    double byte_rate = 1.0 * total_size / mill_second * 1000;
    double count_rate = 1.0 * log_count / mill_second * 1000;
    LOG_INFO(
        log,
        "Append performance, size {}, count {}, total_size {}, micro second {}, byte rate {}, count rate {}",
        log_size,
        log_count,
        total_size,
        mill_second,
        byte_rate,
        count_rate);
    cleanDirectory(log_dir);
}

#if defined(__has_feature)
#   if not __has_feature(thread_sanitizer) && not __has_feature(undefined_behavior_sanitizer)
/// Append log performance test will invoke `append` method in a parallel fashion
/// which will lead to data race.
/// In real case we invoke append log just in one thread.
/// So we just ignore the test for TSAN.
TEST(RaftPerformance, appendLogThread)
{
    Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    std::string log_dir(LOG_DIR + "/51");
    cleanDirectory(log_dir);
    //auto log_store = LogSegmentStore::getInstance(log_dir, true);
    ptr<NuRaftFileLogStore> file_store = cs_new<NuRaftFileLogStore>(log_dir, true);
    //ASSERT_EQ(log_store->init(), 0);
    int key_bytes = 256;
    int value_bytes = 1024;
    //256 byte
    std::string key;
    for (int i = 0; i < key_bytes; i++)
    {
        key.append("k");
    }
    //1024 byte
    std::string data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }

    std::vector<int> thread_vec = {1, 2, 4, 8};
    std::atomic<int> log_index = 0;
    int log_count = LOG_COUNT;
    for (auto thread_count : thread_vec)
    {
        int thread_log_count = log_count / thread_count;
        FreeThreadPool thread_pool(thread_count);
        Stopwatch watch;
        watch.start();
        for (int thread_idx = 0; thread_idx < thread_count; thread_idx++)
        {
            thread_pool.trySchedule([&file_store, &log_index, thread_log_count, &key, &data] {
                UInt64 log_idx;
                UInt64 term = 1;
                LogOpTypePB op = OP_TYPE_CREATE;

                for (auto idx = 0; idx < thread_log_count; idx++)
                {
                    ptr<LogEntryPB> entry_pb;
                    createEntryPB(term, 0, op, key, data, entry_pb);
                    ptr<buffer> msg_buf = LogEntry::serializePB(entry_pb);
                    ptr<log_entry> entry_log = cs_new<log_entry>(term, msg_buf);
                    log_idx = file_store->append(entry_log);
                    log_index.store(log_idx, std::memory_order_relaxed);
                }
            });
        }
        //LOG_INFO(log, "Max thread count {}, running {}", thread_pool.getMaxThreads(), thread_pool.active());
        thread_pool.wait();
        watch.stop();
        int mill_second = watch.elapsedMilliseconds();
        int log_size = ((key_bytes + value_bytes) + sizeof(UInt32) * 4 + sizeof(UInt32) * 6);
        double total_size = 1.0 * log_size * log_count / 1000 / 1000; //M
        double byte_rate = 1.0 * total_size / mill_second * 1000;
        double count_rate = 1.0 * log_count / mill_second * 1000;
        LOG_INFO(
            log,
            "Append performance : thread_count {}, size {} Byte/One Log, count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS "
            "{}",
            thread_pool.getMaxThreads(),
            log_size,
            log_count,
            total_size,
            mill_second,
            byte_rate,
            count_rate);
    }
    cleanDirectory(log_dir);
}
#   endif
#endif

TEST(RaftPerformance, machineCreateThread)
{
    Poco::Logger * log = &(Poco::Logger::get("RaftStateMachine"));
    std::string snap_dir(LOG_DIR + "/51");
    cleanDirectory(snap_dir);
    KeeperResponsesQueue queue;
    RaftSettingsPtr setting_ptr = RaftSettings::getDefault();

    std::mutex new_session_id_callback_mutex;
    std::unordered_map<int64_t, ptr<std::condition_variable>> new_session_id_callback;

    NuRaftStateMachine machine(queue, setting_ptr, snap_dir, 10, 3, new_session_id_callback_mutex, new_session_id_callback);
    int key_bytes = 256;
    int value_bytes = 1024;
    //256 byte
    std::string key("/");
    for (int i = 0; i < key_bytes - 16; i++)
    {
        key.append("k");
    }
    //1024 byte
    std::string data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }

    std::vector<int> thread_vec = {1, 2, 4, 8};
    for (auto thread_size : thread_vec)
    {
        int send_count = LOG_COUNT;
        //int thread_size = 10;
        FreeThreadPool thread_pool(thread_size);
        Stopwatch watch;
        watch.start();
        for (int thread_idx = 0; thread_idx < thread_size; thread_idx++)
        {
            thread_pool.scheduleOrThrowOnError([&machine, thread_idx, thread_size, send_count, &key, &data] {
                char key_buf[257];
                int log_count = send_count / thread_size;
                int begin = thread_idx * log_count;
                int end = (thread_idx + 1) * log_count;
                //auto * thread_log = &Poco::Logger::get("client_thread");
                //LOG_INFO(thread_log, "Begin run thread {}/{}, send_count {}, range[{} - {}) ", thread_idx, thread_size, send_count, begin, end);
                while (begin < end)
                {
                    snprintf(key_buf, 257, "%s%02d%02d%010d", key.data(), thread_size, thread_idx, begin);
                    std::string key_str(key_buf);
                    //LOG_INFO(thread_log, "KEY:[{}] ", key_str);
                    createZNode(machine, key_str, data);
                    begin++;
                }
            });
        }
        //LOG_INFO(log, "Max thread count {}, running {}", thread_pool.getMaxThreads(), thread_pool.active());
        thread_pool.wait();
        watch.stop();
        int mill_second = watch.elapsedMilliseconds();
        int log_size = ((key_bytes + value_bytes) + sizeof(UInt32) * 4 + sizeof(UInt32) * 6);
        double total_size = 1.0 * log_size * send_count / 1000 / 1000;
        double byte_rate = 1.0 * total_size / mill_second * 1000;
        double count_rate = 1.0 * send_count / mill_second * 1000;
        LOG_INFO(
            log,
            "Append performance : thread_count {}, size {} Byte/OneLog, count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS "
            "{}",
            thread_size,
            log_size,
            send_count,
            total_size,
            mill_second,
            byte_rate,
            count_rate);
    }
    machine.shutdown();
    cleanDirectory(snap_dir);
}
