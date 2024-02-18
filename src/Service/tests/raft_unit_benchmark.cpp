#include <iostream>
#include <memory>
#include <string>
#include <time.h>

#include <Poco/File.h>
#include <Poco/Util/Application.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/randomSeed.h>
#include <boost/program_options.hpp>
#include <common/argsToConfig.h>
#include <libnuraft/nuraft.hxx>
#include <loggers/Loggers.h>

#include <Service/KeeperUtils.h>
#include <Service/LogEntry.h>
#include <Service/NuRaftLogSegment.h>
#include <Service/NuRaftLogSnapshot.h>
#include <Service/Settings.h>
#include <Service/tests/raft_test_common.h>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/ZooKeeperImpl.h>

using namespace Coordination;
using namespace RK;
using namespace nuraft;

namespace RK
{

int parseLine(char * line)
{
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char * p = line;
    while (*p < '0' || *p > '9')
        p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

struct ProcessMem
{
    uint32_t virtual_mem;
    uint32_t physical_mem;
};

ProcessMem getProcessMem()
{
    FILE * file = fopen("/proc/self/status", "r");
    char line[128];
    ProcessMem process_mem;

    while (fgets(line, 128, file) != nullptr)
    {
        if (strncmp(line, "VmSize:", 7) == 0)
        {
            process_mem.virtual_mem = parseLine(line);
            break;
        }

        if (strncmp(line, "VmRSS:", 6) == 0)
        {
            process_mem.physical_mem = parseLine(line);
            break;
        }
    }
    fclose(file);
    return process_mem;
}

}

void getCurrentTime(String & date_str)
{
    const char time_fmt[] = "%Y%m%d%H%M%S";
    time_t curr_time;
    time(&curr_time);
    char tmp_buf[24];
    std::strftime(tmp_buf, sizeof(tmp_buf), time_fmt, localtime(&curr_time));
    date_str = tmp_buf;
}

#define ASSERT_EQ_LOG(log, v1, v2) \
    { \
        if (v1 != v2) \
            LOG_WARNING(log, "v1 {}, v2 {} not equal.", v1, v2); \
    }

void logSegmentThread()
{
    Poco::Logger * log = &(Poco::Logger::get("RaftLog"));
    String log_dir(LOG_DIR + "/10");
    cleanDirectory(log_dir);

    auto log_store = LogSegmentStore::getInstance(log_dir, true);
    //10M
    UInt32 max_seg_count = 10;
    ASSERT_EQ_LOG(log, log_store->init(10000000, max_seg_count), 0)

    int key_bytes = 256;
    int value_bytes = 1024;
    //256 byte
    String key;
    for (int i = 0; i < key_bytes; i++)
    {
        key.append("k");
    }
    //1024 byte
    String data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }

    //std::mutex index_mutex;
    std::vector<int> thread_vec = {16};
    std::atomic<int> log_index = 0;
    int log_count = 100000;
    for (auto thread_count : thread_vec)
    {
        //int end_index = log_index + log_count;
        int thread_log_count = log_count / thread_count;
        FreeThreadPool thread_pool(thread_count);
        Stopwatch watch;
        watch.start();
        for (int thread_idx = 0; thread_idx < thread_count; thread_idx++)
        {
            thread_pool.trySchedule(
                [&log_store, &log_index, thread_count, thread_idx, thread_log_count, log_count, max_seg_count, &key, &data]
                {
                    UInt64 log_idx;
                    UInt64 term = 1;

                    auto * thread_log = &Poco::Logger::get("client_thread");
                    LOG_INFO(
                        thread_log,
                        "Begin run thread size {}/{}, append count {}/{}",
                        thread_idx,
                        thread_count,
                        thread_log_count,
                        log_count);

                    for (auto idx = 0; idx < thread_log_count; idx++)
                    {
                        auto entry_log = createLogEntry(term, key, data);
                        try
                        {
                            log_idx = log_store->appendEntry(entry_log);
                        }
                        catch (std::exception & ex)
                        {
                            LOG_ERROR(thread_log, "append exception : {}", ex.what());
                            continue;
                        }

                        ptr<log_entry> new_log;
                        try
                        {
                            new_log = log_store->getEntry(log_idx);
                        }
                        catch (std::exception & ex)
                        {
                            LOG_ERROR(thread_log, "get exception : {}", ex.what());
                            continue;
                        }
                        if (new_log == nullptr)
                        {
                            continue;
                        }
                        ASSERT_EQ_LOG(thread_log, new_log->get_term(), term)
                        auto deserialized_log = getZookeeperCreateRequest(new_log);
                        ASSERT_EQ_LOG(thread_log, key, deserialized_log->path)
                        ASSERT_EQ_LOG(thread_log, data, deserialized_log->data)

                        log_index.store(log_idx, std::memory_order_release);
                        if (log_store->getClosedSegments().size() + 1 >= max_seg_count)
                        {
                            //remove segment
                            log_store->removeSegment();
                        }
                    }
                    // }
                    // catch (std::exception & ex)
                    // {
                    //     LOG_ERROR(thread_log, "thread exception : {}", ex.what());
                    // }
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


void snapshotVolume(int last_index)
{
    Poco::Logger * log = &(Poco::Logger::get("RaftSnapshot"));
    String snap_dir(SNAP_DIR + "/100");
    cleanDirectory(snap_dir);
    KeeperSnapshotManager snap_mgr(snap_dir, 1000000, 20);
    ptr<cluster_config> config = cs_new<cluster_config>(1, 0);

    RaftSettingsPtr raft_settings(RaftSettings::getDefault());
    KeeperStore storage(raft_settings->dead_session_check_period_ms);

    auto mem1 = getProcessMem();
    Stopwatch watch;
    watch.start();
    UInt32 term = 1;
    //UInt32 last_index = 1000000;
    int value_bytes = 300;
    //300 BYTE
    String data;
    for (int i = 0; i < value_bytes; i++)
    {
        data.append("v");
    }

    int thread_size = 4;
    FreeThreadPool thread_pool(thread_size);
    int send_count = last_index;
    for (int thread_idx = 0; thread_idx < thread_size; thread_idx++)
    {
        thread_pool.scheduleOrThrowOnError(
            [&storage, thread_idx, thread_size, send_count, &data]
            {
                Poco::Logger * thread_log = &(Poco::Logger::get("RaftSnapshot"));
                int log_count = send_count / thread_size;
                int begin = thread_idx * log_count;
                int end = (thread_idx + 1) * log_count;
                LOG_INFO(
                    thread_log, "Begin run thread {}/{}, send_count {}, range[{} - {}) ", thread_idx, thread_size, send_count, begin, end);
                while (begin < end)
                {
                    String key = std::to_string(begin + 1);
                    setNode(storage, key, data);
                    begin++;
                }
            });
    }
    thread_pool.wait();
    /*
    for (int i = 0; i < last_index; i++)
    {
        String key = std::to_string(i + 1);
        setNode(storage, key, data);
    }
    */
    watch.stop();
    int mill_second = watch.elapsedMilliseconds();
    int total_size = 1.0 * (value_bytes + 100) * last_index / 1000000; //MB
    double byte_rate = 1.0 * total_size / mill_second * 1000;
    double count_rate = 1.0 * last_index / mill_second * 1000;
    auto mem2 = getProcessMem();
    LOG_INFO(
        log,
        "Append log : count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS {}, physicalMem {} M, virtualMem {}",
        last_index,
        total_size,
        mill_second,
        byte_rate,
        count_rate,
        1.0 * (mem2.physical_mem - mem1.physical_mem) / 1000000,
        1.0 * (mem2.virtual_mem - mem1.virtual_mem) / 1000000);

    watch.start();
    snapshot meta(last_index, term, config);
    size_t object_size = snap_mgr.createSnapshot(meta, storage);
    (void)(object_size);
    watch.stop();
    mill_second = watch.elapsedMilliseconds();
    byte_rate = 1.0 * total_size / mill_second * 1000;
    count_rate = 1.0 * last_index / mill_second * 1000;
    LOG_INFO(
        log,
        "Save snapshot : count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS {}",
        last_index,
        total_size,
        mill_second,
        byte_rate,
        count_rate);

    KeeperStore new_storage(raft_settings->dead_session_check_period_ms);
    watch.start();
    snap_mgr.parseSnapshot(meta, new_storage);

    watch.stop();
    mill_second = watch.elapsedMilliseconds();
    byte_rate = 1.0 * total_size / mill_second * 1000;
    count_rate = 1.0 * last_index / mill_second * 1000;
    LOG_INFO(
        log,
        "Load snapshot : count {}, total_size {} M, milli second {}, byte rate {} M/S, TPS {}",
        last_index,
        total_size,
        mill_second,
        byte_rate,
        count_rate);
    cleanDirectory(snap_dir);
}

int main(int argc, char ** argv)
{
    if (argc < 2)
    {
        std::cout << "Please run: xxx tag trace|notice|information" << std::endl;
        return 0;
    }
    char * tag = argv[1];
    RK::TestServer app;
    app.init(argc, argv);
    app.run();
    if (strcmp(tag, "logSegmentThread") == 0)
    {
        logSegmentThread();
    }
    else if (strcmp(tag, "snapshotVolume") == 0)
    {
        int node_size = atoi(argv[3]);
        snapshotVolume(node_size);
    }
    return 0;
}
