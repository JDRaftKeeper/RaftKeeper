#pragma once

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <random>

#include <Common/ThreadPool.h>

#include <ZooKeeper/ZooKeeper.h>
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperConstants.h>
#include <ZooKeeper/ZooKeeperImpl.h>

using String = std::string;
using Strings = std::vector<String>;
using Ops = std::vector<std::pair<Coordination::OpNum, size_t>>;
using ZooKeeperPtr = std::shared_ptr<Coordination::ZooKeeper>;

struct ClickHouseWorkloadOptions
{
    bool enabled = false;
    UInt64 target_read_qps = 30000;
    UInt64 target_write_qps = 15000;
    UInt64 target_create_qps = 0;
    size_t metadata_groups = 1;
    size_t children_per_list = 1000;
    size_t multi_size = 4;
    String root_name;
    bool skip_setup = false;
    bool setup_only = false;
    bool drop_late_requests = true;
    int32_t session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
    int32_t operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
};


class ReservoirSampler
{
public:
    static constexpr UInt64 DEFAULT_SIZE = 102400;

    void update(UInt64 value);

    std::vector<UInt64> getSnapshot() const;

private:
    std::atomic<UInt64> count{0};
    std::vector<std::atomic<UInt64>> values = std::vector<std::atomic<UInt64>>(DEFAULT_SIZE);
};

class Stat
{
public:
    void add(UInt64 value)
    {
        ++count;
        sum += value;
        reservoir_sampler.update(value);
    }

    static double getValue(const std::vector<UInt64> & numbers, double quantile);

    Strings report() const;

    UInt64 getCount() const { return count.load(); }

private:
    ReservoirSampler reservoir_sampler;
    std::atomic<UInt64> count{0};
    std::atomic<UInt64> sum{0};
};

inline String toString(const Ops & ops)
{
    String result;
    for (auto && [op, cnt] : ops)
    {
        result += toString(op) + ':' + RK::toString(cnt) + " ";
    }
    return result;
}


inline String toString(const Strings & strs)
{
    String result;
    const String & split_char = "\t";
    for (auto && s : strs)
    {
        result += s + split_char;
    }
    return result;
}

inline String toString(const std::vector<Coordination::ZooKeeper::Node> & nodes)
{
    String result;
    const String & split_char = " ";
    for (auto && node : nodes)
    {
        result += node.address.toString() + split_char;
    }
    return result;
}


class Runner
{
public:
    Runner(
        const Strings & hosts_strings_,
        const String & bench_path,
        size_t bench_cnt_,
        size_t concurrency_,
        size_t pipeline_depth_,
        bool shared_keeper_,
        size_t key_size_,
        size_t data_size_,
        uint32_t duration_sec_,
        bool delete_node_,
        const Ops & ops_,
        const ClickHouseWorkloadOptions & clickhouse_workload_);

    void runBenchmark();

    ZooKeeperPtr getConnection();

private:
    void work(ZooKeeperPtr, size_t thread_id);
    void workImpl(ZooKeeperPtr, size_t thread_id);
    void setupClickHouseWorkload(ZooKeeperPtr zk);
    void validateClickHouseWorkload(ZooKeeperPtr zk);
    void shutdownWorkers();

    String bench_path;
    size_t bench_cnt;
    size_t concurrency;
    size_t pipeline_depth;
    bool shared_keeper;
    size_t key_size;
    size_t data_size;
    std::optional<ThreadPool> pool;
    uint32_t duration_sec;
    bool delete_node;
    Ops ops;
    ClickHouseWorkloadOptions clickhouse_workload;

    std::atomic<bool> shutdown{false};
    std::atomic<bool> start{false};
    std::atomic<size_t> ready_workers{0};
    std::atomic<UInt64> issued_requests{0};
    std::atomic<Int64> benchmark_start_ns{0};
    std::atomic<UInt64> dropped_request_slots{0};
    std::atomic<UInt64> max_schedule_lag_us{0};
    std::condition_variable shutdown_cv;
    std::mutex shutdown_mutex;
    String hostname;
    String run_token;


    std::vector<Coordination::ZooKeeper::Node> nodes;

    Poco::Logger * logger;

    Stat read_stat;
    Stat write_stat;
    Stat stat;
    std::atomic<UInt64> error_count{0};
    String path_rand_fill_str;
    String data_rand_fill_str;

    String root_path;
};
