#pragma once
#include <ZooKeeper/ZooKeeperCommon.h>
#include <ZooKeeper/ZooKeeperConstants.h>
#include <ZooKeeper/ZooKeeperImpl.h>
#include <Common/ThreadPool.h>
#include <ZooKeeper/ZooKeeper.h>
#include <random>

using String = std::string;
using Strings = std::vector<String>;
using Ops = std::vector<std::pair<Coordination::OpNum, size_t>>;
using ZooKeeperPtr = std::shared_ptr<Coordination::ZooKeeper>;


class ReservoirSampler
{
public:
    static constexpr UInt64 DEFAULT_SIZE = 102400;

    size_t size() const
    {
        UInt64 c = count.load();
        return c > DEFAULT_SIZE ? c : DEFAULT_SIZE;
    }

    void update(UInt64 value);

    void reset()
    {
        count.store(0);
    }

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
        count ++;
        sum += value;
        reservoir_sampler.update(value);
    }

    static double getValue(const std::vector<UInt64>& numbers, double quantile);

    Strings report() const;

    UInt64 get_cnt()
    {
        return count;
    }

private:
    String name;
    ReservoirSampler reservoir_sampler;
    std::atomic<UInt64> count{0};
    std::atomic<UInt64> sum{0};
};

String toString(const Ops& ops)
{
    String result;
    for (auto && [op, cnt] : ops)
    {
       result += toString(op) + ':' + RK::toString(cnt) + " ";
    }
    return result;
}


String toString(const Strings & strs)
{
    String result;
    const String & split_char = "\t";
    for (auto && s : strs)
    {
        result += s + split_char;
    }
    return result;
}

String toString(const std::vector<Coordination::ZooKeeper::Node> & nodes)
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
        bool shared_keeper_,
        size_t key_size_,
        size_t data_size_,
        uint32_t duration_sec_,
        bool delete_node_,
        Ops & ops_);

    void runBenchmark();

    ZooKeeperPtr getConnection();
private:
    void work(ZooKeeperPtr, size_t thread_id);

    Strings hosts_strings;
    String bench_path;
    size_t bench_cnt;
    size_t concurrency;
    bool shared_keeper;
    size_t key_size;
    size_t data_size;
    std::optional<ThreadPool> pool;
    uint32_t duration_sec;
    bool delete_node;
    Ops ops;

    __pid_t pid;

    std::atomic<bool> shutdown{false};
    String hostname;


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

