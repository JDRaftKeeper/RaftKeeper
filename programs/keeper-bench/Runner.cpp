#include "Runner.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <deque>
#include <future>
#include <iostream>
#include <optional>
#include <thread>
#include <unistd.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/DNS.h>

#include <Common/Stopwatch.h>
#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>

#include "ZooKeeper/KeeperException.h"


using namespace RK;

namespace RK::ErrorCodes
{
extern const int LOGICAL_ERROR;
}


std::vector<UInt64> ReservoirSampler::getSnapshot() const
{
    size_t s = std::min(DEFAULT_SIZE, count.load());

    std::vector<UInt64> copy(s);

    for (size_t i = 0; i < s; i++)
        copy[i] = values[i].load();

    return copy;
}

void ReservoirSampler::update(RK::UInt64 value)
{
    UInt64 c = count.fetch_add(1);

    if (c < DEFAULT_SIZE)
    {
        values[c].store(value);
        return;
    }

    static thread_local std::mt19937 gen{std::random_device{}()};
    std::uniform_int_distribution<UInt64> dis(0, c);

    UInt64 i = dis(gen);
    if (i < DEFAULT_SIZE)
        values[i].store(value);
}

double Stat::getValue(const std::vector<UInt64> & numbers, double quantile)
{
    if (quantile < 0.0 || quantile > 1.0)
    {
        LOG_ERROR(&Poco::Logger::get("AdvanceSummary"), "Quantile {} is not in [0..1]", quantile);
        return 0.0;
    }

    if (numbers.empty())
        return 0.0;

    size_t index = static_cast<size_t>(std::ceil(quantile * numbers.size()));
    return numbers[index == 0 ? 0 : index - 1];
}

Strings Stat::report() const
{
    auto numbers = reservoir_sampler.getSnapshot();
    std::sort(numbers.begin(), numbers.end());

    Strings results;
    UInt64 cnt = count.load();
    UInt64 s = sum.load();
    double average = cnt == 0 ? 0.0 : static_cast<double>(s) / static_cast<double>(cnt);

    results.emplace_back(fmt::format("avg:{:.1f}", average));
    results.emplace_back(fmt::format("p50:{:.1f}", getValue(numbers, 0.5)));
    results.emplace_back(fmt::format("p90:{:.1f}", getValue(numbers, 0.9)));
    results.emplace_back(fmt::format("p99:{:.1f}", getValue(numbers, 0.99)));
    results.emplace_back(fmt::format("p999:{:.1f}", getValue(numbers, 0.999)));
    results.emplace_back(fmt::format("cnt:{}", cnt));
    results.emplace_back(fmt::format("sum:{}", s));
    return results;
}

std::string generateRandomString(size_t length)
{
    const std::string characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static thread_local std::mt19937 engine{std::random_device{}()};
    std::uniform_int_distribution<> dist(0, characters.size() - 1);

    std::string randomString;
    randomString.reserve(length);
    for (size_t i = 0; i < length; ++i)
        randomString += characters[dist(engine)];

    return randomString;
}

template <typename T>
void shuffleArray(std::vector<T> & array)
{
    static thread_local std::mt19937 generator{std::random_device{}()};
    std::shuffle(array.begin(), array.end(), generator);
}

Runner::Runner(
    const Strings & hosts_strings_,
    const String & bench_path_,
    size_t bench_cnt_,
    size_t concurrency_,
    size_t pipeline_depth_,
    bool shared_keeper_,
    size_t key_size_,
    size_t data_size_,
    uint32_t duration_sec_,
    bool delete_node_,
    const Ops & ops_,
    const ClickHouseWorkloadOptions & clickhouse_workload_)
    : bench_path(bench_path_)
    , bench_cnt(bench_cnt_)
    , concurrency(concurrency_)
    , pipeline_depth(pipeline_depth_)
    , shared_keeper(shared_keeper_)
    , key_size(key_size_)
    , data_size(data_size_)
    , duration_sec(duration_sec_)
    , delete_node(delete_node_)
    , ops(ops_)
    , clickhouse_workload(clickhouse_workload_)
{
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    logger = &Poco::Logger::get("RaftKeeperBench");

    logger->setChannel(console_channel);
    logger->root().setChannel(console_channel);

    for (const auto & host : hosts_strings_)
    {
        nodes.emplace_back(Poco::Net::SocketAddress{host}, false);
    }


    pool.emplace(concurrency);
    hostname = Poco::Net::DNS::hostName();
    run_token = fmt::format("{}_{}_{}", hostname, getpid(), generateRandomString(8));

    if (clickhouse_workload.root_name.empty())
        root_path = bench_path + "/bench_" + run_token;
    else
        root_path = bench_path + "/" + clickhouse_workload.root_name;

    path_rand_fill_str = generateRandomString(key_size);
    data_rand_fill_str = generateRandomString(data_size);
}

std::shared_ptr<Coordination::ZooKeeper> Runner::getConnection()
{
    shuffleArray(nodes);

    return std::make_shared<Coordination::ZooKeeper>(
        nodes,
        "",
        "",
        "",
        Poco::Timespan(0, static_cast<Int64>(clickhouse_workload.session_timeout_ms) * 1000),
        Poco::Timespan(0, 1000 * 1000),
        Poco::Timespan(0, static_cast<Int64>(clickhouse_workload.operation_timeout_ms) * 1000));
}


Coordination::Error
createImpl(ZooKeeperPtr zk, const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
{
    Coordination::Error code = Coordination::Error::ZOK;
    Poco::Event event;

    auto callback = [&](const Coordination::CreateResponse & response)
    {
        SCOPE_EXIT(event.set());
        code = response.error;
        if (code == Coordination::Error::ZOK)
            path_created = response.path_created;
    };

    zk->create(path, data, mode & 1, mode & 2, {}, callback); /// TODO better mode
    event.wait();
    return code;
}

void createIfNotExists(ZooKeeperPtr zk, const std::string & path, const std::string & data)
{
    std::string path_created;

    Coordination::Error code = createImpl(zk, path, data, zkutil::CreateMode::Persistent, path_created);

    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
        return;

    throw zkutil::KeeperException(code, path);
}

void createOrSet(ZooKeeperPtr zk, const std::string & path, const std::string & data)
{
    std::string path_created;
    auto error = createImpl(zk, path, data, zkutil::CreateMode::Persistent, path_created);
    if (error == Coordination::Error::ZOK)
        return;
    if (error != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException(error, path);

    auto promise = std::make_shared<std::promise<Coordination::Error>>();
    auto future = promise->get_future();
    zk->set(path, data, -1, [promise](const Coordination::SetResponse & response) { promise->set_value(response.error); });
    error = future.get();
    if (error != Coordination::Error::ZOK)
        throw zkutil::KeeperException(error, path);
}

void createEphemeral(ZooKeeperPtr zk, const std::string & path, const std::string & data)
{
    std::string path_created;
    Coordination::Error code = createImpl(zk, path, data, zkutil::CreateMode::Ephemeral, path_created);
    if (code != Coordination::Error::ZOK)
        throw zkutil::KeeperException(code, path);
}

void createBatch(ZooKeeperPtr zk, const Coordination::Requests & requests)
{
    if (requests.empty())
        return;

    auto promise = std::make_shared<std::promise<Coordination::Error>>();
    auto future = promise->get_future();
    zk->multi(requests, [promise](const Coordination::MultiResponse & response) { promise->set_value(response.error); });

    auto error = future.get();
    if (error != Coordination::Error::ZOK)
        throw zkutil::KeeperException(error);
}

const Coordination::ACLs & getDefaultACLs()
{
    static const Coordination::ACLs acls = []
    {
        Coordination::ACL acl;
        acl.permissions = Coordination::ACL::All;
        acl.scheme = "world";
        acl.id = "anyone";
        return Coordination::ACLs{std::move(acl)};
    }();
    return acls;
}

String getMetadataGroupPath(const String & root_path, size_t group_index)
{
    return fmt::format("{}/rmt/table_shard_{:04}", root_path, group_index);
}

String getPartName(size_t index)
{
    return fmt::format("part_{:036}", index);
}

String getBlockName(size_t index)
{
    return fmt::format("block_{:067}", index);
}

void updateAtomicMax(std::atomic<UInt64> & maximum, UInt64 value)
{
    UInt64 current = maximum.load(std::memory_order_relaxed);
    while (value > current && !maximum.compare_exchange_weak(current, value, std::memory_order_relaxed))
    {
    }
}


std::future<Coordination::ListResponse>
asyncTryGetChildrenNoThrow(ZooKeeperPtr zk, const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ListResponse & response) mutable { promise->set_value(response); };

    zk->list(path, std::move(callback), std::move(watch_callback));
    return future;
}


void getChildren(ZooKeeperPtr zk, const std::string & path, Strings & result)
{
    auto future_result = asyncTryGetChildrenNoThrow(zk, path, nullptr);

    auto response = future_result.get();
    Coordination::Error code = response.error;
    if (code == Coordination::Error::ZOK)
        result = response.names.toStrings();

    if (code == Coordination::Error::ZOK)
        return;

    throw zkutil::KeeperException(code, path);
}

void Runner::setupClickHouseWorkload(ZooKeeperPtr zk)
{
    const String model_root = root_path + "/rmt";
    createIfNotExists(zk, model_root, "");

    const String part_data = generateRandomString(97);
    const String block_data = generateRandomString(42);
    const String log_data = generateRandomString(510);
    static constexpr size_t CREATE_BATCH_SIZE = 100;

    for (size_t group_index = 0; group_index < clickhouse_workload.metadata_groups; ++group_index)
    {
        const String group_path = getMetadataGroupPath(root_path, group_index);
        const String replica_path = group_path + "/replicas";
        createIfNotExists(zk, group_path, "");
        createIfNotExists(zk, group_path + "/blocks", "");
        createIfNotExists(zk, group_path + "/log", "");
        createIfNotExists(zk, group_path + "/churn", "");
        createIfNotExists(zk, group_path + "/hot", part_data);
        createIfNotExists(zk, replica_path, "");
        createIfNotExists(zk, replica_path + "/r0", "");
        createIfNotExists(zk, replica_path + "/r0/parts", "");
        createIfNotExists(zk, replica_path + "/r1", "");
        createIfNotExists(zk, replica_path + "/r1/parts", "");

        Coordination::Requests requests;
        requests.reserve(CREATE_BATCH_SIZE);
        auto add_create = [&](const String & path, const String & data)
        {
            auto request = std::make_shared<Coordination::CreateRequest>();
            request->path = path;
            request->data = data;
            requests.emplace_back(std::move(request));
            if (requests.size() == CREATE_BATCH_SIZE)
            {
                createBatch(zk, requests);
                requests.clear();
            }
        };

        for (size_t i = 0; i < clickhouse_workload.children_per_list; ++i)
        {
            add_create(group_path + "/blocks/" + getBlockName(i), block_data);
            add_create(replica_path + "/r0/parts/" + getPartName(i), part_data);
            add_create(replica_path + "/r1/parts/" + getPartName(i), part_data);
        }
        for (size_t i = 0; i < 10; ++i)
            add_create(fmt::format("{}/log/log-{:010}", group_path, i), log_data);
        createBatch(zk, requests);

        if ((group_index + 1) % std::max<size_t>(1, clickhouse_workload.metadata_groups / 10) == 0
            || group_index + 1 == clickhouse_workload.metadata_groups)
        {
            LOG_INFO(logger, "Prepared ClickHouse metadata groups {}/{}", group_index + 1, clickhouse_workload.metadata_groups);
        }
    }
}

void Runner::validateClickHouseWorkload(ZooKeeperPtr zk)
{
    auto validate_group = [&](size_t group_index)
    {
        const String group_path = getMetadataGroupPath(root_path, group_index);
        const Strings paths{
            group_path + "/blocks",
            group_path + "/replicas/r0/parts",
            group_path + "/replicas/r1/parts",
        };
        for (const auto & path : paths)
        {
            Strings children;
            getChildren(zk, path, children);
            if (children.size() < clickhouse_workload.children_per_list)
            {
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "ClickHouse workload path {} has {} children, expected at least {}",
                    path,
                    children.size(),
                    clickhouse_workload.children_per_list);
            }
        }
    };

    validate_group(0);
    if (clickhouse_workload.metadata_groups > 1)
        validate_group(clickhouse_workload.metadata_groups - 1);
}

void Runner::shutdownWorkers()
{
    shutdown.store(true);
    shutdown_cv.notify_all();
}


void Runner::work(ZooKeeperPtr zk, size_t thread_idx)
{
    try
    {
        workImpl(std::move(zk), thread_idx);
    }
    catch (...)
    {
        shutdownWorkers();
        start.store(true);
        std::cerr << getCurrentExceptionMessage(true, true /* check embedded stack trace */) << std::endl;
        throw;
    }
}

void Runner::workImpl(ZooKeeperPtr zk, size_t thread_idx)
{
    struct RequestResult
    {
        Coordination::Error error;
        UInt64 elapsed_microseconds;
    };

    struct InFlightRequest
    {
        Coordination::ZooKeeperRequestPtr request;
        std::future<RequestResult> future;
        bool measured;
    };

    std::deque<InFlightRequest> in_flight;

    auto collect_request = [&](InFlightRequest & in_flight_request)
    {
        auto result = in_flight_request.future.get();
        if (result.error != Coordination::Error::ZOK)
        {
            error_count.fetch_add(1);
            return;
        }

        if (!in_flight_request.measured)
            return;

        stat.add(result.elapsed_microseconds);
        if (in_flight_request.request->isReadRequest())
            read_stat.add(result.elapsed_microseconds);
        else
            write_stat.add(result.elapsed_microseconds);
    };

    auto submit_request = [&](
        Coordination::ZooKeeperRequestPtr request,
        bool measured = true,
        std::optional<std::chrono::steady_clock::time_point> scheduled_at = std::nullopt)
    {
        if (in_flight.size() >= pipeline_depth)
        {
            collect_request(in_flight.front());
            in_flight.pop_front();
        }

        auto promise = std::make_shared<std::promise<RequestResult>>();
        auto future = promise->get_future();
        // Paced requests include schedule debt and the pipeline-capacity wait above.
        const auto started_at = scheduled_at.value_or(std::chrono::steady_clock::now());
        Coordination::ResponseCallback callback = [promise, started_at](const Coordination::Response & response)
        {
            const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - started_at);
            promise->set_value({response.error, static_cast<UInt64>(elapsed.count())});
        };

        try
        {
            zk->executeGenericRequest(request, std::move(callback));
        }
        catch (...)
        {
            shutdownWorkers();
            throw;
        }
        in_flight.push_back({std::move(request), std::move(future), measured});
    };

    size_t loop_index = 0;
    String work_path = fmt::format("{}/worker_{}", root_path, thread_idx);

    ready_workers.fetch_add(1);
    while (!start.load() && !shutdown.load())
        std::this_thread::yield();

    std::mt19937 generator{std::random_device{}()};

    if (clickhouse_workload.enabled)
    {
        const UInt64 target_qps = clickhouse_workload.target_read_qps + clickhouse_workload.target_write_qps;
        std::uniform_int_distribution<size_t> group_distribution(0, clickhouse_workload.metadata_groups - 1);
        std::uniform_int_distribution<size_t> child_distribution(0, clickhouse_workload.children_per_list - 1);
        std::uniform_int_distribution<int> replica_distribution(0, 1);
        std::uniform_int_distribution<int> read_operation_distribution(0, 99);
        std::uniform_int_distribution<UInt64> workload_distribution(1, target_qps);
        const String multi_payload = generateRandomString(510);
        const auto request_interval = std::chrono::nanoseconds(
            std::max<Int64>(1, static_cast<Int64>((1'000'000'000ULL * concurrency) / target_qps)));
        auto next_request_time = std::chrono::steady_clock::time_point(std::chrono::nanoseconds(benchmark_start_ns.load()))
            + std::chrono::nanoseconds(static_cast<Int64>((1'000'000'000ULL * thread_idx) / target_qps));

        while (!shutdown.load())
        {
            auto now = std::chrono::steady_clock::now();
            if (now < next_request_time)
            {
                std::unique_lock lock(shutdown_mutex);
                if (shutdown_cv.wait_until(lock, next_request_time, [this] { return shutdown.load(); }))
                    break;
                now = std::chrono::steady_clock::now();
            }

            if (now > next_request_time)
            {
                auto lag = now - next_request_time;
                updateAtomicMax(
                    max_schedule_lag_us,
                    static_cast<UInt64>(std::chrono::duration_cast<std::chrono::microseconds>(lag).count()));
                if (clickhouse_workload.drop_late_requests && lag > request_interval)
                {
                    dropped_request_slots.fetch_add(static_cast<UInt64>(lag / request_interval), std::memory_order_relaxed);
                    next_request_time = now;
                }
            }
            const auto scheduled_at = next_request_time;
            next_request_time += request_interval;

            if (shutdown.load())
                break;

            UInt64 request_index = issued_requests.fetch_add(1);
            size_t group_index = group_distribution(generator);
            String group_path = getMetadataGroupPath(root_path, group_index);
            bool write_request = workload_distribution(generator) > clickhouse_workload.target_read_qps;
            if (write_request)
            {
                bool create_only = clickhouse_workload.target_create_qps == clickhouse_workload.target_write_qps;
                if (!create_only && clickhouse_workload.target_create_qps > 0)
                {
                    create_only = std::uniform_int_distribution<UInt64>(1, clickhouse_workload.target_write_qps)(generator)
                        <= clickhouse_workload.target_create_qps;
                }

                if (create_only)
                {
                    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
                    request->path = fmt::format(
                        "{}/replicas/r{}/parts/{}",
                        group_path,
                        replica_distribution(generator),
                        fmt::format("growth_{}_{:020}", run_token, request_index));
                    request->data = data_rand_fill_str;
                    request->acls = getDefaultACLs();
                    submit_request(std::move(request), true, scheduled_at);
                    continue;
                }

                String churn_path = fmt::format("{}/churn/w{}_{}", group_path, thread_idx, request_index);
                Coordination::Requests requests;
                requests.reserve(clickhouse_workload.multi_size);

                auto create_request = std::make_shared<Coordination::CreateRequest>();
                create_request->path = churn_path;
                create_request->data = multi_payload;
                requests.emplace_back(std::move(create_request));

                for (size_t i = 0; i + 2 < clickhouse_workload.multi_size; ++i)
                {
                    auto set_request = std::make_shared<Coordination::SetRequest>();
                    if (i == 0)
                    {
                        set_request->path = group_path + "/hot";
                    }
                    else
                    {
                        set_request->path = fmt::format(
                            "{}/replicas/r{}/parts/{}",
                            group_path,
                            replica_distribution(generator),
                            getPartName(child_distribution(generator)));
                    }
                    set_request->data = data_rand_fill_str;
                    requests.emplace_back(std::move(set_request));
                }

                auto remove_request = std::make_shared<Coordination::RemoveRequest>();
                remove_request->path = churn_path;
                requests.emplace_back(std::move(remove_request));

                submit_request(std::make_shared<Coordination::ZooKeeperMultiRequest>(requests, getDefaultACLs()), true, scheduled_at);
            }
            else
            {
                int operation = read_operation_distribution(generator);
                if (operation < 65)
                {
                    auto request = std::make_shared<Coordination::ZooKeeperGetRequest>();
                    request->path = fmt::format(
                        "{}/replicas/r{}/parts/{}",
                        group_path,
                        replica_distribution(generator),
                        getPartName(child_distribution(generator)));
                    submit_request(std::move(request), true, scheduled_at);
                }
                else if (operation < 98)
                {
                    auto request = std::make_shared<Coordination::ZooKeeperListRequest>();
                    int list_index = std::uniform_int_distribution<int>(0, 2)(generator);
                    if (list_index == 0)
                        request->path = group_path + "/blocks";
                    else
                        request->path = fmt::format("{}/replicas/r{}/parts", group_path, list_index - 1);
                    submit_request(std::move(request), true, scheduled_at);
                }
                else
                {
                    auto request = std::make_shared<Coordination::ZooKeeperExistsRequest>();
                    request->path = fmt::format(
                        "{}/replicas/r{}/parts/{}",
                        group_path,
                        replica_distribution(generator),
                        getPartName(child_distribution(generator)));
                    submit_request(std::move(request), true, scheduled_at);
                }
            }
        }

        for (auto & request : in_flight)
            collect_request(request);
        return;
    }

    while (!shutdown.load())
    {
        size_t create_index = 0;
        String loop_path = fmt::format("{}/loop_{}", work_path, loop_index);
        createIfNotExists(zk, loop_path, "bench");

        createIfNotExists(zk, fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, create_index), data_rand_fill_str);


        for (const auto & [op, count] : ops)
        {
            if (op == Coordination::OpNum::Create)
            {
                for (size_t i = 0; i < count && !shutdown.load(); ++i)
                {
                    ++create_index;
                    auto create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
                    create_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, create_index);
                    create_request->data = data_rand_fill_str;
                    create_request->acls = getDefaultACLs();

                    submit_request(std::move(create_request));
                }
            }
            else if (op == Coordination::OpNum::SimpleList)
            {
                for (size_t i = 0; i < count && !shutdown.load(); ++i)
                {
                    auto list_request = std::make_shared<Coordination::ZooKeeperListRequest>();
                    list_request->path = loop_path;
                    submit_request(std::move(list_request));
                }
            }
            else if (op == Coordination::OpNum::Get)
            {
                for (size_t i = 0; i < count && !shutdown.load(); ++i)
                {
                    auto get_request = std::make_shared<Coordination::ZooKeeperGetRequest>();

                    std::uniform_int_distribution<size_t> distribution(0, create_index);
                    get_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, distribution(generator));
                    submit_request(std::move(get_request));
                }
            }
            else if (op == Coordination::OpNum::Set)
            {
                for (size_t i = 0; i < count && !shutdown.load(); ++i)
                {
                    auto set_request = std::make_shared<Coordination::ZooKeeperSetRequest>();
                    std::uniform_int_distribution<size_t> distribution(0, create_index);
                    set_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, distribution(generator));
                    set_request->data = data_rand_fill_str;
                    submit_request(std::move(set_request));
                }
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported operation {}", toString(op));
            }
        }

        if (delete_node && !shutdown.load())
        {
            for (size_t i = 0; i <= create_index; ++i)
            {
                auto delete_request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
                delete_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, i);
                submit_request(std::move(delete_request), false);
            }

            auto delete_loop_request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
            delete_loop_request->path = loop_path;
            submit_request(std::move(delete_loop_request), false);
        }

        ++loop_index;
    }

    for (auto & request : in_flight)
        collect_request(request);
}


void Runner::runBenchmark()
{
    auto zk = getConnection();

    String bench_info = fmt::format(
        "Run Keeper Benchmark with keeper server: {}, bench_path: {}, bench_cnt: {}, concurrency: {}, pipeline_depth: {}, "
        "duration_sec: {}, key_size: {}, data_size: {}, delete_node: {}, clickhouse_rmt: {}, target_read_qps: {}, target_write_qps: {}, "
        "target_create_qps: {}, metadata_groups: {}, children_per_list: {}, multi_size: {}, skip_setup: {}, setup_only: {}, "
        "drop_late_requests: {}, "
        "operation_timeout_ms: {}, session_timeout_ms: {}, ops: {}",
        toString(nodes),
        bench_path,
        bench_cnt,
        concurrency,
        pipeline_depth,
        duration_sec,
        key_size,
        data_size,
        delete_node,
        clickhouse_workload.enabled,
        clickhouse_workload.target_read_qps,
        clickhouse_workload.target_write_qps,
        clickhouse_workload.target_create_qps,
        clickhouse_workload.metadata_groups,
        clickhouse_workload.children_per_list,
        clickhouse_workload.multi_size,
        clickhouse_workload.skip_setup,
        clickhouse_workload.setup_only,
        clickhouse_workload.drop_late_requests,
        clickhouse_workload.operation_timeout_ms,
        clickhouse_workload.session_timeout_ms,
        toString(ops));
    LOG_INFO(logger, bench_info);

    createIfNotExists(zk, bench_path, toString(bench_cnt));
    const String ready_path = bench_path + "/ready";
    createIfNotExists(zk, ready_path, "");
    if (clickhouse_workload.enabled)
    {
        if (clickhouse_workload.skip_setup)
        {
            validateClickHouseWorkload(zk);
            createOrSet(zk, root_path, bench_info);
        }
        else
        {
            createIfNotExists(zk, root_path, bench_info);
            setupClickHouseWorkload(zk);
            if (clickhouse_workload.setup_only)
            {
                LOG_INFO(logger, "ClickHouse workload setup complete at {}", root_path);
                return;
            }
        }
    }
    else
    {
        createIfNotExists(zk, root_path, bench_info);
        for (size_t i = 0; i < concurrency; ++i)
            createIfNotExists(zk, fmt::format("{}/worker_{}", root_path, i), "bench");
    }

    try
    {
        /// should create zk client for different threads
        if (!shared_keeper)
        {
            std::vector<std::shared_ptr<Coordination::ZooKeeper>> zks(concurrency);
            for (size_t i = 0; i < concurrency; ++i)
            {
                zks[i] = getConnection();
            }

            for (size_t i = 0; i < concurrency; ++i)
            {
                LOG_INFO(logger, "Start threads {}", i);
                pool->scheduleOrThrowOnError([this, worker_zk = zks[i], i]() { work(worker_zk, i); });
            }
        }
        else
        {
            for (size_t i = 0; i < concurrency; ++i)
            {
                LOG_INFO(logger, "Start threads {}", i);
                pool->scheduleOrThrowOnError([this, zk, i]() { work(zk, i); });
            }
        }
    }
    catch (...)
    {
        shutdownWorkers();
        start.store(true);
        pool->wait();
        throw;
    }

    while (ready_workers.load() != concurrency && !shutdown.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    if (shutdown.load())
    {
        start.store(true);
        pool->wait();
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A benchmark worker failed during startup");
    }

    LOG_INFO(logger, "All local workers are ready");
    try
    {
        createEphemeral(zk, fmt::format("{}/{}_{}", ready_path, hostname, getpid()), "ready");
        if (bench_cnt > 1)
        {
            LOG_INFO(logger, "Wait for {} benchmark clients", bench_cnt);
            while (!shutdown.load())
            {
                Strings clients;
                getChildren(zk, ready_path, clients);

                if (clients.size() >= bench_cnt)
                    break;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }
    catch (...)
    {
        shutdownWorkers();
        start.store(true);
        pool->wait();
        throw;
    }

    Stopwatch benchmark_watch;
    benchmark_start_ns.store(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
    start.store(true);
    LOG_INFO(logger, "Run benchmark for {} seconds", duration_sec);
    while (!shutdown.load() && benchmark_watch.elapsedSeconds() < duration_sec)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    shutdownWorkers();

    pool->wait();
    benchmark_watch.stop();
    const auto elapsed_seconds = benchmark_watch.elapsedSeconds();

    LOG_INFO(logger, "All work finished, please delete node {} manually", root_path);

    LOG_INFO(
        logger,
        "Finish Keeper Benchmark with keeper server: {}, concurrency: {}, pipeline_depth: {}, elapsed_sec: {:.3f}, key_size: {}, "
        "data_size: {}, ops: {}",
        toString(nodes),
        concurrency,
        pipeline_depth,
        elapsed_seconds,
        key_size,
        data_size,
        toString(ops));


    auto all_report = toString(stat.report());
    auto read_report = toString(read_stat.report());
    auto write_report = toString(write_stat.report());

    LOG_INFO(
        logger,
        "Result for all (time unit us) qps:{:.1f}\terrors:{}\t{}",
        static_cast<double>(stat.getCount()) / elapsed_seconds,
        error_count.load(),
        all_report);
    if (clickhouse_workload.enabled)
    {
        LOG_INFO(
            logger,
            "Offered requests qps:{:.1f}\tissued:{}\ttarget_qps:{}\tdrop_late_requests:{}\tdropped_slots:{}\tmax_schedule_lag_us:{}",
            static_cast<double>(issued_requests.load()) / elapsed_seconds,
            issued_requests.load(),
            clickhouse_workload.target_read_qps + clickhouse_workload.target_write_qps,
            clickhouse_workload.drop_late_requests,
            dropped_request_slots.load(),
            max_schedule_lag_us.load());
    }
    LOG_INFO(
        logger, "Result for reads (time unit us) qps:{:.1f}\t{}", static_cast<double>(read_stat.getCount()) / elapsed_seconds, read_report);
    LOG_INFO(
        logger,
        "Result for writes (time unit us) qps:{:.1f}\t{}",
        static_cast<double>(write_stat.getCount()) / elapsed_seconds,
        write_report);


    createOrSet(zk, fmt::format("{}/all", root_path), all_report);
    createOrSet(zk, fmt::format("{}/read", root_path), read_report);
    createOrSet(zk, fmt::format("{}/write", root_path), write_report);
}


std::pair<Coordination::OpNum, size_t> parsePair(const String & pair_str)
{
    String delimiter = ":";
    size_t pos = pair_str.find(delimiter);

    if (pos == String::npos)
    {
        std::cerr << "Invalid format" << std::endl;
        throw po::validation_error(po::validation_error::invalid_option_value, pair_str);
    }


    String key = pair_str.substr(0, pos);
    String value_string = pair_str.substr(pos + 1);
    size_t value = 0;

    try
    {
        size_t parsed_characters = 0;
        auto parsed_value = std::stoull(value_string, &parsed_characters);
        if (parsed_characters != value_string.size())
            throw std::out_of_range("operation count");
        value = static_cast<size_t>(parsed_value);
    }
    catch (...)
    {
        throw po::validation_error(po::validation_error::invalid_option_value, pair_str);
    }

    if (value == 0)
        throw po::validation_error(po::validation_error::invalid_option_value, pair_str);

    if (key == "create")
    {
        return std::make_pair(Coordination::OpNum::Create, value);
    }
    else if (key == "list")
    {
        return std::make_pair(Coordination::OpNum::SimpleList, value);
    }
    else if (key == "get")
    {
        return std::make_pair(Coordination::OpNum::Get, value);
    }
    else if (key == "set")
    {
        return std::make_pair(Coordination::OpNum::Set, value);
    }

    throw po::validation_error(po::validation_error::invalid_option_value, pair_str);
}

Ops parseOps(const Strings & pairs)
{
    Ops result;

    for (auto && pair : pairs)
    {
        result.emplace_back(parsePair(pair));
    }

    return result;
}

int mainEntryRaftKeeperBench(int argc, char ** argv)
{
    try
    {
        using boost::program_options::value;

        boost::program_options::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
        auto opt_list = desc.add_options();

        opt_list("help,h", "produce help message");
        opt_list("server,s", po::value<Strings>()->multitoken()->required(), "keeper hosts with port");
        opt_list("bench-path", po::value<String>()->default_value("/bench"), "root path used by the benchmark");
        opt_list("bench-cnt", po::value<size_t>()->default_value(1), "number of benchmark processes to wait for");
        opt_list("concurrency,c", po::value<size_t>()->default_value(10), "number of worker threads, default 10");
        opt_list("pipeline-depth", po::value<size_t>()->default_value(16), "maximum in-flight requests per worker, default 16");
        opt_list(
            "shared-keeper", po::value<bool>()->default_value(true), "control whether multiple threads share a single keeper instance");
        opt_list("key-size", po::value<size_t>()->default_value(100), "request key (node path) size, default 100 bytes");
        opt_list("data-size", po::value<size_t>()->default_value(100), "request data (node data) size, default 100 bytes");
        opt_list("duration-sec", po::value<uint32_t>()->default_value(120), "benchmark duration, default 120 seconds");
        opt_list("delete-create-node", po::value<bool>()->default_value(false), "clean up generated data nodes after each operation cycle");
        opt_list(
            "clickhouse-rmt-workload", po::value<bool>()->default_value(false), "run the production-like ReplicatedMergeTree workload");
        opt_list("target-read-qps", po::value<UInt64>()->default_value(30000), "offered read request rate for the ClickHouse workload");
        opt_list(
            "target-write-qps", po::value<UInt64>()->default_value(15000), "offered Multi write request rate for the ClickHouse workload");
        opt_list("target-create-qps", po::value<UInt64>()->default_value(0), "successful creates per second included in target-write-qps");
        opt_list("metadata-groups", po::value<size_t>()->default_value(1), "number of simulated table/shard metadata trees");
        opt_list("children-per-list", po::value<size_t>()->default_value(1000), "children under blocks and each replica parts path");
        opt_list("multi-size", po::value<size_t>()->default_value(5), "subrequests in each ClickHouse workload Multi request");
        opt_list("workload-root-name", po::value<String>()->default_value(""), "stable root name for reusing an initialized workload tree");
        opt_list("skip-setup", po::value<bool>()->default_value(false), "reuse an existing ClickHouse workload tree");
        opt_list("setup-only", po::value<bool>()->default_value(false), "initialize the ClickHouse workload tree and exit");
        opt_list("drop-late-requests", po::value<bool>()->default_value(true), "drop overdue offered-load slots instead of catching up");
        opt_list(
            "session-timeout-ms",
            po::value<int32_t>()->default_value(Coordination::DEFAULT_SESSION_TIMEOUT_MS),
            "ZooKeeper session timeout in milliseconds");
        opt_list(
            "operation-timeout-ms",
            po::value<int32_t>()->default_value(Coordination::DEFAULT_OPERATION_TIMEOUT_MS),
            "ZooKeeper operation timeout in milliseconds");
        opt_list(
            "operations",
            po::value<Strings>()->multitoken()->default_value(Strings{"create:1000", "list:1000"}, "create:1000 list:1000"),
            "query ops nums, default create:1000 list:1000, only support create,set,get,list ops");


        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), options);

        if (options.count("help"))
        {
            String command = argv[0];
            if (!command.ends_with("keeper-bench"))
                command += " keeper-bench";
            std::cout << "Usage: " << command << " [options]" << std::endl;
            std::cout << desc << std::endl;
            return 0;
        }

        po::notify(options);

        if (options["bench-cnt"].as<size_t>() == 0)
            throw po::validation_error(po::validation_error::invalid_option_value, "bench-cnt");
        if (options["concurrency"].as<size_t>() == 0)
            throw po::validation_error(po::validation_error::invalid_option_value, "concurrency");
        if (options["pipeline-depth"].as<size_t>() == 0)
            throw po::validation_error(po::validation_error::invalid_option_value, "pipeline-depth");
        if (options["duration-sec"].as<uint32_t>() == 0)
            throw po::validation_error(po::validation_error::invalid_option_value, "duration-sec");

        ClickHouseWorkloadOptions clickhouse_workload{
            .enabled = options["clickhouse-rmt-workload"].as<bool>(),
            .target_read_qps = options["target-read-qps"].as<UInt64>(),
            .target_write_qps = options["target-write-qps"].as<UInt64>(),
            .target_create_qps = options["target-create-qps"].as<UInt64>(),
            .metadata_groups = options["metadata-groups"].as<size_t>(),
            .children_per_list = options["children-per-list"].as<size_t>(),
            .multi_size = options["multi-size"].as<size_t>(),
            .root_name = options["workload-root-name"].as<String>(),
            .skip_setup = options["skip-setup"].as<bool>(),
            .setup_only = options["setup-only"].as<bool>(),
            .drop_late_requests = options["drop-late-requests"].as<bool>(),
            .session_timeout_ms = options["session-timeout-ms"].as<int32_t>(),
            .operation_timeout_ms = options["operation-timeout-ms"].as<int32_t>(),
        };
        if (clickhouse_workload.enabled)
        {
            if (clickhouse_workload.target_read_qps + clickhouse_workload.target_write_qps == 0)
                throw po::validation_error(po::validation_error::invalid_option_value, "target-read-qps/target-write-qps");
            if (clickhouse_workload.target_create_qps > clickhouse_workload.target_write_qps)
                throw po::validation_error(po::validation_error::invalid_option_value, "target-create-qps");
            if (clickhouse_workload.metadata_groups == 0)
                throw po::validation_error(po::validation_error::invalid_option_value, "metadata-groups");
            if (clickhouse_workload.children_per_list == 0)
                throw po::validation_error(po::validation_error::invalid_option_value, "children-per-list");
            if (clickhouse_workload.multi_size < 3)
                throw po::validation_error(po::validation_error::invalid_option_value, "multi-size");
            if (clickhouse_workload.skip_setup && clickhouse_workload.root_name.empty())
                throw po::validation_error(po::validation_error::invalid_option_value, "workload-root-name");
            if (clickhouse_workload.setup_only && (clickhouse_workload.skip_setup || clickhouse_workload.root_name.empty()))
                throw po::validation_error(po::validation_error::invalid_option_value, "setup-only");
            if (clickhouse_workload.operation_timeout_ms <= 0
                || clickhouse_workload.operation_timeout_ms > clickhouse_workload.session_timeout_ms)
            {
                throw po::validation_error(po::validation_error::invalid_option_value, "operation-timeout-ms/session-timeout-ms");
            }
        }

        auto ops = parseOps(options["operations"].as<Strings>());

        Runner runner(
            options["server"].as<Strings>(),
            options["bench-path"].as<String>(),
            options["bench-cnt"].as<size_t>(),
            options["concurrency"].as<size_t>(),
            options["pipeline-depth"].as<size_t>(),
            options["shared-keeper"].as<bool>(),
            options["key-size"].as<size_t>(),
            options["data-size"].as<size_t>(),
            options["duration-sec"].as<uint32_t>(),
            options["delete-create-node"].as<bool>(),
            ops,
            clickhouse_workload);

        try
        {
            runner.runBenchmark();
        }
        catch (...)
        {
            std::cerr << getCurrentExceptionMessage(true) << '\n';
            return getCurrentExceptionCode();
        }
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return getCurrentExceptionCode();
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << std::endl;
        return getCurrentExceptionCode();
    }


    return 0;
}
