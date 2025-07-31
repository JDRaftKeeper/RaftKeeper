#include "Runner.h"

#include <iostream>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/DNS.h>

#include <Common/Stopwatch.h>
#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>

#include "ZooKeeper/KeeperException.h"


using namespace RK;


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
    std::uniform_int_distribution<> dis(0, c);

    UInt64 i = dis(gen);
    if (i < DEFAULT_SIZE)
        values[i].store(value);
}

double Stat::getValue(const std::vector<UInt64>& numbers, double quantile)
{
    if (quantile < 0.0 || quantile > 1.0)
    {
        LOG_ERROR(&Poco::Logger::get("AdvanceSummary"), "Quantile {} is not in [0..1]", quantile);
        return 0.0;
    }

    if (numbers.empty())
        return 0.0;

    auto index = quantile * (numbers.size() + 1);
    size_t pos = static_cast<size_t>(index);

    if (pos < 1)
        return numbers[0];


    if (pos >= numbers.size())
        return numbers.back();

    auto lower = numbers[pos - 1];
    auto upper = numbers[pos];
    return lower + (index - std::floor(index)) * (upper - lower);
}

Strings Stat::report() const
{
    auto numbers = reservoir_sampler.getSnapshot();
    std::sort(numbers.begin(), numbers.end());

    Strings results;
    UInt64 cnt = count.load();
    UInt64 s = sum.load();
    float avgRt = (cnt == 0.0 ? cnt : float(s)/cnt);

    results.emplace_back(fmt::format("avgRt:{:.1f}", avgRt));
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
    std::default_random_engine engine(static_cast<unsigned long>(std::time(nullptr)));
    std::uniform_int_distribution<> dist(0, characters.size() - 1);

    std::string randomString;
    for (size_t i = 0; i < length; ++i) {
        randomString += characters[dist(engine)];
    }

    return randomString;
}

template <typename T>
void shuffleArray(std::vector<T>& arr)
{
    for (int i = arr.size() - 1; i > 0; --i) {
        static thread_local std::mt19937 gen{std::random_device{}()};
        std::uniform_int_distribution<> dis(0, i);
        std::swap(arr[i], arr[dis(gen)]);
    }
}

Runner::Runner(
    const Strings & hosts_strings_,
    const String & bench_path_,
    size_t bench_cnt_,
    size_t concurrency_,
    bool shared_keeper_,
    size_t key_size_,
    size_t data_size_,
    uint32_t duration_sec_,
    bool delete_node_,
    Ops & ops_):hosts_strings(hosts_strings_), bench_path(bench_path_) ,bench_cnt(bench_cnt_), concurrency(concurrency_), shared_keeper(shared_keeper_),
    key_size(key_size_), data_size(data_size_), duration_sec(duration_sec_), delete_node(delete_node_), ops(ops_)
{
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    logger = &Poco::Logger::get("RaftKeeperBench");

    logger->setChannel(console_channel);
    logger->root().setChannel(console_channel);

    for (auto && host : hosts_strings)
    {
        nodes.emplace_back(Poco::Net::SocketAddress{host}, false);
    }



    pool.emplace(concurrency);
    pid = getpid();

    hostname = Poco::Net::DNS::hostName();

    root_path = fmt::format("{}/bench_{}_{}", bench_path, hostname, pid);

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
        Poco::Timespan(0, Coordination::DEFAULT_SESSION_TIMEOUT_MS * 1000),
        Poco::Timespan(0, 1000* 1000),
        Poco::Timespan(0, Coordination::DEFAULT_OPERATION_TIMEOUT_MS * 1000));
}


Coordination::Error createImpl(ZooKeeperPtr zk, const std::string & path, const std::string & data, int32_t mode, std::string & path_created)
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

    zk->create(path, data, mode & 1, mode & 2, {}, callback);  /// TODO better mode
    event.wait();
    return code;
}

void createIfNotExists(ZooKeeperPtr zk, const std::string & path, const std::string & data)
{
    std::string path_created;

    Coordination::Error code = createImpl(zk, path, data, zkutil::CreateMode::Persistent, path_created);

    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
        return;
    else
        throw zkutil::KeeperException(code, path);
}



std::future<Coordination::ListResponse> asyncTryGetChildrenNoThrow(
    ZooKeeperPtr zk, const std::string & path, Coordination::WatchCallback watch_callback)
{
    auto promise = std::make_shared<std::promise<Coordination::ListResponse>>();
    auto future = promise->get_future();

    auto callback = [promise](const Coordination::ListResponse & response) mutable
    {
        promise->set_value(response);
    };

    zk->list(path, std::move(callback), watch_callback);
    return future;
}


void get_child(ZooKeeperPtr zk, const std::string & path, Strings & res)
{

    auto future_result = asyncTryGetChildrenNoThrow(zk, path, nullptr);

    auto response = future_result.get();
    Coordination::Error code = response.error;
    if (code == Coordination::Error::ZOK)
    {
        res = response.names;
    }

    if (code == Coordination::Error::ZOK || code == Coordination::Error::ZNODEEXISTS)
        return;
    else
        throw zkutil::KeeperException(code, path);
}


void Runner::work(ZooKeeperPtr zk, size_t thread_idx)
{
    size_t loop_index = 0;
    String work_path = fmt::format("{}/worker_{}", root_path, thread_idx);
    createIfNotExists(zk, work_path, "bench");

    auto do_request = [&](Coordination::ZooKeeperRequestPtr request) {
        auto promise = std::make_shared<std::promise<bool>>();
        auto future = promise->get_future();
        Coordination::ResponseCallback callback = [promise](const Coordination::Response & response)
        {
            bool set_exception = true;

            if (response.error == Coordination::Error::ZOK)
            {
                set_exception = false;
            }
            promise->set_value(set_exception);
        };

        Stopwatch watch;
        zk->excuteRequest(request, callback);

        try
        {
            if (future.get())
                error_count ++;
            auto microseconds = watch.elapsedMicroseconds();

            if (shutdown)
                return;

            stat.add(microseconds);

            if (request->isReadRequest())
                read_stat.add(microseconds);
            else
                write_stat.add(microseconds);
        }
        catch (...)
        {
            std::cerr << getCurrentExceptionMessage(true, true /*check embedded stack trace*/) << std::endl;
            shutdown = true;
            throw;
        }
    };


    while (!shutdown)
    {
        size_t create_index = 0;
        String loop_path = fmt::format("{}/loop_{}", work_path, loop_index);
        createIfNotExists(zk, loop_path, "bench");

        createIfNotExists(zk, fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, create_index), "");

        std::random_device rd;
        std::mt19937 gen(rd());


        for (auto && [op, cnt] : ops)
        {
            if (shutdown)
            {
                return;
            }
            if (op == Coordination::OpNum::Create)
            {
                for (size_t i = 0; i < cnt; i++)
                {
                    create_index ++;
                    auto create_request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
                    create_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, create_index);

                    do_request(create_request);
                }
            }
            else if (op == Coordination::OpNum::SimpleList)
            {
                for (size_t i = 0; i < cnt; i++)
                {
                    auto list_request = std::make_shared<Coordination::ZooKeeperListRequest>();
                    list_request->path = loop_path;
                    do_request(list_request);
                }
            }
            else if (op == Coordination::OpNum::Get)
            {
                for (size_t i = 0; i < cnt; i++)
                {
                    auto get_request = std::make_shared<Coordination::ZooKeeperGetRequest>();

                    std::uniform_int_distribution<> dis(0, create_index);
                    int random_number = dis(gen);
                    get_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, random_number);
                    do_request(get_request);
                }
            }
            else if (op == Coordination::OpNum::Set)
            {
                for (size_t i = 0; i < cnt; i++)
                {
                    auto set_request = std::make_shared<Coordination::ZooKeeperSetRequest>();
                    std::uniform_int_distribution<> dis(0, create_index);
                    int random_number = dis(gen);
                    set_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, random_number);
                    set_request->data = data_rand_fill_str;
                    do_request(set_request);
                }
            }
            else
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't support op {}", toString(op));
            }
        }

        if (delete_node)
        {
            for (size_t i = 0 ; i <= create_index; ++i)
            {
                auto delete_request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
                delete_request->path = fmt::format("{}/{}_{}", loop_path, path_rand_fill_str, i);
                do_request(delete_request);
            }
        }

        loop_index ++;
    }
}


void Runner::runBenchmark()
{
    auto zk = getConnection();

    String bench_info = fmt::format("Run Keeper Benchmark with keeper server: {}, bench_path: {}, bench_cnt: {}, concurrency: {}, duration_sec: {}, key_size: {}, data_size: {}, delete_node: {}, ops: {}",
                                    toString(nodes), bench_path, bench_cnt, concurrency, duration_sec, key_size, data_size, delete_node, toString(ops));
    LOG_INFO(logger, bench_info);

    createIfNotExists(zk, bench_path, toString(bench_cnt));
    createIfNotExists(zk, root_path, bench_info);

    // should wait other bench client ready
    if (bench_cnt > 1)
    {
        LOG_INFO(logger, "Wait other work ready");
        while (true)
        {
            Strings clients;
            get_child(zk, bench_path, clients);

            if (clients.size() == bench_cnt)
                break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
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
                pool->scheduleOrThrowOnError([this, zks, i]() { work(zks[i], i); });
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
        shutdown = true;
        pool->wait();
        throw;
    }

    LOG_INFO(logger, "Main thread sleep {} sec", duration_sec);
    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    shutdown = true;

    pool->wait();

    LOG_INFO(logger, "All work finished, please delete node {} manually", root_path);

    LOG_INFO(logger, "Finish Keeper Benchmark with keeper server: {}, concurrency: {}, duration_sec: {}, key_size: {}, data_size: {}, ops: {}",
             toString(nodes), concurrency, duration_sec, key_size, data_size, toString(ops));


    auto all_report = toString(stat.report());
    auto read_report = toString(read_stat.report());
    auto write_report = toString(write_stat.report());

    LOG_INFO(logger, "Result for all (time unit us) qps:{}\t{}", stat.get_cnt() / duration_sec, all_report);
    LOG_INFO(logger, "Result for reads (time unit us) {}", read_report);
    LOG_INFO(logger, "Result for write (time unit us) {}", write_report);


    createIfNotExists(zk, fmt::format("{}/all", root_path), all_report);
    createIfNotExists(zk, fmt::format("{}/read", root_path), read_report);
    createIfNotExists(zk, fmt::format("{}/write", root_path), write_report);

}



std::pair<Coordination::OpNum, int> parse_pair(const String & pair_str)
{
    String delimiter = ":";
    size_t pos = pair_str.find(delimiter);

    if (pos == String::npos)
    {
        std::cerr << "Invalid format" << std::endl;
        throw po::validation_error(po::validation_error::invalid_option_value, pair_str);
    }


    String key = pair_str.substr(0, pos);
    String valueStr = pair_str.substr(pos + 1);
    size_t value;

    try
    {
        value = std::stoul(valueStr);
    }
    catch (...)
    {
        std::cerr << "Invalid number: " << valueStr << std::endl;
        throw po::validation_error(po::validation_error::invalid_option_value, pair_str);
    }

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
        result.emplace_back(parse_pair(pair));
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
        opt_list("bench-path", po::value<String>()->default_value("/bench"), "keeper bench cnt");
        opt_list("bench-cnt", po::value<size_t>()->default_value(1), "keeper bench cnt");
        opt_list("concurrency,c", po::value<size_t>()->default_value(10), "number of parallel queries, default 100");
        opt_list("shared-keeper", po::value<bool>()->default_value(true), "control whether multiple threads share a single keeper instance");
        opt_list("key-size", po::value<size_t>()->default_value(100), "request key (node path) size, default 100 bytes");
        opt_list("data-size", po::value<size_t>()->default_value(100), "request data (node data) size, default 100 bytes");
        opt_list("duration-sec", po::value<uint32_t>()->default_value(120), "benchmark launch time, default 100 seconds");
        opt_list("delete-create-node", po::value<bool>()->default_value(false), "whether to clean up the nodes after each operation execution");
        opt_list("operations", po::value<Strings>()->multitoken()->default_value(Strings{"create:1000", "list:1000"}, "create:1000 list:1000"),
                 "query ops nums, default create:1000 list:1000, only support create,set,get,list ops");


        po::variables_map options;
        po::store(po::command_line_parser(argc, argv).options(desc).run(), options);

        if (options.count("help"))
        {
            std::cout << "Usage: " << argv[0] << " bench" << std::endl;
            std::cout << desc << std::endl;
            return 0;
        }

        auto ops = parseOps(options["operations"].as<Strings>());

        Runner runner(options["server"].as<Strings>(),
                      options["bench-path"].as<String>(),
                      options["bench-cnt"].as<size_t>(),
                      options["concurrency"].as<size_t>(),
                      options["shared-keeper"].as<bool>(),
                      options["key-size"].as<size_t>(),
                      options["data-size"].as<size_t>(),
                      options["duration-sec"].as<uint32_t>(),
                      options["delete-create-node"].as<bool>(),
                      ops);

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
