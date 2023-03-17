#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <time.h>
#include <boost/program_options.hpp>
#include <loggers/Loggers.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/Application.h>
#include "Common/StringUtils.h"
#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/KeeperException.h>
#include <ZooKeeper/Types.h>
#include <ZooKeeper/ZooKeeper.h>
#include <ZooKeeper/ZooKeeperImpl.h>
#include <Common/randomSeed.h>
#include <common/argsToConfig.h>
#include <common/find_symbols.h>
#include <common/logger_useful.h>

using namespace Coordination;
using namespace RK;

namespace RK
{
class TestServer : public Poco::Util::Application, public Loggers
{
public:
    TestServer() { }
    ~TestServer() override { }
    void init(int argc, char ** argv)
    {
        char * log_level = argv[7];

        namespace po = boost::program_options;
        /// Don't parse options with Poco library, we prefer neat boost::program_options
        stopOptionsProcessing();
        /// Save received data into the internal config.
        config().setBool("stacktrace", true);
        config().setBool("logger.console", true);
        config().setString("logger.log", "./benchmark_test.logs");
        config().setString("logger.level", log_level);
        config().setBool("ignore-error", false);

        std::vector<std::string> arguments;
        for (int arg_num = 1; arg_num < argc; ++arg_num)
            arguments.emplace_back(argv[arg_num]);
        argsToConfig(arguments, config(), 100);

        if (config().has("logger.console") || config().has("logger.level") || config().has("logger.log"))
        {
            // force enable logging
            config().setString("logger", "logger");
            // sensitive data rules are not used here
            buildLoggers(config(), logger(), "clickhouse-local");
        }
    }
};

}

void getCurrentTime(std::string & date_str)
{
    const char TIME_FMT[] = "%Y%m%d%H%M%S";
    time_t curr_time;
    time(&curr_time);
    char tmp_buf[24];
    std::strftime(tmp_buf, sizeof(tmp_buf), TIME_FMT, localtime(&curr_time));
    date_str = tmp_buf;
}

//IP PORT THREAD_SIZE SEND_COUNT KEY_SIZE VALUE_SIZE
int main(int argc, char ** argv)
{
    if (argc < 8)
    {
        std::cout << "Please run:xxx ip port thread_size send_count key_size value_size debug/trace" << std::endl;
        return 0;
    }
    char buf[64];
    char * ip = argv[1];
    char * port_str = argv[2];
    snprintf(buf, 64, "%s:%s", ip, port_str);
    std::string ip_port(buf);
    std::cout << "Connect server[" << ip_port << "]." << std::endl;

    int thread_size = 5;
    int send_count = 1000000;
    int key_size = 256;
    int value_size = 1024;

    thread_size = atoi(argv[3]);
    send_count = atoi(argv[4]);
    key_size = atoi(argv[5]);
    value_size = atoi(argv[6]);

    std::cout << "thread_size " << thread_size << ", send_count " << send_count << ", key_size " << key_size << ", value_size "
              << value_size << std::endl;

    RK::TestServer app;
    app.init(argc, argv);
    app.run();

    auto * log = &Poco::Logger::get("raft_service_client");
    LOG_INFO(log, "Run test server!");

    std::string identity_ = "";
    std::vector<std::string> hosts_strings;
    hosts_strings.emplace_back(ip_port);
    Coordination::ZooKeeper::Nodes nodes;
    nodes.reserve(hosts_strings.size());

    for (auto & host_string : hosts_strings)
    {
        try
        {
            bool secure = bool(startsWith(host_string, "secure://"));
            if (secure)
                host_string.erase(0, strlen("secure://"));

            nodes.emplace_back(Coordination::ZooKeeper::Node{Poco::Net::SocketAddress{host_string}, secure});
        }
        catch (const Poco::Net::DNSException & e)
        {
            LOG_ERROR(log, "Cannot use SvsKeeper host {}, reason: {}", host_string, e.displayText());
        }
    }

    std::string key = "/";
    //8+6=14 byte
    std::string curr_time;
    getCurrentTime(curr_time);
    key.append(curr_time);
    for (int i = 0; i < key_size - 25; i++)
    {
        key.append("k");
    }

    LOG_INFO(log, "Create prefix key {}", key);

    //1024 byte
    std::string data;
    for (int i = 0; i < value_size; i++)
    {
        data.append("v");
    }

    sleep(1);

    FreeThreadPool thread_pool(thread_size);
    Stopwatch watch;
    watch.start();
    for (int i = 0; i < thread_size; i++)
    {
        thread_pool.scheduleOrThrowOnError([ip_port, i, thread_size, send_count, &key, &data] {
            auto * thread_log = &Poco::Logger::get("client_thread");
            char key_buf[257];
            zkutil::ZooKeeper::Ptr zookeeper = std::make_shared<zkutil::ZooKeeper>(ip_port, "", 60000, 30000);
            int log_count = send_count / thread_size;
            int begin = i * log_count;
            int end = (i + 1) * log_count;
            LOG_INFO(thread_log, "Begin run thread {}/{}, send_count {}, range[{} - {}) ", i, thread_size, send_count, begin, end);
            while (begin < end)
            {
                snprintf(key_buf, 257, "%s%010d", key.data(), begin);
                Coordination::Error ret = Coordination::Error::ZOK;
                try
                {
                    ret = zookeeper->tryCreate(key_buf, data.data(), zkutil::CreateMode::Persistent);                
                    //LOG_DEBUG(thread_log, "Response code {}, errmsg {}, key {}", ret, errorMessage(ret), key_buf);
                }
                catch (RK::Exception & ex)
                {
                    LOG_INFO(thread_log, "Response code {}, errmsg {}, key {}, exception {}", ret, errorMessage(ret), key_buf, ex.message());
                    sleep(1);
                    //make new
                    zookeeper = std::make_shared<zkutil::ZooKeeper>(ip_port, "", 60000, 30000);
                }
                begin++;
            }
        });
    }
    LOG_INFO(log, "Max thread count {}, running {}", thread_pool.getMaxThreads(), thread_pool.active());
    thread_pool.wait();
    watch.stop();
    int mill_second = watch.elapsedMilliseconds();
    int log_size = ((key_size + value_size) + sizeof(UInt32) * 4 + sizeof(UInt32) * 6);
    double total_size = 1.0 * log_size * send_count / 1000 / 1000;
    double byte_rate = 1.0 * total_size / mill_second * 1000;
    double count_rate = 1.0 * send_count / mill_second * 1000;
    LOG_INFO(
        log,
        "Append performance, size {}, count {}, total_size {}, micro second {}, byte rate {}, count rate {}",
        log_size,
        send_count,
        total_size,
        mill_second,
        byte_rate,
        count_rate);
    return 0;
}
