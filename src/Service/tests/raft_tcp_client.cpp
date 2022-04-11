#include <iostream>
#include <memory>
#include <string>

#include <thread>

#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Common/ZooKeeper/Types.h>
#include <Poco/Net/NetException.h>
#include <common/logger_useful.h>
#include <common/find_symbols.h>
#include <Common/randomSeed.h>
#include <Common/StringUtils/StringUtils.h>

using namespace Coordination;


int main(int, char **)
{
    std::string identity_ = "";

    std::vector<std::string> hosts_strings;
    hosts_strings.emplace_back("127.0.0.1:5102");
    Coordination::ZooKeeper::Nodes nodes;
    nodes.reserve(hosts_strings.size());
    auto * log = &Poco::Logger::get("raft_tcp_client");
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

    zkutil::ZooKeeper::Ptr zookeeper = std::make_shared<zkutil::ZooKeeper>(hosts_strings[0], identity_.empty() ? "" : "digest", 60000, 30000);

    for (size_t i = 0; i < 100; ++i)
    {
        zookeeper->asyncCreate("/testdfg" + std::to_string(i), "", zkutil::CreateMode::Persistent);
    }

    sleep(100);
//    std::unique_ptr<Coordination::IKeeper> impl = std::make_unique<Coordination::ZooKeeper>(
//        nodes,
//        "/clickhouse",
//        identity_.empty() ? "" : "digest",
//        identity_,
//        Poco::Timespan(0, 30 * 1000),
//        Poco::Timespan(0, 10 * 1000),
//        Poco::Timespan(0, 10 * 1000));

//    sleep(1);
//    ACLs acls;
//    std::cout << "1: " << std::to_string(static_cast<int32_t>(zookeeper->tryCreate("/clickhouse", "1", zkutil::CreateMode::Persistent))) << std::endl;
//    std::cout << "2: " << zookeeper->get("/clickhouse") << std::endl;
//
//    Coordination::Stat stat;
//    zkutil::EventPtr event = std::make_shared<Poco::Event>();
//
//    zookeeper->tryRemove("/clickhouse/node1", -1);
//
//    std::string res;
//    std::cout << "3: " << zookeeper->exists("/clickhouse/node1", &stat, event) << std::endl;
//    std::thread thread([&]()
//    {
//        sleep(3);
//        auto r = zookeeper->create("/clickhouse/node1", "2", zkutil::CreateMode::Persistent);
//        std::cout << "4: " << r << std::endl;
//    });
//
//    std::cout << "wait znode created..." << std::endl;
//    event->wait(); /// wait znode created
//    Coordination::Stat stat1;
//    auto data = zookeeper->get("/clickhouse/node1", &stat1);
//    std::cout << "5: " << data << std::endl;
//    std::cout << "czxid = " << stat1.czxid << std::endl;
//    std::cout << "mzxid = " << stat1.mzxid << std::endl;
//    std::cout << "ctime = " << stat1.ctime << std::endl;
//    std::cout << "mtime = " << stat1.mtime << std::endl;
//    std::cout << "version = " << stat1.version << std::endl;
//    std::cout << "cversion = " << stat1.cversion << std::endl;
//    std::cout << "aversion = " << stat1.aversion << std::endl;
//    std::cout << "ephemeralOwner = " << stat1.ephemeralOwner << std::endl;
//    std::cout << "dataLength = " << stat1.dataLength << std::endl;
//    std::cout << "numChildren = " << stat1.numChildren << std::endl;
//    std::cout << "pzxid = " << stat1.pzxid << std::endl;
//
//    zookeeper->set("/clickhouse/node1", "hello", 10);
//    Coordination::Stat stat2;
//    auto data1 = zookeeper->get("/clickhouse/node1", &stat2);
//    std::cout << "6: " << data1 << std::endl;
//    std::cout << "czxid = " << stat2.czxid << std::endl;
//    std::cout << "mzxid = " << stat2.mzxid << std::endl;
//    std::cout << "ctime = " << stat2.ctime << std::endl;
//    std::cout << "mtime = " << stat2.mtime << std::endl;
//    std::cout << "version = " << stat2.version << std::endl;
//    std::cout << "cversion = " << stat2.cversion << std::endl;
//    std::cout << "aversion = " << stat2.aversion << std::endl;
//    std::cout << "ephemeralOwner = " << stat2.ephemeralOwner << std::endl;
//    std::cout << "dataLength = " << stat2.dataLength << std::endl;
//    std::cout << "numChildren = " << stat2.numChildren << std::endl;
//    std::cout << "pzxid = " << stat2.pzxid << std::endl;
//
//    thread.join();
    return 0;
}
