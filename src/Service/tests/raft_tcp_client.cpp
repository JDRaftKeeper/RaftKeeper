#include <memory>
#include <string>
#include <ZooKeeper/IKeeper.h>
#include <ZooKeeper/Types.h>
#include <ZooKeeper/ZooKeeper.h>
#include <ZooKeeper/ZooKeeperImpl.h>
#include <Poco/Net/NetException.h>
#include <Common/StringUtils.h>

using namespace Coordination;


int main(int, char **)
{
    std::string identity_;

    std::vector<std::string> hosts_strings;
    hosts_strings.emplace_back("127.0.0.1:8101");
    Coordination::ZooKeeper::Nodes nodes;
    nodes.reserve(hosts_strings.size());
    auto * log = &Poco::Logger::get("raft_tcp_client");
    for (auto & host_string : hosts_strings)
    {
        try
        {
            bool secure = (startsWith(host_string, "secure://");

            if (secure)
                host_string.erase(0, strlen("secure://"));

            nodes.emplace_back(Coordination::ZooKeeper::Node{Poco::Net::SocketAddress{host_string}, secure});
        }
        catch (const Poco::Net::DNSException & e)
        {
            LOG_ERROR(log, "Cannot use SvsKeeper host {}, reason: {}", host_string, e.displayText());
        }
    }

    zkutil::ZooKeeper::Ptr zookeeper
        = std::make_shared<zkutil::ZooKeeper>(hosts_strings[0], identity_.empty() ? "" : "digest", 60000, 30000);

    for (size_t i = 0; i < 100; ++i)
    {
        zookeeper->asyncCreate("/testdfg" + std::to_string(i), "", zkutil::CreateMode::Persistent);
    }

}
