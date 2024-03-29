#include <iostream>
#include "ZooKeeper/ZooKeeperImpl.h"


int main()
try
{
    Coordination::ZooKeeper zookeeper({Coordination::ZooKeeper::Node{Poco::Net::SocketAddress{"localhost:2181"}, false}}, "", "", "", {30, 0}, {0, 50000}, {0, 50000});

    zookeeper.create("/test", "hello", false, false, {}, [](const Coordination::CreateResponse & response)
    {
        if (response.error != Coordination::Error::ZOK)
            std::cerr << "Error: " << Coordination::errorMessage(response.error) << "\n";
        else
            std::cerr << "Path created: " << response.path_created << "\n";
    });

    sleep(100);

    return 0;
}
catch (...)
{
    RK::tryLogCurrentException(__PRETTY_FUNCTION__);
    return 1;
}
