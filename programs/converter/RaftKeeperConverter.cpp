#include <iostream>
#include <optional>
#include <boost/program_options.hpp>

#include <Service/NuRaftLogSnapshot.h>
#include <Service/ZooKeeperDataReader.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Common/TerminalSize.h>


int mainEntryRaftKeeperConverter(int argc, char ** argv)
{
    using namespace RK;
    namespace po = boost::program_options;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    auto opt_list = desc.add_options();

    opt_list("help,h", "produce help message");
    opt_list("zookeeper-logs-dir", po::value<std::string>(), "Path to directory with ZooKeeper logs");
    opt_list("zookeeper-snapshots-dir", po::value<std::string>(), "Path to directory with ZooKeeper snapshots");
    opt_list("output-dir", po::value<std::string>(), "Directory to place output raftkeeper snapshot");

    po::variables_map options;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), options);
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    Poco::Logger * logger = &Poco::Logger::get("RaftKeeperConverter");

    logger->setChannel(console_channel);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0]
                  << " --zookeeper-logs-dir /var/lib/zookeeper/data/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/data/version-2 "
                     "--output-dir /var/lib/raftkeeper/raft_snapshots"
                  << std::endl;
        std::cout << desc << std::endl;
        return 0;
    }

    try
    {
        RK::KeeperStore store(500);
        RK::deserializeKeeperStoreFromSnapshotsDir(store, options["zookeeper-snapshots-dir"].as<std::string>(), logger);
        LOG_INFO(
            logger,
            "Deserialize snapshot to store done: nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
            store.getNodesCount(),
            store.getTotalEphemeralNodesCount(),
            store.getSessionCount(),
            store.getSessionIDCounter(),
            store.getZxid());
        RK::deserializeLogsAndApplyToStore(store, options["zookeeper-logs-dir"].as<std::string>(), logger);
        LOG_INFO(
            logger,
            "Deserialize logs to store done: nodes {}, ephemeral nodes {}, sessions {}, session_id_counter {}, zxid {}",
            store.getNodesCount(),
            store.getTotalEphemeralNodesCount(),
            store.getSessionCount(),
            store.getSessionIDCounter(),
            store.getZxid());
        nuraft::ptr<snapshot> new_snapshot(nuraft::cs_new<snapshot>(store.getZxid(), 1, std::make_shared<nuraft::cluster_config>()));
        nuraft::ptr<KeeperSnapshotManager> snap_mgr = nuraft::cs_new<KeeperSnapshotManager>(
            options["output-dir"].as<std::string>(), 3600 * 1, KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE);
        snap_mgr->createSnapshot(*new_snapshot, store, store.getZxid(), store.getSessionIDCounter());
        std::cout << "Snapshot serialized to path:" << options["output-dir"].as<std::string>() << std::endl;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
