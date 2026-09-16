#include <iostream>
#include <optional>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

#include <Common/TerminalSize.h>
#include <boost/program_options.hpp>

#include <Service/NuRaftLogSnapshot.h>
#include <Service/SnapshotConverter.h>
#include <Service/ZooKeeperDataReader.h>


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
    opt_list("raftkeeper-snapshots-dir", po::value<std::string>(), "Stable directory containing RaftKeeper snapshots to downgrade");
    opt_list("target-snapshot-version", po::value<unsigned>(), "Required downgrade target: 2 (raw) or 3 (zstd)");
    opt_list(
        "snapshot-prefix", po::value<std::string>(), "Select one snapshot filename prefix, without its object number (default: latest)");

    po::variables_map options;
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    Poco::Logger * logger = &Poco::Logger::get("RaftKeeperConverter");

    logger->setChannel(console_channel);
    logger->root().setChannel(console_channel);

    try
    {
        po::store(po::command_line_parser(argc, argv).options(desc).run(), options);
        po::notify(options);
        if (options.count("help"))
        {
            std::cout << "Usage: raftkeeper converter --raftkeeper-snapshots-dir SOURCE --output-dir NEW_DIR "
                         "--target-snapshot-version 2 [--snapshot-prefix PREFIX]\n"
                         "   or: raftkeeper converter --zookeeper-logs-dir LOGS --zookeeper-snapshots-dir SNAPSHOTS --output-dir OUTPUT\n"
                      << desc << std::endl;
            return 0;
        }
        if (!options.count("output-dir"))
            throw po::error("--output-dir is required");
        if (options.count("raftkeeper-snapshots-dir"))
        {
            if (options.count("zookeeper-logs-dir") || options.count("zookeeper-snapshots-dir"))
                throw po::error("RaftKeeper and ZooKeeper input modes are mutually exclusive");
            if (!options.count("target-snapshot-version"))
                throw po::error("--target-snapshot-version is required for RaftKeeper snapshots");
            auto target = options["target-snapshot-version"].as<unsigned>();
            if (target != 2 && target != 3)
                throw po::error("--target-snapshot-version must be 2 or 3");
            auto result = downgradeSnapshot(
                options["raftkeeper-snapshots-dir"].as<std::string>(),
                options["output-dir"].as<std::string>(),
                static_cast<SnapshotVersion>(target),
                options.count("snapshot-prefix") ? options["snapshot-prefix"].as<std::string>() : "");
            std::cout << "Converted " << result.prefix << " (term=" << result.term << ", log_index=" << result.log_index << ") "
                      << toString(result.source_version) << " -> " << toString(result.target_version)
                      << ", objects=" << result.source_objects << " -> " << result.target_objects << ", output=" << result.output_dir
                      << "\n"
                      << "Snapshot conversion does not convert Raft logs or guarantee binary downgrade compatibility.\n";
            return 0;
        }
        if (options.count("target-snapshot-version") || options.count("snapshot-prefix"))
            throw po::error("Downgrade options require --raftkeeper-snapshots-dir");
        if (!options.count("zookeeper-logs-dir") || !options.count("zookeeper-snapshots-dir"))
            throw po::error("Specify RaftKeeper input or both ZooKeeper input directories");

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
            options["output-dir"].as<std::string>(), 3600 * 1, MAX_OBJECT_NODE_SIZE);
        snap_mgr->createSnapshot(*new_snapshot, store, store.getZxid(), store.getSessionIDCounter());
        std::cout << "Snapshot serialized to path:" << options["output-dir"].as<std::string>() << std::endl;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return 1;
    }

    return 0;
}
