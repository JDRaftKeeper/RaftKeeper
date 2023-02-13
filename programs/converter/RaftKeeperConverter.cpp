/**
 * Copyright 2016-2023 ClickHouse, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <iostream>
#include <optional>
#include <boost/program_options.hpp>

#include <Service/NuRaftLogSnapshot.h>
#include <Service/ZooKeeperDataReader.h>
#include <Common/TerminalSize.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>


int mainEntryRaftKeeperConverter(int argc, char ** argv)
{
    using namespace RK;
    namespace po = boost::program_options;

    po::options_description desc = createOptionsDescription("Allowed options", getTerminalWidth());
    desc.add_options()
        ("help,h", "produce help message")
        ("zookeeper-logs-dir", po::value<std::string>(), "Path to directory with ZooKeeper logs")
        ("zookeeper-snapshots-dir", po::value<std::string>(), "Path to directory with ZooKeeper snapshots")
        ("output-dir", po::value<std::string>(), "Directory to place output raftkeeper snapshot")
    ;
    po::variables_map options;
    po::store(po::command_line_parser(argc, argv).options(desc).run(), options);
    Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);

    Poco::Logger * logger = &Poco::Logger::get("RaftKeeperConverter");
    logger->setChannel(console_channel);

    if (options.count("help"))
    {
        std::cout << "Usage: " << argv[0] << " --zookeeper-logs-dir /var/lib/zookeeper/data/version-2 --zookeeper-snapshots-dir /var/lib/zookeeper/data/version-2 --output-dir /var/lib/raftkeeper/raft_snapshots" << std::endl;
        std::cout << desc << std::endl;
        return 0;
    }

    try
    {
        RK::KeeperStore store(500);

        RK::deserializeKeeperStoreFromSnapshotsDir(store, options["zookeeper-snapshots-dir"].as<std::string>(), logger);
        RK::deserializeLogsAndApplyToStore(store, options["zookeeper-logs-dir"].as<std::string>(), logger);
        std::cout << "storage.container.size():" << store.container.size() << std::endl;
        nuraft::ptr<snapshot> new_snapshot
            ( nuraft::cs_new<snapshot>(store.zxid, 1, std::make_shared<nuraft::cluster_config>()) ); // TODO 1 ?
        nuraft::ptr<KeeperSnapshotManager> snap_mgr = nuraft::cs_new<KeeperSnapshotManager>(options["output-dir"].as<std::string>(), 3600 * 1, KeeperSnapshotStore::MAX_OBJECT_NODE_SIZE);
        snap_mgr->createSnapshot(*new_snapshot, store);
        std::cout << "Snapshot serialized to path:" << options["output-dir"].as<std::string>() << std::endl;
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << '\n';
        return getCurrentExceptionCode();
    }

    return 0;
}
