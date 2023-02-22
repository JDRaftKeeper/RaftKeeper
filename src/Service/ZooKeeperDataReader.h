#pragma once
#include <string>
#include <Service/KeeperStore.h>
#include <common/logger_useful.h>

namespace RK
{

void deserializeKeeperStoreFromSnapshot(KeeperStore & store, const std::string & snapshot_path, Poco::Logger * log);
void deserializeKeeperStoreFromSnapshotsDir(KeeperStore & store, const std::string & path, Poco::Logger * log);

void deserializeLogAndApplyToStore(KeeperStore & store, const std::string & log_path, Poco::Logger * log);
void deserializeLogsAndApplyToStore(KeeperStore & store, const std::string & path, Poco::Logger * log);

}
