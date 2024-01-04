#pragma once

#include <string>
#include <Service/KeeperStore.h>
#include <common/logger_useful.h>

namespace RK
{

/// deserialize one snapshot segment
void deserializeKeeperStoreFromSnapshot(KeeperStore & store, const String & snapshot_path, Poco::Logger * log);
/// deserialize snapshot
void deserializeKeeperStoreFromSnapshotsDir(KeeperStore & store, const String & path, Poco::Logger * log);

///deserialize one log segment
void deserializeLogAndApplyToStore(KeeperStore & store, const String & log_path, Poco::Logger * log);
/// deserialize log
void deserializeLogsAndApplyToStore(KeeperStore & store, const String & path, Poco::Logger * log);

}
