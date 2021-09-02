#pragma once
#include <string>
#include <Service/SvsKeeperStorage.h>
#include <common/logger_useful.h>

namespace DB
{

void deserializeSvsKeeperStorageFromSnapshot(SvsKeeperStorage & storage, const std::string & snapshot_path, Poco::Logger * log);

void deserializeSvsKeeperStorageFromSnapshotsDir(SvsKeeperStorage & storage, const std::string & path, Poco::Logger * log);

void deserializeLogAndApplyToStorage(SvsKeeperStorage & storage, const std::string & log_path, Poco::Logger * log);

void deserializeLogsAndApplyToStorage(SvsKeeperStorage & storage, const std::string & path, Poco::Logger * log);

}
