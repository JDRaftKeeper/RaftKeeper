#pragma once

#include <Core/Defines.h>
#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <IO/WriteBufferFromString.h>

namespace DB
{

struct Settings;

/** These settings represent fine tunes for internal details of Coordination storages
  * and should not be changed by the user without a reason.
  */

#define SVS_LIST_OF_COORDINATION_SETTINGS(M) \
    M(Milliseconds, session_timeout_ms, Coordination::DEFAULT_SESSION_TIMEOUT_MS, "Default client session timeout", 0) \
    M(Milliseconds, operation_timeout_ms, Coordination::DEFAULT_OPERATION_TIMEOUT_MS, "Default client operation timeout", 0) \
    M(Milliseconds, dead_session_check_period_ms, 1000, "How often leader will check sessions to consider them dead and remove", 0) \
    M(Milliseconds, heart_beat_interval_ms, 500, "Heartbeat interval between quorum nodes", 0) \
    M(Milliseconds, election_timeout_lower_bound_ms, 1000, "Lower bound of election timer (avoid too often leader elections)", 0) \
    M(Milliseconds, election_timeout_upper_bound_ms, 2000, "Lower bound of election timer (avoid too often leader elections)", 0) \
    M(UInt64, reserved_log_items, 10000000, "How many log items to store (don't remove during compaction)", 0) \
    M(UInt64, snapshot_distance, 3000000, "How many log items we have to collect to write new snapshot", 0) \
    M(UInt64, max_stored_snapshots, 5, "How many snapshots we want to store", 0) \
    M(Bool, auto_forwarding, true, "Allow to forward write requests from followers to leader", 0) \
    M(Milliseconds, shutdown_timeout, 5000, "How many time we will until RAFT shutdown", 0) \
    M(Milliseconds, startup_timeout, 6000000, "How many time we will until RAFT to start", 0) \
    M(LogsLevel, raft_logs_level, LogsLevel::information, "Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'", 0) \
    M(UInt64, rotate_log_storage_interval, 100000, "How many records will be stored in one log storage file", 0) \
    M(UInt64, nuraft_thread_size, 32, "NuRaft thread pool size", 0) \
    M(UInt64, fresh_log_gap, 200, "When node became fresh", 0) \
    M(UInt64, configuration_change_tries_count, 30, "How many times we will try to apply configuration change (add/remove server) to the cluster", 0) \
    M(Bool, force_sync, true, "Call fsync on each change in RAFT changelog", 0) \
    M(UInt64, max_batch_size, 1000, "Max batch size for append_entries", 0) \
    M(UInt64, fsync_interval, 1, "How many logs do once fsync when async_fsync is false", 0) \
    M(Bool, async_fsync, true, "If `true`, users can let the leader append logs parallel with their replication", 0) \
    M(Bool, session_consistent, true, "Request-response will follow the session xid order", 0) \
    M(Bool, async_snapshot, false, "async snapshot", 0)

DECLARE_SETTINGS_TRAITS(SvsKeeperSettingsTraits, SVS_LIST_OF_COORDINATION_SETTINGS)


struct SvsKeeperSettings : public BaseSettings<SvsKeeperSettingsTraits>
{
    void loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config);
};

using SvsKeeperSettingsPtr = std::shared_ptr<SvsKeeperSettings>;

/// Coordination settings + some other parts of keeper configuration
/// which are not stored in settings. Allows to dump configuration
/// with 4lw commands.
struct KeeperConfigurationAndSettings
{
    static constexpr int NOT_EXIST = -1;
    static const String DEFAULT_FOUR_LETTER_WORD_CMD;

    KeeperConfigurationAndSettings();
    int server_id;

    int tcp_port;
    String host;

    int internal_port;
    int thread_count;

    int snapshot_create_interval;
    int snapshot_start_time;
    int snapshot_end_time;

    String four_letter_word_white_list;

    String super_digest;

    bool standalone_keeper;
    SvsKeeperSettingsPtr coordination_settings;

    String log_storage_path;
    String snapshot_storage_path;

    void dump(WriteBufferFromOwnString & buf) const;
    static std::shared_ptr<KeeperConfigurationAndSettings>
    loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);

private:
    static String getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);
    static String getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_);
};

using KeeperConfigurationAndSettingsPtr = std::shared_ptr<KeeperConfigurationAndSettings>;
}
