#pragma once

#include <Core/Defines.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/Message.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Service/LoggerWrapper.h>

namespace DB
{

/** These settings represent fine tunes for internal details of Coordination storages
  * and should not be changed by the user without a reason.
  */

struct SvsKeeperSettings
{
    /// Default client session timeout
    UInt64 session_timeout_ms;
    /// Default client operation timeout
    UInt64 operation_timeout_ms;
    /// How often leader will check sessions to consider them dead and remove
    UInt64 dead_session_check_period_ms;
    /// Heartbeat interval between quorum nodes
    UInt64 heart_beat_interval_ms;
    /// Lower bound of election timer (avoid too often leader elections)
    UInt64 election_timeout_lower_bound_ms;
    /// Lower bound of election timer (avoid too often leader elections)
    UInt64 election_timeout_upper_bound_ms;
    /// How many log items to store (don't remove during compaction)
    UInt64 reserved_log_items;
    /// How many log items we have to collect to write new snapshot
    UInt64 snapshot_distance;
    /// How many snapshots we want to store
    UInt64 max_stored_snapshots;
    /// Allow to forward write requests from followers to leader
    bool auto_forwarding;
    /// How many time we will until RAFT shutdown
    UInt64 shutdown_timeout;
    /// How many time we will until RAFT to start
    UInt64 startup_timeout;
    /// Log internal RAFT logs into main server log level. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal'
    LogLevel raft_logs_level;
    /// How many records will be stored in one log storage file
    UInt64 rotate_log_storage_interval;
    /// NuRaft thread pool size
    UInt64 nuraft_thread_size;
    /// When node became fresh
    UInt64 fresh_log_gap;
    /// How many times we will try to apply configuration change (add/remove server) to the cluster
    UInt64 configuration_change_tries_count;
    /// Call fsync on each change in RAFT changelog
    bool force_sync;
    /// Max batch size for append_entries
    UInt64 max_batch_size;
    /// How many logs do once fsync when async_fsync is false
    UInt64 fsync_interval;
    /// If `true`, users can let the leader append logs parallel with their replication
    bool async_fsync;
    /// Request-response will follow the session xid order
    bool session_consistent;
    /// Whether async snapshot
    bool async_snapshot;

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
