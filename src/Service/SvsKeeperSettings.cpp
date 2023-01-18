#include <Service/Defines.h>
#include <Service/SvsKeeperSettings.h>
#include <common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <filesystem>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

void SvsKeeperSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        auto get_key = [&config_elem] (String key)-> String
        {
            return config_elem + "." + key;
        };

        session_timeout_ms = config.getUInt(get_key("session_timeout_ms"), Coordination::DEFAULT_SESSION_TIMEOUT_MS);
        operation_timeout_ms = config.getUInt(get_key("operation_timeout_ms"), Coordination::DEFAULT_OPERATION_TIMEOUT_MS);
        dead_session_check_period_ms = config.getUInt(get_key("dead_session_check_period_ms"), 1000);
        dead_session_check_period_ms = config.getUInt(get_key("dead_session_check_period_ms"), 1000);
        heart_beat_interval_ms = config.getUInt(get_key("heart_beat_interval_ms"), 500);
        election_timeout_lower_bound_ms = config.getUInt(get_key("election_timeout_lower_bound_ms"), 10000);
        election_timeout_upper_bound_ms = config.getUInt(get_key("election_timeout_upper_bound_ms"), 20000);
        reserved_log_items = config.getUInt(get_key("reserved_log_items"), 10000000);
        snapshot_distance = config.getUInt(get_key("snapshot_distance"), 3000000);
        max_stored_snapshots = config.getUInt(get_key("max_stored_snapshots"), 5);
        auto_forwarding = config.getBool(get_key("auto_forwarding"), true);
        shutdown_timeout = config.getUInt(get_key("shutdown_timeout"), 5000);
        startup_timeout = config.getUInt(get_key("startup_timeout"), 6000000);

        String log_level = config.getString(get_key("raft_logs_level"), "information");
        raft_logs_level = parseLogLevel(log_level);
        rotate_log_storage_interval = config.getUInt(get_key("rotate_log_storage_interval"), 100000);
        nuraft_thread_size = config.getUInt(get_key("nuraft_thread_size"), 32);
        fresh_log_gap = config.getUInt(get_key("stafresh_log_gaprtup_timeout"), 200);
        configuration_change_tries_count = config.getUInt(get_key("configuration_change_tries_count"), 30);
        force_sync = config.getBool(get_key("force_sync"), true);
        max_batch_size = config.getUInt(get_key("max_batch_size"), 1000);
        fsync_interval = config.getUInt(get_key("fsync_interval"), 1);
        async_fsync = config.getBool(get_key("async_fsync"), true);
        session_consistent = config.getBool(get_key("session_consistent"), true);
        async_snapshot = config.getBool(get_key("async_snapshot"), false);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in Coordination settings config");
        throw;
    }
}

const String KeeperConfigurationAndSettings::DEFAULT_FOUR_LETTER_WORD_CMD = "conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro";

KeeperConfigurationAndSettings::KeeperConfigurationAndSettings()
: server_id(NOT_EXIST)
, tcp_port(NOT_EXIST)
, standalone_keeper(false)
, coordination_settings(std::make_shared<SvsKeeperSettings>())
{
}

void KeeperConfigurationAndSettings::dump(WriteBufferFromOwnString & buf) const
{
    auto write_int = [&buf](int64_t value)
    {
        writeIntText(value, buf);
        buf.write('\n');
    };

    auto write_bool = [&buf](bool value)
    {
        String str_val = value ? "true" : "false";
        writeText(str_val, buf);
        buf.write('\n');
    };

    writeText("my_id=", buf);
    write_int(server_id);

    if (tcp_port != NOT_EXIST)
    {
        writeText("service_port=", buf);
        write_int(tcp_port);
    }

    writeText("host=", buf);
    writeText(host, buf);
    buf.write('\n');

    writeText("internal_port=", buf);
    write_int(internal_port);

    writeText("thread_count=", buf);
    write_int(thread_count);

    writeText("snapshot_create_interval=", buf);
    write_int(snapshot_create_interval);

    writeText("snapshot_start_time=", buf);
    write_int(snapshot_start_time);

    writeText("snapshot_end_time=", buf);
    write_int(snapshot_end_time);

    writeText("four_letter_word_white_list=", buf);
    writeText(four_letter_word_white_list, buf);
    buf.write('\n');

    writeText("log_dir=", buf);
    writeText(log_storage_path, buf);
    buf.write('\n');

    writeText("snapshot_dir=", buf);
    writeText(snapshot_storage_path, buf);
    buf.write('\n');

    /// coordination_settings

    writeText("session_timeout_ms=", buf);
    write_int(uint64_t(coordination_settings->session_timeout_ms));
    writeText("operation_timeout_ms=", buf);
    write_int(uint64_t(coordination_settings->operation_timeout_ms));
    writeText("dead_session_check_period_ms=", buf);
    write_int(uint64_t(coordination_settings->dead_session_check_period_ms));

    writeText("heart_beat_interval_ms=", buf);
    write_int(uint64_t(coordination_settings->heart_beat_interval_ms));
    writeText("election_timeout_lower_bound_ms=", buf);
    write_int(uint64_t(coordination_settings->election_timeout_lower_bound_ms));
    writeText("election_timeout_upper_bound_ms=", buf);
    write_int(uint64_t(coordination_settings->election_timeout_upper_bound_ms));

    writeText("reserved_log_items=", buf);
    write_int(coordination_settings->reserved_log_items);
    writeText("snapshot_distance=", buf);
    write_int(coordination_settings->snapshot_distance);
    writeText("max_stored_snapshots=", buf);
    write_int(coordination_settings->max_stored_snapshots);

    writeText("auto_forwarding=", buf);
    write_bool(coordination_settings->auto_forwarding);
    writeText("shutdown_timeout=", buf);
    write_int(uint64_t(coordination_settings->shutdown_timeout));
    writeText("startup_timeout=", buf);
    write_int(uint64_t(coordination_settings->startup_timeout));

    writeText("raft_logs_level=", buf);
    writeText(logLevelToString(coordination_settings->raft_logs_level), buf);
    buf.write('\n');
    writeText("rotate_log_storage_interval=", buf);
    write_int(coordination_settings->rotate_log_storage_interval);
    writeText("force_sync=", buf);
    write_bool(coordination_settings->force_sync);

    writeText("nuraft_thread_size=", buf);
    write_int(coordination_settings->nuraft_thread_size);
    writeText("fresh_log_gap=", buf);
    write_int(coordination_settings->fresh_log_gap);

}

KeeperConfigurationAndSettingsPtr
KeeperConfigurationAndSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    std::shared_ptr<KeeperConfigurationAndSettings> ret = std::make_shared<KeeperConfigurationAndSettings>();

    ret->server_id = config.getInt("service.my_id");
    ret->standalone_keeper = standalone_keeper_;

    ret->tcp_port = config.getInt("service.service_port", 5102);

    ret->host = config.getString("service.host", "0.0.0.0");

    ret->internal_port = config.getInt("service.internal_port", 5103);
    ret->thread_count = config.getInt("service.thread_count", 16);

    ret->snapshot_create_interval = config.getInt("service.snapshot_create_interval", 3600);
    ret->snapshot_create_interval = std::max(ret->snapshot_create_interval, 1);

    ret->snapshot_start_time = config.getInt("service.snapshot_start_time", 7200);
    ret->snapshot_end_time = config.getInt("service.snapshot_end_time", 79200);

    ret->four_letter_word_white_list = config.getString("service.four_letter_word_white_list", DEFAULT_FOUR_LETTER_WORD_CMD);

    ret->log_storage_path = getLogsPathFromConfig(config, standalone_keeper_);
    ret->snapshot_storage_path = getSnapshotsPathFromConfig(config, standalone_keeper_);

    ret->coordination_settings->loadFromConfig("service.coordination_settings", config);

    return ret;
}

String KeeperConfigurationAndSettings::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool)
{
    return config.getString("service.log_dir", "./raft_log");
}

String KeeperConfigurationAndSettings::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool)
{
    return config.getString("service.snapshot_dir", "./raft_snapshot");
}

}
