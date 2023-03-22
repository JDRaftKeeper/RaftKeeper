#include <filesystem>
#include <Common/IO/WriteHelpers.h>
#include <Service/Settings.h>
#include <common/logger_useful.h>


namespace RK
{
namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}

namespace FsyncModeNS
{
FsyncMode parseFsyncMode(const String & in)
{
    if (in == "fsync_parallel")
        return FsyncMode::FSYNC_PARALLEL;
    else if (in == "fsync")
        return FsyncMode::FSYNC;
    else if (in == "fsync_batch")
        return FsyncMode::FSYNC_BATCH;
    else
        throw Exception("Unknown config 'log_fsync_mode'.", ErrorCodes::UNKNOWN_SETTING);
}

String toString(FsyncMode mode)
{
    if (mode == FsyncMode::FSYNC_PARALLEL)
        return "fsync_parallel";
    else if (mode == FsyncMode::FSYNC)
        return "fsync";
    else if (mode == FsyncMode::FSYNC_BATCH)
        return "fsync_batch";
    else
        throw Exception("Unknown config 'log_fsync_mode'.", ErrorCodes::UNKNOWN_SETTING);
}

}

void RaftSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
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
        dead_session_check_period_ms = config.getUInt(get_key("dead_session_check_period_ms"), 100);
        heart_beat_interval_ms = config.getUInt(get_key("heart_beat_interval_ms"), 500);
        election_timeout_lower_bound_ms = config.getUInt(get_key("election_timeout_lower_bound_ms"), 10000);
        election_timeout_upper_bound_ms = config.getUInt(get_key("election_timeout_upper_bound_ms"), 20000);
        reserved_log_items = config.getUInt(get_key("reserved_log_items"), 1000000);
        snapshot_distance = config.getUInt(get_key("snapshot_distance"), 3000000);
        max_stored_snapshots = config.getUInt(get_key("max_stored_snapshots"), 5);
        startup_timeout = config.getUInt(get_key("startup_timeout"), 6000000);
        shutdown_timeout = config.getUInt(get_key("shutdown_timeout"), 5000);

        String log_level = config.getString(get_key("raft_logs_level"), "information");
        raft_logs_level = parseNuRaftLogLevel(log_level);
        rotate_log_storage_interval = config.getUInt(get_key("rotate_log_storage_interval"), 100000);
        /// TODO set a value according to CPU size
        nuraft_thread_size = config.getUInt(get_key("nuraft_thread_size"), 32);
        fresh_log_gap = config.getUInt(get_key("fresh_log_gap"), 200);
        configuration_change_tries_count = config.getUInt(get_key("configuration_change_tries_count"), 30);
        max_batch_size = config.getUInt(get_key("max_batch_size"), 1000);
        log_fsync_mode = FsyncModeNS::parseFsyncMode(config.getString(get_key("log_fsync_mode"), "fsync_parallel"));
        log_fsync_interval = config.getUInt(get_key("log_fsync_interval"), 1000);
        session_consistent = config.getBool(get_key("session_consistent"), true);
        async_snapshot = config.getBool(get_key("async_snapshot"), false);
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in configuration.");
        throw;
    }
}

RaftSettingsPtr RaftSettings::getDefault()
{
    RaftSettingsPtr settings = std::make_shared<RaftSettings>();
    settings->session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
    settings->operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
    settings->dead_session_check_period_ms =100;
    settings->heart_beat_interval_ms = 500;
    settings->election_timeout_lower_bound_ms = 10000;
    settings->election_timeout_upper_bound_ms = 20000;
    settings->reserved_log_items = 10000000;
    settings->snapshot_distance = 3000000;
    settings->max_stored_snapshots = 5;
    settings->shutdown_timeout = 5000;
    settings->startup_timeout = 6000000;

    settings->raft_logs_level = NuRaftLogLevel::LOG_INFORMATION;
    settings->rotate_log_storage_interval = 100000;
    settings->nuraft_thread_size = 32;
    settings->fresh_log_gap = 200;
    settings->configuration_change_tries_count = 30;
    settings->max_batch_size = 1000;
    settings->log_fsync_interval = 1000;
    settings->log_fsync_mode = FsyncMode::FSYNC_PARALLEL;
    settings->session_consistent = true;
    settings->async_snapshot = false;

    return settings;
}

const String Settings::DEFAULT_FOUR_LETTER_WORD_CMD = "conf,cons,crst,envi,ruok,srst,srvr,stat,wchs,dirs,mntr,isro,lgif,rqld";

Settings::Settings()
: my_id(NOT_EXIST)
, port(NOT_EXIST)
, standalone_keeper(false)
, raft_settings(RaftSettings::getDefault())
{
}

void Settings::dump(WriteBufferFromOwnString & buf) const
{
    auto write_int = [&buf](int64_t value)
    {
        writeIntText(value, buf);
        buf.write('\n');
    };

//    auto write_bool = [&buf](bool value)
//    {
//        String str_val = value ? "true" : "false";
//        writeText(str_val, buf);
//        buf.write('\n');
//    };

    writeText("my_id=", buf);
    write_int(my_id);

    if (port != NOT_EXIST)
    {
        writeText("port=", buf);
        write_int(port);
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
    writeText(log_dir, buf);
    buf.write('\n');

    writeText("snapshot_dir=", buf);
    writeText(snapshot_dir, buf);
    buf.write('\n');

    /// raft_settings

    writeText("session_timeout_ms=", buf);
    write_int(raft_settings->session_timeout_ms);
    writeText("operation_timeout_ms=", buf);
    write_int(raft_settings->operation_timeout_ms);
    writeText("dead_session_check_period_ms=", buf);
    write_int(raft_settings->dead_session_check_period_ms);

    writeText("heart_beat_interval_ms=", buf);
    write_int(raft_settings->heart_beat_interval_ms);
    writeText("election_timeout_lower_bound_ms=", buf);
    write_int(raft_settings->election_timeout_lower_bound_ms);
    writeText("election_timeout_upper_bound_ms=", buf);
    write_int(raft_settings->election_timeout_upper_bound_ms);

    writeText("reserved_log_items=", buf);
    write_int(raft_settings->reserved_log_items);
    writeText("snapshot_distance=", buf);
    write_int(raft_settings->snapshot_distance);
    writeText("max_stored_snapshots=", buf);
    write_int(raft_settings->max_stored_snapshots);

    writeText("shutdown_timeout=", buf);
    write_int(raft_settings->shutdown_timeout);
    writeText("startup_timeout=", buf);
    write_int(raft_settings->startup_timeout);

    writeText("raft_logs_level=", buf);
    writeText(nuRaftLogLevelToString(raft_settings->raft_logs_level), buf);
    buf.write('\n');
    writeText("rotate_log_storage_interval=", buf);
    write_int(raft_settings->rotate_log_storage_interval);

    writeText("log_fsync_mode=", buf);
    writeText(FsyncModeNS::toString(raft_settings->log_fsync_mode), buf);
    buf.write('\n');
    writeText("log_fsync_interval=", buf);
    write_int(raft_settings->log_fsync_interval);

    writeText("nuraft_thread_size=", buf);
    write_int(raft_settings->nuraft_thread_size);
    writeText("fresh_log_gap=", buf);
    write_int(raft_settings->fresh_log_gap);

}

SettingsPtr Settings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, bool standalone_keeper_)
{
    std::shared_ptr<Settings> ret = std::make_shared<Settings>();

    ret->my_id = config.getInt("keeper.my_id");
    ret->standalone_keeper = standalone_keeper_;

    ret->port = config.getInt("keeper.port", 8101);

    ret->host = config.getString("keeper.host", "0.0.0.0");

    ret->internal_port = config.getInt("keeper.internal_port", 8103);
    ret->thread_count = config.getInt("keeper.thread_count", 16);

    ret->snapshot_create_interval = config.getInt("keeper.snapshot_create_interval", 3600);
    ret->snapshot_create_interval = std::max(ret->snapshot_create_interval, 1);

    ret->snapshot_start_time = config.getInt("keeper.snapshot_start_time", 7200);
    ret->snapshot_end_time = config.getInt("keeper.snapshot_end_time", 79200);

    ret->super_digest = config.getString("keeper.superdigest", "");

    ret->four_letter_word_white_list = config.getString("keeper.four_letter_word_white_list", DEFAULT_FOUR_LETTER_WORD_CMD);

    ret->log_dir = getLogsPathFromConfig(config, standalone_keeper_);
    ret->snapshot_dir = getSnapshotsPathFromConfig(config, standalone_keeper_);

    ret->raft_settings->loadFromConfig("keeper.raft_settings", config);

    return ret;
}

String Settings::getLogsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool)
{
    return config.getString("keeper.log_dir", "./data/log");
}

String Settings::getSnapshotsPathFromConfig(const Poco::Util::AbstractConfiguration & config, bool)
{
    return config.getString("keeper.snapshot_dir", "./data/snapshot");
}

}
