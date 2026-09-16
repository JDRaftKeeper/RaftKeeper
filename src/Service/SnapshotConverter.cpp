#include <algorithm>
#include <charconv>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <optional>
#include <unistd.h>

#include <Service/SnapshotConverter.h>
#ifdef __linux__
#    include <fcntl.h>
#    include <sys/syscall.h>

#    include <linux/fs.h>
#elif defined(__APPLE__)
#    include <stdio.h>
#endif

#include <Common/Exception.h>
#include <Common/IO/ReadBufferFromFile.h>
#include <Common/IO/ReadHelpers.h>
#include <common/scope_guard.h>

#include <Service/NuRaftLogSnapshot.h>

namespace RK
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CORRUPTED_SNAPSHOT;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

    struct SourceSnapshot
    {
        SnapObject identity;
        std::map<UInt64, String> objects;
    };

    bool isWithin(const std::filesystem::path & path, const std::filesystem::path & parent)
    {
        return std::mismatch(parent.begin(), parent.end(), path.begin(), path.end()).first == parent.end();
    }

    std::map<String, SourceSnapshot> findSnapshots(const std::filesystem::path & input)
    {
        std::map<String, SourceSnapshot> snapshots;
        for (const auto & entry : std::filesystem::directory_iterator(input))
        {
            auto name = entry.path().filename().string();
            if (!name.starts_with("snapshot_"))
                continue;
            if (entry.is_symlink() || !entry.is_regular_file())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Snapshot object must be a regular file: {}", entry.path().string());

            Strings tokens;
            splitInto<'_'>(tokens, name);
            if ((tokens.size() != 4 && tokens.size() != 5) || tokens[1].empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid snapshot filename {}", name);
            for (size_t i = 1; i < tokens.size(); ++i)
            {
                UInt64 value;
                auto [end, error] = std::from_chars(tokens[i].data(), tokens[i].data() + tokens[i].size(), value);
                if (error != std::errc() || end != tokens[i].data() + tokens[i].size())
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid snapshot filename {}", name);
            }
            SnapObject identity;
            if (!identity.parseInfoFromObjectName(name) || identity.object_id == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid snapshot filename {}", name);
            auto prefix = name.substr(0, name.rfind('_'));
            auto & snapshot = snapshots[prefix];
            snapshot.identity = identity;
            if (!snapshot.objects.emplace(identity.object_id, entry.path().string()).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate snapshot object {} for {}", identity.object_id, prefix);
        }
        return snapshots;
    }

    void verifyState(KeeperStore & source, KeeperStore & output)
    {
        if (source.getNodesCount() != output.getNodesCount() || source.getZxid() != output.getZxid()
            || source.getSessionIDCounter() != output.getSessionIDCounter()
            || source.getSessionAndTimeOut() != output.getSessionAndTimeOut() || source.getSessionAndAuth() != output.getSessionAndAuth()
            || source.getEphemerals() != output.getEphemerals() || source.getACLMap().getMapping() != output.getACLMap().getMapping())
            throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Converted snapshot metadata differs from source");

        for (UInt32 bucket = 0; bucket < source.getDataTreeBucketNum(); ++bucket)
        {
            source.getDataTree().getMap(bucket).forEach(
                [&output](const String & path, const KeeperNodePtr & node)
                {
                    auto restored = output.getNode(path);
                    /// KeeperNode/Stat equality does not compare every Stat field. Compare their complete
                    /// serialized representation instead, plus the separately reconstructed children set.
                    if (!restored || node->children != restored->children
                        || serializeKeeperNode(path, node, SnapshotVersion::V2) != serializeKeeperNode(path, restored, SnapshotVersion::V2))
                        throw Exception(ErrorCodes::CORRUPTED_SNAPSHOT, "Converted snapshot node differs from source: {}", path);
                });
        }
    }

    void publishDirectory(const String & source, const String & destination)
    {
#ifdef __linux__
        if (::syscall(SYS_renameat2, AT_FDCWD, source.c_str(), AT_FDCWD, destination.c_str(), RENAME_NOREPLACE) != 0)
            throwFromErrno(
                "Cannot publish converted snapshot without overwriting " + destination, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
#elif defined(__APPLE__)
        if (::renamex_np(source.c_str(), destination.c_str(), RENAME_EXCL) != 0)
            throwFromErrno(
                "Cannot publish converted snapshot without overwriting " + destination, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
#else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Atomic no-replace snapshot publication is not supported on this platform");
#endif
    }

}

SnapshotConversionResult
downgradeSnapshot(const String & input_dir, const String & output_dir, SnapshotVersion target_version, const String & snapshot_prefix)
{
    namespace fs = std::filesystem;
    if (target_version != SnapshotVersion::V2 && target_version != SnapshotVersion::V3)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Target snapshot version must be 2 or 3");
    if (input_dir.empty() || output_dir.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input and output directories are required");

    auto input = fs::canonical(input_dir);
    auto requested_output = fs::absolute(output_dir);
    while (requested_output != requested_output.root_path() && requested_output.filename().empty())
        requested_output = requested_output.parent_path();
    auto output = fs::weakly_canonical(requested_output);
    while (output != output.root_path() && output.filename().empty())
        output = output.parent_path();
    if (!fs::is_directory(input) || !fs::is_directory(output.parent_path()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input and output parent must be existing directories");
    if (isWithin(input, output) || isWithin(output, input))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input and output directories must not overlap");
    if (fs::exists(fs::symlink_status(requested_output)) || fs::is_symlink(fs::symlink_status(requested_output)))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Output directory must not exist: {}", output_dir);

    auto snapshots = findSnapshots(input);
    if (snapshots.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No snapshots found in {}", input.string());
    auto selected = snapshots.end();
    if (!snapshot_prefix.empty())
    {
        selected = snapshots.find(snapshot_prefix);
        if (selected == snapshots.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Snapshot prefix not found: {}", snapshot_prefix);
    }
    else
    {
        auto key = [](const auto & item) { return std::pair(item.second.identity.log_last_term, item.second.identity.log_last_index); };
        selected
            = std::max_element(snapshots.begin(), snapshots.end(), [&](const auto & lhs, const auto & rhs) { return key(lhs) < key(rhs); });
        if (std::count_if(snapshots.begin(), snapshots.end(), [&](const auto & item) { return key(item) == key(*selected); }) != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Ambiguous latest snapshot; specify --snapshot-prefix");
    }

    auto & source = selected->second;
    std::optional<SnapshotVersion> source_version;
    for (const auto & [id, path] : source.objects)
    {
        ReadBufferFromFile in(path);
        UInt64 magic;
        readIntBinary(magic, in);
        auto format = isSnapshotFileHeader(magic) ? readSnapshotFormat(in) : SnapshotFormat(SnapshotVersion::V0);
        if (source_version && *source_version != format.version)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Mixed snapshot versions in {}", selected->first);
        source_version = format.version;
    }
    if (target_version >= *source_version)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Only snapshot downgrades are allowed (source {}, target {})",
            toString(*source_version),
            toString(target_version));

    auto & identity = source.identity;
    auto config
        = nuraft::cs_new<nuraft::cluster_config>(identity.log_last_index, identity.log_last_index == 0 ? 0 : identity.log_last_index - 1);
    nuraft::snapshot meta(identity.log_last_index, identity.log_last_term, config);
    KeeperSnapshotStore reader(input.string(), meta);
    for (auto & [id, path] : source.objects)
        reader.addObjectPath(id, path);
    KeeperStore store(500);
    reader.loadLatestSnapshot(store, /*require_complete_state=*/true);

    String staging = (output.parent_path() / (output.filename().string() + ".tmp.XXXXXX")).string();
    if (!::mkdtemp(staging.data()))
        throwFromErrno("Cannot create snapshot staging directory", ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
    bool published = false;
    SCOPE_EXIT({
        if (!published)
        {
            std::error_code ignored;
            fs::remove_all(staging, ignored);
        }
    });

    meta.set_size(store.getNodesCount());
    KeeperSnapshotStore writer(staging, meta, MAX_OBJECT_NODE_SIZE, SAVE_BATCH_SIZE, target_version);
    writer.init(identity.create_time);
    auto count = writer.createObjects(store, store.getZxid(), store.getSessionIDCounter());
    KeeperStore restored(500);
    writer.loadLatestSnapshot(restored, /*require_complete_state=*/true);
    verifyState(store, restored);
    publishDirectory(staging, output.string());
    published = true;
    return {
        selected->first,
        identity.log_last_term,
        identity.log_last_index,
        *source_version,
        target_version,
        source.objects.size(),
        count,
        output.string()};
}

}
