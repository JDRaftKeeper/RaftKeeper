#pragma once

#include <Service/SnapshotCommon.h>

namespace RK
{

struct SnapshotConversionResult
{
    String prefix;
    UInt64 term;
    UInt64 log_index;
    SnapshotVersion source_version;
    SnapshotVersion target_version;
    size_t source_objects;
    size_t target_objects;
    String output_dir;
};

/// Offline, lossless downgrade of one complete RaftKeeper snapshot into a new directory.
/// The caller must provide a stable source (stopped server or an immutable backup).
SnapshotConversionResult
downgradeSnapshot(const String & input_dir, const String & output_dir, SnapshotVersion target_version, const String & snapshot_prefix = "");

}
