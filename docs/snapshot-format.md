# Snapshot format and metadata objects

Related issue: https://github.com/JDRaftKeeper/RaftKeeper/issues/364

V4 combines the three small metadata objects into one. Node records, request logs,
and sequential-node allocation are unchanged.

| Layout | Object 1 | Object 2 | Object 3 | Data objects |
| --- | --- | --- | --- | --- |
| V0–V3 | counters | sessions and authentication | ACL map | start at 4 |
| V4 | counters, sessions/authentication, ACL map | first data object | next data object, if needed | start at 2 |

The metadata object contains the existing typed batches in that order, with the
usual batch size limit. Large session or ACL collections still span multiple
batches. `OBJECTCOUNT` includes the metadata object and all data objects. Both
synchronous and asynchronous snapshot writers use the same metadata encoding.
Every snapshot has two fewer object files; a one-data-object snapshot goes from
four files to two. It does not require a monolithic in-memory metadata buffer.

## File header

Legacy V0–V3 headers remain `SnapHead` (8 bytes) followed by version (1 byte).
The reader also accepts the historical headerless V0 format.

V4 has a fixed 16-byte header:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | ASCII `SnapHead`, without a trailing NUL |
| 8 | 1 | format version, `4` |
| 9 | 1 | codec: `0` = none, `1` = zstd |
| 10 | 2 | flags, currently zero |
| 12 | 4 | reserved, currently zero |

Unknown versions/codecs and nonzero flags/reserved bytes are rejected. V4 does
not assign any flag bits yet. Its node layout is the same as V2/V3; future changes
to node fields require a new data-format version, not a new version per codec.

Each batch remains `data_length` (4 bytes), `data_crc` (4 bytes), and the stored
body. Compression applies only to the body; length and CRC describe the stored
(possibly compressed) bytes. The tail remains `SnapTail` (8 bytes) and the
cumulative batch checksum (4 bytes). Batch framing keeps the existing encoding.

## Configuration and upgrades

Under `keeper.raft_settings`:

```xml
<snapshot_format_version>4</snapshot_format_version>
<snapshot_compression>zstd</snapshot_compression>
```

`snapshot_format_version` accepts `2` or `4`. The default is `4`; compression
defaults to `none`. Both compression choices write V4; only the codec byte differs.
Explicit format `2` preserves legacy writing: `none` writes V2 and `zstd` writes V3.
Invalid values are rejected.

While old servers remain in a cluster, explicitly configure format `2` on upgraded
servers. Use format `4` only after all snapshot consumers support it. The new
default is not safe for mixed-version clusters without this explicit setting.
Changing compression does not affect
the ability to read existing snapshots: each object describes its own codec.
This setting follows the existing startup-loaded Raft settings; it is not a new
hot-reload mechanism.

Selecting format `2` again changes future writes, not existing V4 files. Older
binaries cannot read V4 headers. Do not downgrade until a complete legacy snapshot
and its required log tail are retained and usable by the target binary; keep a
backup before upgrading. A read/write version setting alone does not make an
already-written new snapshot readable by an old binary.

## Offline snapshot downgrade

RaftKeeper release **2.1.1** uses snapshot format **V2**, not a format named 2.1.1.
The converter supports only these RaftKeeper format downgrades:

| Source | Target | Output codec |
| --- | --- | --- |
| V4, either codec | V2 | none |
| V4, either codec | V3 | zstd |
| V3 | V2 | none |

Run the new binary, even when preparing snapshots for an old server:

```bash
raftkeeper converter \
  --raftkeeper-snapshots-dir /backup/snapshots \
  --output-dir /backup/snapshots-v2 \
  --target-snapshot-version 2
```

The target version is required. Upgrades, same-version conversion, V0/V1 output,
unknown formats and cross-implementation conversion are rejected. The existing
ZooKeeper import mode retains its original arguments; its output now defaults to
V4 like the other snapshot writers. Its input options cannot be combined with the
new RaftKeeper downgrade options.

The input must be a stopped server's directory or an immutable backup. Conversion
operates on one complete snapshot, not one object file. It defaults to the newest
term/log-index in the directory and fails if that snapshot is corrupt or incomplete
instead of silently using an older one. To choose an older snapshot, pass its
filename prefix without the final object number:

```bash
raftkeeper converter \
  --raftkeeper-snapshots-dir /backup/snapshots \
  --output-dir /backup/selected-v2 \
  --target-snapshot-version 2 \
  --snapshot-prefix snapshot_20260915010101_7_77
```

Two prefixes with the same term/index require an explicit selection. Duplicate
objects, mixed object format versions, malformed headers, unsupported flags,
missing `OBJECTCOUNT`, missing objects and bad checksums fail conversion.

The output directory must not exist, its parent must exist, and input and output
must not overlap (including aliases through symlinks). Source files are read-only.
The tool writes to a uniquely named sibling staging directory, reloads and compares
the complete node and metadata state, then atomically publishes without replacing
an existing destination. Failures return a nonzero exit status and clean this
invocation's staging directory. A forced process kill can leave a staging directory;
it must not be installed as a completed snapshot.
The output directory belongs to the converter's user and is initially private
(mode 0700); ensure the service account can read it before installing it.

Node data, all Stat fields, ACL/auth, sessions, ephemeral nodes, ZXID and session
counter are preserved. Snapshot timestamp, term and log-index are preserved; object
counts and checksums are recomputed. No sessions are created or expired by the tool.
It materializes both source and verification stores, so allow approximately twice
the expanded snapshot size in RAM, plus buffers and output disk space.

For production rollback, back up the entire data directory, stop the server,
convert a stable snapshot into a separate directory, verify the result, and only
then install that complete directory manually. Do not mix converted objects with
the original V4 objects. **Raft logs are not converted.** The log tail and other
server metadata must independently be compatible with the target binary; do not
delete committed logs just to make an old binary start. Snapshot conversion alone
does not guarantee that a software downgrade is safe.

## Tests

`SnapshotFormatTest` covers the format matrix and offline converter failure paths.
`test_snapshot_format_multinode` forces real snapshot installation by stopping a
follower and compacting the leader's log beyond that follower's position.
`test_snapshot_downgrade` exercises the CLI and snapshot-only server recovery,
including session/auth/ephemeral preservation using the genuine 2.1.1 binary:

- The complete none/zstd and synchronous/asynchronous matrix includes both direct
  V4-to-V2 recovery and chained V4-to-V3-to-V2 recovery on 2.1.1. ACL checks cover
  both authorized access and rejection of a separate unauthenticated session.
- Latest and explicitly selected historical snapshots are started as servers and
  checked for the expected data, Stat and snapshot index, not only CLI messages.
- Corrupted latest snapshots cover header fields, truncation, metadata/data CRCs,
  missing objects, mixed versions, duplicate objects and ambiguous identities.
  Both codecs must fail without fallback, source changes or partial output;
  explicit recovery from an intact older snapshot is also checked.
- After downgrade, 2.1.1 must allocate sequential nodes, commit transactions,
  allocate new sessions and replay its newly written log tail after restart.
- Explicit legacy V2/V3 writing transitions to default V4; codec changes and a
  forced restart with an empty log directory verify per-file format detection.
- Three-node recovery combines snapshot installation and a subsequent log tail,
  with ACL, ephemeral-owner and session-close cleanup checks. Installation evidence
  is restricted to the current test's log suffix and snapshot index to exclude
  stale log matches.

```bash
cd tests/integration
./runner --binary ../../build/programs/raftkeeper \
  --old-binary /path/to/2.1.1/raftkeeper \
  --base-configs-dir ../../programs/server \
  'test_snapshot_downgrade test_snapshot_format_multinode'
```

The downgrade integration suite checks the old binary's version and fails rather
than substituting the current binary when 2.1.1 is unavailable.
