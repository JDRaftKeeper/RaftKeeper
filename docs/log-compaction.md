# Log compaction

RaftKeeper separates NuRaft's visible log range from the files retained for
snapshot recovery. Compaction updates the visible start index before returning,
but physical deletion runs on an owned background worker without holding the
Raft or segment-map lock.

## Retention takes priority over disk reclamation

Let `L` be the visible start index and `R` the minimum index of the retained
snapshots. Only complete segments ending before `min(L, R)` may be deleted.
A segment straddling the boundary is retained. Without a durable snapshot anchor,
no log file is reclaimed.

A small `reserved_log_items` still advances NuRaft's logical boundary, but cannot
override snapshot retention. This can retain more files than older versions.
The default remains `INT32_MAX`; it postpones NuRaft compaction rather than
disabling it. During startup, unverified older snapshots conservatively protect
their logs. Snapshot pruning waits until enough durable snapshots are known, so
the configured snapshot count can temporarily be exceeded.

Retained cache entries stay valid; compacted indices are filtered at read time.
Tail truncation and log-pack replacement invalidate affected cached entries.

## Completion, failures, and shutdown

`compact()` completes the logical update and schedules reclamation; it is not an
unlink barrier. `compact_async()` publishes the same boundary immediately and
calls its completion handler after this request's first deletion attempts.
No-op requests do not wait behind older cleanup. Retention-protected files are
not part of a deletion request.

Deletion failures are reported per file without stopping the rest of a batch.
The worker retains failed files and retries after 1 second, doubling the delay
up to 60 seconds. Retry success does not invoke the original callback again.
Shutdown finishes accepted first attempts and callbacks after snapshot-on-exit,
but does not retry failed files indefinitely. A blocked filesystem syscall still
depends on the operating system.

## Recovery and durability

Startup first validates snapshot contents and the corresponding log chain
without deleting, renaming, or repairing files. It then chooses a valid snapshot,
makes it durable, repairs only the selected active log tail, and starts the
cleanup worker. Corrupt latest snapshots can fall back to older retained
snapshots using logs hidden by the previous process's logical boundary.

Gaps covered by the selected snapshot are distinguishable from gaps or conflicting
ranges after it; the latter fail recovery. Old open-file names left by compaction
are classified by their actual index ranges. Only an unambiguous tail is reopened
for writing. Without a valid snapshot, missing required history is not silently
treated as an empty database.

Snapshot objects and their directory are synced before publication as a cleanup
anchor. Incomplete incoming snapshots are kept outside the published snapshot
catalog. Snapshot deletion removes a whole catalog entry only after its files
have been removed successfully. No compaction-boundary file is written, and
append/rotation does not sync additional reclamation metadata.

The earlier experimental `compacted_to` format was never released. A directory
containing `compacted_to` or `compacted_to.tmp` is rejected explicitly. Do not
delete these files to force startup: recover that experimental test directory
with the build that created it, or restore a compatible snapshot/log backup.

## Regression checks

`LogCompactionTest.*` covers blocked deletion, ongoing log operations, conservative
retention, retries, no-op requests, callback/shutdown lifetime, and abrupt child
process exits during cleanup. `SnapshotDurabilityTest.*` covers file/directory
sync failures and publication of received snapshots. The
`test_create_snapshot_on_exist` integration suite exercises snapshot-on-exit and
older-snapshot recovery with `reserved_log_items=1`.

### Validation on 2026-09-16

- ZooKeeper-compatible build: 114 unit tests passed.
- ClickHouse-compatible build: 115 unit tests passed.
- Each full suite also has one self-exec child helper skipped in the parent run
  and two pre-existing disabled benchmarks.
- ThreadSanitizer: all 23 compaction/durability tests passed for 50 consecutive
  rounds, including the self-exec crash and invariant-violation checks.
- Docker: eight snapshot-on-exit, retained-snapshot fallback, snapshot-restart,
  and persistent-log integration cases passed.

In the deterministic blocking test, the retired segment was locked against
deletion for at least 150 ms while `compact_async()` returned in 125 microseconds
on this machine. Reads, append/rotation, overwrite, and another compaction also
completed before deletion was unblocked. This checks dependency on reclamation,
not production throughput or the latency of a real filesystem unlink.
