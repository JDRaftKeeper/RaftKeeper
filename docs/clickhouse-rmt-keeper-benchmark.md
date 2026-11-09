# ClickHouse ReplicatedMergeTree Keeper workload

## Production profile

Source: `LFRH_CK_Pub_490`, Asia/Shanghai, 2026-09-14 02:00:00 through
2026-09-14 10:00:00.

- 113 shards, two replicas, and 226 Keeper sessions distributed 77/71/78.
- Approximately 3.92 million znodes and 1.03 GB of znode data.
- The dominant insert wrote about 14.56 million rows and issued about 4,736
  Keeper Multi requests per execution. It ran about once every ten minutes on
  every ClickHouse host.
- The eight-hour window contained 31.04 million NewPart, 22.52 million
  DownloadPart, and 4.08 million MergeParts events.
- DownloadPart peaked at 3,065 events/s; MergeParts peaked at 782 events/s.
- A representative table/shard contained 1,000 block-id nodes and 1,066 part
  nodes per replica. Part names averaged 41 bytes and part metadata 97 bytes.

`system.part_log.ProfileEvents` is suitable for DownloadPart and MergeParts
operation mixes. It must not be summed across multi-part NewPart rows because a
single profile-counter snapshot can be attached to every generated part.

## Reproduction profile

The `--clickhouse-rmt-workload true` benchmark mode uses:

- 30,000 read requests/s and 15,000 Multi write requests/s by default;
- 65% get, 33% list, and 2% exists within reads;
- Multi writes containing create, set, and remove operations;
- two replica part lists and one block-id list per metadata group;
- 1,000 children per list by default.

The load is open-loop paced. Use 226 non-shared connections to match production
sessions. `metadata-groups` controls state size independently from list width.

## Test result

Test cluster: `11.161.218.98:2181`, `11.161.221.82:2181`, and
`11.161.221.208:2181`.

Configuration: 226 workers, pipeline depth 16, 32 metadata groups, 1,000
children/list, Multi size 5, and 60 seconds. The setup created 96,992 znodes.

| Metric | Result |
|---|---:|
| Offered QPS | 44,999.9 |
| Completed QPS | 44,996.1 |
| Read QPS | 30,001.2 |
| Write QPS | 14,994.9 |
| Errors | 0 |
| Overall average | 764.3 us |
| Overall p99 | 1,395 us |
| Overall p99.9 | 94,393 us |
| Read average | 549.7 us |
| Write average | 1,193.8 us |

At 20 seconds, `csnp` was sent to the leader. Snapshot serialization took
1,166 ms and reported 4 ms of blocking. One-second counter deltas were:

| Phase | Read QPS | Write QPS | Read avg | Write avg |
|---|---:|---:|---:|---:|
| Normal | 30,048.9 | 14,966.5 | 0.031 ms | 0.732 ms |
| Snapshot second | 30,056.9 | 14,959.0 | 11.295 ms | 14.787 ms |
| Post-snapshot | 30,056.8 | 14,966.7 | 0.029 ms | 0.708 ms |

The fixed offered load remained sustainable, but snapshot activity increased
server-side mean latency by roughly 11-20x for one second and raised client
p99.9 to approximately 94-96 ms. The leader's Raft request batch average rose
from 5.73 normally to 7.12 during the snapshot second, indicating that requests
accumulated while commit progress slowed.

The generated benchmark path was removed after the run.

## Full production-size growth run

The production growth scenario initialized 1,242 metadata groups and then ran
for ten minutes with 30,000 reads/s, 11,400 Multi writes/s, and 3,600 persistent
creates/s. It reproduced the observed znode growth from approximately 3.75
million to 5,910,196 nodes. The measured active growth rate was 3,593 nodes/s.

The run completed 27,000,094 requests at 44,999.6 requests/s with no errors.
One snapshot was triggered near the five-minute mark, at approximately 4.94
million znodes. It took 11,244 ms and reported 463 ms of blocking.

The initial global rate limiter tried to catch up missed request slots after
the snapshot. It was changed to discard expired per-worker slots. A subsequent
fixed-rate snapshot run reused the 5.91-million-node tree and produced:

- 44,214.9 completed requests/s from a 45,000 requests/s offered load;
- 7.85 ms average, 85.78 ms p99, and 761.14 ms p99.9 client latency;
- 11,936 ms snapshot time and 574 ms reported blocking time;
- a one-second throughput drop to about 8.2k reads/s and 3.9k writes/s when the
  snapshot completed;
- 86.6 ms read and 99.9 ms write average server latency in the following
  second.

After fixing the limiter, the 5.91-million-node tree was tested again. A
no-snapshot run sustained 43,464 requests/s from the 45,000 requests/s target,
with 30.53 ms average client latency. This places the v2.1.1 test cluster close
to its capacity knee for this workload.

The final snapshot comparison discarded missed rate-limit slots instead of
catching up. Snapshot serialization took 11,936 ms and reported 574 ms of
blocking. Server counter deltas were:

| Phase | Read QPS | Write QPS | Read avg | Write avg | Leader batch |
|---|---:|---:|---:|---:|---:|
| Pre-snapshot | 29,934.7 | 14,864.6 | 2.720 ms | 4.373 ms | 5.65 |
| Snapshot/recovery | 28,130.3 | 14,050.4 | 16.210 ms | 20.722 ms | 7.13 |
| Post-snapshot | 30,110.8 | 15,134.2 | 4.057 ms | 6.316 ms | 6.37 |

When the snapshot completed, one-second throughput dropped to approximately
8.2k reads/s and 3.9k writes/s. In the next second, average read/write latency
reached 86.6/99.9 ms. Client-side p99.9 for the complete run was 761 ms.

The full tree remains under
`/codex_ch_growth_20260917_024221/rmt-production-profile` so that optimized
binaries can be compared against exactly the same state without rebuilding the
tree. Remove `/codex_ch_growth_20260917_024221` after the comparison campaign.

## Backlog mode

`--drop-late-requests false` retains overdue offered-load slots. With pipeline
depth 128, the default 3-second operation timeout expired sessions during the
snapshot. Matching the production 35-second operation timeout completed the
run without errors.

The 40-second backlog run completed 43,968 requests/s from a 45,000 requests/s
target. Average client latency was 136.7 ms, p99 was 750 ms, and p99.9 was
809 ms. Maximum offered-schedule lag was 325 ms and no slots were dropped.

After the snapshot completed, one-second mean read/write latency rose as
follows:

| Second after completion | Read avg | Write avg |
|---:|---:|---:|
| 0 | 294.5 ms | 304.8 ms |
| 1 | 432.5 ms | 442.2 ms |
| 2 | 564.6 ms | 575.3 ms |
| 3 | 658.1 ms | 668.2 ms |
| 4 | 718.9 ms | 734.1 ms |

This reproduces the production symptom where average latency, not only tail
latency, reaches several hundred milliseconds. The buildup comes from retained
client backlog while snapshot completion and log reclamation temporarily reduce
service capacity.
