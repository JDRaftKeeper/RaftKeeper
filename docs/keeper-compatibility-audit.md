# RaftKeeper ↔ ClickHouse Keeper command-behavior audit

Comparison of every ZooKeeper opcode's server-side behavior between RaftKeeper and ClickHouse Keeper
(`/data6/lizhuoyu/ClickHouse_OpenSource/ClickHouse`, `src/Coordination/KeeperStorageImpl.cpp`).

**Compatibility model.** RaftKeeper has the compile flag `COMPATIBLE_MODE_ZOOKEEPER` (`CMakeLists.txt`):
ON → behave like Apache ZooKeeper; OFF → behave like ClickHouse Keeper. So standard-op divergences are
mode-gated; ClickHouse-specific opcodes (500–507) are matched to ClickHouse in both modes (a pure
ZooKeeper client never sends them).

Verdict legend: **FIXED** (this branch), **GATED** (mode-conditional, this branch), **DEFERRED**,
**BY DESIGN** (intended divergence), **N/A** (feature absent, tracked separately).

---

## A. Same opcode, different observable behavior — addressed on this branch

| # | Opcode | Divergence (before) | Verdict |
|---|--------|--------------------|---------|
| 1 | `TryRemove` (505) | RK returned `ZBADVERSION`/`ZNOTEMPTY` on version-mismatch/non-empty; CH is best-effort and returns `ZOK` without deleting (`KeeperStorageImpl.cpp:562-563,587-588`) | **FIXED** — `KeeperStore.cpp StoreRequestRemove::process`; version-mismatch & non-empty now `ZOK` (no mutation) when `try_remove` |
| 2 | `CheckStat` (504) | RK wire carried only `version+cversion+aversion` and compared 3 raw fields; CH sends `path+version+Stat` and checks all 11 via `checkNodeStat` (`KeeperStorageImpl.cpp:1162-1188`) — wire-incompatible | **FIXED** — `CheckStatRequest` now carries a full `Stat` (`IKeeper.h`, `ZooKeeperCommon.cpp`); server compares all 11 fields against `statForResponse()` (`-1` = wildcard). RK's own gtests were the only prior consumers |
| 3 | `FilteredList` / `FilteredListWithStatsAndData` (500/506) | RK exposed child stat/data/ephemeral-flag with no per-child ACL; CH requires Read on each child and fails the whole request with `ZNOAUTH` (`KeeperStorageImpl.cpp:1102-1107`) | **FIXED** — `StoreRequestList::process`; per-child Read ACL pre-pass when filtering or `with_stat`/`with_data`. Plain `ALL` list still skips child ACLs (existence isn't secret) |
| 4 | `RemoveRecursive` (503) | RK allowed `RemoveRecursive("/")` and checked only the root's parent ACL; CH rejects `/` (`:666-669`) and checks Delete on every node (`:688`) | **FIXED** — `StoreRequestRemoveRecursive::process`; rejects `/` with `ZBADARGUMENTS` and checks Delete ACL on every subtree node before mutating |
| 5 | `ListRecursive` (507) | RK returned all descendants ignoring child ACLs; CH skips ACL-forbidden children (`:771-777`) | **FIXED** — per-child Read ACL skip in traversal. `names` vs `children` C++ field name is NOT a wire issue (positional serialization is identical). DFS + `max_entries==0`=unlimited kept (see Deferred) |
| 6 | `GetACL` / `SetACL` stat | Returned raw `node->stat`; every other read path returns `node->statForResponse()` → same node reported different cversion/numChildren | **FIXED** — both now use `statForResponse()` (internal consistency, both modes) |
| 7 | Reported `cversion` (Get/Exists/List) | RK reports `cversion*2 - numChildren` (ZK emulation); CH reports raw stored cversion | **GATED** — `statForResponse()`: ZK mode keeps the transform, ClickHouse mode reports raw |
| 8 | Parent `cversion` on `Set` | Apache ZK does not bump parent cversion on child Set; CH does (`KeeperStorageImpl.cpp:882-889`) | **GATED** — `StoreRequestSet::process` bumps parent cversion only in ClickHouse mode (with undo) |
| 9 | Parent `cversion` on `Remove`/`RemoveRecursive` | Same ZK-vs-CH split as #8 | **GATED** — surviving parent cversion bumped only in ClickHouse mode (with undo). `Create` already bumps in both modes (correct for both) |

Net for ClickHouse mode: internal cversion = creates + removes + child-Sets, reported raw → matches
ClickHouse Keeper. ZK mode is unchanged.

---

## B. Deferred (documented, not implemented)

- **`remove_nodes_limit` / `children_nodes_limit` "unlimited" sentinel differs (RemoveRecursive 503,
  ListRecursive 507).** RaftKeeper treats limit `0` as *unlimited*; ClickHouse Keeper treats `0` as a
  literal limit of zero (`nodes_visited + queue.size() > limit` / `children.size()-1 >= limit`), so with
  `0` it removes/returns nothing. ClickHouse's own client always sends a large sentinel (`uint32_max`) for
  unlimited, never 0. Consequence: cross-client calls and back-to-back tests must pass an explicit
  positive limit to get matching behavior (the `test_clickhouse_keeper_back_to_back` suite does this).
  Aligning RaftKeeper to the literal-zero semantics would break its own `remove_recursive`/`list_recursive`
  clients and gtests that rely on `0 = unlimited`; needs a product decision, so not changed here.
- **Exact ClickHouse sequential-number counter.** RK derives the sequential suffix from the parent's
  internal cversion (`KeeperStore.cpp` `StoreRequestCreate`). In ClickHouse mode cversion now also counts
  removes/Sets, so suffixes stay monotonic (no collisions) but won't equal ClickHouse's dedicated
  per-node counter. Exact parity needs a new per-node field → snapshot-format change. `ponytail:` note in code.
- **`__keeper_system/` write guard.** CH rejects writes to internal system paths. RaftKeeper has no
  equivalent internal paths (no TTL/container GC, no client reconfig), so the guard would protect nothing.

---

## C. Missing opcodes (feature gaps — separate work, not behavioral fixes)

RaftKeeper does not implement these; requests now yield a clean error (post `9b663e2397`) instead of
crash/hang. Tracked here for completeness; **N/A** for this branch.

| Opcode | ClickHouse | RaftKeeper |
|--------|-----------|-----------|
| `Create2` (15) | full (stat in response) | not registered |
| `CreateContainer` (19) | container nodes + GC | `ZBADARGUMENTS` at deserialize |
| `CreateTTL` (21) | TTL nodes + GC | `ZUNIMPLEMENTED` at deserialize |
| `Reconfig` (16) | client-driven dynamic reconfig | not defined; RK reconfigures via NuRaft `add_srv`/`remove_srv` only |
| `AddWatch` (106) | persistent / persistent-recursive watches | absent — RK watches are one-shot only |
| `SetWatch2` (105) | restores persistent watches on reconnect | absent |
| `CheckWatch` (17) / `RemoveWatch` (18) | `ZOK`/`ZNOWATCHER` | absent |

---

## D. Session / config differences (separate work — not behavioral opcode fixes)

- **Session-timeout bounds (compiled defaults).** RK min/max = 1s/3600s; CH = 10s/100s (default 30s both).
  Client-visible for non-default requested timeouts; overridable by config.
- **Session reconnect.** RK restores an existing session via internal `UpdateSession`(998)/`NewSession`(-10);
  CH ignores `previous_session_id` and always allocates a new session. RK's `UpdateSession` silently
  ignores a renegotiated timeout.
- **`ZNOTREADONLY` (-119)** exists in CH's error enum, absent in RaftKeeper.

---

## E. Confirmed consistent (no change needed)

Multi atomicity & per-subrequest error propagation (`ZOK`/actual-error/`ZRUNTIMEINCONSISTENCY`), shared
zxid across a Multi's sub-ops, intermediate-state visibility within a Multi, `Sync`, `Heartbeat`, `Close`
(ephemeral + watch cleanup), `Auth` (digest-only, super-user bypass), `Exists` setting a watch on a
non-existent node, `Check`/`CheckNotExists` version semantics. RaftKeeper's `SetWatches` list-watch restore
fires `CHILD` (closer to ZooKeeper than CH's `CHANGED`) — left as-is.
