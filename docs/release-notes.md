# Release notes

## Unreleased

### CheckStat client compatibility

`CheckStat` (opcode 504) now uses the ClickHouse Keeper request layout:
`path + version (int32) + Stat` (all 11 stat fields). This replaces RaftKeeper's
previous proprietary layout, `path + version (int32) + cversion (int32) + aversion (int32)`.
The change applies to both ZooKeeper- and ClickHouse-compatible builds.

This is a breaking wire-format change for clients using the old RaftKeeper-specific
opcode 504. There is no version negotiation or legacy-format fallback. Update those
clients to send the full `Stat`, using `-1` for fields that should not be checked,
and coordinate the client/server upgrade. Avoid sending CheckStat during a rolling
upgrade while servers with different opcode 504 layouts coexist. Standard ZooKeeper
operations are unaffected; the new layout matches ClickHouse Keeper clients.
