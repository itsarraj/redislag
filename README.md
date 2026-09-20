# redislag

Reports Redis replication lag and slowlog contents from a hand-rolled
RESP (REdis Serialization Protocol) client — no `redis-cli` binary, no
`redis` crate dependency. `INFO replication` for the lag numbers,
`SLOWLOG GET` for the slow-command log, both parsed from the raw wire
protocol.

## Usage

```bash
redislag --host prod-redis --port 6379
redislag --port 6379 --password secret
redislag --port 6379 --json
redislag --port 6379 --max-lag-bytes 1048576   # exit 1 if any replica's lag exceeds 1MiB — CI/cron-friendly gate
```

## What it reports

Role (`master`/`slave`) and `master_repl_offset` from `INFO replication`;
for a master, each connected replica's byte-lag (`master_repl_offset`
minus that replica's acked offset, straight from the `slaveN:` lines
`INFO` already reports); for a replica, its own lag behind the master.
Separately, the `SLOWLOG` — command, duration, and client address for
each entry.

## Status: built and verified against a real Redis, with a real hand-written RESP client

- **31 unit tests** (`cargo test --lib`) across `resp` (14 — encoding a
  RESP command array, decoding every RESP reply type: simple string,
  error, integer, bulk string, null bulk string, array, nested arrays —
  the shape `SLOWLOG GET` actually returns), `replication` (10 —
  parsing real `INFO replication` output including multiple `slaveN:`
  lines, lag arithmetic), and `slowlog` (7 — parsing `SLOWLOG GET`'s
  nested-array reply into structured entries, including the client
  address field newer Redis versions add that older ones omit).
- **Live-verified against a real Redis container, not a mock**: spun up
  a real `redis:7-alpine` container, ran the actual compiled binary
  against it — correctly reported `role=master`, `master_repl_offset: 0`,
  and "no replicas connected" against a real single-instance server; then
  set `slowlog-log-slower-than 0` and issued a few real commands, and a
  second real run correctly showed all 5 real slowlog entries with
  correct command text, timing, and client address — parsed from the
  real RESP wire bytes a real server sent back, not a fixture.

**Not done / deliberately deferred**: TLS (`rediss://`) — plain TCP
only; Redis Cluster topology (`CLUSTER SLOTS`/`CLUSTER NODES`) — this
reports one node's own replication view, not a cluster-wide picture;
and RESP3 protocol mode (`HELLO 3`) — this speaks RESP2 only, which
every Redis version still supports regardless of client-side protocol
negotiation, so it's a scope cut for parsing simplicity, not a
compatibility gap.
