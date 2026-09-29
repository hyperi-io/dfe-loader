<!--
  Project:      dfe-loader
  File:         docs/OPERATIONS.md
  Purpose:      Running, observing, and operating the loader
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Operations

How to run dfe-loader, check a config before it boots, watch it, and reason
about what it does when something goes wrong.

## Subcommands

The loader is a standard HyperI CLI app (the `cli` scaffolding in
scalo). The default subcommand is `run`.

| Subcommand | Purpose |
|------------|---------|
| `run` (default) | start the loader: wire transports, pipeline, ClickHouse, and serve the admin endpoints |
| `config-check` | resolve the full config cascade and validate it, then exit -- no connections opened |
| `version` | print version and build metadata |
| `top` | live TUI dashboard polling the running loader's `/metrics` |
| `metrics-manifest` | print the metric catalogue as JSON without starting the loader |

Environment variables use generic names (`LOG_LEVEL`, not a loader-specific
prefix); the config cascade derives the prefixed forms. See
[CONFIGURATION.md](CONFIGURATION.md).

## Check before you boot

`config-check` runs the same validation the orchestrator runs at startup, so a
bad config fails fast and visibly rather than at first insert. The guards
include the transport/format matrix:

- `insert_format = json_each_row` with `transport = native` is rejected --
  JSONEachRow needs HTTP. Use `transport = http`, or `insert_format =
  row_binary` (which works on both).
- Port/transport mismatches (native on 8123, HTTP on 9000/9440) are flagged.

See [clickhouse/INSERT-FORMATS.md](clickhouse/INSERT-FORMATS.md).

## Observe

- **Metrics** -- Prometheus text on `/metrics`. The loader emits the DFE metric
  groups (dual-emit: legacy `loader_*` names alongside the standard group
  names), plus rdkafka consumer stats and config-reload counters. On the direct
  gRPC transport there are no broker offsets, so `kafka_offsets_committed_total`
  and the consumer offset counters stay flat while the transport counters move.
- **Catalogue** -- `dfe-loader metrics-manifest` prints every metric the loader
  registers, so a dashboard or scaling rule can be written against the
  catalogue rather than a running pod.
- **Counted in records** -- a message carrying several records is split into one
  record per element before anything counts it, so `messages_received_total` and
  `events_per_second` read in records, matching `rows_inserted_total`. The two
  wire shapes are counted apart, so the pair that moves says which producer is
  batching: `dfe_loader_batched_array_messages_total` /
  `..._batched_array_records_total` for a JSON array, and
  `dfe_loader_batched_ndjson_messages_total` / `..._batched_ndjson_records_total`
  for newline-separated records.
- **`top`** -- `dfe-loader top` renders those metrics as a TUI. `--once`,
  `--json`, and `--filter` make it scriptable; the JSON/TSV output pipes into
  shell tooling.
- **Health** -- the probes (`/livez`, `/readyz`) from the
  scalo health pillar, for Kubernetes liveness/readiness/startup.

## Hot-reload

Config changes are picked up live. Fields that only affect the running pipeline
(buffer thresholds, capture modes, enrichment toggles, routing rules) apply
immediately. Fields baked into a connection or client at build time
(`clickhouse.protocol`, ClickHouse URL/hosts, `insert_format`, TLS settings,
Kafka connection) are restart-required: the reloader logs a warning naming the
field and keeps the running value rather than applying a half-change. See
[CONFIGURATION.md](CONFIGURATION.md#hot-reload).

## When inserts fail

The insert tail is built to degrade, not stall:

- **Batch salvage** -- a data error binary-splits the batch to isolate the bad
  row(s); only those go to the DLQ, the good rows still land.
- **Sink-down gate** -- a flush cycle in which every insert failed opens the
  circuit gate on the scaling signal (`dfe_loader_scaling_circuit_open`), which
  zeroes the pressure KEDA reads: more pods cannot relieve a dead ClickHouse.
  The next successful insert closes it. A failed insert never sends its batch to
  the DLQ. The batch is held and retried (below).
- **Schema-cache recovery** -- a drift error (a column added by an out-of-band
  `ALTER`) invalidates the cached schema and the retry re-fetches and
  re-encodes. Any server code outside the retry list (a busy server, the network, Keeper, the disk or object store under the table, or an operator fix such as a missing table or grant) is a refusal of the rows: against a table that has not changed it is the row's own data, never drift, so salvage dead-letters that row and the rest of the batch lands. See [clickhouse/SCHEMA-CACHE.md](clickhouse/SCHEMA-CACHE.md).
- **One commit per flush cycle** -- Kafka offsets commit once per cycle, after the inserts and the DLQ hand-over. On each partition the commit stops below the lowest offset not placed yet: a buffered row, a row waiting on its schema, a failed batch, or a dead letter the DLQ refused. The loader holds a failed batch or refused dead letter and retries it with jittered backoff until it lands. A dead letter no DLQ backend can ever hold -- one over a Kafka-only DLQ's `message.max.bytes` once its payload is base64'd -- is dropped and counted in `pipeline_dead_letters_dropped_total{reason="too_large"}` instead, so it never holds a commit for good. A held batch whose table ClickHouse has since reported absent is retried against the default table, where new records for that table already go. The loader only learns a table was dropped when it next re-reads that table's schema, so until then the affected partition's commit stays below the held batch -- up to about `schema.cache_ttl_secs` (default 300 s) plus the 60 s schema refresh interval. Rows above the floor re-deliver as duplicates after a restart (at-least-once).

## Rebalances

A roll starts the new pod while the old one still buffers rows, and the rebalance hands some of the old pod's partitions to the new one, which reads them again from the committed offset. Before every write the loader checks the lease each partition's rows were read under. A partition whose lease has ended has its rows discarded -- from the buffers, the pending-schema buffer, the holds and the dead letters -- so only the new owner writes them. Each discarded record counts in `transport_revoke_discarded_total{stage="buffer"}`.

With `kafka.acknowledgements.enabled: false` offsets commit at receipt, so the buffered rows are their only copy and nothing is discarded.

## DLQ

Dead-lettered messages go to the DLQ (Kafka primary, file fallback). Routing
misses, unparseable payloads, and salvaged bad rows all land there with enough
context to replay or inspect. DLQ and consumer errors are debounced in the logs
so a burst does not flood.

A JSON record nested deeper than 64 levels is dead-lettered as it arrives, before any stage parses it, with the reason `payload nesting exceeds the maximum parse depth of 64`, and counted on `validation_failures_total{reason="out_of_range"}`. A batch too deep to split goes to the DLQ whole. The bound is the one scalo's parse path uses.

## Memory pressure

The loader runs under a cgroup-aware memory guard: it checks pressure before
each Kafka poll and pauses the consumer when memory is tight, rather than
risking an OOM kill mid-batch.

The figure it checks is what the kernel charges the loader. On scalo 2.12.2 and later that is the cgroup's `memory.current` (v1 `memory.usage_in_bytes` on an older host), else `VmRSS` from `/proc/self/status`. The cgroup figure is the one the OOM killer compares against `memory.max`, and it counts what jemalloc never sees: librdkafka's allocations, thread stacks and pages jemalloc keeps after a free. Only where neither can be read, such as a macOS dev build, does a `jemalloc` build register jemalloc's live heap instead, so the guard is not left counting only the batch engine's reservations. The `usage_source` field on the `memory guard initialised` log line names the source in force: `cgroup-v2`, `cgroup-v1` or `proc-status` on Linux, `explicit` only in that fallback.

`memory.current` also counts reclaimable page cache, such as the mmap'd MaxMind database or a file DLQ, so the brake can engage a little early. That is the safe direction.

## Source of truth

| Data | Source |
|------|--------|
| Version | `git describe --tags` / `version` subcommand |
| Tasks | GitHub Issues |
| History | `git log` |
| Config schema | [CONFIGURATION.md](CONFIGURATION.md) |
