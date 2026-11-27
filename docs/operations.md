<!--
  Project:      dfe-loader
  File:         docs/operations.md
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
| `config-schema` | write `config-schema.{json,yaml}` and `capability-catalog.{json,yaml}` (`--dir`, default `docs`) |
| `generate-artefacts` | write the metrics manifest, the deployment contract and the container spec (`--output-dir`, default `docs`) |

Loader settings take `DFE_LOADER_*` environment variables: flat
(`DFE_LOADER_CLICKHOUSE_HOSTS`) or nested on a double underscore
(`DFE_LOADER_CLICKHOUSE__PROTOCOL`). scalo's CLI flags alone have generic
env names (`LOG_LEVEL`, `METRICS_ADDR`). See
[configuration.md](configuration.md).

## Check before you boot

`config-check` loads the cascade and runs `Config::validate`, so an empty broker or host list, an unknown protocol, an unread TLS key, a zero flush trigger or a default table that is `dlq_load.table` fails before boot. Two checks run only when the pipeline starts, before the readiness probe, so `config-check` passes them:

- `clickhouse.insert_format: jsoneachrow` with `clickhouse.protocol: native`
  is refused -- JSONEachRow needs HTTP. Use `protocol: http`, or the
  `rowbinary` default (which works on both).
- HTTP on a native port (9000, 9440) is refused, and native on 8123 is logged as a warning.

See [clickhouse/insert-formats.md](clickhouse/insert-formats.md).

## Observe

- Prometheus text on `/metrics`: scalo's DFE metric groups, the older names such as `buffer_rows` and `batches_flushed_total` emitted beside them, consumer stats and config-reload counters. On the gRPC transport there are no broker offsets, so `kafka_offsets_committed_total` and the consumer offset counters stay flat as the transport counters move.
- `dfe-loader metrics-manifest` prints every metric the loader registers, so a dashboard or scaling rule can be written against it rather than a running pod. The per-table series appear on first use and are not in it: `dfe_loader_unknown_table_fallback_total`, `dfe_loader_permanent_reject_total`, `loader_messages_by_table_total`, `loader_json_max_paths_exceeded_total`, `loader_buffer_rows_by_table` and `loader_buffer_bytes_by_table`.
- A message carrying several records is split before anything counts it, so `messages_received_total` and `events_per_second` read in records, like `rows_inserted_total`. `dfe_loader_batched_array_*` counts JSON arrays and `dfe_loader_batched_ndjson_*` newline-separated records, so the pair that moves names the producer that batches.
- A failing [CEL](https://github.com/cel-expr/cel-spec/blob/master/doc/langdef.md) expression leaves its column unset and the record goes on, counted in `dfe_loader_computed_eval_failures_total{table, column, stage}`. See [clickhouse/ddl-directives.md](clickhouse/ddl-directives.md).
- A date-time value no column can read never fails the row: `_timestamp` and `_timestamp_received` take the ingest time, and any other date-time column leaves it out. Each is counted in `dfe_loader_timestamp_replaced_total{reason, column}`, `reason` being `unparseable`, or `out_of_bounds` for a `_timestamp` outside `timestamp_dq`'s bounds. See [transport/common-header.md](transport/common-header.md#what-the-loader-writes).
- `dfe-loader top` renders the metrics as a TUI. `--once` prints one scrape to stdout, `--json` prints it as JSON, and `--filter` takes a name regex.
- `/livez` and `/readyz` come from scalo's health pillar. `/readyz` stays 503 until ClickHouse accepts the configured user and answers reads of `system.tables` and `system.columns` for the configured database, and `pipeline_ready` reads 1 once it passes. Each refusal is logged with ClickHouse's code (516 for a wrong password) and retried on the `sink_stack` backoff, doubling from `sink_stack.min_backoff_ms` (100 ms) to `sink_stack.max_backoff_ms` (30 s), +/-20% jitter, never past the ceiling. Until it passes the loader joins no consumer group and opens no gRPC listener, so a pod with a wrong password takes no partitions from the pods that can write. Kafka and the gRPC peer play no part in readiness.
- A transport that cannot start, such as Kafka with no broker reachable, is retried on the same backoff, logged at `warn` and counted in `pipeline_retries_total{stage="transport_start"}`. A config the transport rejects exits, and one attempt can take the 10 s Kafka metadata timeout, so a shutdown may wait that long.

Hot-reload rules are in [configuration.md](configuration.md#hot-reload).

## When inserts fail

The insert tail degrades rather than stalls:

- A data error binary-splits the batch down to the bad rows, which go to the DLQ, and the rest land.
- A flush cycle in which every insert failed opens the circuit gate on the scaling signal (`scaling_circuit_open`), which zeroes the pressure KEDA reads, since more pods cannot relieve a dead ClickHouse. The next successful insert closes it. A failed insert never sends its batch to the DLQ.
- A column dropped or retyped by an out-of-band `ALTER` is drift: the cached schema is dropped and the retry re-encodes against a fresh read. A server code on the retry list (a busy server, the network, Keeper, the disk or object store under the table, or an operator fix such as a missing table or grant) holds the batch. Any other code against a table that has not changed is blamed on the row, so salvage dead-letters that row. See [clickhouse/schema-cache.md](clickhouse/schema-cache.md).
- Kafka offsets commit once per cycle, after the inserts and the DLQ hand-over. On each partition the commit stops below the lowest offset not placed yet: a buffered row, a row waiting on its schema, a failed batch, or a dead letter the DLQ refused. The loader holds a failed batch or refused dead letter and retries it with jittered backoff until it lands. A dead letter over a Kafka-only DLQ's `message.max.bytes` is dropped and counted in `pipeline_dead_letters_dropped_total{reason="too_large"}`, so it never holds a commit for good. Rows above the floor re-deliver as duplicates after a restart.
- A held batch, other than one bound for `dlq_load.table`, is retried against the default table once ClickHouse reports its table absent, and counted in `dfe_loader_unknown_table_fallback_total`. A routing reload that removes the batch's source makes the loader ask about its table at once. Any other dropped table is found gone at its next schema read, up to about `schema.cache_ttl_secs` plus the 60 s refresh interval, and the partition's commit stays below the held batch until then.

Readiness ignores an outage, since flipping it would pull every loader out of its Service at once. `clickhouse_connected` reports it: 1 once ClickHouse answered the startup probe or the latest insert, 0 once an insert went unanswered. On Kafka the held batches' reservations grow until the memory governor pauses the assigned partitions, and on gRPC a held Push is answered `UNAVAILABLE`.

## Rebalances

A roll starts the incoming pod as the old one still buffers rows, and the rebalance hands some of the old pod's partitions to the incoming one, which reads them again from the committed offset. Before every write the loader checks the lease each partition's rows were read under. A partition with an ended lease has its rows discarded -- from the buffers, the pending-schema buffer, the holds and the dead letters -- so the next owner alone writes them. Each discarded record counts in `transport_revoke_discarded_total{stage="buffer"}`.

With `kafka.acknowledgements.enabled: false` offsets commit at receipt, so the buffered rows are their only copy and nothing is discarded.

## DLQ

Dead-lettered messages go to the DLQ (Kafka first, file fallback, file only on gRPC). Payloads that are not JSON or fail processing, records with a schema that never arrived within `schema.pending_max_age_secs`, and salvaged bad rows land there with their reason. A routing miss does not: it lands in the default table. DLQ and consumer errors are debounced in the logs so a burst does not flood.

A JSON record nested deeper than 64 levels is dead-lettered as it arrives, before any stage parses it, with the reason `payload nesting exceeds the maximum parse depth of 64`, and counted on `validation_failures_total{reason="out_of_range"}`. A batch nested past that depth goes to the DLQ unsplit. The bound is the one scalo's parse path uses.

A record with `_org_id` or `_source` more than once in its top-level object, counting a spelling that decodes to the key, such as one with a unicode escape for the underscore, is dead-lettered the same way on every pipeline mode, with the reason `payload carries the _org_id header key more than once`, and counted on `dfe_loader_duplicate_header_total`. JSON readers disagree on which of the two values wins, so the loader routes neither. A record with no `\` byte and at most one quoted spelling of each key goes through without a scan.

## Memory pressure

The loader runs under a cgroup-aware memory guard: it checks pressure before
each Kafka poll and pauses the consumer when memory is tight, rather than
risking an OOM kill mid-batch.

The figure it checks is what the kernel charges the loader. That is the cgroup's `memory.current` (v1 `memory.usage_in_bytes` on an older host), else `VmRSS` from `/proc/self/status`. The cgroup figure is the one the OOM killer compares against `memory.max`, and it counts what jemalloc never sees: librdkafka's allocations, thread stacks and pages jemalloc keeps after a free. Only where neither can be read, such as a macOS dev build, does a `jemalloc` build register jemalloc's live heap instead, so the guard is not left counting only the batch engine's reservations. The `usage_source` field on the `memory guard initialised` log line names the source in force: `cgroup-v2`, `cgroup-v1` or `proc-status` on Linux, `explicit` only in that fallback.

`memory.current` counts reclaimable page cache as well, such as the mmap'd MaxMind database or a file DLQ, so the brake can engage a little early. That is the safe direction.

## Scaling

The contract's ScaledObject scales on CPU and has a disabled Kafka lag trigger, so raw consumer lag never scales the loader. In a DFE deployment, dfe-infra's values set the CPU target to 70% and add the scaling-pressure trigger (target 70, read through dfe-keda-shim). The cooldown is the contract's 300 s. The slim profile clears the pressure trigger, so slim scales on CPU alone. The engine's [KEDA scaling page](https://github.com/hyperi-io/dfe-engine/blob/main/docs/deployment/keda-scaling.md) covers the triggers for every app.
