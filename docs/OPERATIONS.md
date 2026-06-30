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
  names), plus rdkafka consumer stats and config-reload counters.
- **`top`** -- `dfe-loader top` renders those metrics as a TUI. `--once`,
  `--json`, and `--filter` make it scriptable; the JSON/TSV output pipes into
  shell tooling.
- **Health** -- the probe trinity (`/healthz`, `/readyz`, `/startupz`) from the
  scalo health pillar, for Kubernetes liveness/readiness/startup.

## Hot-reload

Config changes are picked up live. Fields that only affect the running pipeline
(buffer thresholds, capture modes, enrichment toggles, routing rules) apply
immediately. Fields baked into a connection or client at build time
(`clickhouse.transport`, ClickHouse URL/hosts, `insert_format`, TLS settings,
Kafka connection) are restart-required: the reloader logs a warning naming the
field and keeps the running value rather than applying a half-change. See
[CONFIGURATION.md](CONFIGURATION.md#hot-reload).

## When inserts fail

The insert tail is built to degrade, not stall:

- **Batch salvage** -- a data error binary-splits the batch to isolate the bad
  row(s); only those go to the DLQ, the good rows still land.
- **Circuit breaker** -- a repeatedly failing table trips open and its inserts
  go straight to the DLQ until a probe succeeds, so one bad table does not back
  up the others.
- **Schema-cache recovery** -- a drift error (a column added by an out-of-band
  `ALTER`) invalidates the cached schema and the retry re-fetches and
  re-encodes. See [clickhouse/SCHEMA-CACHE.md](clickhouse/SCHEMA-CACHE.md).
- **Per-table offset commit** -- offsets commit per table after its insert
  succeeds (at-least-once); one table's failure does not block another's.

## DLQ

Dead-lettered messages go to the DLQ (Kafka primary, file fallback). Routing
misses, unparseable payloads, and salvaged bad rows all land there with enough
context to replay or inspect. DLQ and consumer errors are debounced in the logs
so a burst does not flood.

## Memory pressure

The loader runs under a cgroup-aware memory guard: it checks pressure before
each Kafka poll and pauses the consumer when memory is tight, rather than
risking an OOM kill mid-batch.

## Source of truth

| Data | Source |
|------|--------|
| Version | `git describe --tags` / `version` subcommand |
| Tasks | GitHub Issues |
| History | `git log` |
| Config schema | [CONFIGURATION.md](CONFIGURATION.md) |
