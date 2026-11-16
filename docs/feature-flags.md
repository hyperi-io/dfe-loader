<!--
  Project:      dfe-loader
  File:         docs/feature-flags.md
  Purpose:      Cargo feature flags and the ClickHouse client crates
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Feature flags

dfe-loader ships as a binary, its library target serving that binary, the tests and the benches, so it has far fewer features than scalo. The defaults are empty: everything the running loader needs is always compiled, and the flags exist for the allocator, test scaffolding and the PGO build.

| Feature | Default | Pulls in | Purpose |
|---------|---------|----------|---------|
| `jemalloc` | off | `tikv-jemallocator`, `tikv-jemalloc-ctl` | jemalloc allocator (DFE policy: jemalloc at every channel, no mimalloc) |
| `transport-memory` | off | `scalo/transport-memory` | in-process Memory transport for unit tests |
| `testcontainers` | off | -- | enable the integration tests that run against ClickHouse and Kafka containers, see `tests/TESTING.md` |
| `pgo-driver` | off | `rdkafka` | build the PGO workload producer binary, leaving the main loader binary unaffected |

`default = []`. hyperi-ci adds `jemalloc` to every release-track build, and `.hyperi-ci.yaml` lists `testcontainers`, `jemalloc` and `transport-memory` among the feature sets its tests compile. See [performance/overview.md](performance/overview.md) for what each channel builds.

The loader does not feature-gate its ClickHouse client. It always builds the
official `clickhouse` crate from crates.io, with the `lz4`, `time`, `inserter`
and rustls TLS features, plus `dfe-clickhouse` with `ext` (the server-exception parser), `tls` (the native-protocol client) and `dynamic` (the dynamic RowBinary insert). Neither is patched or taken from git. See
[clickhouse/clickhouse-ext.md](clickhouse/clickhouse-ext.md).

The bulk of the loader's capability surface comes from scalo (config
cascade, logging, metrics, health, shutdown, transports, memory guard, CLI,
top). Those are selected in the `scalo` dependency entry, pinned to a
crates.io release (never a path or patch override). For the scalo feature tree
see the scalo docs, and for how the loader wires them at startup see
[architecture.md](architecture.md).
