<!--
  Project:      dfe-loader
  File:         docs/feature-flags.md
  Purpose:      Cargo feature flags and the ClickHouse client crates
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Feature flags

dfe-loader is a binary, not a library, so it carries far fewer features than
scalo. The defaults are empty -- everything the running loader needs is
always compiled; the flags exist for the allocator, test scaffolding, and the
PGO build.

## Loader features

| Feature | Default | Pulls in | Purpose |
|---------|---------|----------|---------|
| `jemalloc` | off | `tikv-jemallocator`, `tikv-jemalloc-ctl` | jemalloc allocator (DFE policy: jemalloc at every channel, no mimalloc) |
| `transport-memory` | off | `scalo/transport-memory` | in-process Memory transport for unit tests |
| `testcontainers` | off | -- | enable Docker-backed integration tests (`TEST_MODE=docker`) |
| `pgo-driver` | off | `rdkafka` | build the PGO workload producer binary; the main loader binary is unaffected |

`default = []`. Production builds opt in to `jemalloc` explicitly; CI opts in to
`testcontainers` for the docker test lane. See
[deployment/](deployment/README.md) for the release build.

## The ClickHouse client is not a feature

The loader does not feature-gate its ClickHouse client. It always builds the
official `clickhouse` crate from crates.io, with the `lz4`, `time`, `inserter`
and rustls TLS features, plus `dfe-clickhouse` with only its `ext` feature for
the server-exception parser. Neither is patched or taken from git. See
[clickhouse/clickhouse-ext.md](clickhouse/clickhouse-ext.md).

## scalo features

The bulk of the loader's capability surface comes from scalo (config
cascade, logging, metrics, health, shutdown, transports, memory guard, CLI,
top). Those are selected in the `scalo` dependency entry, pinned to a
crates.io release (never a path or patch override). For the scalo feature tree
see the scalo docs; for how the loader wires them at startup see
[architecture.md](architecture.md).
