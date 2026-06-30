<!--
  Project:      dfe-loader
  File:         docs/FEATURE-FLAGS.md
  Purpose:      Cargo feature flags and the fork patch mechanism
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
[deployment/](deployment/) for the release build.

## The ClickHouse fork is not a feature

The loader does not feature-gate its ClickHouse transport. It always builds
against the HyperI `clickhouse-rs` fork, swapped in by `[patch.crates-io]` in
`Cargo.toml`:

```toml
[patch.crates-io]
clickhouse = { git = "https://github.com/hyperi-io/clickhouse-rs.git", tag = "<consumer-pin-tag>" }
```

The pin is an immutable git tag, never a branch -- the `hyperi-port/*` chain is
force-pushed on every cascade, so a branch pin floats and old lockfiles rot. The
loader's `[dependencies]` entry for `clickhouse` is unchanged; the patch
re-points it. The fork's own features (`tcp`, `inserter`, the rustls TLS
features) are selected through that dependency. See
[clickhouse/CLICKHOUSE-EXT.md](clickhouse/CLICKHOUSE-EXT.md).

## scalo features

The bulk of the loader's capability surface comes from scalo (config
cascade, logging, metrics, health, shutdown, transports, memory guard, CLI,
top). Those are selected in the `scalo` dependency entry, pinned to a
crates.io release (never a path or patch override). For the scalo feature tree
see the scalo docs; for how the loader wires them at startup see
[ARCHITECTURE.md](ARCHITECTURE.md).
