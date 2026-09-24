<!--
  Project:      dfe-loader
  File:         README.md
  Purpose:      Entry point: what the loader is, how to run it, where the docs are
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# dfe-loader

Loads Kafka and gRPC streams into ClickHouse, batching small writes into large inserts. Runs standalone or as part of the HyperI Data Fusion Engine.

## Quick start

The first success needs no broker and no datastore:

```bash
cargo build
./target/debug/dfe-loader --config config.example.yaml config-check
```

That prints `[ok] configuration is valid` and the resolved config, and opens no connections. `--config` is a global option, so it goes BEFORE the subcommand -- `config-check --config ...` is rejected by the argument parser.

To move real data, bring up a local broker and datastore:

```bash
docker compose -f docker-compose.dev.yaml up -d
cargo run -- --config config.dev.yaml
```

Metrics land on `http://localhost:9090/metrics`, with `/livez` and `/readyz` alongside. `docker compose -f docker-compose.dev.yaml down -v` takes it down.

## What it does

dfe-loader reads records off Kafka or a direct gRPC listener, works out which ClickHouse table each one belongs to, and inserts batches. The per-message work is kept cheap and the expensive work deferred to the flush:

| Stage | What happens |
|-------|--------------|
| Parse | JSON or MessagePack, auto-detected. SIMD parse, payload kept as `Arc<[u8]>` |
| Route | `db.table` from configured fields, resolved BEFORE flattening |
| Promote | Schema read from `system.columns`, matching fields become typed columns, the rest stays in `_json` |
| Enrich | Optional GeoIP, reputation and risk columns |
| Buffer | Per-table, flushed on row count, byte size or age |
| Insert | RowBinary by default, so ClickHouse skips JSON parsing. JSONEachRow is the fallback |
| Commit | Kafka offsets commit once per flush cycle, never past a row not yet in ClickHouse or the DLQ -- at-least-once |

Failures degrade rather than stall: a batch with a bad row is binary-split so only the bad rows are dead-lettered, a batch whose insert fails is held and retried with backoff while the commit stays below it, and a schema drift error invalidates the cached schema and retries.

## Configuration essentials

One YAML file, named by `--config`. The chart mounts it at `/etc/dfe/loader.yaml`. `config.example.yaml` is the annotated reference, and every setting has a default, so an empty config boots.

Environment overrides use the `DFE_LOADER` prefix in two forms, and the difference matters: `DFE_LOADER_KAFKA_GROUP_ID` is the flat single-underscore set, a fixed contract with dfe-engine, while `DFE_LOADER__CLICKHOUSE__DATABASE` uses double underscores to nest into any config key.

| Setting | Default | Note |
|---------|---------|------|
| `clickhouse.hosts` | `localhost:8123` | HTTP port family |
| `clickhouse.protocol` | `http` | The only accepted value. `native` is refused at startup |
| `clickhouse.database` | `dfe` | The connection database, separate from `routing.default_db` |
| `clickhouse.insert_format` | `rowbinary` | `jsoneachrow` is the fallback |
| `clickhouse.tls.enabled` | unset | The ONLY honoured TLS key. Mount a private CA into the container trust store rather than naming cert files |
| `kafka.group` | `dfe-loader` | |
| `routing.default_db` / `default_table` | `dfe` / `main` | Where a record lands when no routing field matches |
| `buffer.flush_rows` / `flush_bytes` / `flush_age_secs` | `10000` / `1048576` / `5` | Per-table flush triggers |
| `metrics.address` | `0.0.0.0:9090` | |

Full cascade and per-setting behaviour: [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Documentation

[docs/README.md](docs/README.md) is the index.

| Topic | Doc |
|-------|-----|
| Layers and the `clickhouse_ext` boundary | [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) |
| Config cascade and behaviour | [docs/CONFIGURATION.md](docs/CONFIGURATION.md) |
| Running, metrics, hot-reload, DLQ | [docs/OPERATIONS.md](docs/OPERATIONS.md) |
| Insert formats, schema cache, types, TLS, DDL directives | [docs/clickhouse/](docs/clickhouse/) |
| Routing, coercion, enrichment, capture modes | [docs/pipeline/](docs/pipeline/) |
| Kafka, gRPC, the common header | [docs/transport/](docs/transport/) |
| Container and chart publishing | [docs/deployment/](docs/deployment/) |
| Test layout and what needs infra | [tests/TESTING.md](tests/TESTING.md) |

[CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [COMMERCIAL.md](COMMERCIAL.md).

## License

BUSL-1.1, licensor HYPERI PTY LIMITED. Each version converts to Apache 2.0 on the third anniversary of its first public distribution. See [LICENSE](LICENSE) and [COMMERCIAL.md](COMMERCIAL.md).

## Context

### What this is

dfe-loader writes rows into ClickHouse -- the last hop of the DFE core data path and the only component that writes there.

NOT a transform stage, NOT a schema manager. Its only outbound topic is the DLQ. dfe-engine creates the destination tables, and the loader holds records pending schema rather than issuing DDL. The dynamic insert layer is in `src/clickhouse_ext/`, not the patched `clickhouse-rs` fork, which keeps insertion compile-time typed.

### Where things live

| Path | Holds |
|------|-------|
| `src/main.rs`, `src/config/` | CLI entry, config structs, `Config::validate()`, flat env |
| `src/pipeline/` | Orchestrator and per-message processor -- the hot path |
| `src/routing/`, `src/transform/`, `src/enrich/` | Routing, coercion and capture, enrichment |
| `src/buffer/`, `src/clickhouse/` | Per-table buffers; query client, schema cache, inserter |
| `src/clickhouse_ext/` | Dynamic insert: runtime type parser, RowBinary encoder |
| `chart/` | Helm chart, generated from the deployment contract and committed |
| `tests/` | `smoke.rs`, `integration/`, `e2e/` -- see `tests/TESTING.md` |

### Commands that prove a change

```bash
make check                                  # hyperi-ci check -- the full local gate
cargo nextest run --lib                     # unit, no infra
cargo nextest run --test smoke              # startup smoke, no infra
cargo nextest run --test integration_tests  # integration, some cases need ClickHouse
cargo nextest run -- --ignored              # e2e, needs Kafka AND ClickHouse
```

Green lies three ways: `skip_if_no_clickhouse!()` and `skip_if_no_kafka!()` return early and PASS when the datastore or broker is absent, the e2e suite is `#[ignore]` so a plain run never touches the real insert path, and `default = []` compiles a feature-gated test out while CI still reports green. `ci.yml` sets `paths-ignore` for `docs/**` and `**.md`, so a docs-only push runs no jobs.

### What tends to bite

| Don't | Do | Why |
|-------|----|-----|
| Bump scalo and commit without regenerating the chart | `dfe-loader --emit-helm chart` | `committed_chart_matches_the_generator` (`tests/integration/deployment.rs:250`) compares `chart/` to the generator file by file. A failure is the guard working -- dfe-fetcher shipped a chart missing `keda-triggerauth.yaml` that its ScaledObject referenced, and never scaled (dfe-fetcher#71) |
| Gate a test behind a cargo feature without adding it to `.hyperi-ci.yaml` | Add `default,<feature>` to the feature-set list | `default = []` (`Cargo.toml:327`), so it compiles out of every CI run and CI still reports green. `helm_contract.rs:106` asserts `transport-memory` and `testcontainers` stay listed |
| Inject a nested config key as `DFE_LOADER_SECTION_FIELD` | `DFE_LOADER__SECTION__FIELD` | figment strips exactly `DFE_LOADER_`, so it arrived as `_kafka.sasl.username`, matched no field and was dropped silently. Pods ran with no SASL and an empty ClickHouse password (`tests/integration/config_reachability.rs`) |
| Set `clickhouse.protocol: native` | `http`, on an 8123-family port | The pinned fork has no TCP row fetch, so schema queries stall silently and messages back up pending schema (#115). `validate()` rejects it by name |
| Name `clickhouse.tls.ca_cert_file`, `cert_file`, `key_file` or `skip_verify` | Set `tls.enabled`, mount the CA into the trust store | Only `enabled` reaches a client. The rest parsed and did nothing, so `validate()` now fails naming them |
| Widen the `cel` range past scalo's | Keep it on `>=0.13, <0.14` | `cel::Program` crosses the scalo boundary. Wider resolves two semver-incompatible `cel` crates and `Program` stops being the same type (`Cargo.toml:41-45`) |
| Pin the `clickhouse` fork by branch | Pin by `rev` or tag | The `hyperi-port/*` chain is force-pushed, so a branch pin rots with no warning (`Cargo.toml:313-321`) |
| Mechanically sync dfe-engine's loader validation to `Config::validate()` | Read both, keep the divergence | dfe-engine scopes the broker check to the Kafka transport and adds a `grpc.listen` check this side lacks. A blind sync rejects valid gRPC-only configs at author time |

### Where this sits

Inbound, declared in `dfe-infra/suite.yaml`:

- **scalo-rs -> dfe-loader** (`cargo-dep`) -- `Cargo.toml:35` takes `scalo` by range for transports, config cascade, CLI, metrics, deployment contract and DLQ. A scalo release reaches this repo here.
- **scalo-rs -> dfe-loader** (`generated-file`, lockstep) -- `Dockerfile` is emitted by `scalo::deployment::generate_dockerfile()` at schema version 3. Regenerate and commit the diff.

Outbound, so what a change here can break:

- **dfe-loader -> dfe-infra** (`image-pin`, lockstep) -- `dfe-infra/helm/charts/dfe-loader/Chart.yaml:6` pins this image as tag plus digest.
- **dfe-loader -> dfe-engine** (`mirrored-logic`) -- dfe-engine hand-reimplements this repo's config validation in `plugins_builtin/loader.py` to catch errors when it authors a config. Nothing is copied, so no script compares them.

Runtime, not build edges: dfe-engine's `dfe-schema` entry point creates every destination table before the loader can insert, and producers reach the loader over Kafka (`*_land` / `*_load` topics) or direct gRPC (default topic `main_land`) -- in the suite that producer is dfe-receiver.
