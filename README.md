<!--
  Project:      dfe-loader
  File:         README.md
  Purpose:      Entry point: what the loader is, how to run it, where the docs are
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# dfe-loader

[![Build Status](https://github.com/hyperi-io/dfe-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/hyperi-io/dfe-loader/actions)
[![License](https://img.shields.io/badge/license-BUSL--1.1-blue)](LICENSE)

Loads Kafka and gRPC streams into ClickHouse, batching small writes into large inserts. In a DFE deployment it is the last hop of the core data path, taking what dfe-receiver sends over Kafka or direct gRPC and landing it in the tables dfe-schemas defines.

## What it gives you

- Routing to `db.table` from fields in each record, so one loader can feed many tables.
- Columns read from the live table's `system.columns`: matching fields become typed columns and the rest stays in `_json`, with no schema compiled in.
- RowBinary inserts by default, so ClickHouse does no JSON parsing on the way in.
- Per-table buffers flushed on row count, byte size or age, whichever comes first.
- At-least-once delivery by default: Kafka offsets commit once per flush, never past a row not yet in ClickHouse or the DLQ. `kafka.acknowledgements.enabled: false` commits at receipt instead, and a row only a DLQ could take is dropped and counted in `dfe_loader_rows_lost_total` when no DLQ is working.
- A batch with a bad row is split until only the bad rows are dead-lettered, and a schema drift error refreshes the cached schema and retries.
- Optional GeoIP, reputation and risk columns at load time.

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
| Parse | JSON only. SIMD parse, payload kept as `Arc<[u8]>`. A record that is not JSON is dead-lettered |
| Route | `db.table` from configured fields, resolved BEFORE flattening |
| Promote | Schema read from `system.columns`, matching fields become typed columns, the rest stays in `_json` |
| Enrich | Optional GeoIP, reputation and risk columns |
| Buffer | Per-table, flushed on row count, byte size or age |
| Insert | RowBinary by default, so ClickHouse skips JSON parsing. JSONEachRow is the fallback |
| Commit | Kafka offsets commit once per flush cycle, never past a row not yet in ClickHouse or the DLQ -- at-least-once |

JSON is the only payload format. MessagePack, supported in DFE/XDR 2.0 and 2.1, is deprecated in DFE 2.2 and no longer accepted: the JSON path (SIMD parsing with sonic-rs, zstd on the wire) is fast enough that MessagePack gave no CPU saving.

Failures degrade rather than stall: a batch with a bad row is binary-split so only the bad rows are dead-lettered, a batch whose insert fails is held and retried with backoff while the commit stays below it, and a schema drift error invalidates the cached schema and retries.

## Configuration essentials

One YAML file, named by `--config`. The chart mounts it at `/etc/dfe/loader.yaml`. `config.example.yaml` is the annotated reference, and every setting has a default, so an empty config boots.

Environment overrides use the `DFE_LOADER` prefix in two forms, and the difference matters: `DFE_LOADER_KAFKA_GROUP_ID` is the flat single-underscore set, a fixed contract with dfe-engine, while `DFE_LOADER__CLICKHOUSE__DATABASE` uses double underscores to nest into any config key.

| Setting | Default | Note |
|---------|---------|------|
| `clickhouse.hosts` | `localhost:8123` | HTTP port family |
| `clickhouse.protocol` | `http` | `native` (9000, 9440 with TLS) also works, for `rowbinary` inserts |
| `clickhouse.database` | `dfe` | The connection database, separate from `routing.default_db` |
| `clickhouse.insert_format` | `rowbinary` | `jsoneachrow` is the fallback |
| `clickhouse.tls.enabled` | unset | The ONLY honoured TLS key. Mount a private CA into the container trust store rather than naming cert files |
| `kafka.group` | `dfe-loader` | |
| `routing.default_db` / `default_table` | `dfe` / `main` | Where a record lands when no routing field matches |
| `buffer.flush_rows` / `flush_bytes` / `flush_age_secs` | `20000` / `67108864` (64 MiB) / `5` | Per-table flush triggers, each greater than 0 |
| `metrics.address` | `0.0.0.0:9090` | |

Full cascade and per-setting behaviour: [docs/configuration.md](docs/configuration.md).

## Documentation

[docs/README.md](docs/README.md) is the index.

| Topic | Doc |
|-------|-----|
| Layers and the dynamic insert | [docs/architecture.md](docs/architecture.md) |
| Config cascade and behaviour | [docs/configuration.md](docs/configuration.md) |
| Running, metrics, hot-reload, DLQ | [docs/operations.md](docs/operations.md) |
| Insert formats, schema cache, types, TLS, DDL directives | [docs/clickhouse/](docs/clickhouse/) |
| Routing, coercion, enrichment, capture modes | [docs/pipeline/](docs/pipeline/) |
| Kafka, gRPC, the common header | [docs/transport/](docs/transport/) |
| Container and chart publishing | [docs/deployment/](docs/deployment/) |
| Test layout and what needs infra | [tests/TESTING.md](tests/TESTING.md) |

[CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [COMMERCIAL.md](COMMERCIAL.md).

The suite's published docs are at <https://dfe.docs.hyperi.io>, and this repository's pages at <https://dfe.docs.hyperi.io/dfe-loader/>.

## Help and contributing

How to work in this repo, the commands that prove a change and what tends to bite are in [CONTRIBUTING.md](CONTRIBUTING.md). A question that is not a bug goes in a new issue.

Open bugs: [this repo](https://github.com/hyperi-io/dfe-loader/issues?q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor) | [whole DFE suite](https://github.com/search?type=issues&q=is%3Aopen+is%3Aissue+type%3ABug+-label%3Abacklog+-label%3Anext-minor+repo%3Ahyperi-io%2Fdfe-infra+repo%3Ahyperi-io%2Fdfe-docker+repo%3Ahyperi-io%2Fdfe-deploy+repo%3Ahyperi-io%2Fdfe-engine+repo%3Ahyperi-io%2Fdfe-loader+repo%3Ahyperi-io%2Fdfe-receiver+repo%3Ahyperi-io%2Fdfe-fetcher+repo%3Ahyperi-io%2Fdfe-archiver+repo%3Ahyperi-io%2Fdfe-transform-vrl+repo%3Ahyperi-io%2Fdfe-transform-vector+repo%3Ahyperi-io%2Fdfe-transform-elastic+repo%3Ahyperi-io%2Fdfe-ui+repo%3Ahyperi-io%2Fdfe-hyperdx+repo%3Ahyperi-io%2Fscalo-rs+repo%3Ahyperi-io%2Fscalo-py+repo%3Ahyperi-io%2Flogreducer+repo%3Ahyperi-io%2Fdfe-schemas+repo%3Ahyperi-io%2Fvector-vrl+repo%3Ahyperi-io%2Fculvert+repo%3Ahyperi-io%2Fdfe-clickhouse-rs+repo%3Ahyperi-io%2Ffactbook-rs)

## License

BUSL-1.1, licensor HYPERI PTY LIMITED. Each version converts to Apache 2.0 on the third anniversary of its first public distribution. See [LICENSE](LICENSE) and [COMMERCIAL.md](COMMERCIAL.md).
