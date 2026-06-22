<!--
  Project:      dfe-loader
  File:         docs/CONFIGURATION.md
  Purpose:      Config cascade and the settings that change behaviour
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Configuration

dfe-loader is configured through an 8-layer cascade (hyperi-rustlib). Higher
layers win. Every setting has a safe default, so an empty config boots.

```mermaid
flowchart TB
    CLI["1. CLI args (--clickhouse.transport=native)"]
    ENV["2. ENV (LOADER_CLICKHOUSE_TRANSPORT=native)"]
    DOTENV["3. .env (gitignored)"]
    ENVYAML["4. settings.{env}.yaml"]
    YAML["5. settings.yaml"]
    DEF["6. defaults.yaml"]
    LIB["7. rustlib built-ins"]
    HARD["8. hard-coded"]
    CLI --> ENV --> DOTENV --> ENVYAML --> YAML --> DEF --> LIB --> HARD
```

ENV names are auto-derived: `clickhouse.transport` -> `LOADER_CLICKHOUSE_TRANSPORT`.

> A config value is only useful if the ACTION matches it. The integration tests
> assert the observable outcome (the row written, the transport used), not just
> that the value parsed -- see [the test note](#verifying-config-drives-behaviour).

## Settings that change behaviour

### ClickHouse insert path

| Setting | Values | Effect |
|---------|--------|--------|
| `clickhouse.transport` | `native` (default), `http` | TCP native protocol (port 9000/9440) vs HTTP (8123/8543) |
| `clickhouse.insert_format` | `row_binary` (default), `json_each_row` | binary (server skips JSON parsing) vs self-describing JSON |
| `clickhouse.hosts` | list | multi-host failover (native pool round-robins, skips a refusing endpoint) |

The transport and format compose. The default `native + row_binary` is the
fast path; `json_each_row` is the diagnostic fallback:

| `insert_format` | `transport = http` | `transport = native` |
|-----------------|--------------------|----------------------|
| `row_binary` (default) | `insert_formatted_with` FORMAT RowBinary | `insert_native_with_columns` (`with_columns_tcp`) |
| `json_each_row` | `insert_formatted_with` FORMAT JSONEachRow | **rejected at config-check** |

**Guarded combination:** `insert_format = json_each_row` with `transport =
native` is rejected at `config-check`. JSONEachRow is sent over HTTP, and a
native client has no HTTP insert endpoint for it. RowBinary works on both
transports -- see [clickhouse/INSERT-FORMATS.md](clickhouse/INSERT-FORMATS.md).

### Transport security (TLS, private CAs)

One trust mechanism covers both transports. By default a TLS connection trusts
the OS native root store plus the compiled Mozilla (webpki) bundle. Internal
clusters behind a private CA are supported by pointing at the CA PEM -- no proxy
termination, no `InsecureSkipVerify`.

| Setting | Values | Effect |
|---------|--------|--------|
| `clickhouse.tls` | bool | TLS for the connection (HTTPS 8543 / secure native 9440) |
| `clickhouse.tls_ca_file` | path | private CA PEM (may bundle many certs; all are loaded). Augments native + webpki |
| `clickhouse.tls_ca_exclusive` | bool | trust ONLY `tls_ca_file`, ignore native + webpki |

A CA file with no usable certificate is a hard error (it never silently falls
back to public roots). See [clickhouse/TLS.md](clickhouse/TLS.md).

### Capture (what gets stored)

| Setting | Values | Effect |
|---------|--------|--------|
| `metadata.capture_mode` | `full` (default), `raw_only`, `extracted_only` | `_json` + `_raw` population -- see [ARCHITECTURE.md](ARCHITECTURE.md#capture-modes) |
| `metadata.table_capture_modes` | map | per-table override of `capture_mode` |
| DDL `@capture_mode: <mode>` | comment tag | highest-priority override, per table |

Precedence, highest wins: **DDL tag > per-table > global**.

### Routing

| Setting | Effect |
|---------|--------|
| `routing.db_fields` | fields whose value sets the database (empty = shared `default_db`) |
| `routing.table_fields` | fields whose value sets the table (dot-notation for nested) |
| `routing.default_db` / `default_table` | fallbacks when no field matches |
| `routing.route_all_by_org` / `routed_orgs` | per-org database routing |

### Enrichment, resilience

| Setting | Effect |
|---------|--------|
| `enrichment.geoip` / `reputation` / `risk` | inject enriched columns on the promoted row |
| `buffer.flush_rows` / `flush_bytes` / `flush_age_secs` | per-table flush triggers |
| salvage / circuit breaker / `max_concurrent_inserts` | the insert resilience layer |

## Hot-reload

Config hot-reloads on change. Some fields are safe to apply live; some require a
restart and are logged as a warning rather than silently applied:

- **Restart-required:** `clickhouse.transport`, ClickHouse URL/hosts,
  `clickhouse.insert_format`, TLS settings (`tls`, `tls_ca_file`,
  `tls_ca_exclusive`), Kafka connection -- all of these are baked into the
  `Client` at build time.
- **Safe to reload:** buffer thresholds, capture modes, enrichment toggles,
  routing rules.

## Verifying config drives behaviour

Config-loading is necessary but not sufficient -- the ACTION must align with the
setting. dfe-loader's tests assert the observable outcome:

- `capture_mode=raw_only` -> the row actually written has `_raw` = the full
  payload and `_json` NULL (offline processor test).
- `insert_format` + `transport` -> the row actually lands over the configured
  format/transport (live integration test, docker or dev cluster).
- ENV over yaml -> the resolved setting actually changes the behaviour, not just
  the parsed value.

See `src/pipeline/processor.rs` (capture-mode action tests) and
`tests/integration/` (live insert tests).
