## CI UNAVAILABLE — Commit with [skip ci]

The CI system is being completely rewritten. Until migration instructions are provided:

- **All commits MUST include `[skip ci]` in the commit message**
- Do not trigger CI runs or rely on CI for validation
- Run `./ci/local-build.sh` for local validation if it exists

---

# Project State

**Project:** dfe-loader
**DFE:** Data Fusion Engine
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Reference:** Feature parity (or better) with `/projects/clickhouse-loader` (Go version)

---

## CI Workflow Rule

**Always run `./ci/local-build.sh` and confirm it passes locally before pushing to CI.**

Do not push to CI to discover compile or test failures — fix them locally first.

---

## Licensing

| Component | Value |
|-----------|-------|
| License | FSL-1.1-ALv2 (Functional Source License) |
| Licensor | HYPERI PTY LIMITED (ABN 31 622 581 748) |
| SPDX ID | `FSL-1.1-ALv2` |
| Apache 2.0 Conversion | 2 years after each release |

**Source file headers:**
```rust
// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED
```

See [LICENSE](LICENSE), [COMMERCIAL.md](COMMERCIAL.md), and [CONTRIBUTING.md](CONTRIBUTING.md) for details.

---

## Build Settings

```bash
# Limit Cargo resource consumption (optional)
export CARGO_BUILD_JOBS=2
```

**SIMD Flags:** `.cargo/config.toml` configures `-C target-cpu=native` for sonic-rs SIMD optimizations.

---

## Dependency Management

### hyperi-rustlib via Artifactory (MANDATORY)

**hyperi-rustlib MUST be consumed via Artifactory, NOT local path.**

```toml
# ✅ CORRECT - Via Artifactory private registry
hyperi-rustlib = { version = "x.y.z", registry = "hyperi", features = ["transport-kafka"] }

# ❌ WRONG - Local path (development only, never commit)
# hyperi-rustlib = { path = "../hyperi-rustlib", features = ["transport-kafka"] }
```

### Artifactory Configuration

| Component | Value |
|-----------|-------|
| JFrog Domain | `hypersec.jfrog.io` |
| Registry Name | `hyperi` |
| Virtual Repo | `hyperi-cargo-virtual` |
| Local Repo | `hyperi-cargo-local` |
| Index URL | `sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hyperi-cargo-virtual/index/` |

### Local Setup

1. **Configure registry** in `.cargo/config.toml` (already done in this project):
   ```toml
   [registries.hyperi]
   index = "sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hyperi-cargo-virtual/index/"
   ```

2. **Set credentials** in `~/.cargo/credentials.toml`:
   ```toml
   [registries.hyperi]
   token = "Bearer <your-artifactory-token>"
   ```

   Get token: `jf config export hyperi-token | base64 -d | jq -r '.accessToken'`

### Version Update Workflow

1. Make changes in `/projects/hyperi-rustlib`
2. Commit and push to hyperi-rustlib repo
3. CI builds and publishes new version to Artifactory
4. Update dfe-loader `Cargo.toml` with new version: `hyperi-rustlib = { version = "0.2.0", ... }`
5. Run `cargo update -p hyperi-rustlib` to pull from Artifactory
6. Test and commit

### Submodule Push Access

By default, CI/AI submodules are read-only (`no-push`). To enable push access:

```bash
# Enable push for ci submodule (run once per clone)
cd ci && git remote set-url --push origin https://github.com/hyperi-io/ci.git

# Enable push for ai submodule
cd ai && git remote set-url --push origin https://github.com/hyperi-io/ai.git
```

---

## Architecture

### Pipeline

```text
Kafka
  │
  ▼
Parse JSON/MsgPack (sonic-rs SIMD) → serde_json::Value
  │
  ▼
Route (pre-flatten, dot notation) → db.table
  │
  ▼
Transform (flatten, timestamp, _org_id, _tags, _raw, field sanitize) → Map<String, Value>
  │
  ▼
BufferManager — per-table Vec<Map<String, Value>> + KafkaOffset accumulation
  │   (flush when: row count, byte size, or time threshold exceeded)
  ▼
Inserter — JSONEachRow via reqwest HTTP POST → ClickHouse
  │   (batch salvage on failure, circuit breaker, concurrent semaphore)
  ▼
Kafka offset commit (at-least-once delivery)
```

### Key Design Decisions

1. **JSONEachRow inserts**: `reqwest` HTTP POST with NDJSON body. Bypasses `clickhouse::Row` trait's compile-time schema requirement — dynamic `Map<String, Value>` serialises naturally to JSON. ClickHouse handles type coercion.
2. **Per-table buffers**: `HashMap<db.table, TableBuffer>` — each table accumulates rows independently. High-volume tables flush more often.
3. **Pre-flatten routing**: Extract db.table from the raw Value BEFORE flattening. Dot notation (`tags.event.category`) for nested field access.
4. **sonic-rs on-demand routing**: `get_from_slice()` for routing field extraction without full DOM parse (4-8x faster than full parse for routing-only).
5. **Kafka offset tracking**: Per-batch `Vec<KafkaOffset>`. Committed only after successful ClickHouse insert (at-least-once).
6. **Dual ClickHouse clients**: `clickhouse::Client` for DDL/queries (static Row types, system.columns), `reqwest::Client` for JSONEachRow data inserts.

### Development Principles

- **Batch everywhere**: No row-by-row processing — accumulate, then flush as a batch
- **Pre-flatten routing**: Always route before flattening (dot notation for nested access)
- **Per-table buffers**: Each db.table gets its own buffer for independent flush control
- **SIMD parsing**: sonic-rs for JSON, rmp-serde for MessagePack
- **No schema at compile time**: `Map<String, Value>` — ClickHouse coerces from JSON

### Libraries

| Purpose | Library | Notes |
|---------|---------|-------|
| ClickHouse DDL/queries | `clickhouse` (official, HTTP) | Static Row types for system.columns |
| ClickHouse inserts | `reqwest` (HTTP) | JSONEachRow, dynamic `Map<String, Value>` |
| JSON parsing (SIMD) | `sonic-rs` | `from_slice::<Value>()` + `get_from_slice()` for routing |
| MessagePack | `rmp-serde` | `from_slice::<Value>()` |
| Internal hash maps | `rustc-hash` (FxHashMap) | 5-10% faster than std for short string keys |
| Short strings | `compact_str` | Stack-allocated ≤24 bytes — used for `FlushBatch.table` |
| Kafka | `rdkafka` (via rustlib) | SASL/SCRAM-SHA-512 auth |

---

## Hot Path Optimisations

Active optimisations in the pipeline (parse → route → transform → buffer):

1. **sonic-rs on-demand field access** (`get_from_slice`): Extract routing fields without full DOM — navigates directly to field via SIMD path lookup.
2. **`route_value()`**: Router operates on already-parsed `Value`, avoiding double-parse.
3. **`Arc<str>` for topic strings**: Messages from the same topic share the Arc — no per-message clone.
4. **`flatten_value_owned()`**: Takes ownership of Value, eliminating leaf value clones during flatten.
5. **Conditional sanitization**: `sanitize_fields()` is a no-op when sanitization is disabled.
6. **Fast path for simple fields**: Router's `get_nested_field()` checks for `.` before splitting — 90%+ of fields have no nesting.
7. **Lazy warnings allocation**: `Vec<String>` for transform warnings only allocated when needed (99%+ of messages have zero warnings).
8. **Cached `Utc::now()`**: Called once per message, reused for timestamp validation, injection, fallback.
9. **`Cow<str>` for routing values**: Borrows from the Value where possible — no String allocation.
10. **Pre-allocated flush Vec**: `Vec::with_capacity()` for batch collection in BufferManager.
11. **FxHashMap**: `BufferManager`, `Router`, `SchemaCache` all use FxHashMap.
12. **CompactString for table names**: `FlushBatch.table` stored on stack for typical ≤24-byte names.

---

## Common Header Schema (v2)

The destination tables have a minimal required schema. All other fields are dynamic.
**All fields use underscore prefix** to avoid name collisions with source data.

| Column | Type | Default | Nullable | Notes |
|--------|------|---------|----------|-------|
| `_timestamp` | DateTime64(3) | - | **NO** | Event occurrence time (milliseconds) |
| `_timestamp_load` | DateTime64(3) | `now64(3)` | **NO** | Load time (ClickHouse DEFAULT, loader omits) |
| `_timestamp_received` | DateTime64(3) | - | YES | When receiver/loader received the event |
| `_uuid` | UUID | `generateUUIDv7()` | **NO** | Unique event ID — loader omits, ClickHouse generates |
| `_org_id` | String | - | **NO** | Organisation ID for multi-tenancy and RLS |
| `_raw` | String | - | YES | Original raw data (tailed log line, DB row). Configurable per-table. |
| `_json` | JSON | - | YES | Complete Kafka message as native JSON type |
| `_tags` | JSON | - | YES | Meta info + collector/agent info as JSON |

### Routing and Multi-Tenancy Fields

| Field | Config | Purpose |
|-------|--------|---------|
| `org_id` | `org_id_field` (default: `"org_id"`) | Extracted to `_org_id` field (stored in destination) |
| Database routing | `db_fields` (default: empty) | Sets **database** for insert (shared schema by default) |
| Table routing | `table_fields` | Sets **table** for insert |

**Key behaviour:**
- `org_id` is **STORED** as `_org_id` in destination (required for row-level security)
- Default: all data goes to `common.{table}` regardless of org_id (shared schema)
- Optional per-org routing via `routed_orgs` allowlist or `route_all_by_org = true`
- Routing fields are **removed before insert** (not in destination schema)

### Config: Tags Handling

```toml
[metadata]
tags_fields = ["tags", "_tags", "meta", "metadata.tags"]  # First match wins
tags_output = "_tags"
drop_tags = false  # Set true to not store after routing extraction
```

### Config: Per-Table _raw Handling

```toml
[metadata]
include_raw = true  # Global default

[metadata.raw_overrides]
"events" = false    # Drop _raw for catch-all events table
"syslog" = true     # Always keep _raw for syslog
```

### Field Notes

- **`_json`**: Injected by `BufferManager` from raw Kafka bytes — NOT by the Transformer. Stored as native ClickHouse JSON type (GA v25.3). Path-based access: `_json.user.name`.
- **`_raw`**: Original wire format — NOT the same as `_json`. Full-text indexed when enabled.
- **`_uuid`**: Let ClickHouse generate via `DEFAULT generateUUIDv7()` — loader omits the field.
- **`_timestamp_load`**: Let ClickHouse generate via `DEFAULT now64(3)` — loader omits the field.

---

## Dynamic db.table Routing

Routing happens **PRE-flattening** using dot notation for nested field access.

### Shared Schema (Default)

**Default behaviour:** All data goes to `common.{table}` regardless of org_id.

```rust
db_fields: []                                           // Empty = shared schema (common db)
table_fields: ["event_category", "tags.event.category"] // First matching = table
default_db: "dfe"                                       // Used when db_fields is empty
default_table: "default"                                // Fallback if no table field found
org_id_field: Some("org_id")                            // Extract for _org_id field (RLS)
```

### Per-Org Routing (Optional)

**Allowlist mode:** Only specific orgs get their own database
```rust
db_fields: ["org_id"]
routed_orgs: ["acme", "bigcorp"]  // Only these get their own db
default_db: "common"              // Everyone else goes here
```

**Route all mode:** Every org gets its own database
```rust
db_fields: ["org_id"]
route_all_by_org: true
default_db: "common"  // Fallback if org_id missing
```

---

## Per-Table Buffer Architecture

```rust
struct BufferManager {
    buffers: FxHashMap<String, TableBuffer>,  // Key: "db.table"
    schemas: SchemaCache,                     // TTL-based, fetched from system.columns
}

struct TableBuffer {
    rows: Vec<Map<String, Value>>,  // Accumulated rows for JSONEachRow insert
    offsets: Vec<KafkaOffset>,      // For at-least-once commit
    created_at: Instant,            // Time-based flush trigger
}
```

### Flush Triggers

- **Row count**: `max_rows_per_batch` threshold
- **Byte size**: `max_bytes_per_batch` threshold (estimated from row count × avg size)
- **Time**: `max_flush_interval` elapsed since first row in buffer

### FlushBatch

```rust
struct FlushBatch {
    table: CompactString,              // "db.table" (stack-allocated for ≤24 bytes)
    batch: Vec<Map<String, Value>>,    // Rows ready for JSONEachRow
    offsets: Vec<KafkaOffset>,         // Committed to Kafka on success
}
```

---

## Resilience Features

### Batch Salvage

Binary-split retry on insert failure — isolates individual bad rows rather than dropping the whole batch:

1. Insert full batch → fails
2. Split in half, retry each half independently
3. Recurse until single failing row found
4. Route single failing rows to DLQ
5. Configurable max depth (default: 20, supports up to 2^20 = 1M rows)

### Circuit Breaker

Per-table failure detection (`src/clickhouse/circuit_breaker.rs`):
- **Closed**: normal operation
- **Open**: table is failing, inserts skip and go to DLQ immediately
- **HalfOpen**: probe with one insert to test recovery
- Configurable: `failure_threshold`, `success_threshold`, `open_duration`

### Schema Cache

TTL-based with background refresh (`src/clickhouse/schema.rs`):
- Fetches from `system.columns` via `HttpClickHouseClient`
- Background refresh task via `start_background_refresh()`
- Error-based invalidation on schema mismatch

### Concurrent Insert Semaphore

`max_concurrent_inserts` in `InserterConfig` (default: 8, 0 = unlimited). Prevents overwhelming ClickHouse with too many parallel connections.

---

## Test Environment

Located at `clickhouse.devex.hyperi.io` (3-node replicated cluster, Keeper-managed):

- ClickHouse: `clickhouse.devex.hyperi.io:8123` (HTTP), `:9000` (native)
- Kafka: `kafka.devex.hyperi.io:9092` with SCRAM-SHA-512
- See `.env` for credentials

### Test Cluster Database Types

| Database | Engine | Notes |
|----------|--------|-------|
| `benchmark` | `Atomic` | Use `ON CLUSTER 'default'` + `MergeTree()`. Tables are independent per node — no data replication. Avoid query-back tests. |
| `default` | `Replicated` | DDL auto-propagated. Use `ReplicatedMergeTree()` (no args — ZK paths auto-filled). DO NOT specify explicit ZK paths (Code 36). DO NOT use `ON CLUSTER` for CREATE. |

### Test Table Convention

**Standard pattern (most tests — `benchmark` database):**
```rust
// ON CLUSTER creates on all 3 nodes. MergeTree stays as MergeTree (no data replication).
"CREATE TABLE {} ON CLUSTER 'default' (...) ENGINE = MergeTree() ORDER BY tuple()"
```

**Query-back verification pattern (`default` Replicated database):**
```rust
// No ON CLUSTER — Replicated DB propagates DDL automatically and replicates data.
// ReplicatedMergeTree() with no args — ZK paths auto-filled by Replicated DB.
"CREATE TABLE default.{} (...) ENGINE = ReplicatedMergeTree() ORDER BY ..."
// After INSERT, sync before query-back:
"SYSTEM SYNC REPLICA ON CLUSTER 'default' default.{table_name}"
```

**Wrong (do not use):**
```rust
// ❌ Explicit ZK paths forbidden in Replicated database (Code 36)
"ENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/{{database}}/{{table}}', '{{replica}}')"
// ❌ No ON CLUSTER in Atomic database — only creates on one node
"CREATE TABLE {} (...) ENGINE = MergeTree()"
```

**Why:** Load-balanced cluster with no sticky sessions — INSERT and SELECT may hit different nodes. Without ON CLUSTER in Atomic DBs, or without data replication in Replicated DBs, query-back returns 0.

---

## Reference Projects

- **Go loader**: `/projects/clickhouse-loader` — reference implementation for feature parity
- **ClickHouse source**: `/projects/ClickHouse` — server source for protocol research
- **clickhouse-rs fork**: `/projects/clickhouse-rs` — feature branch with native protocol + full type support (Phase 5.5)
- **klickhouse fork**: Archived — was used for Variant/Dynamic/JSON/Nested type research (no longer used)

---

## Enrichment Modules

### GeoIP Enrichment (`src/enrich/geoip.rs`)

- **MaxMind MMDB support**: City and ASN databases
- **LRU cache**: 100K entries, 25% eviction on capacity
- **Private IP fast path**: RFC1918, loopback, link-local, CGNAT detection
- **Batch deduplication**: Process unique IPs once
- **Schema-aware output**: `to_schema_map()` for selective field output
- **Feature flags**: `mmap` + `simdutf8` for SIMD UTF-8 validation

```toml
maxminddb = { version = ">=0.24", features = ["mmap", "simdutf8"] }
```

### Reputation Enrichment (`src/enrich/reputation.rs`)

- **Threat types**: VPN, proxy, Tor, relay, datacenter, residential, botnet, spam, scanner, malware, phishing, bruteforce, exploit
- **Dual storage**: O(1) FxHashMap for individual IPs + CIDR prefix matching
- **LRU cache**: 100K entries with atomic hit/miss counters
- **Blocklist loading**: Plain text (IP or CIDR per line)

### Risk Scoring (`src/enrich/risk.rs`)

- **Component scores**: Geographic, reputation, privacy, threat (each 0-100)
- **Weighted composite**: Configurable weights (default: geo 15%, rep 25%, priv 25%, threat 35%)
- **Risk levels**: Minimal (0-19), Low (20-39), Medium (40-59), High (60-79), Critical (80-100)
- **Presets**: UsEnterprise, EuEnterprise, ApacEnterprise, Global, HighSecurity
- **Integer math only**: All u8 scores, no floating point in hot path

---

## CI Cross-Compilation

### aarch64 Sysroot Approach

The CI builds both `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu` binaries.
Cross-compilation uses a private sysroot approach in `build.sh`:

1. Auto-detect native `-dev` packages via `.pc` files
2. Download cross-arch equivalents with two-level dependency resolution
3. Apply usrmerge (merge `lib/` into `usr/lib/`)
4. Patch GNU ld scripts to use sysroot-relative paths
5. Install `libc6-dev:arm64` system-wide (Multi-Arch: same, needed for dynamic linker)
6. Linker wrapper adds `-L` flags for sysroot library paths
7. `CFLAGS_aarch64_unknown_linux_gnu` adds arch-specific include paths

### GitHub Actions Artifact Permissions

`actions/upload-artifact@v4` strips Unix file permissions. Binary publish scripts
must `chmod +x` after downloading artifacts before `find -perm -u=x` searches.

---

## Claude Code: Auto-Approval Settings

`.claude/settings.local.json` grants broad auto-approval for CI monitoring so Claude Code does not
block waiting for the user to press OK on every command during long builds.

**Covered without prompting:** `gh *`, `git *`, `sleep *`, `bash *`, `cargo *`, `helm *`,
`Read/Write/Edit` on dfe-loader and ci project trees.

**Requires new session to take effect** — Claude Code reads `settings.local.json` at startup.
Start a fresh session after any changes to this file.

**CI fix workflow (no approval needed):**
1. Claude monitors `gh run list` / `gh run view --log-failed`
2. Finds root cause in logs
3. If CI bug → fixes in `/projects/ci`, commits, pushes, then `git submodule update --remote ci` in dfe-loader
4. If dfe-loader bug → fixes here, commits, pushes
5. New CI run triggers automatically; Semantic Release → Publish follows on success

---

## Decisions Log

| Decision | Rationale |
|----------|-----------|
| FSL-1.1-ALv2 licensing | Source-available with Apache 2.0 conversion after 2 years |
| JFrog domain stays `hypersec.jfrog.io` | Account-level, not user-facing; repo names updated to `hyperi-*` |
| Parallel cargo jobs = 2 | Prevents CPU starvation on local builds and CI |
| HyperI casing | Capital H, capital I for brand; HYPERI for legal entity |
| Registry over git deps | Required for cargo publish to work |
| settings.local.json broad permissions | Prevents Claude Code blocking on approval during AFK CI monitoring sessions |
| MSRV 1.94 (rust-version = "1.94") | Pin to current stable. Track latest until OSS, then stabilise as the project matures |
| Never kill cargo processes | Multiple projects share this host — NEVER kill cargo to free locks. Wait for builds to finish. |
| Edition 2024 | Using Rust edition 2024. `std::env::set_var/remove_var` require `unsafe` blocks. Pattern matching on `&mut T` is implicit in 2024 (remove `ref mut` from `if let Some` on `&mut Option`). |
| Drop Arrow/clickhouse-arrow | Benchmarks: sonic-rs→Map 6-9x faster than Arrow building; insert paths within noise (network-dominated). Arrow adds complexity with zero insert throughput benefit. |
| Drop Mison structural index | Benchmarks: zero throughput advantage over sonic-rs. 3,436 lines removed. |
| JSONEachRow via reqwest | Bypasses `clickhouse::Row` compile-time trait. `Map<String, Value>` serialises naturally. Serde overhead ~3-5% of pipeline time vs 40-75ms network I/O. |
| Test tables: ON CLUSTER + MergeTree/ReplicatedMergeTree | 3-node load-balanced cluster, no sticky sessions. CREATE without ON CLUSTER creates on one node only — inserts to other nodes cannot be queried back. Use benchmark DB (Atomic + ON CLUSTER + MergeTree) for most tests; default DB (Replicated + ReplicatedMergeTree) only when query-back verification required. |

---
