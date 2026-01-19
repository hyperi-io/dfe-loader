update # Project State

**Project:** dfe-loader
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** Arrow-Only Pipeline Complete with SIMD Optimizations
**Reference:** Feature parity (or better) with `/projects/clickhouse-loader` (Go version)

---

## Build Settings

```bash
# Limit Cargo resource consumption (optional)
export CARGO_BUILD_JOBS=2
```

**SIMD Flags:** `.cargo/config.toml` configures `-C target-cpu=native` for sonic-rs SIMD optimizations.

---

## Dependency Management

### hs-rustlib via Artifactory (MANDATORY)

**hs-rustlib MUST be consumed via Artifactory, NOT local path.**

```toml
# ✅ CORRECT - Via Artifactory private registry
hs-rustlib = { version = "x.y.z", registry = "hypersec", features = ["transport-kafka"] }

# ❌ WRONG - Local path (development only, never commit)
# hs-rustlib = { path = "../hs-rustlib", features = ["transport-kafka"] }
```

### Artifactory Configuration

| Component | Value |
|-----------|-------|
| JFrog Domain | `hypersec.jfrog.io` |
| Registry Name | `hypersec` |
| Virtual Repo | `hypersec-cargo-virtual` |
| Local Repo | `hypersec-cargo-local` |
| Index URL | `sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hypersec-cargo-virtual/index/` |

### Local Setup

1. **Configure registry** in `.cargo/config.toml` (already done in this project):
   ```toml
   [registries.hypersec]
   index = "sparse+https://hypersec.jfrog.io/artifactory/api/cargo/hypersec-cargo-virtual/index/"
   ```

2. **Set credentials** in `~/.cargo/credentials.toml`:
   ```toml
   [registries.hypersec]
   token = "Bearer <your-artifactory-token>"
   ```

   Get token: `jf config export hypersec-token | base64 -d | jq -r '.accessToken'`

### Version Update Workflow

1. Make changes in `/projects/hs-rustlib`
2. Commit and push to hs-rustlib repo
3. CI builds and publishes new version to Artifactory
4. Update dfe-loader `Cargo.toml` with new version: `hs-rustlib = { version = "0.2.0", ... }`
5. Run `cargo update -p hs-rustlib` to pull from Artifactory
6. Test and commit

### Submodule Push Access

By default, CI/AI submodules are read-only (`no-push`). To enable push access:

```bash
# Enable push for ci submodule (run once per clone)
cd ci && git remote set-url --push origin https://github.com/hypersec-io/ci.git

# Enable push for ai submodule
cd ai && git remote set-url --push origin https://github.com/hypersec-io/ai.git
```

**Projects with push access enabled:**

- `/projects/dfe-loader/ci` - Enabled 2026-01-13 (on feat/test-tiers branch)
- `/projects/dfe-loader` - Enable after rename

---

## Development Principles

1. **Clean Arrow approach**: Don't munge existing code patterns into new Arrow approach if a clean rewrite or restructure is better
2. **Batch everywhere**: No row-by-row processing - batch everything for columnar efficiency
3. **Per-table buffers**: Each db.table gets its own ArrowBatchBuilder for schema uniformity
4. **Pre-flatten routing**: Extract db.table BEFORE flattening (dot notation for nested access)
5. **Arrow-only inserts**: No JSON fallback - native Arrow protocol only
6. **SIMD everywhere**: Use SIMD-accelerated parsing and conversion where possible

---

## Current Status (2025-12-25)

### Architecture: Per-Table Arrow Buffers with Native Protocol

```text
Kafka → Parse JSON/MsgPack (SIMD) → Route → Per-table buffer → Arrow RecordBatch (SIMD) → ClickHouse (native)
```

### Key Design Decisions

1. **Per-table buffers**: `HashMap<db.table, ArrowBatchBuilder>` - schema uniformity per batch
2. **Pre-flatten routing**: Extract db.table from event BEFORE flattening (dot notation for nested)
3. **Schema introspection**: Fetch from ClickHouse via Arrow client on first write, refresh periodically
4. **No backwards compatibility**: Clean break from old JSON approach
5. **Kafka offset tracking**: Per-batch offset list for at-least-once delivery (committed after successful insert)
6. **Native Arrow inserts**: clickhouse-arrow for direct Arrow RecordBatch inserts (**NO fallback**)

### DLQ Routing (Implemented)

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Per-table routing - `db.table.dlq` topics
- **Option 2**: Common DLQ topic for all failures

DLQ messages include: original payload, error reason, original topic/partition/offset, timestamp.

### Missing db.table Handling

Two options (configurable via ENV/config cascade):
- **Option 1 (default)**: Send to DLQ
- **Option 2**: Send to "common" (for either db or table)

### Libraries

| Purpose        | Library          | Status      | Notes                                    |
|----------------|------------------|-------------|------------------------------------------|
| ClickHouse     | clickhouse-arrow | Active      | Native Arrow inserts (DFE fork with full type support) |
| Arrow          | arrow            | Active      | Columnar data format                     |
| JSON→Arrow     | arrow-json       | Active      | SIMD JSON to Arrow (via ReaderBuilder)   |
| JSON (SIMD)    | sonic-rs         | Active      | Fast SIMD JSON parsing                   |
| MsgPack        | rmp-serde        | Active      | MessagePack to serde_json::Value         |

**clickhouse-arrow DFE Fork Types:**
- Variant, Dynamic, Nested (ClickHouse 24.1+, tested with 25.12)
- BFloat16 (ML workloads)
- Time/Time64 (time-of-day)
- AggregateFunction, SimpleAggregateFunction (materialized views)

**Removed:**
- klickhouse - Moved to `/projects/klickhouse-hypersec` as read-only reference

---

## SIMD Optimizations

### Implemented

1. **sonic-rs JSON parsing**: Direct SIMD-accelerated parsing to serde_json::Value
   - Fixed wasteful sonic-rs → string → serde_json conversion
   - Now uses `sonic_rs::from_slice::<serde_json::Value>()` directly

2. **arrow-json SIMD conversion**: Batch JSON bytes → Arrow RecordBatch
   - `json_bytes_to_arrow_simd()` for raw bytes with known schema
   - `SimdBatchBuilder` for accumulating raw JSON and batch converting
   - Schema inference via `infer_schema_from_json_bytes()`

3. **Pre-computed schemas**: Cache Arrow schemas per table to avoid repeated inference

### Performance Hierarchy (fastest to slowest)

1. `json_bytes_to_arrow_simd()` - Direct SIMD bytes→Arrow with known schema
2. `SimdBatchBuilder.build()` - Accumulate bytes, batch convert with SIMD
3. `json_batch_to_arrow()` - Batch conversion with schema inference
4. `json_to_arrow_batch()` - Single-row conversion (avoid in hot path)

### Hot Path Optimizations (2025-12-25)

**Round 1 - Core Optimizations:**

1. **Eliminated double JSON parsing**: Added `route_value()` method to Router that operates on
   already-parsed `serde_json::Value`. Orchestrator now parses once and routes without re-parsing.

2. **Arc<str> for topic strings**: `KafkaOffset.topic` and `KafkaMessage.topic` now use `Arc<str>`
   instead of `String`. Messages from the same topic share the same Arc, avoiding clones.

3. **Ownership-based flattening**: Added `flatten_value_owned()` that takes ownership of the Value,
   eliminating all leaf value clones. Transformer now uses this in the hot path.

4. **Conditional sanitization**: `sanitize_fields()` now returns input unchanged when no sanitization
   is enabled. When sanitization is needed, individual keys are checked before allocating.

**Round 2 - Deep Lateral Analysis:**

5. **Per-table destination storage**: `ArrowBatchBuilder` stores destination once per table, not
   per-row. Saves N-1 String allocations per batch where N = row count.

6. **Fast path for simple fields**: Router's `get_nested_field()` checks for `.` first. Simple
   field access (90%+ of cases) avoids iterator allocation from `split('.')`.

7. **Lazy warnings allocation**: Transformer only allocates warnings Vec when actually needed
   (timestamp correction/invalid). 99%+ of messages have zero warnings.

8. **Cached current time**: Transformer caches `Utc::now()` once per message and reuses for
   timestamp validation, injection, and fallback. Reduces syscalls from 2-3 to 1 per message.

**Round 3 - JSON Parser & Routing Optimizations (2025-12-25):**

9. **sonic-rs on-demand field access**: Uses `get_from_slice()` for 4-8x faster routing field
   extraction without building full DOM tree. Navigates directly to field via SIMD path lookup.

10. **Cow<str> for routing values**: `extract_first_match_from_value()` returns `&str` borrowed
    from the Value, eliminating String allocation. Uses `Cow<str>` to handle owned vs borrowed.

11. **Efficient db.table string building**: Replaced `format!("{}.{}", db, table)` with
    pre-allocated `String::with_capacity()` + `push_str()`. Avoids format macro overhead.

12. **Collector timestamp ownership transfer**: Uses `data.remove()` instead of `get().cloned()`
    to take ownership of collector timestamp without cloning the Value.

**Round 4 - Data Structures & Memory (2025-12-25):**

13. **SIMD rustflags**: Added `.cargo/config.toml` with `-C target-cpu=native` for AVX2/SSE4.2
    SIMD instructions in sonic-rs. Required for full SIMD performance.

14. **FxHashMap for internal maps**: Replaced `std::collections::HashMap` with `rustc_hash::FxHashMap`
    in BufferManager, Router, and SchemaCache. FxHash is 5-10% faster for short string keys.

15. **CompactString for table names**: `FlushBatch.table` uses `compact_str::CompactString` which
    stores strings ≤24 bytes on the stack. Typical "db.table" names (~15-20 bytes) avoid heap.

16. **Eliminated flush Vec allocation**: `get_ready_for_flush()` now iterates directly with
    `iter_mut()` instead of collecting table names first. Saves one Vec<String> allocation per flush.

17. **Pre-allocated flush Vec**: Both `get_ready_for_flush()` and `flush_all()` use
    `Vec::with_capacity()` to avoid reallocation during batch collection.

### Future Optimizations

1. **simd-json integration**: Replace sonic-rs with simd-json for even faster parsing
2. **Vectorized flattening**: SIMD-accelerated nested JSON flattening
3. **Arrow compute kernels**: Use arrow-rs compute functions for transformations

---

## Common Header Schema (v2 - Minimal)

The destination tables have a minimal required schema. All other fields are dynamic.

| Column | Type | Default | Nullable | Notes |
|--------|------|---------|----------|-------|
| `timestamp` | DateTime64(3) | - | **NO** | Event occurrence time (milliseconds) |
| `timestamp_load` | DateTime64(3) | `now64(3)` | **NO** | Load time (ClickHouse DEFAULT) |
| `_uuid` | UUID | `generateUUIDv7()` | **NO** | Unique event ID. UUIDv7 (time-ordered) |
| `_org_id` | String | - | **NO** | Organisation ID for multi-tenancy and RLS |
| `logoriginal` | String | - | YES | Original unparsed log line |
| `logjson` | JSON | - | YES | Complete Kafka message as JSON type |
| `_tags` | JSON | - | YES | Meta info + collector/agent info as JSON |

### Routing and Multi-Tenancy Fields

| Field | Config | Purpose |
|-------|--------|---------|
| `org_id` | `org_id_field` (default: `"org_id"`) | Extracted to `_org_id` field (stored in destination) |
| Database routing | `db_fields` (default: empty) | Sets **database** for insert (shared schema by default) |
| Table routing | `table_fields` | Sets **table** for insert |

**Key Changes in v2:**
- `org_id` is now **STORED** as `_org_id` in destination (required for row-level security)
- Default behavior: All data goes to `common.{table}` regardless of org_id (shared schema)
- Optional per-org routing via `routed_orgs` allowlist or `route_all_by_org = true`
- Database and table routing fields are **removed before insert** (not in destination schema)

### Config: Tags Handling

```toml
[metadata]
# Fields to check for tags (first match wins)
tags_fields = ["tags", "_tags", "meta", "metadata.tags"]
# Output field name (underscore prefix avoids collision)
tags_output = "_tags"
# Drop tags entirely after routing extraction (saves storage)
drop_tags = false
```

### Implementation Requirements

1. **`timestamp`** (NOT NULLABLE)
   - DateTime64(3) for millisecond precision
   - Copy-from field logic using source metadata
   - Fallback to `now64(3)` if missing/invalid
   - Already implemented in `TimestampValidator`

2. **`timestamp_load`** (NOT NULLABLE)
   - DateTime64(3) for millisecond precision
   - ClickHouse DEFAULT `now64(3)` - loader omits field
   - Note: All rows in a batch get same timestamp (acceptable)

3. **`_uuid`** (NOT NULLABLE, was `event_hash`)
   - Renamed to `_uuid` (underscore prefix avoids collision with source data)
   - Auto-generate UUIDv7, ignore any incoming value
   - Time-ordered (sortable), unique per event
   - Let ClickHouse generate via DEFAULT `generateUUIDv7()`

4. **`_org_id`** (NOT NULLABLE) - **NEW in v2**
   - String field extracted from source data (configurable via `org_id_field`)
   - Required for row-level security (RLS) in shared schema deployments
   - Injected by Transformer during processing
   - Used by ClickHouse row policies for data isolation
   - See `reference/clickhouse_rls.md` for row policy setup

5. **`logjson`** - **NEW**
   - Store complete original Kafka message as JSON
   - Capture before any transformation

6. **`_tags`** - **CHANGED** (was `tags`)
   - Renamed to `_tags` (underscore prefix avoids collision)
   - Config-driven source field list (first match wins)
   - Optional: `drop_tags = true` to not store after routing extraction
   - Stored as JSON column (not flattened)

### _uuid: UUIDv7 Generation

UUIDv7 is time-ordered (millisecond precision) with random suffix - ideal for event IDs.

**Recommendation**: Use ClickHouse DEFAULT - simpler, no client dependency.

```sql
_uuid UUID DEFAULT generateUUIDv7()
```
- ClickHouse 24.8+ has native `generateUUIDv7()` (tested with 25.12)
- Monotonic within timestamp, sortable
- Loader doesn't need to generate - just omit field

**Variants available in ClickHouse 24.8+ (tested with 25.12):**
| Function | Monotonicity | Notes |
|----------|--------------|-------|
| `generateUUIDv7()` | Thread-monotonic | Guarantees ordering within thread |
| `generateUUIDv7ThreadMonotonic()` | Same as above | Explicit name |
| `generateUUIDv7NonMonotonic()` | None | Slightly faster, no ordering guarantee |

---

## Dynamic db.table Routing

Routing happens **PRE-flattening** using dot notation for nested field access.

### Shared Schema (Default)

**Default behavior:** All data goes to `common.{table}` regardless of org_id.

```rust
// Config (from ENV/config cascade)
db_fields: []                                       // Empty = shared schema (common db)
table_fields: ["event_category", "tags.event.category"]  // First matching = table
default_db: "common"                                // Used when db_fields is empty
default_table: "events"                             // Fallback if no table field found
org_id_field: Some("org_id")                        // Extract for _org_id field (RLS)
routed_orgs: []                                     // Empty = all orgs use default_db
route_all_by_org: false                             // false = shared schema
```

**Example event:**
```json
{"org_id": "acme", "event_category": "auth", "action": "login"}
```

**Result:** `common.events_auth` (shared schema, org_id extracted to `_org_id` field)

### Per-Org Routing (Optional)

Enable per-org databases via allowlist OR global switch:

**Option 1 - Allowlist:** Only specific orgs get their own database
```rust
db_fields: ["org_id"]                               // Field to extract for database name
routed_orgs: ["acme", "bigcorp"]                    // Only these orgs get org_id.table
route_all_by_org: false                             // Allowlist mode
default_db: "common"                                // Everyone else goes here
```

**Option 2 - Route All:** Every org gets its own database
```rust
db_fields: ["org_id"]                               // Field to extract for database name
routed_orgs: []                                     // Not used when route_all_by_org = true
route_all_by_org: true                              // All orgs get their own database
default_db: "common"                                // Fallback if org_id missing
```

**Benefits of Shared Schema:**
- Simpler infrastructure (one database instead of hundreds)
- Easier cross-org analytics
- Row-level security handles data isolation
- Better resource utilization (shared buffer pools, caches)

---

## Per-Table Buffer Architecture

```rust
struct BufferManager {
    buffers: HashMap<String, TableBuffer>,  // Key: "db.table"
    schemas: HashMap<String, TableSchema>,  // Cached from ClickHouse
}

struct TableBuffer {
    builder: ArrowBatchBuilder,    // Accumulates JSON objects
    offsets: Vec<KafkaOffset>,     // For at-least-once ack
    created_at: Instant,           // For time-based flush
}
```

### Flow

1. **Parse**: Kafka message → JSON/MsgPack → `serde_json::Value` (SIMD)
2. **Route**: Extract db.table from event data (pre-flatten)
3. **Buffer**: Push to per-table `ArrowBatchBuilder` with Kafka offset
4. **Flush**: When threshold reached, build `RecordBatch` (SIMD), insert to ClickHouse
5. **Ack**: On success, commit Kafka offsets from batch

### Benefits

- **Schema uniformity**: Each RecordBatch has consistent schema (same table)
- **Independent flush**: High-volume tables flush more often
- **Efficient batching**: Accumulate N messages before Arrow conversion
- **Memory efficient**: Data stays as JSON until batch build time

---

## Work Completed

### Feature Implementation (2025-12-25)

- [x] arrow-json SIMD conversion (`SimdBatchBuilder`, `json_bytes_to_arrow_simd`)
- [x] Kafka offset commit after successful ClickHouse insert (at-least-once delivery)
- [x] DLQ routing with db.table topic naming (`DlqProducer`, `DlqRoutingMode`)
- [x] Fixed sonic-rs JSON parsing (removed wasteful conversion)
- [x] Added `offsets_committed` metric to Prometheus

### klickhouse Removal (2025-12-25)

- [x] Moved klickhouse fork to `/projects/klickhouse-hypersec` (read-only reference)
- [x] Removed all klickhouse dependencies from Cargo.toml
- [x] Rewrote `Inserter` to use Arrow-only (no JSON fallback)
- [x] Moved `ColumnInfo` and `TableSchema` to `types.rs`
- [x] Updated `ArrowClickHouseClient` with `query()`, `table_exists()`, `list_tables()`
- [x] Rewrote integration tests to use Arrow inserts
- [x] All 13 integration tests passing with Arrow-native inserts

### clickhouse-arrow Integration (2025-12-24)

- [x] Added clickhouse-arrow dependency to Cargo.toml
- [x] Created `ArrowClickHouseClient` wrapper
- [x] Updated `Inserter` to support native Arrow inserts
- [x] Configurable db.table routing implemented
- [x] All tests passing

### Per-Table Buffer (2025-12-24)

- [x] `BufferManager` with `HashMap<table, TableBuffer>`
- [x] `ArrowBatchBuilder` in `transform/arrow.rs`
- [x] `TableBuffer` with offset tracking
- [x] Orchestrator updated for new API
- [x] All tests passing

### clickhouse-arrow Fork

Added new ClickHouse types to local fork:

- `Type::Variant(Vec<Type>)` - Discriminated union
- `Type::Dynamic { max_types: Option<usize> }` - Runtime-typed
- `Type::Nested(Vec<(String, Type)>)` - Parallel arrays
- `Value::Variant(u8, Box<Value>)` - Variant value type
- `Value::Dynamic(String, Box<Value>)` - Dynamic value type

Location: `crates/clickhouse-arrow/`

### Variant/Dynamic/Nested Serialization (2025-12-25)

- [x] Created `serialize/variant.rs` with VariantSerializer
- [x] Created `serialize/dynamic.rs` with DynamicSerializer
- [x] Created `serialize/nested.rs` with NestedSerializer
- [x] Wired up all serializers in serialize.rs and types.rs
- [x] Integration test against ClickHouse 25.12

### Row-Level Security (RLS) (2025-12-29)

- [x] **_org_id field injection**: Transformer extracts org_id from source and injects as `_org_id`
- [x] **Shared schema routing**: Default behavior routes all data to `common.{table}`
- [x] **Optional per-org routing**: Via `routed_orgs` allowlist or `route_all_by_org = true`
- [x] **ClickHouse RLS documentation**: `reference/clickhouse_rls.md` with row policy examples
- [x] **Updated Common Header**: Added `_org_id` field to schema (NOT NULLABLE)
- [x] **Integration tests**: 4 RLS tests verify org_id extraction and injection

### Resilience Features (2025-12-25)

- [x] **Batch Salvage**: Binary-split retry on insert failure
  - Splits failed batch in half, recursively retries
  - Isolates single failing rows for DLQ routing
  - Configurable max depth (default: 20 = 2^20 = 1M rows)
  - `InsertResult` with `inserted` count and `FailedRow` list

- [x] **Circuit Breaker**: Per-table failure detection
  - Three states: Closed (normal), Open (failing), HalfOpen (testing)
  - Configurable thresholds (failure_threshold, success_threshold, open_duration)
  - Prevents cascading failures on unhealthy tables
  - `CircuitBreakerStats` for monitoring

- [x] **Schema Cache Enhancement**: Periodic refresh with error invalidation
  - TTL-based caching with configurable refresh interval
  - Background refresh task via `start_background_refresh()`
  - Error-based invalidation for schema mismatch errors
  - Metrics: hits, misses, refreshes, invalidations

- [x] **On-Demand JSON Field Access**: sonic-rs SIMD optimization
  - Uses `get_from_slice()` for 4-8x faster routing field extraction
  - No full DOM parse - navigates directly to field
  - `extract_field_json()` for top-level, `extract_nested_field_json()` for dot notation

- [x] **Concurrent Insert Semaphore**: Configurable parallel insert limit
  - `max_concurrent_inserts` in InserterConfig (default: 8, 0 = unlimited)
  - Prevents overwhelming ClickHouse with too many concurrent connections
  - Applied to `insert_batches()` and `insert_batches_with_salvage()`

---

## Current Sprint: Performance Optimisation

**Benchmark Priority (measure in this order):**

1. **CPU consumption** - cycles per message
2. **Latency** - time for event throughput processing (p50/p95/p99)
3. **Memory consumption** - peak and steady-state

### In Progress

- [x] **Production Load Testing** - Benchmark infrastructure
  - `benches/pipeline.rs` - E2E throughput benchmarks
  - `benches/transform.rs` - Transform operation benchmarks

- [ ] **Zero-Copy Routing** - Eliminate String allocations
- [ ] **Buffer Pool** - Object pool for buffer reuse
- [ ] **Config Hot-Reload** - File watcher for config changes

### Deferred

- [ ] **Vectorised JSON Flattening** - Current implementation already optimised
- [ ] **simd-json integration** - sonic-rs benchmarks show it's already faster

---

## Library Performance Research (2025-12-28)

### Keep (Benchmarked/Optimized)

| Library | Purpose | Status | Notes |
|---------|---------|--------|-------|
| `sonic-rs` | JSON parsing | **KEEP** | Extensively benchmarked, fastest SIMD JSON |
| `rdkafka` | Kafka client | **KEEP** | librdkafka wrapper, most mature |
| `arrow` + `arrow-json` | Columnar data | **KEEP** | Apache Arrow, SIMD JSON→Arrow |
| `rustc-hash` (FxHashMap) | Fast hashmap | **KEEP** | 5-10% faster for short keys |
| `compact_str` | Small strings | **KEEP** | Stack-allocated ≤24 bytes |

### New Libraries Evaluated

| Library | Purpose | Feature Flags | Notes |
|---------|---------|---------------|-------|
| `maxminddb` | GeoIP MMDB | `mmap`, `simdutf8`, `unsafe-str-decode` | Enable `mmap` + `simdutf8` for best perf |
| `moka` | LRU cache | - | TinyLFU policy, concurrent, Caffeine-inspired |
| `quick_cache` | LRU cache | - | Lower overhead than moka, no TTL support |
| `ratatui` | TUI dashboard | - | Primary Rust TUI framework (tui-rs fork) |

### maxminddb Performance Flags

```toml
# Cargo.toml - enable for best performance
maxminddb = { version = ">=0.24", features = ["mmap", "simdutf8"] }
```

- **mmap**: Memory-mapped file access (reduces memory for long-running apps)
- **simdutf8**: SIMD-accelerated UTF-8 validation during string decoding
- **unsafe-str-decode**: ~20% faster lookups (mutually exclusive with simdutf8, requires trusted data)

### Reputation Check Options

1. **ipqs_db_reader** - IPQualityScore flat file database (commercial, requires subscription)
2. **Custom radix trie** - Use `iprange-rs` for fast IP prefix matching with blocklists

### TUI Dashboard Stack

- **ratatui** + **crossterm** - Standard stack for Rust TUIs
- Consume existing Prometheus metrics endpoint (like vector.dev TUI)
- Built-in widgets: Gauge, Chart, Sparkline, Table for real-time monitoring

---

## All Previously Completed

### Core Pipeline

1. [x] Implement configurable db.table field routing
2. [x] Wire up clickhouse-arrow for native protocol inserts
3. [x] Schema introspection on-demand with refresh
4. [x] Variant/Dynamic/Nested serializers in clickhouse-arrow fork
5. [x] Remove klickhouse dependency - Arrow-only path
6. [x] Integration test Arrow inserts work end-to-end
7. [x] Use arrow-json for SIMD JSON → Arrow conversion
8. [x] Kafka offset commit on successful insert
9. [x] DLQ routing with db.table topic naming
10. [x] Hot path optimisations (4 rounds)
11. [x] BFloat16/Time/Time64/AggregateFunction types in clickhouse-arrow fork

### Resilience (2025-12-25)

- [x] Batch Salvage - Binary-split retry on insert failure
- [x] Circuit Breaker - Per-table failure detection
- [x] Schema Cache Enhancement - Periodic refresh, error invalidation
- [x] Concurrent Insert Semaphore - Configurable parallel limit

### Common Header v2

1. [x] Capture `logjson` before transform
2. [x] Config-driven `_tags` extraction
3. [x] Add `drop_tags` config option
4. [x] Remove routing fields from output
5. [x] Let ClickHouse generate `_uuid`
6. [x] Let ClickHouse generate `timestamp_load`
7. [x] Update table DDL template

---

## Test Environment

Located at k8s.tyrell.com.au with:

- ClickHouse 25.12: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

## Reference Projects

- **klickhouse-hypersec**: `/projects/klickhouse-hypersec` - Fork with Variant/Dynamic/JSON/Nested types (read-only reference)
- **ClickHouse source**: `/projects/ClickHouse` - Server source for protocol research
- **Go loader**: `/projects/clickhouse-loader` - Reference implementation

---

## Enrichment Modules (2025-12-28)

### GeoIP Enrichment (`src/enrich/geoip.rs`)

- **MaxMind MMDB support**: City and ASN databases
- **LRU cache**: 100K entries, 25% eviction on capacity
- **Private IP fast path**: RFC1918, loopback, link-local, CGNAT detection
- **Batch deduplication**: Process unique IPs once
- **Schema-aware output**: `to_schema_map()` for selective field output
- **Feature flags**: `mmap` + `simdutf8` for SIMD UTF-8 validation

### Reputation Enrichment (`src/enrich/reputation.rs`)

- **Threat types**: VPN, proxy, Tor, relay, datacenter, residential, botnet, spam, scanner, malware, phishing, bruteforce, exploit
- **Threat sources**: TorProject, FireHOL, AbuseIPDB, AbuseCH, Spamhaus, MaxMind, IPInfo, CrowdSec, GreyNoise, Custom
- **Dual storage**: O(1) FxHashMap for individual IPs + CIDR prefix matching
- **LRU cache**: 100K entries with atomic hit/miss counters
- **Blocklist loading**: Plain text format (IP or CIDR per line)
- **Common blocklists**: Tor exit nodes, Feodo botnet C2, FireHOL Level1, Spamhaus DROP

### Risk Scoring (`src/enrich/risk.rs`)

- **Component scores**: Geographic, reputation, privacy, threat (each 0-100)
- **Weighted composite**: Configurable weights (default: geo 15%, rep 25%, priv 25%, threat 35%)
- **Risk levels**: Minimal (0-19), Low (20-39), Medium (40-59), High (60-79), Critical (80-100)
- **Presets**: UsEnterprise, EuEnterprise, ApacEnterprise, Global, HighSecurity
- **Risk factors**: Human-readable flags (e.g., "tor_detected", "high_risk_country")
- **Integer math only**: All u8 scores, no floating point in hot path

---

## Current Session (2026-01-19)

### Accomplished This Session

#### Auto-Initialization & Schema Optimization

Implemented comprehensive auto-initialization for happy path setup:

**New Files:**
- `schemas/common_table.sql` - DDL template with `{db}`, `{table}`, `{engine}` placeholders
- `schemas/common_header.csv` - Field definitions (column, type, default, nullable, codec, comment)
- `src/schema/mod.rs` - Schema module with compile-time embedding and capability detection

**Config (`src/config/loader.rs`):**
```toml
[auto_init]
enabled = true           # Master switch (default: true)
create_topics = true     # Kafka topic creation
create_database = true   # ClickHouse database creation
create_table = true      # ClickHouse table creation
create_text_index = true # Text search index on logoriginal
topic_partitions = 3
topic_replication_factor = 1
```

**Engine Auto-Detection (`src/pipeline/auto_init.rs`):**
1. Queries `SELECT version()` for ClickHouse version
2. Queries `system.table_engines` for SharedMergeTree availability
3. Queries `system.clusters` for clustered deployment
4. Selection: SharedMergeTree → ReplicatedMergeTree → MergeTree

**Text Search Index:**
- `full_text(0)` on ClickHouse 25.1+ (GA)
- `ngrambf_v1(3, 256, 2, 0)` bloom filter fallback for older versions
- Added via `ALTER TABLE ... ADD INDEX` after table creation

**Optimized Schema (based on query pattern analysis):**
```sql
ORDER BY (_org_id, timestamp_load, _uuid)    -- org first for RLS
PARTITION BY (toYYYYMM(timestamp_load), _org_id)  -- monthly + org
LowCardinality(String) for _org_id           -- dictionary encoding
INDEX idx_timestamp timestamp TYPE minmax    -- event time queries
```

**Key Design Decisions:**
1. `_org_id` first in ORDER BY - every query uses org_id for RLS
2. `timestamp_load` is primary query filter (not `timestamp`)
3. `timestamp` gets minmax index for event time range queries
4. Monthly + org partitions acceptable for <100 orgs
5. Text search configurable (default ON) with version-based index selection

### Fixes Applied

- **hs-rustlib async_trait dependency** - Fixed by adding `async-trait` to transport feature (commit `bbf1ea1`)
- **Test files clickhouse import** - Updated 3 test files to use `clickhouse_arrow` module path
- **Schema CSV type** - Changed `_org_id` from `LowCardinality(String)` to `String` (CSV = logical type, DDL = storage type)

### Git State

**dfe-loader:**

- **Branch:** main
- **Upstream:** ahead by 3 commits (not pushed)
- **Commits:**
  - `b593600` fix: update test files to use clickhouse_arrow module path
  - `e698b25` docs: update STATE.md with auto-initialization session progress
  - `d52ce40` feat: add auto-initialization for Kafka topics and ClickHouse schema

**hs-rustlib:**

- **Branch:** main
- **Upstream:** ahead by 1 commit (not pushed)
- **Commits:**
  - `bbf1ea1` fix: add async-trait to transport feature dependencies

### Test Results

- **283 unit tests** - all passing
- **124 integration tests** - all passing
- **3 performance tests** - all passing

### Key Files Modified

| File | Description |
|------|-------------|
| `src/config/loader.rs` | Added `AutoInitConfig` struct |
| `src/config/mod.rs` | Re-exported `AutoInitConfig` |
| `src/schema/mod.rs` | New schema module (320 lines) |
| `src/pipeline/auto_init.rs` | New auto-initializer (460 lines) |
| `src/pipeline/mod.rs` | Re-exported `AutoInitializer` |
| `src/pipeline/orchestrator.rs` | Wired up auto-init at startup |
| `schemas/common_table.sql` | DDL template with engine placeholder |
| `schemas/common_header.csv` | Field definitions |

### Session Context Summary

Implemented auto-initialization feature ensuring the happy path always works.
On startup, dfe-loader now auto-creates Kafka topics, ClickHouse database/table,
and text search index. Engine selection (SharedMergeTree/Replicated/MergeTree)
is auto-detected from ClickHouse capabilities. Schema optimized for multi-tenant
RLS queries with _org_id first in ORDER BY. Fixed hs-rustlib async_trait dependency
and all tests pass. Ready to push both repos.

---

## Previous Session (2026-01-14)

### Accomplished That Session

#### Artifactory Registry Publishing Complete

Published all crates to HyperSec Artifactory private registry:

| Crate | Version | Registry | Status |
|-------|---------|----------|--------|
| `clickhouse-arrow-derive` | 0.3.0 | hypersec | ✅ Published |
| `clickhouse-arrow` | 0.3.0 | hypersec | ✅ Published |
| `hs-rustlib` | 0.3.0 | hypersec | ✅ Published |

#### Private CI for clickhouse-arrow Fork

Created workflow dispatch pattern for private CI on public fork:

- Created [ci/.github/workflows/clickhouse-arrow.yml](ci/.github/workflows/clickhouse-arrow.yml)
- Triggers: `repository_dispatch`, `workflow_dispatch`, `schedule` (daily 06:00 UTC)
- Public fork stays clean for upstream PRs
- Private CI runs from hypersec-io/ci repo

#### hs-rustlib Registry Dependency

Updated hs-rustlib to consume clickhouse-arrow from registry instead of git:

```toml
# Before (git dependency - can't publish)
clickhouse-arrow = { git = "https://github.com/hypersec-io/clickhouse-arrow", ... }

# After (registry dependency - publishable)
clickhouse-arrow = { version = "0.3.0", registry = "hypersec", ... }
```

### Key Files Modified

| File | Description |
|------|-------------|
| `/projects/hs-rustlib/Cargo.toml` | Registry dependency for clickhouse-arrow |
| `/projects/dfe-loader/ci/.github/workflows/clickhouse-arrow.yml` | New CI workflow |
| `/projects/clickhouse-arrow-publish/Cargo.toml` | Version 0.3.0, registry config |
| `/projects/clickhouse-arrow-publish/clickhouse-arrow/Cargo.toml` | Derive via registry |
| `/projects/clickhouse-arrow-publish/.cargo/config.toml` | Hypersec registry config |

### Decisions Made

1. **Registry over git dependencies** - Required for cargo publish to work
2. **Workflow dispatch pattern** - Private CI without polluting public fork
3. **Version 0.3.0** - Aligned clickhouse-arrow, derive, and hs-rustlib versions

### Git State

- **Branch:** main
- **Upstream:** up to date with origin/main
- **Uncommitted:** ci submodule updated (new workflow) - `ci | 2 +-`

### Next Steps

1. **Update dfe-loader** - Use `hs-rustlib = { version = "0.3.0", registry = "hypersec" }`
2. **Commit ci submodule** - Record new clickhouse-arrow workflow
3. **Test integration** - Verify dfe-loader works with registry dependencies

### Session Context Summary

Completed Artifactory publishing pipeline for clickhouse-arrow fork and hs-rustlib.
All three crates (clickhouse-arrow-derive, clickhouse-arrow, hs-rustlib) now at
v0.3.0 in hypersec registry. Created private CI workflow for the public fork.
dfe-loader can now consume hs-rustlib with clickhouse feature from Artifactory.

---

## Previous Session (2026-01-13)

**Tier 2 Test Infrastructure Enhancement - Complete** (commit 2521324)

1. **Fixture Builder Library** (1,191 lines)
   - `tests/fixtures/events.rs` - EventBuilder, BatchEventBuilder for test data generation
   - `tests/fixtures/config.rs` - Builders for BufferConfig, ClickHouseConfig, RoutingConfig, TimestampConfig, MetadataConfig
   - `tests/fixtures/arrow_schema.rs` - ArrowSchemaBuilder + pre-defined schemas (event, RLS, auth, API)
   - `tests/fixtures/ddl.rs` - DdlBuilder + DDL template functions for ClickHouse tables
   - Builder pattern eliminates test code duplication

2. **Query-Back Verification Pattern**
   - Updated `tests/integration/clickhouse.rs` - 3 levels of verification (count after each batch, category GROUP BY)
   - Updated `tests/integration/inserter.rs` - Row count + data integrity with Binary array handling
   - Updated `tests/integration/datatypes.rs` - Row count + org distribution + NULL field verification
   - Added `query_count()` and `query_one()` helpers to `tests/common/mod.rs`
   - **Critical insight**: ClickHouse returns String as Binary via Arrow protocol

3. **Property-Based Tests** (11 new tests)
   - Created `tests/integration/property.rs` using proptest
   - Routing: arbitrary org_ids, nested fields, extraction (3 tests)
   - Transformation: underscore fields, nested flattening, type handling (3 tests)
   - Buffer: accumulation, multiple tables (2 tests)
   - Timestamp: edge cases (1970-3000), RFC3339 strings (2 tests)
   - Catches edge cases manual tests miss

4. **Performance Metrics Infrastructure**
   - Created `tests/common/metrics.rs` (430 lines) - MetricsSnapshot system
   - Created `tests/performance_example.rs` - Working example with baseline/current comparison
   - Created `tests/PERFORMANCE_TESTING.md` - Documentation
   - Auto-detects improvements vs regressions (latency↓=good, throughput↑=good)
   - Generates JSON snapshots + Markdown reports

5. **Testcontainers Infrastructure**
   - Created `tests/common/containers.rs` - Docker-based test isolation
   - Added `testcontainers` feature flag to Cargo.toml
   - Ready for CI/CD (not yet integrated into tests - future work)

6. **Fixed RLS Test**
   - Updated `tests/integration/rls.rs` - Explicit Arrow schema instead of JSON inference
   - Added comprehensive query-back verification (total count, per-org counts, specific actions)
   - Removed `#[ignore]` marker - now passing consistently
   - **Root cause**: JSON schema inference creates String type instead of Timestamp type

7. **Comprehensive Documentation**
   - Created `tests/TESTING.md` (547 lines) covering:
     - Test structure (unit/integration/property/performance)
     - Testing patterns (query-back, explicit schemas, fixtures)
     - Best practices and common issues
     - Running tests and configuration
     - Future enhancements (Tier 3)

8. **Test Fixes**
   - Fixed `tests/common/metrics.rs` - Floating-point comparison for latency delta
   - Fixed `tests/performance_example.rs` - Exclude histogram bucket counts from regression detection

### Key Files Created

| File                               | Lines | Purpose                          |
| ---------------------------------- | ----- | -------------------------------- |
| `tests/fixtures/events.rs`         | 255   | Event data builders              |
| `tests/fixtures/config.rs`         | 366   | Configuration builders           |
| `tests/fixtures/arrow_schema.rs`   | 264   | Arrow schema builders            |
| `tests/fixtures/ddl.rs`            | 306   | ClickHouse DDL builders          |
| `tests/common/metrics.rs`          | 430   | Performance metrics snapshots    |
| `tests/integration/property.rs`    | 330   | Property-based tests             |
| `tests/TESTING.md`                 | 547   | Testing documentation            |
| `tests/PERFORMANCE_TESTING.md`     | 147   | Performance testing guide        |

### Critical Insights

1. **ClickHouse Arrow Protocol Quirk**: String columns returned as Binary type

   ```rust
   use arrow::array::BinaryArray;
   if let Some(col) = batch.column(0).as_any().downcast_ref::<BinaryArray>() {
       let value = std::str::from_utf8(col.value(0))?;
   }
   ```

2. **Explicit Schemas Required**: JSON schema inference fails for timestamps

   ```rust
   // ❌ BAD - JSON inference creates String type
   let batch = json_batch_to_arrow(&rows)?;

   // ✅ GOOD - Explicit schema with TimestampMillisecondArray
   let schema = Arc::new(Schema::new(vec![
       Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
   ]));
   let batch = RecordBatch::try_new(schema, columns)?;
   ```

3. **Query Verification Catches Silent Failures**: INSERT row count can succeed even if data is malformed

### Git State

- **Branch:** main
- **Upstream:** up to date with origin/main
- **Uncommitted:** clean
- **Commit:** 2521324 - "test: beef up test infrastructure like we mean it"

### Test Results

```text
All 421 tests passing:
- 294 unit tests
- 124 integration tests (including 11 property tests)
- 3 performance tests
- 11 ignored/optional tests
```

### Next Steps (Tier 3 - Future)

1. **Row Policy Enforcement Testing** - Multi-user, multi-role ClickHouse policy verification
2. **Mutation Testing** - cargo-mutants to validate test quality
3. **Fuzzing** - cargo-fuzz for parser/transformer edge cases
4. **Chaos Testing** - testcontainers + toxiproxy for failure simulation

### Session Context Summary

Completed Tier 2 test infrastructure enhancement from the approved 3-tier plan.
Built comprehensive fixture library, added query-back verification to all integration
tests, created 11 property-based tests, implemented performance metrics snapshot
system, and documented all patterns in TESTING.md. Fixed RLS test by using explicit
Arrow schemas. All 421 tests passing with production-ready verification patterns.

---

## Previous Sessions

### Session 2025-12-29 - Transport Abstraction

- Implemented transport abstraction layer in hs-rustlib (Kafka/Zenoh/Memory)
- Integrated TransportAdapter into dfe-loader with zero-copy design
- Created 15 unit tests using MemoryTransport (no infrastructure required)
- 292 tests passing (277 lib + 15 unit)

### Session 2025-12-28 - Mison Structural Index

- All 39 tests pass, benchmarks ready
- Single-pass batch extraction O(colons + fields)
- Runtime SIMD detection (AVX2/SSE4.2/NEON)
- Awaiting benchmark validation on dedicated host

---

**Last Updated:** 2026-01-14
**ClickHouse:** 25.12 (native protocol)
**Version:** 0.13.0-test-infrastructure
**Status:** Artifactory Publishing Complete - All Crates v0.3.0
