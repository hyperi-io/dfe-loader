# Design Document: dfe-loader-clickhouse

**Project:** Rust port of clickhouse-loader (Go)
**Purpose:** High-performance Kafka → ClickHouse data loader
**Status:** Design Phase

---

## 1. Source Project Analysis

### 1.1 Original Architecture (Go)

```
Kafka Consumer → Router → Transformer → Buffer Manager → ClickHouse Inserter
                   ↓ (invalid)
                  DLQ Producer
```

### 1.2 Key Components from Go Implementation

| Component | Go Package | Lines | Purpose |
|-----------|------------|-------|---------|
| Pipeline | `internal/pipeline/` | ~500 | Orchestrates all components |
| Consumer | `internal/kafka/` | ~400 | franz-go based Kafka consumer |
| Router | `internal/routing/` | ~600 | Fast-path event_category routing |
| Transformer | `internal/transform/` | ~2000 | Flatten, coerce, project, rename |
| Buffer | `internal/buffer/` | ~300 | Columnar per-table buffering |
| Inserter | `internal/clickhouse/` | ~800 | Native protocol, batch salvage |
| Enrichment | `internal/enrich/` | ~1500 | GeoIP, IP reputation (optional) |
| Config | `internal/config/` | ~400 | 7-layer config cascade |
| Metrics | `internal/metrics/` | ~300 | Prometheus metrics |

**Total:** ~45K lines of Go code

### 1.3 Critical Design Patterns

1. **Columnar Buffering** - Data stored column-wise for efficient ClickHouse insertion
2. **Schema-Driven Processing** - ClickHouse schema introspected via `system.columns`
3. **Batch Salvage** - Binary-split to isolate bad rows on insert failure
4. **Circuit Breaker** - Per-table failure detection prevents cascading failures
5. **Fast-Path Routing** - Extract `event_category` before full JSON parse
6. **Buffer Pooling** - Reuse buffers to reduce allocations
7. **Worker Pressure Metric** - KEDA scaling on `worker_pressure` (0..1+)

---

## 2. Rust Port Design

### 2.1 Project Structure

```
dfe-loader-clickhouse/
├── src/
│   ├── main.rs                 # CLI entry point (clap)
│   ├── lib.rs                  # Library exports
│   ├── pipeline/
│   │   ├── mod.rs
│   │   └── orchestrator.rs     # Main pipeline coordinator
│   ├── kafka/
│   │   ├── mod.rs
│   │   ├── consumer.rs         # + inline unit tests (#[cfg(test)])
│   │   └── dlq.rs
│   ├── routing/
│   │   ├── mod.rs
│   │   ├── router.rs           # + inline unit tests
│   │   └── mapping.rs
│   ├── transform/
│   │   ├── mod.rs
│   │   ├── transformer.rs      # + inline unit tests
│   │   ├── flatten.rs
│   │   ├── coerce.rs
│   │   ├── project.rs
│   │   └── timestamp.rs
│   ├── buffer/
│   │   ├── mod.rs
│   │   ├── columnar.rs         # + inline unit tests
│   │   ├── manager.rs
│   │   └── pool.rs
│   ├── clickhouse/
│   │   ├── mod.rs
│   │   ├── client.rs           # ClickHouseClient trait + KlickhouseClient
│   │   ├── inserter.rs
│   │   ├── schema.rs
│   │   └── salvage.rs
│   ├── enrich/
│   │   ├── mod.rs
│   │   ├── geoip.rs
│   │   ├── reputation.rs
│   │   └── risk.rs
│   ├── config/
│   │   ├── mod.rs
│   │   └── loader.rs
│   ├── metrics/
│   │   ├── mod.rs
│   │   └── prometheus.rs
│   └── error.rs
│
├── tests/                      # Integration & E2E tests (separate crate)
│   ├── common/
│   │   └── mod.rs              # Shared fixtures, test env detection
│   ├── integration/
│   │   ├── mod.rs
│   │   ├── kafka.rs            # Kafka consumer/producer tests
│   │   └── clickhouse.rs       # ClickHouse insert/schema tests
│   └── e2e/
│       ├── mod.rs
│       └── pipeline.rs         # Full pipeline tests
│
├── benches/                    # Criterion benchmarks
│   ├── json_parsing.rs
│   └── transform.rs
│
├── tests/fixtures/
│   └── init.sql                # ClickHouse test schema
│
├── Cargo.toml
├── config.example.yaml
├── config.dev.yaml
└── docker-compose.dev.yaml     # Local test environment
```

**Rust Test Convention:**

- **Unit tests**: Inline with source code using `#[cfg(test)] mod tests { }`
- **Integration tests**: `tests/integration/*.rs` - test against real services
- **E2E tests**: `tests/e2e/*.rs` - full pipeline tests
- **Benchmarks**: `benches/*.rs` - criterion performance tests

### 2.2 Target Platforms

**Linux only, dual architecture:**

| Target | Triple | Use Case |
|--------|--------|----------|
| Linux amd64 | `x86_64-unknown-linux-gnu` | Cloud VMs, Intel/AMD servers |
| Linux arm64 | `aarch64-unknown-linux-gnu` | AWS Graviton, ARM servers |

**Build outputs:**

- Static binaries for both architectures
- Multi-arch container images (single manifest)
- No Windows/macOS release artifacts (dev only)

**CI build matrix:**

```yaml
strategy:
  matrix:
    target:
      - x86_64-unknown-linux-gnu
      - aarch64-unknown-linux-gnu
```

### 2.3 Dependency Selection

#### 2.2.1 ClickHouse Client (Decision Required)

| Option | Protocol | Performance | Arrow | Maintenance |
|--------|----------|-------------|-------|-------------|
| **klickhouse** | Native TCP | Very High | No | Active |
| **clickhouse-arrow** | Native TCP | Excellent | Yes | Active |
| clickhouse (official) | HTTP only | Medium-High | No | ClickHouse Inc |

**Recommendation:** Start with `klickhouse` for Go parity, evaluate `clickhouse-arrow` for zero-copy path.

#### 2.2.2 Other Dependencies

| Purpose | Go Library | Rust Crate | Notes |
|---------|------------|------------|-------|
| Kafka | franz-go | `rdkafka` | librdkafka bindings, production-proven |
| Kafka (alt) | - | `rskafka` | Pure Rust, fewer features |
| JSON | json-iterator | `simd-json` | SIMD-accelerated parsing |
| JSON (alt) | - | `serde_json` | Standard, slower |
| GeoIP | maxminddb-golang | `maxminddb` | Direct equivalent |
| Metrics | prometheus/client | `prometheus` | Direct equivalent |
| Config | viper | `config` + `figment` | 7-layer cascade |
| CLI | pflag | `clap` | Derive macros |
| Async | goroutines | `tokio` | Runtime |
| Logging | slog | `tracing` | Structured logging |
| Errors | - | `thiserror` + `anyhow` | Custom + context |

### 2.3 Concurrency Model

**Go Model:**
- Goroutines with channels
- WaitGroup for shutdown coordination
- Atomic counters for stats

**Rust Model:**
- Tokio tasks with mpsc channels
- `tokio::sync::Notify` or `CancellationToken` for shutdown
- `AtomicU64` / `AtomicUsize` for stats
- `Arc<Mutex<_>>` or lock-free structures where needed

```rust
// Pipeline concurrency structure
pub struct Pipeline {
    consumer: Consumer,
    router: Router,
    transformers: DashMap<String, Transformer>,  // Per-table cache
    buffer_manager: BufferManager,
    inserter: Inserter,

    // Channels
    route_tx: mpsc::Sender<RoutedMessage>,
    flush_tx: mpsc::Sender<FlushRequest>,

    // Shutdown
    shutdown: CancellationToken,

    // Stats
    stats: Arc<PipelineStats>,
}
```

### 2.4 Zero-Copy Considerations

**Goal:** Kafka message bytes → ClickHouse insertion with minimal copies

**Strategy:**
1. **Direct JSON field extraction** - Use `simd-json` to extract routing field without full parse
2. **Arena allocation** - Batch allocations for transform phase
3. **Columnar buffers** - Build columns directly, avoid row intermediates
4. **Arrow integration** (future) - If using `clickhouse-arrow`, leverage Arrow's zero-copy

**Hot Path:**
```
Kafka bytes → extract event_category (no full parse)
           → full parse only for matched messages
           → flatten to column builders
           → insert columnar data
```

### 2.5 Error Handling Strategy

```rust
#[derive(Debug, thiserror::Error)]
pub enum LoaderError {
    #[error("Kafka error: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] klickhouse::Error),

    #[error("Transform error for table '{table}': {reason}")]
    Transform { table: String, reason: String },

    #[error("Routing error: no mapping for category '{category}'")]
    Routing { category: String },

    #[error("Configuration error: {0}")]
    Config(#[from] config::ConfigError),
}
```

---

## 3. Configuration Design

### 3.1 7-Layer Cascade (Matching Go)

1. CLI args (`--config`, `--host`)
2. Environment variables (`LOADER_CLICKHOUSE_HOST`)
3. `.env` file
4. `settings.{env}.yaml`
5. `settings.yaml`
6. `defaults.yaml`
7. Hard-coded defaults

### 3.2 Config Structure

```rust
#[derive(Debug, Deserialize)]
pub struct Config {
    pub kafka: KafkaConfig,
    pub clickhouse: ClickHouseConfig,
    pub routing: RoutingConfig,
    pub buffer: BufferConfig,
    pub transform: TransformConfig,
    pub enrichment: Option<EnrichmentConfig>,
    pub metrics: MetricsConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Deserialize)]
pub struct BufferConfig {
    pub max_bytes: usize,      // 1MB default
    pub max_rows: usize,       // 10,000 default
    pub max_age_secs: u64,     // 5 seconds default
    pub flush_workers: usize,  // 4 default
    pub pool_size: usize,      // 4 per table default
}
```

---

## 4. Metrics Design

### 4.1 Prometheus Metrics (Matching Go)

**Namespace:** `ch_loader_`

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `messages_consumed_total` | Counter | topic | Messages read from Kafka |
| `messages_routed_total` | Counter | table | Messages routed to tables |
| `messages_dlq_total` | Counter | reason | Messages sent to DLQ |
| `rows_buffered` | Gauge | table | Current rows in buffer |
| `rows_inserted_total` | Counter | table | Rows inserted to ClickHouse |
| `insert_errors_total` | Counter | table, error_type | Insert failures |
| `insert_latency_seconds` | Histogram | table | Insert duration |
| `buffer_flushes_total` | Counter | table, reason | Flush triggers |
| `worker_pressure` | Gauge | - | KEDA scaling metric (0..1+) |

### 4.2 Cardinality Controls

```rust
const MAX_TABLES: usize = 100;
const MAX_TOPICS: usize = 50;
const MAX_PARTITIONS_PER_TOPIC: usize = 256;
```

---

## 5. Testing Strategy

### 5.1 Unit Tests

- Transform logic (flatten, coerce, project)
- Routing rules
- Buffer management
- Config parsing

### 5.2 Integration Tests

- Kafka consumer with testcontainers
- ClickHouse insertion with testcontainers
- Full pipeline E2E

### 5.3 Benchmarks

- JSON parsing throughput
- Transform throughput
- Insert latency

---

## 6. Critical Library Decisions

### 6.1 JSON Library Decision

**Use Case:** Kafka JSON bytes → Rust structs for transformation. **Parse-only, no serialisation needed.**

#### Options Evaluated

| Library | Performance | Fast-Path Extract | Stability | Platform | Notes |
|---------|-------------|-------------------|-----------|----------|-------|
| **sonic-rs** | Fastest (2-3x serde) | `get_unchecked` 75µs | v0.3, stable Rust | x86_64/aarch64 SIMD | ByteDance/CloudWego |
| **simd-json** | Fast (1.5x serde) | Tape API | v0.14+, mature | x86_64/aarch64 SIMD | 2.6K dependents |
| **serde_json** | Baseline | None | v1.x, very stable | All | Standard, slowest |
| **gjson** | Medium | Path syntax | v0.8, stable | All | Simple extraction |

#### Benchmark Data (twitter.json)

| Operation | sonic-rs | simd-json | serde_json |
|-----------|----------|-----------|------------|
| Deserialize struct | 694-723µs | 1.06-1.11ms | 2.27-2.32ms |
| Deserialize untyped | 525-562µs | 1.19-1.21ms | 2.86-3.85ms |
| Field extraction | 75-77µs (unchecked) | N/A | N/A |

#### Critical Features for Our Use Case

1. **Fast-path routing extraction** - Extract `event_category` without full parse
2. **Full deserialisation** - Parse complete message for transformation
3. **Zero-copy where possible** - LazyValue references into source bytes
4. **Stable on x86_64/aarch64** - Production deployment targets

#### Decision: **sonic-rs** (Primary) + **serde_json** (Fallback)

**Rationale:**
- 2-3x faster than serde_json for full deserialisation
- `get_unchecked` enables fast-path routing (75µs vs 430µs)
- `LazyValue` provides zero-copy field access
- Now supports stable Rust (was nightly-only)
- serde-compatible API for easy migration
- Fallback to serde_json for non-SIMD platforms (rare)

**Risk Mitigation:**
- sonic-rs is pre-1.0 but actively maintained (v0.3, 780 stars)
- API is serde-compatible, so switching cost is low
- Fallback path ensures correctness on all platforms

```rust
// Fast-path routing (no full parse)
let category = sonic_rs::get_unchecked(&bytes, &["event_category"])?;

// Full parse for transformation
let msg: Message = sonic_rs::from_slice(&bytes)?;
```

---

### 6.2 ClickHouse Library Decision

**Use Case:** High-throughput batch inserts, schema introspection. **Native protocol required.**

#### Options Evaluated

| Library | Protocol | Batch Insert | Schema Query | Arrow | Maturity |
|---------|----------|--------------|--------------|-------|----------|
| **klickhouse** | Native TCP | Yes | Manual | No | v0.13, active |
| **clickhouse-arrow** | Native TCP | Yes | Manual | Yes | v0.2.1, newer |
| **clickhouse (official)** | HTTP only | Yes | Yes | No | v0.14, ClickHouse Inc |
| **clickhouse-cpp FFI** | Native TCP | Yes | Yes | No | Requires bindings |

#### Key Considerations

1. **Native TCP mandatory** - HTTP adds latency, not suitable for high-throughput
2. **Batch insert performance** - Primary operation, must be fast
3. **Schema introspection** - Query `system.columns` for type mapping
4. **LZ4 compression** - Reduces network bandwidth
5. **Arrow integration** - Zero-copy potential but adds CPU overhead

#### Arrow Trade-off Analysis

| Factor | Arrow Buffer | Custom Columnar |
|--------|--------------|-----------------|
| Memory | Lower (shared buffers) | Higher (per-column alloc) |
| CPU | Higher (format conversion) | Lower (direct build) |
| Zero-copy | Yes (if source is Arrow) | No |
| Complexity | Higher | Lower |

**Your insight is correct:** For a CPU-bound loader, Arrow's conversion overhead may hurt more than memory savings help. The Go version uses custom columnar buffers successfully.

#### Decision: **klickhouse** (Primary) with abstraction layer

**Rationale:**
- Pure Rust, native TCP protocol (matches Go's clickhouse-go)
- LZ4 compression built-in
- Connection pooling via bb8
- More mature than clickhouse-arrow (longer history)
- Avoids Arrow CPU overhead for our CPU-bound workload

**Abstraction Strategy:**
```rust
// Trait allows swapping implementations
pub trait ClickHouseClient: Send + Sync {
    async fn insert_batch(&self, table: &str, batch: ColumnarBatch) -> Result<InsertResult>;
    async fn query_schema(&self, table: &str) -> Result<TableSchema>;
}

// Initial implementation
pub struct KlickhouseClient { /* ... */ }

// Future option if Arrow proves beneficial
pub struct ArrowClient { /* ... */ }
```

**Future Evaluation:**
- Benchmark klickhouse vs clickhouse-arrow with real workloads
- Consider Arrow if we add Parquet/Flight integration later
- Monitor clickhouse-arrow maturity (currently v0.2.1)

---

### 6.3 Kafka Library Decision

**Use Case:** Consume JSON messages, manual offset commit, at-least-once delivery.

#### Options Evaluated

| Library | Bindings | At-Least-Once | Features | Stability |
|---------|----------|---------------|----------|-----------|
| **rdkafka** | librdkafka C | Full support | Complete | v0.38, production |
| **rskafka** | Pure Rust | Basic | Limited | v0.5, newer |
| **kafka-rust** | Pure Rust | Basic | Limited | Unmaintained |

#### Critical Requirements

1. **Manual offset commit** - Commit only after ClickHouse insert succeeds
2. **At-least-once delivery** - No message loss on failure
3. **SASL/TLS** - Production security
4. **Rebalance callbacks** - Drain buffers before partition loss
5. **Consumer groups** - Horizontal scaling

#### rdkafka At-Least-Once Pattern

```rust
// From rdkafka examples/at_least_once.rs
let config = ClientConfig::new()
    .set("enable.auto.offset.store", "false")  // Manual control
    .set("enable.auto.commit", "true")          // Commit stored offsets
    .set("auto.commit.interval.ms", "5000");

// Process message
let result = process_and_insert(&msg).await?;

// Only store offset after successful insert
consumer.store_offset_from_message(&msg)?;
// Offset will be committed on next auto-commit interval
```

#### Decision: **rdkafka**

**Rationale:**
- Production-proven (librdkafka powers Kafka clients in many languages)
- Full at-least-once delivery support with manual offset control
- Complete feature parity with Go's franz-go
- SASL (PLAIN, SCRAM-SHA-256/512), TLS support
- Rebalance callbacks for graceful partition handoff
- 2,600+ crates.io dependents

**C Dependency Mitigation:**
- librdkafka is well-maintained by Confluent
- Static linking available (`dynamic-linking` feature off)
- Build process is straightforward on Linux/macOS

---

### 6.4 Buffer Strategy Decision

**Use Case:** Accumulate rows per-table, flush on size/count/time triggers.

#### Options

| Strategy | Memory | CPU | Zero-Copy | Complexity |
|----------|--------|-----|-----------|------------|
| **Custom Columnar** | Medium | Low | No | Low |
| **Arrow RecordBatch** | Lower | Higher | Potential | Higher |

#### Decision: **Custom Columnar Buffers** (matching Go)

**Rationale:**
- Go version proves this works at scale
- Lower CPU overhead (no Arrow conversion)
- Simpler implementation and debugging
- Direct control over memory layout
- Can add Arrow later if needed

```rust
pub struct ColumnarBuffer {
    columns: HashMap<String, ColumnData>,
    null_bitmaps: HashMap<String, BitVec>,
    row_count: usize,
    byte_size: usize,
    created_at: Instant,
    offsets: HashMap<i32, i64>,  // partition -> max offset
}

pub enum ColumnData {
    String(Vec<String>),
    Int64(Vec<i64>),
    Float64(Vec<f64>),
    DateTime64(Vec<i64>),
    // ... other types
}
```

---

### 6.5 Memory Limit Strategy

**Requirement:** Graceful non-OOM memory limit (like Go's GOMEMLIMIT)

#### Approach

1. **Buffer memory tracking** - Each buffer tracks its byte size
2. **Global memory budget** - Sum of all buffers vs configured limit
3. **Backpressure on exceed** - Pause consumption, force flush
4. **Metrics exposure** - `memory_used_bytes` gauge for monitoring

```rust
pub struct MemoryController {
    limit_bytes: usize,
    used_bytes: AtomicUsize,
    pressure_threshold: f64,  // 0.8 = 80%
}

impl MemoryController {
    pub fn try_allocate(&self, bytes: usize) -> Result<MemoryGuard, Backpressure> {
        let current = self.used_bytes.fetch_add(bytes, Ordering::SeqCst);
        if current + bytes > self.limit_bytes {
            self.used_bytes.fetch_sub(bytes, Ordering::SeqCst);
            return Err(Backpressure::MemoryExceeded);
        }
        Ok(MemoryGuard { controller: self, bytes })
    }
}
```

---

### 6.6 hs-rustlib Strategy

**Question:** How to structure shared Rust library (equivalent to hs-golib)?

**Options:**
- [ ] Private crates.io alternative (Artifactory)
- [ ] Git submodule with path dependencies
- [ ] Workspace member in monorepo

**Decision:** TBD - needs infrastructure discussion

---

## 7. Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| klickhouse maintenance drops | Medium | Low | Abstraction layer allows swap to clickhouse-arrow |
| sonic-rs breaking changes | Medium | Low | serde-compatible API; fallback to serde_json |
| rdkafka/librdkafka issues | Low | Very Low | Confluent-maintained; static linking available |
| Performance regression vs Go | High | Medium | Benchmark early and continuously |
| Feature parity gaps | Medium | Medium | Prioritise core path; defer enrichment |
| Memory leaks in unsafe code | High | Low | Minimise unsafe; use miri in CI |

---

## 8. Final Dependency Summary

### Core Dependencies (Cargo.toml)

```toml
[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# JSON parsing (primary)
sonic-rs = "0.3"
serde = { version = "1", features = ["derive"] }
serde_json = "1"  # fallback

# Kafka
rdkafka = { version = "0.38", features = ["ssl", "sasl"] }

# ClickHouse
klickhouse = { version = "0.13", features = ["compression", "bb8"] }

# Configuration
config = "0.14"
figment = { version = "0.10", features = ["yaml", "env"] }
dotenvy = "0.15"

# CLI
clap = { version = "4", features = ["derive"] }

# Logging
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }

# Metrics
prometheus = "0.13"

# Error handling
thiserror = "2"
anyhow = "1"

# Utilities
dashmap = "6"           # Concurrent hashmap for transformer cache
bitvec = "1"            # Null bitmaps
uuid = "1"
chrono = "0.4"
tokio-util = "0.7"      # CancellationToken

[dev-dependencies]
testcontainers = "0.23"
criterion = "0.5"
tempfile = "3"
```

### Feature Flags

```toml
[features]
default = ["simd"]
simd = []                    # Enable SIMD JSON parsing
enrichment = ["maxminddb"]   # Optional GeoIP/reputation
```

---

## Appendix A: Go to Rust Mapping

| Go Concept | Rust Equivalent |
|------------|-----------------|
| `goroutine` | `tokio::spawn` |
| `chan T` | `mpsc::channel` / `broadcast` |
| `sync.WaitGroup` | `tokio::task::JoinSet` |
| `sync.Mutex` | `tokio::sync::Mutex` / `std::sync::Mutex` |
| `atomic.Int64` | `AtomicI64` |
| `context.Context` | `CancellationToken` |
| `interface{}` | `dyn Trait` / generics / `enum` |
| `error` | `Result<T, E>` with `thiserror` |
| `defer` | `Drop` trait / `scopeguard` |

---

**Last Updated:** 2024-12-24
**Version:** 0.1.0-design
