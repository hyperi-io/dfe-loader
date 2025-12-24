# Work Breakdown Structure: dfe-loader-clickhouse

**Project:** Rust port of clickhouse-loader (Go)
**Created:** 2024-12-24
**Status:** MVP Complete - Production Hardening Phase

---

## Phase 0: Foundation & Decisions ✅ COMPLETE

### 0.1 Technical Decisions ✅ COMPLETE

| Decision           | Choice              | Rationale                                   |
| ------------------ | ------------------- | ------------------------------------------- |
| JSON Library       | **sonic-rs**        | 2-3x faster, fast-path extraction, serde    |
| ClickHouse Library | **klickhouse**      | Native TCP, LZ4, mature, lower CPU          |
| Kafka Library      | **rdkafka**         | At-least-once, manual commit, production    |
| Buffer Strategy    | **Custom Columnar** | Matches Go, lower CPU, simpler              |
| Memory Control     | **MemoryController**| Graceful backpressure, non-OOM              |

- [x] **0.1.1** Decide JSON library → **sonic-rs** (+ serde_json fallback)
- [x] **0.1.2** Decide ClickHouse library → **klickhouse** (with abstraction)
- [x] **0.1.3** Decide Kafka library → **rdkafka** (librdkafka bindings)
- [x] **0.1.4** Decide buffer strategy → **Custom columnar** (not Arrow)
- [x] **0.1.5** Decide memory limit strategy → **MemoryController with backpressure**
- [ ] **0.1.6** Define hs-rustlib strategy (artifactory, git submodule, workspace)

### 0.2 Project Setup ✅ COMPLETE

- [x] **0.2.1** Create Cargo.toml with initial dependencies
- [x] **0.2.2** Set up project structure (src/, tests/, benches/)
- [ ] **0.2.3** Configure CI pipeline (.github/workflows/)
- [x] **0.2.4** Set up clippy, rustfmt, deny.toml
- [ ] **0.2.5** Create Dockerfile (multi-stage)

---

## Phase 1: Core Infrastructure ✅ COMPLETE

### 1.1 Error Handling ✅ COMPLETE

- [x] **1.1.1** Define error types with thiserror
- [x] **1.1.2** Implement error context and chaining
- [x] **1.1.3** Create Result type aliases

### 1.2 Configuration ✅ COMPLETE

- [x] **1.2.1** Implement 7-layer config cascade
- [x] **1.2.2** Define config structs (Kafka, ClickHouse, Buffer, etc.)
- [x] **1.2.3** Add config validation
- [ ] **1.2.4** Support hot-reload (file watcher)
- [x] **1.2.5** Unit tests for config loading

### 1.3 Logging ✅ COMPLETE

- [x] **1.3.1** Set up tracing with RFC 3339 timestamps
- [x] **1.3.2** JSON output for containers
- [x] **1.3.3** Human-friendly output for console
- [x] **1.3.4** Log level configuration

### 1.4 Metrics ✅ COMPLETE

- [x] **1.4.1** Set up prometheus crate
- [x] **1.4.2** Define core metrics (counters, gauges, histograms)
- [x] **1.4.3** Implement metrics HTTP server
- [x] **1.4.4** Add cardinality controls

---

## Phase 2: Kafka Integration ✅ COMPLETE

### 2.1 Consumer ✅ COMPLETE

- [x] **2.1.1** Implement Kafka consumer wrapper
- [x] **2.1.2** SASL authentication (PLAIN, SCRAM-SHA-256/512)
- [x] **2.1.3** TLS configuration
- [x] **2.1.4** Manual offset commit
- [x] **2.1.5** Partition tracking and rebalance handling
- [x] **2.1.6** Rate-limited logging
- [x] **2.1.7** Unit tests with mocked consumer
- [x] **2.1.8** Integration tests with testcontainers

### 2.2 DLQ Producer ⏳ TODO

- [ ] **2.2.1** Implement DLQ message format
- [ ] **2.2.2** DLQ producer with error context
- [ ] **2.2.3** Configurable DLQ topic naming

---

## Phase 3: Routing ✅ COMPLETE

### 3.1 Router ✅ COMPLETE

- [x] **3.1.1** Fast-path event_category extraction (no full parse)
- [x] **3.1.2** Fallback to tags.event_category
- [x] **3.1.3** Category-to-table mapping
- [x] **3.1.4** Default table routing
- [x] **3.1.5** Sub-schema rules support
- [x] **3.1.6** Unit tests for routing logic

### 3.2 Mapping Management ✅ COMPLETE

- [x] **3.2.1** Load mappings from config
- [x] **3.2.2** External mapping file support
- [ ] **3.2.3** Hot-reload of mappings

---

## Phase 4: Transformation ⚠️ PARTIAL (80%)

### 4.1 JSON Flattening ✅ COMPLETE

- [x] **4.1.1** Nested JSON to flat key-value
- [x] **4.1.2** Collision handling (last_wins, suffix, drop)
- [x] **4.1.3** Configurable separator
- [x] **4.1.4** Benchmarks for flatten performance

### 4.2 Type Coercion ⏳ TODO

- [ ] **4.2.1** String coercion
- [ ] **4.2.2** Int64/Float64 coercion
- [ ] **4.2.3** Bool coercion
- [ ] **4.2.4** DateTime64 (RFC3339, epoch variants)
- [ ] **4.2.5** IPv4/IPv6 validation and coercion
- [ ] **4.2.6** UUID validation and coercion
- [ ] **4.2.7** Array(T) recursive coercion
- [ ] **4.2.8** Nullable(T) handling
- [ ] **4.2.9** Coercer registry pattern

### 4.3 Schema Projection ⏳ TODO

- [ ] **4.3.1** Keep only schema-matching columns
- [ ] **4.3.2** Mandatory field validation
- [ ] **4.3.3** Field renaming rules

### 4.4 Timestamp Assignment ✅ COMPLETE

- [x] **4.4.1** Auto-populate timestamp fields
- [x] **4.4.2** DLQ-specific timestamp handling

### 4.5 Transformer Orchestration ✅ COMPLETE

- [x] **4.5.1** Combine flatten → rename → project → coerce → timestamp
- [ ] **4.5.2** Per-table transformer caching
- [x] **4.5.3** Transform result with warnings
- [x] **4.5.4** Unit tests for full transform pipeline
- [x] **4.5.5** Benchmarks for transform throughput

---

## Phase 5: Buffer Management ✅ COMPLETE

### 5.1 Columnar Buffer ✅ COMPLETE

- [x] **5.1.1** Column-wise data storage
- [x] **5.1.2** Null bitmap tracking
- [x] **5.1.3** Byte size tracking
- [x] **5.1.4** Row count tracking

### 5.2 Buffer Manager ✅ COMPLETE

- [x] **5.2.1** Per-table buffer management
- [x] **5.2.2** Flush triggers (bytes, rows, age)
- [x] **5.2.3** Flush request queue
- [x] **5.2.4** Buffer lifecycle management

### 5.3 Buffer Pool ⏳ TODO (Low Priority)

- [ ] **5.3.1** Object pool for buffer reuse
- [ ] **5.3.2** Configurable pool size per table
- [ ] **5.3.3** Pool metrics

---

## Phase 6: ClickHouse Integration ⚠️ PARTIAL (90%)

### 6.1 Client ✅ COMPLETE

- [x] **6.1.1** Create `ClickHouseClient` trait abstraction
- [x] **6.1.2** Implement `KlickhouseClient` (primary)
- [x] **6.1.3** Connection pooling (bb8)
- [x] **6.1.4** TLS configuration
- [x] **6.1.5** Async insert mode support
- [x] **6.1.6** Pin klickhouse to exact version in Cargo.toml
- [ ] **6.1.7** Document vendor fork process (VENDOR_FORK.md)

### 6.2 Schema Introspection ✅ COMPLETE

- [x] **6.2.1** Query system.columns for schema
- [x] **6.2.2** TTL-based schema caching
- [x] **6.2.3** Auto-refresh on insert errors (via invalidate + get_or_fetch)
- [x] **6.2.4** Full type system support (JSON, Variant, Dynamic, etc.)
- [ ] **6.2.5** Stage detection (Stage 1/2/3 maturity)

### 6.3 Inserter ✅ COMPLETE

- [x] **6.3.1** Batch insertion logic
- [x] **6.3.2** Retry with exponential backoff
- [x] **6.3.3** Insert timeout handling
- [x] **6.3.4** Integration tests with testcontainers

### 6.4 Batch Salvage ⏳ TODO

- [ ] **6.4.1** Binary-split on insert failure
- [ ] **6.4.2** Isolate bad rows
- [ ] **6.4.3** Return bad rows for DLQ
- [ ] **6.4.4** Configurable min rows for salvage

---

## Phase 7: Pipeline Orchestration ⚠️ PARTIAL (95%)

### 7.1 Pipeline Core ✅ COMPLETE

- [x] **7.1.1** Main pipeline coordinator
- [x] **7.1.2** Component wiring (consumer → router → transformer → buffer → inserter)
- [x] **7.1.3** Channel-based communication
- [x] **7.1.4** Statistics tracking (atomic counters)

### 7.2 Concurrency ✅ COMPLETE

- [x] **7.2.1** Tokio task spawning
- [x] **7.2.2** Flush worker pool (configurable parallelism)
- [x] **7.2.3** Backpressure handling

### 7.3 Lifecycle ✅ COMPLETE

- [x] **7.3.1** Graceful shutdown with CancellationToken
- [x] **7.3.2** Drain buffers on shutdown
- [x] **7.3.3** Commit offsets on shutdown

### 7.4 Circuit Breaker ⏳ TODO

- [ ] **7.4.1** Per-table failure detection
- [ ] **7.4.2** States: Closed, Open, HalfOpen
- [ ] **7.4.3** Configurable thresholds

---

## Phase 8: CLI & Commands ✅ COMPLETE

### 8.1 CLI Framework ✅ COMPLETE

- [x] **8.1.1** Set up clap with derive
- [x] **8.1.2** Global flags (--config, --log-level)

### 8.2 Commands ⚠️ PARTIAL

- [x] **8.2.1** `serve` - Run pipeline (default)
- [ ] **8.2.2** `status` - Show running instance status (JSON)
- [ ] **8.2.3** `metrics` - Dump Prometheus metrics
- [ ] **8.2.4** `top` - Real-time statistics display
- [x] **8.2.5** `version` - Show version info

---

## Phase 9: Enrichment (Post-MVP)

### 9.1 GeoIP ⏳ TODO

- [ ] **9.1.1** MaxMind MMDB support
- [ ] **9.1.2** Country, city, coordinates, ASN lookups
- [ ] **9.1.3** Database download helper

### 9.2 IP Reputation ⏳ TODO

- [ ] **9.2.1** VPN, Tor, proxy detection
- [ ] **9.2.2** Botnet classification
- [ ] **9.2.3** Anonymous IP database support

### 9.3 Risk Scoring ⏳ TODO

- [ ] **9.3.1** Malicious, scanner, spam classification
- [ ] **9.3.2** Configurable scoring rules

### 9.4 Enrichment Orchestration ⏳ TODO

- [ ] **9.4.1** Per-table schema awareness
- [ ] **9.4.2** Storage strategies (columns, JSON, hybrid)
- [ ] **9.4.3** Field configuration (src_ip, dst_ip prefixes)

---

## Phase 10: Testing & Quality ⚠️ PARTIAL

### 10.1 Unit Tests ✅ COMPLETE

- [x] **10.1.1** Core logic tests (>80% coverage)
- [x] **10.1.2** Error path testing

### 10.2 Integration Tests ⚠️ PARTIAL

**Test Environment Options:**

1. **External (k8s.tyrell.com.au)** - Set `.env` with real credentials
2. **Local Docker** - Run `docker-compose -f docker-compose.dev.yaml up -d`
3. **Testcontainers** - Auto-spawned in CI/programmatic tests

- [x] **10.2.1** Kafka integration (testcontainers OR docker-compose.dev.yaml)
- [x] **10.2.2** ClickHouse integration (testcontainers OR docker-compose.dev.yaml)
- [ ] **10.2.3** Full E2E pipeline test
- [ ] **10.2.4** klickhouse regression tests (batch insert, schema query, compression)
- [x] **10.2.5** Test environment detection (prefer .env → fallback to local Docker)

### 10.3 Benchmarks ⚠️ PARTIAL

- [x] **10.3.1** JSON parsing benchmarks
- [x] **10.3.2** Transform throughput benchmarks
- [ ] **10.3.3** Insert latency benchmarks
- [ ] **10.3.4** Comparison with Go version

### 10.4 Security ⏳ TODO

- [ ] **10.4.1** cargo audit in CI
- [ ] **10.4.2** No unsafe code (or justify)
- [x] **10.4.3** Input validation

---

## Phase 11: Documentation & Release ⏳ TODO

### 11.1 Documentation ⏳ TODO

- [ ] **11.1.1** README.md with quick start
- [ ] **11.1.2** Configuration reference
- [ ] **11.1.3** Metrics reference
- [ ] **11.1.4** Architecture documentation

### 11.2 Release ⏳ TODO

- [ ] **11.2.1** Semantic release configuration
- [ ] **11.2.2** CHANGELOG.md automation
- [ ] **11.2.3** Container image publishing
- [ ] **11.2.4** Binary releases (Linux, macOS)

---

## Summary

| Phase   | Description          | Status       | Completion |
| ------- | -------------------- | ------------ | ---------- |
| Phase 0 | Foundation           | ✅ Complete  | 95%        |
| Phase 1 | Core Infrastructure  | ✅ Complete  | 100%       |
| Phase 2 | Kafka Integration    | ⚠️ Partial   | 90%        |
| Phase 3 | Routing              | ✅ Complete  | 100%       |
| Phase 4 | Transformation       | ⚠️ Partial   | 80%        |
| Phase 5 | Buffer Management    | ✅ Complete  | 100%       |
| Phase 6 | ClickHouse           | ⚠️ Partial   | 90%        |
| Phase 7 | Pipeline             | ⚠️ Partial   | 95%        |
| Phase 8 | CLI & Commands       | ⚠️ Partial   | 60%        |
| Phase 9 | Enrichment           | ⏳ Post-MVP  | 0%         |
| Phase 10| Testing & Quality    | ⚠️ Partial   | 70%        |
| Phase 11| Documentation        | ⏳ Post-MVP  | 0%         |

---

## Priority Order

**MVP (Minimum Viable Pipeline): ✅ COMPLETE**

1. ~~Phase 0: Foundation (0.1, 0.2)~~
2. ~~Phase 1: Core Infrastructure (1.1-1.4)~~
3. ~~Phase 2: Kafka (2.1)~~
4. ~~Phase 3: Routing (3.1, 3.2)~~
5. ~~Phase 4: Transformation (4.1, 4.4, 4.5)~~
6. ~~Phase 5: Buffer (5.1-5.2)~~
7. ~~Phase 6: ClickHouse (6.1, 6.3)~~
8. ~~Phase 7: Pipeline (7.1-7.3)~~
9. ~~Phase 8: CLI (8.1, 8.2.1)~~

**Production Hardening (Current):**

1. Phase 4.2: Type Coercion (requires 6.2)
2. Phase 6.2: Schema Introspection
3. Phase 2.2: DLQ Producer
4. Phase 6.4: Batch Salvage
5. Phase 7.4: Circuit Breaker

**Post-MVP:**

- Phase 9: Enrichment
- Phase 8.2.2-8.2.4: Additional commands
- Phase 10: Full test suite
- Phase 11: Documentation & Release

---

## Effort Estimates

| Phase    | Complexity | Dependencies     |
| -------- | ---------- | ---------------- |
| Phase 0  | Low        | None             |
| Phase 1  | Medium     | Phase 0          |
| Phase 2  | Medium     | Phase 1          |
| Phase 3  | Low        | Phase 2          |
| Phase 4  | High       | Phase 3, Phase 6.2 |
| Phase 5  | Medium     | Phase 4          |
| Phase 6  | High       | Phase 1          |
| Phase 7  | High       | Phase 2-6        |
| Phase 8  | Low        | Phase 7          |
| Phase 9  | Medium     | Phase 4          |
| Phase 10 | Medium     | All              |
| Phase 11 | Low        | All              |

---

## Milestones

| Milestone           | Phases         | Description                           | Status       |
| ------------------- | -------------- | ------------------------------------- | ------------ |
| M1: Skeleton        | 0, 1           | Project compiles, config loads        | ✅ Complete  |
| M2: Kafka Flow      | 2, 3           | Consume and route messages            | ✅ Complete  |
| M3: Transform       | 4              | Full transformation pipeline          | ⚠️ 80%       |
| M4: ClickHouse      | 5, 6           | Buffer and insert to ClickHouse       | ⚠️ 95%       |
| M5: MVP             | 7, 8.1-8.2.1   | End-to-end pipeline working           | ✅ Complete  |
| M6: Production      | 9, 10, 11      | Full feature parity, tested, documented | ⏳ Pending  |

---

**Last Updated:** 2025-12-24
