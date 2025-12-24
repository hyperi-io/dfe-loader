# Project State

**Project:** dfe-loader-clickhouse
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** MVP Complete - Ready for Production Hardening
**Reference:** Feature parity (or better) with `/projects/clickhouse-loader` (Go version)

---

## Build Settings

```bash
# Limit Cargo resource consumption
export CARGO_BUILD_JOBS=2
```

---

## Current Status (2025-12-24)

### MVP Pipeline: ✅ COMPLETE

The core pipeline is fully functional:

```text
Kafka → Consumer → PayloadDetect → Router → Transform → Buffer → ClickHouse
```

### Phase Completion

| Phase | Component              | Status   | Notes                                            |
| ----- | ---------------------- | -------- | ------------------------------------------------ |
| 0     | Foundation & Decisions | ✅ 100%  | All technical decisions made                     |
| 1     | Core Infrastructure    | ✅ 100%  | Config, logging, errors                          |
| 2     | Kafka Consumer         | ✅ 100%  | Extended auth: SCRAM, PLAIN, OAuth, mTLS, AWS IAM|
| 3     | Router                 | ✅ 100%  | Category extraction, table mapping               |
| 4     | Transform              | ✅ 95%   | Flatten, timestamp, type coercion - projection pending |
| 5     | Buffer Manager         | ✅ 100%  | All 3 flush triggers + NativeBuffer for zero-copy|
| 6     | ClickHouse             | ✅ 95%   | Native insert, retry, schema introspection       |
| 7     | Pipeline               | ✅ 95%   | Orchestrator, shutdown - missing circuit breaker |
| 8     | Metrics & Health       | ✅ 100%  | Prometheus, health endpoints                     |

### Parity with Go clickhouse-loader

| Feature                      | Go Version | Rust Version | Notes                           |
| ---------------------------- | ---------- | ------------ | ------------------------------- |
| Kafka SCRAM-SHA-512          | ✅         | ✅           | Production auth                 |
| Kafka PLAIN                  | ✅         | ✅           | Extended beyond Go              |
| Kafka OAuth/OIDC             | ❌         | ✅           | **Enhancement** beyond Go       |
| Kafka mTLS                   | ✅         | ✅           | Via TLS cert/key config         |
| Kafka AWS MSK IAM            | ❌         | ✅           | **Enhancement** beyond Go       |
| Schema introspection         | ✅         | ✅           | system.columns queries          |
| Type coercion                | ✅         | ✅           | Config-driven, all types        |
| Null handling strategies     | ✅         | ✅           | Default/Error/Passthrough       |
| JSON flattening              | ✅         | ✅           | Dot notation                    |
| Timestamp validation         | ✅         | ✅           | Future/past checks              |
| MessagePack support          | ✅         | ✅           | Auto-detection                  |
| Batch retry                  | ✅         | ✅           | Exponential backoff             |
| Batch salvage                | ✅         | ⏳           | Pending                         |
| DLQ producer                 | ✅         | ⏳           | Pending                         |
| Circuit breaker              | ✅         | ⏳           | Pending                         |
| GeoIP enrichment             | ✅         | ⏳           | Post-MVP                        |

### Inline TODOs in Codebase

| File                              | TODO                       | Priority |
| --------------------------------- | -------------------------- | -------- |
| `src/transform/project.rs:5`      | Implement projection       | Medium   |
| `src/clickhouse/salvage.rs:18`    | Binary search salvage      | Medium   |
| `src/kafka/dlq.rs:18`             | DLQ producer               | Medium   |
| `src/buffer/pool.rs:5`            | Buffer pooling             | Low      |
| `src/pipeline/orchestrator.rs:156`| Send to DLQ                | Medium   |
| `src/kafka/consumer.rs:87`        | OIDC token fetch callback  | Medium   |
| `src/enrich/geoip.rs:25`          | MaxMind implementation     | Post-MVP |
| `src/enrich/reputation.rs:23`     | Reputation database        | Post-MVP |
| `src/enrich/risk.rs:12`           | Risk rules                 | Post-MVP |

---

## Completed Work

### Phase 0: Foundation ✅

- Cargo.toml with all dependencies (sonic-rs, klickhouse, rdkafka, etc.)
- Project structure (src/, tests/, benches/)
- Technical decisions documented

### Phase 1: Core Infrastructure ✅

- Error types with thiserror
- 7-layer config cascade
- Tracing with RFC 3339 timestamps
- JSON/human-friendly log formats

### Phase 2: Kafka Consumer ✅

- Consumer wrapper with rdkafka
- Extended authentication mechanisms:
  - SASL/SCRAM-SHA-512 (default, recommended)
  - SASL/SCRAM-SHA-256
  - SASL/PLAIN (with TLS)
  - SASL/OAUTHBEARER (OAuth 2.0 / OIDC)
  - AWS MSK IAM authentication
  - mTLS (SSL client certificates via TlsConfig)
  - No auth (dev/test only)
- Manual offset commit
- Partition tracking

### Phase 3: Router ✅

- Fast-path category extraction (no full parse)
- Category-to-table mapping
- Default table routing
- Sub-schema rules

### Phase 4: Transform ✅ (95%)

- JSON flattening with configurable separator
- Timestamp validation/correction
- Load timestamp injection
- Field sanitisation
- **Type Coercion (NEW):**
  - Schema-aware type conversion
  - All ClickHouse types: Int, UInt, Float, Decimal, String, Bool
  - DateTime/DateTime64 with epoch detection (s/ms/us/ns)
  - UUID normalization (multiple formats)
  - IPv4/IPv6 validation and integer conversion
  - Array and Map coercion with element types
  - Null handling: Default/Error/Passthrough strategies
  - Config-driven type mappings for custom types
- **Pending:** Schema projection

### Phase 5: Buffer Manager ✅

- Per-table columnar buffers
- Byte size tracking
- Row count tracking
- Age tracking
- Flush triggers: rows, bytes, time
- **NativeBuffer (NEW):**
  - klickhouse RawRow for zero-copy insert
  - Schema-aware type conversion to klickhouse Value types
  - All ClickHouse types supported: Date, DateTime, DateTime64, UUID, IPv4, IPv6, Map, Array
  - Epoch auto-detection (s/ms/us/ns)
  - JSON-inferred types for schemaless operation

### Phase 6: ClickHouse ✅ (95%)

- Native protocol via klickhouse
- JSONEachRow batch insertion
- Retry with exponential backoff
- Concurrent multi-table insertion
- Schema introspection via system.columns
- ParsedType for runtime type parsing
- SchemaCache with TTL and get_or_fetch
- CoercionConfig with type mappings
- **Pending:** Batch salvage

### Phase 7: Pipeline ✅ (95%)

- Main orchestrator
- Channel-based communication
- Statistics tracking
- Graceful shutdown with buffer drain
- **Pending:** Circuit breaker, DLQ integration

### Phase 8: Metrics & Health ✅

- Prometheus metrics endpoint
- Health endpoints (live, ready, startup)
- Core metrics (counters, gauges, histograms)

---

## Remaining for Production

### High Priority

1. **ClickHouse Client Library (Phase 0.2)** - Fork/replace klickhouse for full type support
   - klickhouse missing: JSON, Variant, Dynamic, Nested, AggregateFunction
   - Decision needed: fork vs new implementation

2. **DLQ Producer (Phase 2.2)** - Route bad messages

### Medium Priority

1. **Batch Salvage (Phase 6.4)** - Binary-split on failure
2. **Schema Projection (Phase 4.3)** - Keep only schema columns
3. **Circuit Breaker (Phase 7.4)** - Per-table failure detection
4. **OIDC Token Callback** - Full OAuth token refresh

### Low Priority

1. **Buffer Pool (Phase 5.3)** - Object pool for reuse
2. **Hot-reload (Phase 1.2.4)** - Config file watcher

### Post-MVP

1. **Enrichment (Phase 9)** - GeoIP, reputation, risk scoring
2. **Additional CLI commands** - status, top, metrics dump

---

## Enhancements Beyond Go Version

This Rust implementation includes enhancements not present in the Go clickhouse-loader:

1. **Extended Kafka Authentication:**
   - SASL/OAUTHBEARER (OAuth 2.0 / OIDC)
   - AWS MSK IAM authentication
   - Configurable via environment, config files, or K8s mounts

2. **SIMD-accelerated JSON parsing** via sonic-rs

3. **Config-driven type coercion** allowing new ClickHouse types without code changes

4. **Zero-copy native buffer** using klickhouse RawRow format (no JSON re-serialization on insert)

---

## Test Environment

Located at k8s.tyrell.com.au with:

- ClickHouse: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

## Architecture

### Key Components

1. **Kafka Consumer** - Extended auth (SCRAM, OAuth, mTLS, IAM), at-least-once delivery
2. **Payload Detector** - Auto-detect JSON/MessagePack, caching
3. **Router** - Category field extraction, table mapping
4. **Transform** - Flatten, timestamp validation, type coercion
5. **Buffer Manager** - Per-table columnar buffers
6. **ClickHouse Inserter** - klickhouse native protocol

### Tech Stack

- **Language:** Rust
- **JSON:** sonic-rs (SIMD-accelerated)
- **MessagePack:** rmp-serde
- **Kafka:** rdkafka with extended SASL support
- **ClickHouse:** klickhouse (native protocol)
- **Async:** tokio

---

## Library Decisions

| Purpose     | Library   | Rationale                        |
| ----------- | --------- | -------------------------------- |
| JSON        | sonic-rs  | SIMD, fastest benchmarks         |
| ClickHouse  | klickhouse| Native protocol, async           |
| Kafka       | rdkafka   | librdkafka bindings, full SASL   |
| MessagePack | rmp-serde | Serde integration                |
| UUID        | uuid v4+v7| v7 for time-ordered indexing     |

---

**Last Updated:** 2025-12-24
**Version:** 0.1.0
**Status:** MVP Complete
