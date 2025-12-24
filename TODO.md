# TODO - dfe-loader-clickhouse

**Project Goal:** High-performance Kafka to ClickHouse data loader (Rust port)

**Target:** Production-ready pipeline with feature parity (or better) vs Go clickhouse-loader

**Reference:** `/projects/clickhouse-loader` (Go version)

---

## Current Tasks

### High Priority

- [ ] **ClickHouse Client Library (0.2)** - Fork or replace klickhouse for full type support
  - **Decision:** Fork klickhouse vs write new client
  - **Missing types in klickhouse:**
    - `JSON` (Object) - Native JSON column type (GA in ClickHouse 24.x)
    - `Variant` - Dynamic union type
    - `Dynamic` - Fully dynamic type
    - `Object('json')` - Legacy JSON (deprecated but still used)
    - `Nested` - Nested table structures
    - `SimpleAggregateFunction` / `AggregateFunction`
    - `Geo types` (beyond Point/Ring/Polygon)
  - **Goals:**
    - Full ClickHouse type coverage
    - Zero-copy insert path
    - Efficient columnar serialization
  - **Options:**
    1. Fork klickhouse, add missing types
    2. New library: minimal native protocol client
  - File: `src/clickhouse/client.rs` or new crate

- [ ] **DLQ Producer (2.2)** - Route bad messages to dead letter queue
  - DLQ message format with error context
  - Configurable DLQ topic naming
  - File: `src/kafka/dlq.rs`

### Medium Priority

- [ ] **Batch Salvage (6.4)** - Binary-split on insert failure
  - Isolate bad rows
  - Return bad rows for DLQ
  - File: `src/clickhouse/salvage.rs`

- [ ] **Schema Projection (4.3)** - Keep only schema-matching columns
  - Mandatory field validation
  - Field renaming rules
  - File: `src/transform/project.rs`

- [ ] **Circuit Breaker (7.4)** - Per-table failure detection
  - States: Closed, Open, HalfOpen
  - Configurable thresholds

- [ ] **Orchestrator DLQ Integration** - Wire DLQ into pipeline
  - File: `src/pipeline/orchestrator.rs:156`

- [ ] **OIDC Token Callback** - Full OAuth token refresh for OAUTHBEARER
  - File: `src/kafka/consumer.rs`

- [x] **Native Columnar Buffer** - Migrate buffer to klickhouse-native format - 2025-12-24
  - Zero-copy insert path via klickhouse RawRow
  - File: `src/buffer/native.rs`

### Low Priority

- [ ] **Buffer Pool (5.3)** - Object pool for buffer reuse
  - Configurable pool size per table
  - Pool metrics
  - File: `src/buffer/pool.rs`

- [ ] **Hot-reload (1.2.4)** - Config file watcher

- [ ] **hs-rustlib Strategy (0.1.6)** - Decide on shared library approach
  - Options: artifactory, git submodule, workspace

---

## Completed

- [x] Cargo.toml with all dependencies - 2025-12-24
- [x] Config module with 7-layer cascade - 2025-12-24
- [x] Kafka consumer with SASL/SCRAM - 2025-12-24
- [x] Extended Kafka auth mechanisms - 2025-12-24
  - SASL/SCRAM-SHA-512, SCRAM-SHA-256
  - SASL/PLAIN (with TLS)
  - SASL/OAUTHBEARER (OAuth 2.0 / OIDC)
  - AWS MSK IAM authentication
  - mTLS (SSL client certificates)
  - No auth (dev/test mode)
- [x] Payload format detection (JSON/MessagePack) - 2025-12-24
- [x] Router with category extraction - 2025-12-24
- [x] JSON flattening transform - 2025-12-24
- [x] Timestamp validation/correction - 2025-12-24
- [x] Buffer manager with 3 flush triggers - 2025-12-24
- [x] Native columnar buffer (klickhouse RawRow) - 2025-12-24
- [x] ClickHouse inserter with retry logic - 2025-12-24
- [x] Pipeline orchestrator - 2025-12-24
- [x] Graceful shutdown with buffer drain - 2025-12-24
- [x] Metrics and health endpoints (Phase 8) - 2025-12-24
- [x] Integration tests - 2025-12-24
- [x] Schema introspection (Phase 6.2) - 2025-12-24
  - ParsedType for runtime type parsing
  - SchemaCache with TTL and get_or_fetch
  - ColumnInfo and TableSchema structs
  - CoercionConfig with type mappings and null handling
- [x] Type Coercion (Phase 4.2) - 2025-12-24
  - All ClickHouse types: Int, UInt, Float, Decimal, String, Bool
  - DateTime/DateTime64 with epoch detection (s/ms/us/ns)
  - UUID normalization (standard, no-hyphens, braces)
  - IPv4/IPv6 validation and integer conversion
  - Array and Map coercion with element types
  - Null handling: Default/Error/Passthrough strategies
  - Config-driven type mappings for extensibility

---

## Blocked

- [x] **Full zero-copy path** - Unblocked! NativeBuffer using klickhouse RawRow - 2025-12-24

---

## Backlog (Post-MVP)

- [ ] **GeoIP Enrichment (9.1)** - MaxMind MMDB support
- [ ] **IP Reputation (9.2)** - VPN, Tor, proxy detection
- [ ] **Risk Scoring (9.3)** - Malicious, scanner, spam classification
- [ ] **CLI status command (8.2.2)** - Show running instance status
- [ ] **CLI top command (8.2.4)** - Real-time statistics display
- [ ] **Full E2E tests with testcontainers (10.2.3)**
- [ ] **Benchmark comparison with Go version (10.3.4)**

---

## Parity Tracking vs Go Version

Run parity checks to ensure feature completeness:

```bash
# Compare feature lists
diff <(grep -r "func " /projects/clickhouse-loader/internal/) \
     <(grep -r "pub fn " /projects/dfe-loader-clickhouse/src/)
```

| Feature                 | Go | Rust | Notes                    |
| ----------------------- | -- | ---- | ------------------------ |
| Kafka SCRAM auth        | ✅ | ✅   | Production default       |
| Kafka OAuth/OIDC        | ❌ | ✅   | Enhancement              |
| Kafka AWS IAM           | ❌ | ✅   | Enhancement              |
| Type coercion           | ✅ | ✅   | Config-driven            |
| Schema introspection    | ✅ | ✅   | system.columns           |
| Batch salvage           | ✅ | ⏳   | Pending                  |
| DLQ producer            | ✅ | ⏳   | Pending                  |
| Circuit breaker         | ✅ | ⏳   | Pending                  |

---

## Notes

- Build with `CARGO_BUILD_JOBS=2` to limit resource usage
- Test environment: k8s.tyrell.com.au (see .env for credentials)
- All configs support: ENV vars, config files, K8s mounted files

---

**Last Updated:** 2025-12-24
