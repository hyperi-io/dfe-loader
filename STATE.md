# Project State

**Project:** dfe-loader-clickhouse
**Purpose:** High-performance Kafka to ClickHouse data loader (Rust port of Go clickhouse-loader)
**Status:** MVP in progress

---

## Build Settings

```bash
# Limit Cargo resource consumption
export CARGO_BUILD_JOBS=2
```

---

## Current Session (2025-12-24)

### Session Goals

- [x] Complete Phase 2: Kafka Consumer with SASL/SCRAM
- [x] Complete Phase 3: Router with category extraction
- [x] Complete Phase 4: JSON Transform pipeline
- [ ] Complete remaining pipeline phases

### Progress

**Completed:**

- Cargo.toml with all dependencies (sonic-rs, klickhouse, rdkafka, etc.)
- Config module with full cascade loading
- Kafka consumer with SASL/SCRAM authentication
- Payload format detection (Auto, ForceJson, ForceMessagePack)
- Router with category extraction and table mapping
- JSON flattening transform
- Timestamp validation/correction

**In Progress:**

- Transform pipeline integration
- Buffer manager
- ClickHouse inserter

**Blocked:**

- None

---

## Project Overview

### Architecture

Kafka → Consumer → PayloadDetect → Router → Transform → Buffer → ClickHouse

### Key Components

1. **Kafka Consumer** - SASL/SCRAM, at-least-once delivery
2. **Payload Detector** - Auto-detect JSON/MessagePack, caching
3. **Router** - Category field extraction, table mapping
4. **Transform** - Flatten, timestamp validation
5. **Buffer Manager** - Per-table columnar buffers
6. **ClickHouse Inserter** - klickhouse native protocol

### Tech Stack

- **Language:** Rust
- **JSON:** sonic-rs (SIMD-accelerated)
- **MessagePack:** rmp-serde
- **Kafka:** rdkafka with SASL/SCRAM
- **ClickHouse:** klickhouse (native protocol)
- **Async:** tokio

---

## Library Decisions

| Purpose | Library | Rationale |
|---------|---------|-----------|
| JSON | sonic-rs | SIMD, fastest benchmarks |
| ClickHouse | klickhouse | Native protocol, async |
| Kafka | rdkafka | librdkafka bindings, SASL/SCRAM |
| MessagePack | rmp-serde | Serde integration |
| UUID | uuid v4+v7 | v7 for time-ordered indexing |

---

## Test Environment

Located at k8s.tyrell.com.au with:
- ClickHouse: port 30900 (native), 30123 (HTTP)
- Kafka: port 30092 with SCRAM-SHA-512
- See `.env` for credentials

---

**Last Updated:** 2025-12-24
**Version:** 0.1.0
**Status:** Development
