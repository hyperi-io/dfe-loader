# DFE Loader S3 - Work Breakdown Structure

**Status:** Placeholder - Separate Project
**Purpose:** Direct S3/Object Storage loading (bypass ClickHouse for archival)
**Location:** TBD (separate repository)

---

## Overview

The S3 Loader is a companion service to dfe-loader that:

1. Consumes from the same transport layer (Kafka/Zenoh)
2. Writes directly to S3-compatible object storage
3. Uses Parquet format for columnar efficiency
4. Partitions by time and optionally by org_id/event_category

---

## Architecture Context

```text
                    ┌──► dfe-loader ──► ClickHouse (query)
Transport ──────────┤
(Kafka/Zenoh)       └──► dfe-loader-s3 ──────────► S3/MinIO (archive)
```

**Alternative: Single loader with dual output**
```text
Transport ──► dfe-loader ──┬──► ClickHouse (hot data)
                           └──► S3 (cold archive)
```

---

## Key Requirements (TBD)

- [ ] Parquet output format (columnar, compressed)
- [ ] Time-based partitioning (hourly/daily)
- [ ] Optional org_id/event_category partitioning
- [ ] S3-compatible API (AWS S3, MinIO, R2, GCS)
- [ ] Buffering strategy (memory + local disk)
- [ ] Exactly-once semantics (idempotent writes)

---

## Design Questions

### 1. Separate Service vs Dual-Output Loader?

| Option | Pros | Cons |
|--------|------|------|
| **Separate** | Independent scaling, simpler code | Duplicate parsing, 2x Kafka consumers |
| **Dual-output** | Single parse, shared transform | Complex failure handling, coupling |

**Recommendation:** Start separate, consider merging if parsing becomes bottleneck.

### 2. Parquet vs Arrow IPC?

| Format | Compression | Query Support | Streaming |
|--------|-------------|---------------|-----------|
| **Parquet** | Excellent | DuckDB, Spark, ClickHouse | Chunked |
| **Arrow IPC** | Moderate | Arrow-native only | Streaming |

**Recommendation:** Parquet for archival (better compression, wider tool support).

### 3. Partitioning Strategy?

```
s3://bucket/
  └── org_id=acme/
      └── event_category=auth/
          └── year=2025/
              └── month=12/
                  └── day=29/
                      └── hour=14/
                          └── data-001.parquet
```

### 4. Buffer Strategy?

- **Memory buffer:** Accumulate N rows or M MB
- **Local disk:** Spill to disk on memory pressure
- **Upload trigger:** Time-based (every N minutes) or size-based

---

## Dependencies

- `hs-rustlib` transport module (shared with dfe-loader)
- `arrow` + `parquet` crates for Parquet writing
- `aws-sdk-s3` or `object_store` crate for S3 API

---

## Out of Scope for dfe-loader

This project is explicitly separate from dfe-loader to:

1. Keep ClickHouse loader focused and simple
2. Allow independent deployment and scaling
3. Avoid coupling hot-path (ClickHouse) with cold-path (S3)
4. Enable different SLAs (ClickHouse = low latency, S3 = high throughput)

---

**Last Updated:** 2025-12-29
**Status:** Placeholder - awaiting architectural decision on separate vs dual-output

