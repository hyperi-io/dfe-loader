# Receiver - Work Breakdown Structure

**Status:** Placeholder - Separate Project
**Purpose:** HTTP ingestion service replacing Vector.dev receiver
**Location:** TBD (separate repository)

---

## Overview

The Receiver is an HTTP ingestion service that:

1. Accepts HTTP POST from ESH (Edge Security Hub)
2. Buffers to local disk (survives restart, handles downstream unavailable)
3. Forwards to transport (Kafka or Zenoh)
4. Returns 200 OK after local buffer write (fast response to ESH)

---

## Architecture Context

```text
ESH ──HTTP POST──► RECEIVER ──Transport──► LOADER ──► ClickHouse/S3
                      │
                 [Local Buffer]
                 (disk cache)
```

---

## Key Requirements (TBD)

- [ ] HTTP endpoint for ESH
- [ ] Local disk buffer for resilience
- [ ] Transport output (Kafka or Zenoh)
- [ ] Backpressure handling
- [ ] Metrics and health endpoints

---

## Vector.dev Replacement Details

*To be provided - current Vector.dev receiver configuration and behavior*

---

## Dependencies

- `hs-rustlib` transport module (shared with dfe-loader-clickhouse)

---

**Last Updated:** 2025-12-29
**Status:** Placeholder - awaiting Vector.dev details
