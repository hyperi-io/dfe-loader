<!--
  Project:      dfe-loader
  File:         docs/pipeline/README.md
  Purpose:      Index for the per-message pipeline
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Pipeline

The hot path: bytes in, ClickHouse row out. The pass runs per message, and the batching is per table: each table has a separate buffer, flushed as INSERTs of its rows.

```mermaid
flowchart LR
    accTitle: L4 Detail -- Per-message pass, per-table batches
    accDescr: Each message goes through one pass that parses, routes, extracts and enriches it. The row lands in the buffer of the table it was routed to, such as dfe.main or dfe.auth. Each table's buffer flushes as INSERTs into ClickHouse.

    pass["Per-message pass"]

    subgraph buffers["Per-table buffers"]
        direction TB
        main["dfe.main"] ~~~ auth["dfe.auth"] ~~~ other["Each other table"]
    end

    insert["INSERTs<br/>per table"]
    clickhouse[("ClickHouse")]

    pass -->|rows| buffers
    buffers -->|batches| insert
    insert --> clickhouse

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef store fill:#DDF3EC,stroke:#00785A,stroke-width:1px,color:#0B3327
    classDef group fill:none,stroke:#64748B,stroke-width:1px,stroke-dasharray:2 4
    class clickhouse store
    class buffers group
```

*Figure 1. L4 Detail -- Per-message pass, per-table batches. Each buffer flushes on a row, byte or age trigger.* Up: [L3 Components -- dfe-loader](../README.md).

## Docs

- [overview.md](overview.md) -- the stages, in order.
- [routing.md](routing.md) -- choosing `db.table` from message fields.
- [capture-modes.md](capture-modes.md) -- what is stored as `_json` / `_raw`,
  and the three-level precedence.
- [enrichment.md](enrichment.md) -- GeoIP, reputation, risk scoring.
- [parallelism.md](parallelism.md) -- the parallel and sequential phases of a batch.

For the buffer-and-insert tail see
[../clickhouse/insert-formats.md](../clickhouse/insert-formats.md) and the
resilience model in [../architecture.md](../architecture.md#resilience).
