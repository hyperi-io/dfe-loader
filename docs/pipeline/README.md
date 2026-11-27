<!--
  Project:      dfe-loader
  File:         docs/pipeline/README.md
  Purpose:      Index for the per-message pipeline
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Pipeline

The pipeline is the hot path of [dfe-loader](../architecture.md): bytes in, a typed row in its table's buffer out.

| | |
|---|---|
| Repo | [hyperi-io/dfe-loader](https://github.com/hyperi-io/dfe-loader) |
| Image and chart | `ghcr.io/hyperi-io/dfe-loader`, and the thin chart released beside it |
| Config reference | `payload`, `routing`, `field_mapping`, `computed_columns`, `enrichment`, `geoip`, `metadata` and `buffer` in [config-schema.yaml](../config-schema.yaml) |
| Endpoints | None of its own |
| Metrics | `messages_received_total`, `loader_messages_by_table_total`, `loader_buffer_rows_by_table`, `dfe_loader_unknown_table_fallback_total`, in [operations.md](../operations.md#observe) |

The pass runs per message, and the batching is per table: each table has a separate buffer, flushed as INSERTs of its rows.

```mermaid
---
config:
  theme: base
  look: neo
  fontFamily: Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif
  flowchart:
    curve: linear
    nodeSpacing: 40
    rankSpacing: 64
    padding: 18
    wrappingWidth: 280
    htmlLabels: true
  elk:
    preset: modelOrder
  themeVariables:
    fontSize: 15px
    lineColor: "#64748B"
    primaryTextColor: "#0F172A"
    nodeBorder: "#334155"
    clusterBorder: "#0072B2"
    edgeLabelBackground: "#FFFFFF"
  themeCSS: >-
    p{margin:0}
    .node[data-look] rect,.node[data-look] path,.node[data-look] polygon,.node[data-look] .outer-path{filter:drop-shadow(0 1px 2px rgba(15,23,42,.10)) drop-shadow(0 6px 14px rgba(15,23,42,.10))}
    .node rect{rx:10px;ry:10px}
    .cluster rect{rx:16px;ry:16px}
    .node .label div{min-width:150px}
    .node.decision .label div{min-width:0}
    .cluster[data-look] rect{filter:none}
    .nodeLabel{line-height:1.35}
    .nodeLabel b{font-weight:600;font-size:15px;letter-spacing:-0.01em}
    .nodeLabel span{font-size:12.5px;font-weight:400;color:#334155}
    .cluster-label span.nodeLabel{display:inline-block;background:#FFFFFF;color:#0B2A3F;border:1.5px solid #0072B2;border-radius:999px;padding:2px 12px;font-size:11.5px;font-weight:700;letter-spacing:.09em;text-transform:uppercase}
    .flowchart-link{stroke-width:1.75px}
    .labelBkg{background:transparent!important}
    span.edgeLabel{display:inline-block;background:#FFFFFF!important;color:#1E293B;border:1px solid #94A3B8;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:500}
    span.edgeLabel p{background:transparent!important}
    .label-icon{width:20px;height:20px;vertical-align:-4px;margin-right:6px}
    .icon-shape path[stroke-width="0"][fill="none"]{fill:#FFFFFF;stroke:#FFFFFF;stroke-width:12px;stroke-linejoin:round}
    .icon-shape p{background:transparent!important;color:#0F172A}
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L4 Detail -- Per-message pass, per-table batches
    accDescr: Each message goes through one pass that parses, routes, extracts and enriches it. The row lands in the buffer of the table it was routed to, such as dfe.main or dfe.auth. Each table's buffer flushes as INSERTs into ClickHouse.

    transport("<b>Transport</b><br/><span>Kafka or gRPC</span>")
    subgraph pipeline["Pipeline"]
        direction TB
        pass("<b>Per-message pass</b><br/><span>parse, route, extract, enrich</span>")
        subgraph buffers["Per-table buffers"]
            direction LR
            main("<b>dfe.main</b>")
            auth("<b>dfe.auth</b>")
            other("<b>Each other table</b>")
            main ~~~ auth ~~~ other
        end
        pass -->|rows| buffers
    end
    insert("<b>INSERTs per table</b>")
    clickhouse[("<b>ClickHouse</b>")]
    transport --> pipeline
    pipeline -->|batches| insert
    insert --> clickhouse
  class transport,pass,main,auth,other,insert part
  class clickhouse store
  class pass,main,auth,other,insert part
  class clickhouse store
  classDef ext fill:#F4F6F8,stroke:#94A3B8,stroke-width:1.5px,stroke-dasharray:5 4,color:#0F172A
  classDef ingest fill:#FFFFFF,stroke:#0E8A7D,stroke-width:3px,color:#0F172A
  classDef xform fill:#FFFFFF,stroke:#7B4FB0,stroke-width:3px,color:#0F172A
  classDef load fill:#FFFFFF,stroke:#0072B2,stroke-width:3px,color:#0F172A
  classDef bus fill:#FFFFFF,stroke:#A8517F,stroke-width:3px,color:#0F172A
  classDef store fill:#FFFFFF,stroke:#00785A,stroke-width:3px,color:#0F172A
  classDef ui fill:#FFFFFF,stroke:#D9480F,stroke-width:3px,color:#0F172A
  classDef decision fill:#FFFFFF,stroke:#9A6700,stroke-width:3px,color:#0F172A
  classDef error fill:#FFFFFF,stroke:#C2410C,stroke-width:3px,color:#0F172A
  classDef control fill:#FFFFFF,stroke:#334155,stroke-width:3px,color:#0F172A
  classDef part fill:#FFFFFF,stroke:#94A3B8,stroke-width:1.5px,color:#0F172A
  style pipeline fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style buffers fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Per-message pass, per-table batches. Each buffer flushes on a row, byte or age trigger.* Up: [L3 Components -- dfe-loader](../architecture.md).

## How it works

1. A message carrying several records, a JSON array or newline-separated objects, splits into one message per record. Each keeps its topic, partition and offset, so a batch still commits as one unit.
2. The pass parses each record once with sonic-rs and keeps the bytes as one reference-counted `Arc<[u8]>`. Anything not JSON is dead-lettered.
3. Routing picks `db.table` from `routing.table_fields` before any flattening, and falls back to `routing.default_table`.
4. The fields the table declares are promoted to typed columns. The rest stay in the payload, written as `_json` at serialise time.
5. Enrichment adds flat GeoIP, reputation and risk columns to the row, never to `_json`.
6. The row lands in its table's buffer, which flushes on `buffer.flush_rows`, `flush_bytes` or `flush_age_secs`.

[overview.md](overview.md) has each step, and [parallelism.md](parallelism.md) which parts of a batch run in parallel.

## Interfaces

In: message bytes from the transport, with their topic, partition and offset. Out: per-table batches of typed rows for the ClickHouse insert layer, and dead letters for the DLQ. A record goes to the DLQ when it isn't JSON, is nested deeper than 64 levels, has `_org_id` or `_source` twice, or waits longer than `schema.pending_max_age_secs` for its table.

## Scaling and failure

The pass scales with the worker pool and with pods. Under memory pressure the loader pauses the consumer before the next Kafka poll rather than risk an OOM kill mid-batch. A routing miss is never a dead letter: it lands in the default table. [operations.md](../operations.md#memory-pressure) has the memory guard.

## Key metrics

- `messages_received_total`: records in, after batches are split.
- `loader_messages_by_table_total` and `loader_buffer_rows_by_table`: where records went, and what's waiting to flush.
- `dfe_loader_unknown_table_fallback_total`: records sent to the default table because theirs is gone.

## Related

- [overview.md](overview.md) -- the stages, in order.
- [routing.md](routing.md) -- choosing `db.table` from message fields.
- [capture-modes.md](capture-modes.md) -- what is stored as `_json` / `_raw`,
  and the three-level precedence.
- [enrichment.md](enrichment.md) -- GeoIP, reputation, risk scoring.
- [parallelism.md](parallelism.md) -- the parallel and sequential phases of a batch.

For the buffer-and-insert tail see
[../clickhouse/insert-formats.md](../clickhouse/insert-formats.md) and the
failure handling in [../operations.md](../operations.md#when-inserts-fail).
