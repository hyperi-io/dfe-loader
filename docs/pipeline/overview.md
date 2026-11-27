<!--
  Project:      dfe-loader
  File:         docs/pipeline/overview.md
  Purpose:      The per-message hot path, stage by stage
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# The pipeline

A message becomes a ClickHouse row in one pass. The hot path does the cheap work per message and defers the expensive work to flush: parse once, promote only the columns the table declares, keep the full payload as a reference-counted `Arc<[u8]>`, and splice it in as `_json` only at serialise time. The tail batches per table and inserts.

The pass is eight steps in two stages: the first decides which table a record goes to, and the second builds its row and writes it.

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
    accTitle: L4 Detail -- Loader pipeline
    accDescr: Two stages run in order. Read and route takes message bytes, splits batched messages, parses JSON and routes each record to a table. Extract and write extracts the header and schema columns, enriches, buffers per table, then inserts and commits offsets.

    subgraph pass["Loader pipeline"]
        direction TB
        subgraph s1["1 Read and route"]
            direction TB
            subgraph s1a["Read"]
                direction LR
                bytes(["<b>Message bytes</b>"]) --> split("<b>Split batches</b>") --> parse("<b>Parse JSON</b>")
            end
            subgraph s1b["Route"]
                direction LR
                route("<b>Route to table</b>")
            end
            s1a --> s1b
        end
        subgraph s2["2 Extract and write"]
            direction TB
            subgraph s2a["Extract"]
                direction LR
                extract("<b>Extract columns</b>") --> enrich("<b>Enrich</b>")
            end
            subgraph s2b["Write"]
                direction LR
                buffer("<b>Buffer per table</b>") --> insert(["<b>Insert and commit</b>"])
            end
            s2a --> s2b
        end
        s1 --> s2
    end
  class bytes,split,parse,route,extract,enrich,buffer,insert part
  class bytes,split,parse,route,extract,enrich,buffer,insert part
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
  style pass fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s1a fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s1b fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s2a fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style s2b fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Loader pipeline. Read each stage left to right, then down.* Up: [Pipeline](README.md).

The stages, in order, all in `src/pipeline/` (`MessageProcessor::process` is the
per-message entry point):

## 0. Split batched messages

Every stage below this takes one message to be one record, so a message
carrying several is split into one message per record before anything else
runs. Two wire formats are batches, counted apart so a dashboard says which
producer is batching:

| Format | Producer it came from | Counters |
|-------|----------------------|----------|
| a non-empty JSON array of objects | dfe-receiver forwarding a batched POST | `dfe_loader_batched_array_messages_total` / `dfe_loader_batched_array_records_total` |
| newline-separated JSON objects | dfe-transform-elastic emitting its output | `dfe_loader_batched_ndjson_messages_total` / `dfe_loader_batched_ndjson_records_total` |

Each element keeps its source message's topic, partition and offset, so a batch
still commits as one unit. What is not a batch of records goes through
untouched, so the format check and the DLQ see exactly what arrived: a scalar
array, an empty array, a single object however it is formatted, a truncated
tail, and any body with elements that are not all objects.

The split belongs at the consumer, so a producer's choice of wire format cannot lose records. An array that reaches the processor unsplit is refused as `payload is a JSON array, not a record object` rather than coming back as a ClickHouse encode error on an empty column.

## 1. Parse

A payload must open a JSON object or array (`{` or `[` after any leading
whitespace) and is parsed with sonic-rs (SIMD). The original bytes are kept once as `Arc<[u8]>`, so the full payload can be written as `_raw` or `_json` later without re-serialising. Anything else is dead-lettered as `payload is not JSON` with its first 8 bytes in hex, and a payload that opens like JSON but does not parse is dead-lettered with the parse error.

JSON is the only payload format. MessagePack, which DFE/XDR 2.0 and 2.1 accepted, was removed in DFE 2.2, and a MessagePack record is dead-lettered as not JSON: the JSON path (SIMD parsing with sonic-rs, zstd on the wire) is fast enough that MessagePack gave no CPU saving.

## 2. Route

Routing picks `db.table` from the message before any flattening, using the configured `routing.table_fields` (dot-notation for nested access). No field carrying a value falls back to the topic's source, then `routing.default_table`, and a table ClickHouse says does not exist falls back to `routing.default_table` as well. The database is `routing.default_db` unless `routing.org_routes` lists the record's `_org_id` tenant. See [routing.md](routing.md).

## 3. Extract

The target table's schema (reflected and cached, see
[../clickhouse/schema-cache.md](../clickhouse/schema-cache.md)) decides which
fields are promoted to typed columns. The header extractor pulls the common
header fields, and the rest of the declared columns are pulled from the payload by
name. Anything not promoted stays in the payload, available as `_json`.

## 4. Enrich

Optional enrichment (`geoip`, `reputation`, `risk`) injects flat columns onto
the promoted row only -- never into `_json`. Each is a toggle, and the lookups are
cached. See [enrichment.md](enrichment.md).

## 5. Buffer and capture

The promoted row, plus whatever `capture_mode` dictates for `_json` and `_raw`,
lands in the per-table buffer (rows + raw payloads + Kafka offsets). Capture
mode is resolved per table with a three-level precedence (DDL directive >
per-table config > global). See [capture-modes.md](capture-modes.md).

## 6. Insert and commit

The encoder reshapes only the values ClickHouse cannot take as sent: epoch vs ISO timestamps, hyphen-less UUIDs, integer IPv4, every geo_point form for a `Point`, and a single value for an `Array` (see [../clickhouse/types.md](../clickhouse/types.md)). Each table's buffer flushes on its triggers (`flush_rows` / `flush_bytes` / `flush_age_secs`). The batch is encoded and inserted through `dfe-clickhouse`'s `dynamic` module (RowBinary by default). Kafka offsets commit once per flush cycle, and on each partition stop below the lowest offset not placed yet -- at-least-once. Batch salvage isolates a row ClickHouse rejects for good and sends it to the DLQ. A batch that fails for any other reason is held and retried with jittered backoff until it lands. See [../operations.md](../operations.md#when-inserts-fail) and [../clickhouse/insert-formats.md](../clickhouse/insert-formats.md).

## Parallelism

The hot path scales across a worker pool with per-table batching downstream. [parallelism.md](parallelism.md) describes the parallel and sequential phases.
