<!--
  Project:      dfe-loader
  File:         docs/clickhouse/README.md
  Purpose:      Index for the ClickHouse insert layer
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# ClickHouse insert layer

The ClickHouse insert layer is the last part of [dfe-loader](../architecture.md): it writes each table's batch to ClickHouse and sends the rows ClickHouse refuses for good to the DLQ.

| | |
|---|---|
| Repo | [hyperi-io/dfe-loader](https://github.com/hyperi-io/dfe-loader) |
| Image and chart | `ghcr.io/hyperi-io/dfe-loader`, and the thin chart released beside it |
| Config reference | `clickhouse`, `schema` and `buffer` in [config-schema.yaml](../config-schema.yaml) |
| Endpoints | None of its own. It dials ClickHouse over HTTP, or the native protocol with `clickhouse.protocol: native` |
| Metrics | `rows_inserted_total`, `batches_flushed_total`, `clickhouse_connected`, `scaling_circuit_open`, in [operations.md](../operations.md#observe) |

No `Row` type is compiled in: the schema is read at runtime and each `Map<String, Value>` row is encoded straight to binary.

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
    accTitle: L4 Detail -- Insert layer
    accDescr: The per-table buffer hands batches to the insert layer. Inside, the Inserter passes rows to dfe_clickhouse's DynamicInsert, which writes them through the ChClient. Rows land in ClickHouse, and rows ClickHouse refuses for good go to the DLQ.

    buffer["<b>Buffer per table</b>"]

    subgraph insert["Insert"]
        direction LR
        inserter["<b>Inserter</b>"] --> dynamic["<b>DynamicInsert</b>"] --> chclient["<b>ChClient</b>"]
    end

    clickhouse[("<b>ClickHouse</b>")]
    dlq[("<b>DLQ</b>")]

    buffer -->|batches| insert
    insert -->|rows| clickhouse
    insert -.->|dead letters| dlq
  class buffer,inserter,dynamic,chclient part
  class clickhouse store
  class dlq bus
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
  style insert fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Insert layer. The arrow to DLQ is the salvage path.* Up: [L3 Components -- dfe-loader](../architecture.md). Down: [L4 Detail -- Dynamic insert](#how-it-works), [L4 Detail -- Insert outcome](#scaling-and-failure).

## How it works

The `clickhouse` crate keeps insertion typed (`T: Row`, compile-time schema) --
that is the right upstream default. dfe-loader inserts
`Map<String,Value>` rows with columns from `system.columns` at runtime.
That dynamic layer comes from `dfe-clickhouse`'s `dynamic` module, on top of the upstream
`Client`. The rows are encoded against the schema read at runtime, then sent as RowBinary through the official client over HTTP, or as Native blocks through `dfe_clickhouse::TcpClient` over native.

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
    accTitle: L4 Detail -- Dynamic insert
    accDescr: The Inserter hands rows to dfe_clickhouse's dynamic module. It reads the table's schema from system.columns, encodes each row against it with DynamicRow, and DynamicInsert sends the RowBinary through the clickhouse crate's insert_formatted_with.

    inserter["<b>Inserter</b>"]

    subgraph dynamic["dfe_clickhouse::dynamic"]
        direction LR
        schema["<b>fetch_dynamic_schema</b>"] --> encode["<b>DynamicRow</b>"] --> send["<b>DynamicInsert</b>"]
    end

    client["<b>clickhouse::Client</b><br/><span>insert_formatted_with</span>"]

    inserter -->|rows| dynamic
    dynamic -->|RowBinary| client
  class inserter,schema,encode,send part
  class client ext
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
  style dynamic fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 2. L4 Detail -- Dynamic insert, over HTTP. clickhouse::Client is the crates.io client.* Up: [L4 Detail -- ClickHouse insert layer](README.md).

`ParsedType` parses each column's type string for the encoder. A JSONEachRow insert skips the encoder: `shape_for_type` rewrites the values the server would refuse and the rows go straight to `insert_formatted_with`.

## Interfaces

`ChClient` is the official `clickhouse::Client` over HTTP, or `dfe_clickhouse::TcpClient` with `clickhouse.protocol: native`. The schema cache both clients read is in [schema-cache.md](schema-cache.md).

Over HTTP every insert goes through `insert_formatted_with`: `FORMAT RowBinaryWithNamesAndTypes`, one INSERT for each set of defaulted columns the batch's rows supply, or `JSONEachRow`. With `clickhouse.protocol: native` the rows go out as `FORMAT Native` blocks through `TcpClient::insert_native`, and `JSONEachRow` is refused at startup. See [clickhouse-ext.md](clickhouse-ext.md).

`capture_mode` decides whether the full payload is kept beside the promoted columns, as `_json`, `_raw` or neither, resolved per table from the DDL tag, then per-table config, then the global default. Under the default `full` a JSON record is stored once, in `_json`. [capture-modes.md](../pipeline/capture-modes.md) has the four modes.

## Scaling and failure

A failed RowBinary insert ends one of three ways: held for a later flush, retried against a fresh schema, or split so only its bad rows reach the DLQ.

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
    accTitle: L4 Detail -- Insert outcome
    accDescr: A successful insert lets its offsets join the flush cycle's commit. A timeout, a dropped connection or a server code on the retry list holds the batch. Any other refusal against a changed table is retried against the current schema, and one against an unchanged table goes to salvage and the DLQ.

    subgraph outcome["Insert outcome"]
    direction TB
    subgraph R1["1 Insert"]
        direction LR
        ok{"<b>Insert<br/>answered ok?</b>"}
        commit(["<b>Offsets commit</b>"])
        ok -->|yes| commit
    end
    subgraph R2["2 Failed"]
        direction LR
        retryable{"<b>Retryable<br/>failure?</b>"}
        held(["<b>Batch held</b>"])
        retryable -->|yes| held
    end
    subgraph R3["3 Not retryable"]
        direction LR
        changed{"<b>Table<br/>changed?</b>"}
        retry(["<b>Retry on<br/>fresh schema</b>"])
        salvage(["<b>Salvage<br/>to DLQ</b>"])
        changed -->|yes| retry
        changed -->|no| salvage
    end
    R1 -->|no| R2
    R2 -->|no| R3
    end
  class ok,retryable,changed decision
  class commit,held,retry part
  class salvage error
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
  style outcome fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R3 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 3. L4 Detail -- Insert outcome. Salvage to DLQ is the end that dead-letters rows.* Up: [L4 Detail -- ClickHouse insert layer](README.md).

`Inserter::dynamic_verdict` in `src/clickhouse/inserter.rs` makes the call. Retryable means a timeout, a dropped connection or a code on the retry list in `src/clickhouse/error.rs`. A stale or unreadable cached schema is retried on a fresh read without asking the table, and an encoder refusal of a value goes straight to salvage.

The RowBinary path ships `FORMAT RowBinaryWithNamesAndTypes` through
`insert_formatted_with`, and the header's types make the server refuse rows encoded against a type the column no longer has. A refusal is blamed on the rows once a fresh read shows
the table unchanged since they were encoded, and otherwise the retry encodes
against the current schema. Over native the block declares the types.

A rejected insert is classified by the server's exception code, read with
`dfe_clickhouse::ServerException::parse` over HTTP and carried structurally
over native, never by the words of the message:
a parse rejection echoes the offending row back. The loader keeps at most 1 MiB
of any error text, because the client reads a whole error body into memory.

## Key metrics

- `rows_inserted_total` and `batches_flushed_total`: what landed.
- `dfe_loader_permanent_reject_total`, per table: rows ClickHouse refused for good.
- `clickhouse_connected`: 0 once an insert went unanswered.
- `scaling_circuit_open`: 1 after a cycle in which every insert failed.

## Related

Read in this order:

- [clickhouse-ext.md](clickhouse-ext.md) -- the dynamic insert from
  `dfe-clickhouse`'s `dynamic` module, and how the inserter reads its failures.
- [insert-formats.md](insert-formats.md) -- RowBinary vs JSONEachRow, the
  transport x format matrix, and why RowBinary is the default.
- [schema-cache.md](schema-cache.md) -- reflecting `system.columns`, the TTL
  cache, and mismatch recovery.
- [types.md](types.md) -- the ClickHouse type reference and how each type is
  encoded.
- [ddl-directives.md](ddl-directives.md) -- the `@`-comment directives the
  loader reads off table DDL (capture mode, field mapping).
- [tls.md](tls.md) -- TLS transport security and private-CA trust.
