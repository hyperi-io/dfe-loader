<!--
  Project:      dfe-loader
  File:         docs/architecture.md
  Purpose:      System architecture: layers, the dynamic insert, insert dispatch
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Architecture

dfe-loader is a per-message hot path with a per-table batching tail. A message arrives as bytes, gets routed and promoted into a typed row, accumulates in a per-table buffer, and flushes to ClickHouse as a batch. Kafka offsets commit once per flush cycle, never past a row not placed yet -- at-least-once.

Everything on the hot path follows one rule: do the cheap thing per
message, defer the expensive thing to flush. Parse once (SIMD),
promote only schema columns, keep the full payload as a reference-counted
`Arc<[u8]>`, and splice it as `_json` only at serialise time.

## Layers

```mermaid
flowchart TB
    subgraph L4["L4 -- Orchestration"]
        ORC["Orchestrator<br/>wiring, shutdown, hot-reload"]
        PROC["MessageProcessor<br/>per-message pipeline"]
    end
    subgraph L3["L3 -- Transform"]
        ROUTE["Router (sonic-rs)"]
        EXTRACT["HeaderExtractor"]
        ENRICH["Enrichment (geoip/rep/risk)"]
    end
    subgraph L2["L2 -- Buffer + insert"]
        BUF["BufferManager<br/>per-table rows + raw + offsets"]
        INS["Inserter<br/>format dispatch, retry, salvage"]
    end
    subgraph L1["L1 -- ClickHouse layer"]
        QC["ClickHouseQueryClient<br/>schema, comments, health"]
        EXT["dfe_clickhouse::dynamic<br/>ParsedType, encoder, DynamicInsert"]
        SC["SchemaCache"]
    end
    subgraph Client["crates.io clickhouse + dfe-clickhouse"]
        CLIENT["ChClient: clickhouse::Client (HTTP) or dfe_clickhouse::TcpClient (native)"]
        EXC["dfe_clickhouse::ServerException"]
    end

    ORC --> PROC --> L3 --> BUF --> INS
    INS --> EXT --> CLIENT
    INS --> QC --> CLIENT
    INS --> EXC
    EXT --> SC
    QC --> SC
```

A higher layer depends on lower layers, never the reverse. The insert path's
external dependencies are the official `clickhouse` crate's HTTP `Client`, and
`dfe-clickhouse` for the native-protocol `TcpClient` and for reading the
server's exception code out of an HTTP rejection so the inserter can classify it.

## The dynamic insert

The `clickhouse` crate keeps insertion typed (`T: Row`, compile-time schema) --
that is the right upstream default. dfe-loader inserts
`Map<String,Value>` rows whose columns come from `system.columns` at runtime.
That dynamic layer comes from `dfe-clickhouse`'s `dynamic` module, on top of the upstream
`Client`:

```mermaid
flowchart LR
    subgraph loader["dfe-loader"]
        INS["Inserter"]
        subgraph ext["dfe_clickhouse::dynamic"]
            PT["ParsedType<br/>runtime CH type parser"]
            ENC["DynamicRow encoder<br/>Map -> RowBinary, per column"]
            SCH["schema fetch<br/>system.columns -> ColumnDef"]
            DI["DynamicInsert<br/>write_map + recovery"]
        end
    end
    subgraph client["clickhouse::Client"]
        IFW["insert_formatted_with<br/>FORMAT RowBinaryWithNamesAndTypes | JSONEachRow (HTTP)"]
    end

    INS --> DI
    DI --> ENC --> PT
    DI --> SCH
    DI -->|RowBinary| IFW
    INS -->|JSONEachRow fallback| IFW
```

Over HTTP every insert goes through `insert_formatted_with`: `FORMAT RowBinaryWithNamesAndTypes`, one INSERT for each set of defaulted columns the batch's rows supply, or `JSONEachRow`. With `clickhouse.protocol: native` the rows go out as `FORMAT Native` blocks through `TcpClient::insert_native`, and `JSONEachRow` is refused at startup. See [clickhouse/clickhouse-ext.md](clickhouse/clickhouse-ext.md).

## Insert dispatch

```mermaid
flowchart TB
    F{"insert_format?"}
    RB["DynamicInsert.write_map(s)"]
    HTTP["Client.insert_formatted_with<br/>FORMAT RowBinaryWithNamesAndTypes (HTTP)"]
    JE["Client.insert_formatted_with<br/>FORMAT JSONEachRow (HTTP)"]
    SM{"SchemaMismatch?"}
    REC["invalidate schema cache<br/>re-fetch + retry"]

    F -->|row_binary default| RB --> HTTP
    F -->|json_each_row| JE
    HTTP --> SM
    SM -->|yes| REC --> RB
    SM -->|no| OK["offsets join the one commit of the flush cycle"]
```

The RowBinary path ships `FORMAT RowBinaryWithNamesAndTypes` through
`insert_formatted_with`, and the header's types make the server refuse rows encoded against a type the column no longer has. A refusal is the rows' own only once a fresh read shows
the table unchanged since they were encoded, and otherwise the retry encodes
against the current schema. Over native the block declares the types.

A rejected insert is classified by the server's exception code, read with
`dfe_clickhouse::ServerException::parse` over HTTP and carried structurally
over native, never by the words of the message:
a parse rejection echoes the offending row back. The loader keeps at most 1 MiB
of any error text, because the client reads a whole error body into memory.

## Capture modes

`capture_mode` decides what gets stored alongside the promoted columns.
Resolution is three-level, highest wins: DDL `@capture_mode` comment >
per-table config > global default.

| Mode | `_json` | `_raw` | Use |
|------|---------|--------|-----|
| `full` (default) | full payload (JSON type) | the record's own raw field, else NULL | path queries + text search |
| `json_only` | full payload (JSON type) | NULL | path queries without the `_raw` overhead |
| `raw_only` | NULL, unless a directive fills it | full Kafka payload (String) | CH CPU saving, no JSON-type overhead |
| `extracted_only` | NULL, unless a directive fills it | NULL | promoted columns only |

All four still promote schema columns, and differ only in whether, and where,
the full payload is kept. A directive on `_json` makes it a promoted column under every mode ([pipeline/capture-modes.md](pipeline/capture-modes.md)). Under `full` a JSON record is stored once, in `_json`: `_raw` holds only raw data the source sent with it, such as a syslog line the receiver captured.

## Resilience

- A data error binary-splits the batch down to the bad rows, which go to the DLQ, and the rest land.
- A flush cycle in which every insert failed opens the circuit gate on the KEDA scaling signal, and the next successful insert closes it. A failed insert never sends its batch to the DLQ.
- Drift, a column dropped or retyped by `ALTER`, drops the cached schema, and the retry encodes against a fresh read. An added column breaks no insert, because the INSERT names its columns, so it takes its default until the cached schema refreshes.
- Offsets commit once per cycle, and on each partition stop below the lowest offset not placed yet. A failed batch or a dead letter the DLQ refused is held and retried with jittered backoff, and holds its partition's commit until it lands. A dead letter no DLQ backend can ever hold is dropped and counted instead (`pipeline_dead_letters_dropped_total{reason}`). A held batch whose table ClickHouse has since reported absent is retried against the default table.
- Rows of a partition a rebalance has moved away are discarded before any write, since the partition's next owner reads them again from the committed offset (`transport_revoke_discarded_total{stage="buffer"}`). See [operations.md](operations.md#rebalances).
