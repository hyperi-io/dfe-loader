<!--
  Project:      dfe-loader
  File:         docs/pipeline/capture-modes.md
  Purpose:      capture_mode: what is stored, and the 3-level precedence
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Capture modes

`capture_mode` decides what is stored alongside the promoted columns: the full
payload as `_json` (native JSON type, path-queryable), the raw bytes as `_raw`
(String), or neither. Promoted columns are always written -- capture mode only
governs the two payload-retention columns.

| Mode | `_json` | `_raw` | Use |
|------|---------|--------|-----|
| `full` (default) | full payload (JSON) | a raw field the record sent, else NULL | path queries plus text search |
| `json_only` | full payload (JSON) | NULL | path queries, never a `_raw` copy |
| `raw_only` | NULL, unless a directive fills it | full source payload (String) | save ClickHouse CPU, no JSON-type overhead |
| `extracted_only` | NULL, unless a directive fills it | NULL | promoted columns only |

`_raw` and `_json` are not copies of each other. `_json` is the parsed payload
as a native ClickHouse JSON value (path-based queries). `_raw` is the original
bytes as received (the tailed log line, DB row, or raw syslog). A table can keep
one, both, or neither.

Under `full`, a JSON record fills `_raw` from a raw field it sent (`raw` or `_raw`), kept as sent, such as a line the receiver captured. Any other JSON record is kept once, in `_json`, and `_raw` stays NULL. A text field such as `logoriginal` is not copied into `_raw` by default, because `_json` stores its text. To promote one for a source that needs it, name it in `metadata.raw_source_fields` (first match wins) or in an `@renamed` directive on the `_raw` column.

`metadata.capture_raw` (default `true`) is what lets `raw_source_fields` fill `_raw`. `metadata.capture_json` is read by nothing: `capture_mode` decides `_json`.

A directive on the `_json` column itself, such as `@source: record`, makes `_json` a promoted column. It then holds that field's value, or an empty object when the record has none, under every mode, `raw_only` and `extracted_only` included, and the payload is not spliced in. The DLQ table dfe-schemas renders uses `@source: _json` to keep a dead letter's original payload.

## Precedence

Capture mode is resolved per table: the table's DDL tag wins, then a per-table config entry, then the global default.

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
    .ingest .label-icon{color:#0E8A7D}
    .xform .label-icon{color:#7B4FB0}
    .load .label-icon{color:#0072B2}
    .bus .label-icon{color:#A8517F}
    .store .label-icon{color:#00785A}
    .ui .label-icon{color:#D9480F}
    .ext .label-icon{color:#64748B}
---
flowchart TB
    accTitle: L4 Detail -- Capture mode precedence
    accDescr: A table whose COMMENT carries a capture_mode tag uses that mode. Otherwise a metadata.table_capture_modes entry for the table decides. Otherwise metadata.capture_mode applies.

    subgraph prec["Capture mode precedence"]
        direction TB
        subgraph R1["1 Table tag"]
            direction LR
            tag{"<b>@capture_mode<br/>tag?</b>"}
            tagmode(["<b>The tag's mode</b>"])
            tag -->|yes| tagmode
        end
        subgraph R2["2 Config"]
            direction LR
            entry{"<b>table_capture_modes<br/>entry?</b>"}
            entrymode(["<b>The entry's mode</b>"])
            global(["<b>metadata.capture_mode</b>"])
            entry -->|yes| entrymode
            entry -->|no| global
        end
        R1 -->|no| R2
    end
  class tag,entry decision
  class tagmode,entrymode,global part
  class tag,entry decision
  class tagmode,entrymode,global part
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
  style prec fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Capture mode precedence. The first question answered yes decides.* Up: [Pipeline](README.md).

- A `@capture_mode: <mode>` tag in the table's `COMMENT` wins over any config, since the table owns its capture policy. See [../clickhouse/ddl-directives.md](../clickhouse/ddl-directives.md#table-level-tags).
- `metadata.table_capture_modes` maps a `db.table` to a mode.
- `metadata.capture_mode` applies to any table without a more specific setting.

The processor tests in `src/pipeline/processor.rs` check the row each mode writes, on a table with no directive on `_json`:

- `full` -> the written row has `_json` populated, and `_raw` when the record sent a raw field.
- `json_only` -> the written row has `_json` populated and `_raw` NULL, always.
- `raw_only` -> the written row has `_raw` = the full payload and `_json` NULL.
- `extracted_only` -> the written row has neither `_json` nor `_raw`, just the promoted columns.
