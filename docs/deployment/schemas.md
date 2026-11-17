<!--
  Project:      dfe-loader
  File:         docs/deployment/schemas.md
  Purpose:      Loader-side reference for the shared dfe-schemas definitions
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Shared schema definitions

**The loader reads the DEPLOYED schema, never the YAML.** It queries
`system.columns` at startup and on a TTL, and parses the DFE `@directive`
expressions out of each column's COMMENT. So dfe-schemas is upstream of the
loader by two steps, and nothing here needs a checkout of it.

> **Canonical schema documentation:**
> [`dfe-schemas`](https://github.com/hyperi-io/dfe-schemas)

The schema reaches the loader through ClickHouse alone: dfe-engine applies the dfe-schemas manifest, and the loader reads the tables that result.

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
    accTitle: L4 Detail -- Where a table's schema comes from
    accDescr: dfe-schemas holds the YAML definitions. dfe-engine applies the manifest at boot with CREATE and ALTER statements. The loader reads the result from ClickHouse system.columns into its schema cache, again each time an entry expires.

    subgraph path["Schema path"]
        direction TB
        subgraph R1["1 Define and apply"]
            direction LR
            yaml("<b>dfe-schemas</b><br/><span>YAML</span>")
            engine("<b>dfe-engine</b>")
            yaml -->|manifest| engine
        end
        subgraph R2["2 Read"]
            direction LR
            clickhouse[("<b>ClickHouse</b><br/><span>system.columns</span>")]
            loader("<b>dfe-loader</b><br/><span>SchemaCache</span>")
            clickhouse -->|columns| loader
        end
        R1 -->|CREATE, ALTER| R2
    end
  class yaml,clickhouse store
  class engine control
  class loader load
  class yaml,clickhouse store
  class engine control
  class loader load
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
  style path fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style R2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Where a table's schema comes from. Dashed boxes are other DFE repos.* Up: [Deployment](README.md).

That is why the loader has no submodule and no bundled profile copies: whatever
the applier put in the table IS the contract, and a second copy on disk could
only disagree with it.

## What the loader takes from a column

A schema YAML's `expr` becomes part of the column COMMENT in the deployed DDL,
alongside any human-readable description:

```sql
COMMENT '@renamed: client_ip - Source address of the connection'
```

The parser ends a source path at the first ` - `, so the description after it is never read as part of the path.

The loader reads the directives it acts on and ignores the rest. Config always
wins over a COMMENT -- the exact resolution order is in
[`src/column_meta/mod.rs`](../../src/column_meta/mod.rs).

| Directive | Config field | Action |
|-----------|--------------|--------|
| `@skip` | `skip: true` | Omit the column from the insert |
| `@default:value` | `default:` | Substitute when the column is null or absent |
| `@source:path` | `renamed:` | Source field path(s) for this column, and a `\|` fallback fills `default:` |
| `@renamed:path` | `renamed:` | Source field path(s) for this column |
| `@computed:expr` | `computed:` | CEL expression producing the value |
| `@coerce:category` | `coerce:` | None: the encoder's coercion rules are fixed. The DDL directive logs a warning, the config key is refused |

`@generated` and `@config` are descriptive only: the loader drops both as
unknown directives (`DIRECTIVE_NAMES` in
[`src/column_meta/mod.rs`](../../src/column_meta/mod.rs) lists only the six
directives above). What makes the loader omit a value for a `@generated`
column is ClickHouse's DEFAULT/MATERIALIZED/ALIAS clause on it, reported
as `default_kind` in `system.columns` -- dfe-schemas' renderer writes that clause from
the schema's `default:` and `attribute:` fields, not by parsing the
`@generated:` text. `@config: path` documents that a mapping is driven by
loader config (for example `routing.org_id_field`) rather than hardcoded, and
nothing reads the comment text itself. See
[ddl-directives.md](../clickhouse/ddl-directives.md) for the whole expression
language.

dfe-engine applies the dfe-schemas manifest at boot: it takes the schema lock, renders the manifest for the platform it reaches, and creates what is absent or additively changed. Nothing else creates a DFE table, no Job and no init container. A loader pointed at a database the engine has not applied finds no columns: records wait in the pending-schema buffer, and a missing default table is logged at `warn`.

- [common-header.md](../transport/common-header.md) -- the header columns and what the loader writes into each
- [ddl-directives.md](../clickhouse/ddl-directives.md) -- field mapping expression language
- [dfe-schemas](https://github.com/hyperi-io/dfe-schemas) -- canonical schema documentation
