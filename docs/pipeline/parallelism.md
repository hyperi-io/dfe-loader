<!--
  Project:      dfe-loader
  File:         docs/pipeline/parallelism.md
  Purpose:      Pipeline parallelisation playbook (parallel-then-sequential)
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Pipeline parallelism

Each received batch goes through two phases in `src/pipeline/orchestrator.rs`: per-message work with no shared mutable state runs in parallel, and everything that mutates state runs after it, in one thread.

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
    accTitle: L4 Detail -- Batch phases
    accDescr: A received batch passes the BatchEngine's pre-route filters when a routing field is set. The worker pool runs MessageProcessor process on each message in parallel. The BatchCoordinator then walks the results in one thread, pushing each Ok row into its table's buffer, parking a message that waits on its schema and turning any other error into a dead letter.

    subgraph before["Before the batch splits"]
        direction LR
        recv(["<b>Received batch</b>"])
        preroute("<b>Pre-route filters</b>")
        recv --> preroute
    end
    subgraph phases["Per message"]
        direction LR
        subgraph parallel["Parallel"]
            process("<b>MessageProcessor<br/>process</b>")
        end
        subgraph sequential["Sequential"]
            push("<b>Buffer push</b>")
            dead("<b>Dead letter</b>")
        end
        process -->|Ok| push
        process -.->|Err| dead
    end
    before --> phases
  class recv,preroute,process,push part
  class dead error
  class recv,preroute,process,push part
  class dead error
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
  style before fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style phases fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style parallel fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style sequential fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Batch phases. Pre-route runs only when the BatchEngine has a `routing_field`.* Up: [Pipeline](README.md).

`MessageProcessor` (`src/pipeline/processor.rs`) holds only shared references, and its `process` method does no I/O and no `.await`. For one message it rejects a payload that does not open a JSON object or array, parses it with sonic-rs, holds an external topic's record to its source's tenant, routes it to a table, then extracts and transforms the fields. It goes on to derive the capture settings, apply the field mapping, evaluate computed columns and CEL expressions, and run enrichment. The result is a `ProcessedMessage` (`src/pipeline/types.rs`) or an error.

What it reads is safe to share. `CaptureOverrides::derive_config` (`src/pipeline/capture.rs`) is a pure `&self` method. Enrichment (`src/pipeline/enrichment.rs`) is in-memory GeoIP, reputation and risk lookups, with the GeoIP cache behind a `parking_lot::RwLock`.

The orchestrator runs `process` over the batch on the first pool it has: the `BatchEngine`'s pool, then the runtime's `AdaptiveWorkerPool`, then a plain sequential loop. When the `BatchEngine` has a `routing_field`, its pre-route filters run first and only the passing messages are processed.

## The sequential phase

`BatchCoordinator` (`src/pipeline/coordinator.rs`) holds the mutable references: the buffer manager, the capture, field-mapping and computed-column caches, the pending-schema buffer, metrics and the memory guard. `apply_results` walks the results in order. An `Ok` marks the table pending, ensures its cache entry and pushes the row into the table's buffer. A `SchemaPending` error parks the message in the pending-schema buffer, and any other error becomes a dead letter and releases the message's memory. On Kafka a message the full buffer refuses is overflow: the orchestrator pauses its partition and seeks back to it ([../operations.md](../operations.md#a-partition-is-paused)). It returns a `BatchOutcome` with the counters, the tables that need a schema resolution, the dead letters, which the orchestrator hands to the DLQ, and the overflow.

The coordinator is built only after the processor is dropped, so the borrow checker keeps the two phases apart.

The pool and the engine are scalo's `AdaptiveWorkerPool` and `BatchEngine` (scalo-rs `src/worker/`, the `worker-batch` feature). The runtime builds both, and `src/main.rs` hands them to the orchestrator and gives the pool the orchestrator's memory guard. The pool reads the cascade's `worker_pool` section, with keys and defaults in `WorkerPoolConfig` in scalo-rs `src/worker/config.rs`: thread bounds, the CPU thresholds it grows and shrinks at, a memory pressure cap, the scale interval and the async concurrency limit. [../architecture.md](../architecture.md) covers the layers around this hot path.
