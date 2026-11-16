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
flowchart LR
    accTitle: L4 Detail -- Batch phases
    accDescr: A received batch passes the BatchEngine's pre-route filters when a routing field is set. The worker pool runs MessageProcessor process on each message in parallel. The BatchCoordinator then walks the results in one thread, pushing each Ok row into its table's buffer, parking a message that waits on its schema and turning any other error into a dead letter.

    recv(["Received batch"])
    preroute["Pre-route filters"]

    subgraph parallel["Parallel"]
        process["MessageProcessor<br/>process"]
    end

    subgraph sequential["Sequential"]
        push["Buffer push"]
        dead["Dead letter"]
    end

    recv --> preroute
    preroute --> process
    process -->|Ok| push
    process -.->|Err| dead

    classDef default fill:#F8FAFC,stroke:#64748B,stroke-width:1px,color:#0F172A
    classDef error fill:#FBE3D6,stroke:#C2410C,stroke-width:1px,color:#4A1A05
    classDef group fill:none,stroke:#64748B,stroke-width:1px,stroke-dasharray:2 4
    class dead error
    class parallel,sequential group
```

*Figure 1. L4 Detail -- Batch phases. Pre-route runs only when the BatchEngine has a `routing_field`.* Up: [Pipeline](README.md).

`MessageProcessor` (`src/pipeline/processor.rs`) holds only shared references, and its `process` method does no I/O and no `.await`. For one message it rejects a payload that does not open a JSON object or array, parses it with sonic-rs, holds an external topic's record to its source's tenant, routes it to a table, then extracts and transforms the fields. It goes on to derive the capture settings, apply the field mapping, evaluate computed columns and CEL expressions, and run enrichment. The result is a `ProcessedMessage` (`src/pipeline/types.rs`) or an error.

What it reads is safe to share. `CaptureOverrides::derive_config` (`src/pipeline/capture.rs`) is a pure `&self` method. Enrichment (`src/pipeline/enrichment.rs`) is in-memory GeoIP, reputation and risk lookups, with the GeoIP cache behind a `parking_lot::RwLock`.

The orchestrator runs `process` over the batch on the first pool it has: the `BatchEngine`'s pool, then the runtime's `AdaptiveWorkerPool`, then a plain sequential loop. When the `BatchEngine` has a `routing_field`, its pre-route filters run first and only the passing messages are processed.

## The sequential phase

`BatchCoordinator` (`src/pipeline/coordinator.rs`) holds the mutable references: the buffer manager, the capture, field-mapping and computed-column caches, the pending-schema buffer, metrics and the memory guard. `apply_results` walks the results in order. An `Ok` marks the table pending, ensures its cache entry and pushes the row into the table's buffer. A `SchemaPending` error parks the message in the pending-schema buffer, and any other error becomes a dead letter and releases the message's memory. It returns a `BatchOutcome` with the counters, the tables that need a schema resolution and the dead letters, which the orchestrator hands to the DLQ.

The coordinator is built only after the processor is dropped, so the borrow checker keeps the two phases apart.

The pool and the engine are scalo's `AdaptiveWorkerPool` and `BatchEngine` (scalo-rs `src/worker/`, the `worker-batch` feature). The runtime builds both, and `src/main.rs` hands them to the orchestrator and gives the pool the orchestrator's memory guard. The pool reads the cascade's `worker_pool` section, with keys and defaults in `WorkerPoolConfig` in scalo-rs `src/worker/config.rs`: thread bounds, the CPU thresholds it grows and shrinks at, a memory pressure cap, the scale interval and the async concurrency limit. [../architecture.md](../architecture.md) covers the layers around this hot path.
