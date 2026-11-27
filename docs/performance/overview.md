<!--
  Project:      dfe-loader
  File:         docs/performance/overview.md
  Purpose:      Performance optimisation guide: build optimisations and runtime tuning
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Performance optimisation

Performance work splits in two: build-time optimisations (allocator, LTO, PGO, BOLT) that hyperi-ci applies, and runtime tuning (batch sizes) that lives in config. This page covers both, plus the tools for finding where the time goes.

The build-time choices go into the binary once, and the batch settings tune the running loader without a rebuild.

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
    accTitle: L4 Detail -- Where performance is set
    accDescr: hyperi-ci builds the binary with jemalloc, LTO, and on release PGO then BOLT. The batch flush settings in config tune the running loader.

    subgraph build["Build-time, hyperi-ci"]
        direction TB
        subgraph B1["Allocator and linking"]
            direction LR
            jemalloc("<b>jemalloc</b>")
            lto("<b>LTO</b>")
            jemalloc ~~~ lto
        end
        subgraph B2["Profile-guided"]
            direction LR
            pgo("<b>PGO</b>")
            bolt("<b>BOLT</b>")
            pgo ~~~ bolt
        end
        B1 ~~~ B2
    end
    subgraph run["Run time"]
        direction LR
        binary("<b>Optimised binary</b>")
        batch("<b>Batch settings<br/>in config</b>")
        loader(["<b>Running loader</b>"])
        binary --> loader
        batch --> loader
    end
    build -->|compiles| run
  class jemalloc,lto,pgo,bolt,binary,batch part
  class loader load
  class jemalloc,lto,pgo,bolt,binary,batch part
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
  style build fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style B1 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style B2 fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
  style run fill:none,stroke:#0072B2,stroke-width:1.5px,stroke-dasharray:6 4
```

*Figure 1. L4 Detail -- Where performance is set. BOLT runs on the PGO build.* Up: [Performance](README.md).

## What CI builds

hyperi-ci picks the optimisations by release channel (`src/hyperi_ci/languages/rust/optimize.py` in hyperi-ci):

| Channel | Allocator | LTO | PGO and BOLT |
|---|---|---|---|
| alpha | jemalloc | thin | no |
| beta | jemalloc | fat | no |
| release | jemalloc | fat | PGO when the repo has a workload, BOLT with it unless `build.rust.optimize.bolt.enabled: false` |

This repo's workload is `scripts/pgo-workload.sh`, and `.hyperi-ci.yaml` builds its `pgo-driver` before the workload clock starts. The workload must exercise the hot path a running loader takes: parse, transform, serialise and insert. Port checks, health probes and startup paths mislead the compiler and can make PGO a net loss. Confirm what shipped from the published artefact, not from the build log. hyperi-ci's guide is `docs/runtime/pgo-bolt.md` in its repo.

`Cargo.toml` sets `lto = "thin"` and `codegen-units = 1` for `release`, and CI overrides the LTO mode per channel through `CARGO_PROFILE_RELEASE_LTO`, leaving `Cargo.toml` untouched.

The `jemalloc` feature installs `tikv-jemallocator` as the `#[global_allocator]` in `src/main.rs`. `default = []`, so a local build opts in:

```bash
cargo build --release --features jemalloc
```

Release builds set `strip = true`, so `nm` sees no allocator symbols. `strings` does:

```bash
strings target/release/dfe-loader | grep -ciE 'jemalloc|je_mallctl'
```

`tikv-jemallocator` prefixes jemalloc's symbols, so its options are read from `_RJEM_MALLOC_CONF`, not `MALLOC_CONF`. Heap profiling needs the crate's `profiling` feature, which this build does not enable, and symbols to read the profile with: the `profiling` cargo profile keeps them.

## Runtime tuning

| Setting | Default | Tuning guidance |
|---------|---------|-----------------|
| `buffer.flush_rows` | 20,000 | Increase for throughput, decrease for latency |
| `buffer.flush_bytes` | 64 MiB | A ceiling for oversized rows: at 1-2 KB rows the 20,000-row trigger fires first |
| `buffer.flush_age_secs` | 5 | Lower for real-time, higher for batch |

Insert concurrency is not a setting: at most 8 inserts run at once. RowBinary over HTTP (`FORMAT RowBinaryWithNamesAndTypes`) is the default, and `clickhouse.protocol: native` sends `FORMAT Native` blocks instead. dfe-clickhouse's [insert-formats.md](https://github.com/hyperi-io/dfe-clickhouse-rs/blob/main/docs/insert-formats.md) measures the two against each other. `insert_format: json_each_row` falls back to `FORMAT JSONEachRow` over HTTP.

To find where the time goes:

```bash
# CPU, with perf
perf record -g ./target/release/dfe-loader --config config.yaml
perf report

# Flame graph
cargo install flamegraph
cargo flamegraph --bin dfe-loader -- --config config.yaml
```

For a local PGO or BOLT run, [cargo-pgo](https://github.com/Kobzol/cargo-pgo) builds the instrumented binary (`cargo pgo build`), takes the profile from a workload run, and rebuilds with it (`cargo pgo optimize`, then `cargo pgo bolt optimize --with-pgo` for BOLT, Linux only). Further reading: [The Rust Performance Book](https://nnethercote.github.io/perf-book/), [LLVM BOLT](https://github.com/llvm/llvm-project/tree/main/bolt) and [jemalloc tuning](https://github.com/jemalloc/jemalloc/wiki/Getting-Started).
