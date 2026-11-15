# Benchmarks

The criterion benches in `benches/` measure the hot path's CPU cost. Run them on the machine you are comparing, since a figure from another host or an older build says nothing about this one.

| Bench | Measures |
|---|---|
| `json_parsing` | sonic-rs parsing and field lookups |
| `pipeline` | every per-message stage from parse to transform, with no ClickHouse |
| `bakeoff` | hot-path throughput across payload shapes |
| `extractor` | `HeaderExtractor::extract` per row |
| `transform` | the legacy flatten and transform path |
| `enrichment` | GeoIP, reputation and risk scoring lookups |
| `json_depth` | the nesting-depth guard on the payloads `JSON_DEPTH_CORPUS` names |
| `insert_bakeoff` | JSONEachRow against RowBinary inserts, against the ClickHouse its header's variables name |

```bash
cargo bench --bench pipeline
cargo bench --bench json_parsing -- --noplot   # skip the gnuplot step
```

Reports land under `target/criterion/`. The parser and insert-encoding bake-offs behind the loader's design, with their figures, are in [docs/performance/history.md](docs/performance/history.md).
