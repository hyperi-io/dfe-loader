# Common Header v2.1: Capture Modes, JSON Limits, and Text Search

## Goal

Three changes to the Common Header schema and loader behaviour:

1. **Schema v2.1** — raise `_json` `max_dynamic_paths` to 2048, add `text` index to `_raw`, set ClickHouse 26.2+ as hard deck
2. **Capture modes** — three configurable modes controlling `_json` and `_raw` population, settable at global, per-table, and DDL levels
3. **Error guidance** — debounced log warning when `max_dynamic_paths` limit is hit, with actionable DDL fix instructions

## Hard Deck: ClickHouse 26.2+

**Previous requirement:** ClickHouse 25.3+ (JSON type GA)

**New requirement:** ClickHouse 26.2+ (JSON type GA + `text` index GA)

| Feature | GA Version | Status |
|---------|-----------|--------|
| `JSON` data type | 25.3 | Already required |
| `text` skip index | 26.2 | New requirement |
| `generateUUIDv7()` | 24.8 | Already required |

No experimental settings needed. The loader should check the ClickHouse version at startup and log a clear error if below 26.2.

## Part 1: Schema DDL Changes

### Before (v2.0)

```sql
`_raw` Nullable(String) CODEC(ZSTD(3)),
`_json` Nullable(JSON) CODEC(ZSTD(3)),

INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1
```

### After (v2.1)

```sql
`_raw` Nullable(String) CODEC(ZSTD(3)),
`_json` Nullable(JSON(max_dynamic_paths = 2048)) CODEC(ZSTD(3)),

INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1,
INDEX idx_raw_text _raw TYPE text(tokenizer = 'default') GRANULARITY 64
```

### Changes

| Change | Rationale |
|--------|-----------|
| `JSON` to `JSON(max_dynamic_paths = 2048)` | Default 1024 is too tight for multi-Beats deployments. ECS + Winlogbeat + Sysmon can generate 500-1500 unique paths over time. 2048 covers 99% of real-world security payloads. Hard max is 10,000. |
| `text` index on `_raw` | `_raw` exists for text search. Without the index, full scans are required. GA in CH 26.2, default tokeniser splits on non-alphanumeric (handles JSON delimiters naturally), granularity 64. Replaces the previous `full_text(0)` / `ngrambf_v1` references in COMMON-HEADER.md — those were experimental/deprecated. The `text` index is deterministic (no false positives) with better query performance than bloom filters. |

### `max_dynamic_paths` Reference

| Value | Use case |
|-------|----------|
| 1024 | Single Beat type, controlled schema |
| **2048** | **Mixed Beats (default) — Winlogbeat + Filebeat + Auditbeat** |
| 4096 | All Beats + custom fields + enrichment pipelines |
| 10000 | Hard maximum (ClickHouse limit) |

Per-column DDL only. Cannot be changed after data is inserted (requires column recreation). Paths exceeding the limit are stored in shared data (slower queries, but data is not lost).

### `text` Index Reference

| Property | Value |
|----------|-------|
| Type | `text(tokenizer = 'default')` |
| Granularity | 64 (CH 26.2 default) |
| Insert overhead | ~50% |
| Query speedup | Up to 7-10x vs full scan |
| Storage | Larger than bloom filters, but deterministic (no false positives) |
| Supported functions | `hasToken()`, `hasAnyTokens()`, `hasAllTokens()`, `LIKE`, `ILIKE` |

`_raw` without the index has no reason to exist. If the insert overhead is unacceptable, disable `_raw` entirely via capture mode.

### Profile YAML Changes

**timeseries.yaml** (v1.0.0 to v1.1.0):

```yaml
- name: _raw
  type: text
  use_case: text_search
  index: "text(tokenizer = 'default') GRANULARITY 64"
  expr: "@captured: raw_payload"
  comment: "Original event payload as text (full-text indexed)"

- name: _json
  type: json
  max_dynamic_paths: 2048
  expr: "@captured: raw_payload as JSON"
  comment: "Original event payload as structured JSON"
```

**minimal.yaml** and **passthrough.yaml** (v1.0.0 to v1.1.0):

```yaml
- name: _json
  type: json
  max_dynamic_paths: 2048
  expr: "@captured: raw_payload as JSON"
  comment: "Original event payload as structured JSON"
```

No `_raw` changes (these profiles don't include `_raw`).

## Part 2: Capture Modes

### Three Modes

| `capture_mode` | `_json` | `_raw` | Use case |
|---|---|---|---|
| `full` (default) | Entire payload as `JSON(max_dynamic_paths=2048)` | Extracted from `raw_source_fields` + text indexed | Full observability — JSON path queries + text search |
| `raw_only` | Skipped (NULL) | Entire Kafka payload as UTF-8 String | CH CPU saving — no JSON type overhead, text search only |
| `extracted_only` | Skipped (NULL) | Skipped (NULL) | Minimal — only promoted schema fields, lowest storage + CPU |

All three modes extract promoted fields to schema columns. The only difference is where (or whether) the full payload is preserved. The `passthrough` schema profile (no field extraction) is orthogonal to capture mode.

### Mode Behaviours

#### `full` (default)

Current behaviour. No changes to the data path.

- `_json`: zero-copy splice from raw Kafka payload via `write_map_with_raw()` (RowBinary) or `write_row_with_json()` (JSONEachRow)
- `_raw`: extracted from `raw_source_fields` (first match from `logoriginal`, `raw_log`, etc.) via transformer zero-copy rename
- Both columns populated

#### `raw_only`

New mode. Avoids ClickHouse JSON type CPU cost entirely.

- `_json`: not populated (column stays NULL)
- `_raw`: entire raw Kafka payload written as UTF-8 string — NOT extracted from a field inside the JSON
- The JSON parse still happens (needed for routing, field extraction, timestamps), but the JSON type column is skipped
- Text search still works via `text` index on `_raw`

Implementation: in the inserter, skip the `_json` raw payload passthrough. In the processor, write the raw Kafka bytes to `_raw` in the row map directly (as `Value::String`).

#### `extracted_only`

New mode. Lowest storage and CPU.

- `_json`: not populated (column stays NULL)
- `_raw`: not populated (column stays NULL)
- Only schema-promoted fields stored
- No payload preservation — original event is not recoverable from ClickHouse

### Config Cascade

Highest priority wins (same as existing capture override system):

**1. DDL table comment** (highest):

```sql
ALTER TABLE dfe.metrics COMMENT '@capture_mode: extracted_only';
```

**2. Per-table config**:

```yaml
metadata:
  table_capture_modes:
    dfe.metrics: extracted_only
    dfe.raw_logs: raw_only
```

**3. Global config** (lowest):

```yaml
metadata:
  capture_mode: full
```

### Backward Compatibility

The existing boolean fields (`capture_json`, `capture_raw`, `disable_json_tables`, `disable_raw_tables`) are deprecated but supported for one release cycle. Mapping:

| Old config | New `capture_mode` | Notes |
|---|---|---|
| `capture_json: true, capture_raw: true` | `full` | Exact match |
| `capture_json: false, capture_raw: false` | `extracted_only` | Exact match |
| `capture_json: false, capture_raw: true` | `full` + deprecation warning | See note below |
| `capture_json: true, capture_raw: false` | `full` + deprecation warning | See note below |

**Unsupported old combinations:** The two middle rows have no exact equivalent in the new model. Both map to `full` with a startup warning explaining:
- `capture_json: false, capture_raw: true` — use `capture_mode: raw_only` instead. Note: `raw_only` changes `_raw` semantics from "extracted from `raw_source_fields`" to "entire Kafka payload as UTF-8". This is a deliberate simplification — if you need field-extracted `_raw` without `_json`, use `full` mode and disable `_json` per-table via DDL (`@capture_mode: extracted_only`) or use the `@skip` directive on the `_json` column.
- `capture_json: true, capture_raw: false` — use `capture_mode: full` with per-table `@skip` on the `_raw` column, or use the `extracted_only` mode if `_json` is also unwanted.

If both old and new config are present, the new `capture_mode` takes precedence with a deprecation warning at startup.

### Breaking Change: `_raw` Semantics in `raw_only` Mode

In `raw_only` mode, `_raw` contains the **entire raw Kafka payload** as UTF-8 — not a field extracted from inside the JSON (like `logoriginal`). This is a deliberate change: the purpose of `raw_only` is to capture the complete payload cheaply without JSON type overhead. If you need field-extracted `_raw` (the old `capture_raw` behaviour), use `full` mode — it extracts `_raw` from `raw_source_fields` as before.

The existing `disable_json_tables` / `disable_raw_tables` lists are superseded by `table_capture_modes`. If both are present, `table_capture_modes` wins.

The existing DDL tags `@no_capture_json` and `@no_capture_raw` are superseded by `@capture_mode`. If both are present, `@capture_mode` wins.

### Implementation: CaptureOverrides Changes

`CaptureOverrides` currently holds per-table `TableCaptureConfig { disable_json: bool, disable_raw: bool }`. This changes to:

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureMode {
    #[default]
    Full,           // _json + _raw
    RawOnly,        // _raw only (no _json)
    ExtractedOnly,  // Neither _json nor _raw
}

pub struct TableCaptureConfig {
    pub mode: CaptureMode,
}
```

Resolution in `derive_config()`:
1. Check DDL `@capture_mode` tag (if resolved) — parsed via `TableTags::from_comment`, value must be `full`, `raw_only`, or `extracted_only`
2. Check `table_capture_modes` config map
3. Fall back to global `capture_mode`

When both old tags (`@no_capture_json`, `@no_capture_raw`) and new tag (`@capture_mode`) are present in DDL, the new tag wins. `update_from_comment()` checks for `@capture_mode` first and skips the old tags if found.

### Implementation: Processor Changes

The processor currently decides `_json` and `_raw` population in three places:

1. **`_json` injection** (legacy flatten path): the `capture_json && !disable_json` guard in the transformer output block
2. **`_raw` removal**: the `disable_raw` check that removes the `_raw` key from the row map
3. **Raw payload passthrough**: the `Arc::from(msg.payload.as_slice())` that carries raw bytes alongside the row for zero-copy `_json` splice

`_source` is unaffected by capture mode — it is controlled independently by `capture_source`.

With capture modes:

| Mode | Raw payload to buffer | `_json` in map | `_raw` in map |
|---|---|---|---|
| `full` | Yes (for zero-copy splice) | No (spliced at insert time) | Yes (from `raw_source_fields`) |
| `raw_only` | No (skipped) | No | Yes (from raw Kafka bytes, not `raw_source_fields`) |
| `extracted_only` | No (skipped) | No | No |

Key difference for `raw_only`: `_raw` is NOT extracted from a field inside the JSON (like `logoriginal`). It's the entire raw Kafka payload as a UTF-8 string. This is simpler and avoids the field lookup.

### Implementation: Inserter Changes

The inserter's `insert_rows()` already handles `raw_payloads`:
- Non-empty → pass to `write_map_with_raw()` for zero-copy `_json` splice
- Empty → normal `write_map()`

With capture modes:
- `full`: unchanged (raw_payloads carries Kafka bytes for `_json` splice)
- `raw_only`: raw_payloads is empty (no `_json` splice needed, `_raw` is already in the row map)
- `extracted_only`: raw_payloads is empty (nothing to splice)

No inserter changes required beyond what's already implemented.

## Part 3: Error Guidance for `max_dynamic_paths`

### Error Detection

When the loader encounters a ClickHouse error message containing `max_dynamic_paths` or `Cannot add new dynamic path`:

### Log Output

```
WARN  Table dfe.events: _json column hit max_dynamic_paths limit.
      Paths beyond the limit are stored in shared data (slower queries).
      Fix: ALTER TABLE dfe.events MODIFY COLUMN _json JSON(max_dynamic_paths = 4096)
      Note: Requires empty column — back up data first. Default is 2048.
      Consider capture_mode = 'raw_only' for high-cardinality tables.
```

### Rate Limiting

- Debounced at **5 minutes** (300,000ms) using `log_debounced()` from rustlib
- Static `AtomicU64` per detection site
- The row itself still goes through salvage and DLQ as normal (existing error handling)

### Implementation

```rust
static MAX_PATHS_TS: AtomicU64 = AtomicU64::new(0);

// In the error classification / salvage path
if is_max_dynamic_paths_error(&error_msg) && log_debounced(&MAX_PATHS_TS, 300_000) {
    warn!(
        table = %table,
        "Table _json column hit max_dynamic_paths limit. \
         Paths beyond the limit are stored in shared data (slower queries). \
         Fix: ALTER TABLE {} MODIFY COLUMN _json JSON(max_dynamic_paths = 4096). \
         Note: Requires empty column. Consider capture_mode = 'raw_only' \
         for high-cardinality tables.",
        table
    );
}
```

Detection function:

```rust
fn is_max_dynamic_paths_error(msg: &str) -> bool {
    msg.contains("max_dynamic_paths")
        || msg.contains("Cannot add new dynamic path")
        || (msg.contains("LOGICAL_ERROR") && msg.contains("dynamic path"))
}
```

## Files Changed

### dfe-schemas (submodule)

| File | Change |
|------|--------|
| `common-header/timeseries.yaml` | Bump to v1.1.0, add `max_dynamic_paths: 2048` to `_json`, add `index` to `_raw` |
| `common-header/minimal.yaml` | Bump to v1.1.0, add `max_dynamic_paths: 2048` to `_json` |
| `common-header/passthrough.yaml` | Bump to v1.1.0, add `max_dynamic_paths: 2048` to `_json` |

### dfe-loader

| File | Change |
|------|--------|
| `docs/COMMON-HEADER.md` | Update DDL template, CH version requirement, `_raw` index docs, capture mode docs |
| `CLAUDE.md` | Update CH version requirement, add capture mode reference |
| `src/config/pipeline.rs` | Add `CaptureMode` enum, `capture_mode` field, `table_capture_modes`, deprecation of old booleans |
| `src/pipeline/capture.rs` | Refactor `CaptureOverrides` to use `CaptureMode`, update `derive_config()` |
| `src/pipeline/processor.rs` | Branch on `CaptureMode` for `_json`/`_raw` population |
| `src/clickhouse/error.rs` | Add `is_max_dynamic_paths_error()` detection |
| `src/clickhouse/inserter.rs` | Add debounced warning in salvage/error path |
| `src/schema/mod.rs` | Support `max_dynamic_paths` and `index` in profile YAML → DDL generation (extend `TableTags`) |
| `tests/` | Unit tests for capture mode resolution, integration test for `raw_only` mode |

## Testing

| Test | Type | Validates |
|------|------|-----------|
| `test_capture_mode_default_is_full` | Unit | Default mode produces both `_json` and `_raw` |
| `test_capture_mode_raw_only_skips_json` | Unit | `raw_only` mode: `_json` NULL, `_raw` populated from raw Kafka bytes |
| `test_capture_mode_extracted_only_skips_both` | Unit | `extracted_only` mode: both NULL, only promoted fields |
| `test_capture_mode_ddl_overrides_config` | Unit | DDL `@capture_mode` wins over config |
| `test_capture_mode_per_table_overrides_global` | Unit | `table_capture_modes` wins over global |
| `test_capture_mode_backward_compat_full` | Unit | `capture_json: true, capture_raw: true` maps to `full` |
| `test_capture_mode_backward_compat_extracted` | Unit | `capture_json: false, capture_raw: false` maps to `extracted_only` |
| `test_capture_mode_backward_compat_unsupported` | Unit | `capture_json: false, capture_raw: true` maps to `full` + deprecation warning |
| `test_capture_mode_backward_compat_json_only` | Unit | `capture_json: true, capture_raw: false` maps to `full` + deprecation warning |
| `test_max_dynamic_paths_warning_debounced` | Unit | Warning fires at most once per 5 min |
| `test_rowbinary_raw_only_mode` | Integration | RowBinary insert with `raw_only`: `_raw` populated, `_json` NULL |
| `test_text_index_on_raw` | Integration | `hasToken()` query on `_raw` uses the text index |

## Migration

### For Existing Tables

Tables created with v2.0 schema need two ALTER statements:

```sql
-- 1. Raise max_dynamic_paths (requires empty _json column or table recreation)
ALTER TABLE dfe.events MODIFY COLUMN _json JSON(max_dynamic_paths = 2048);

-- 2. Add text index to _raw
ALTER TABLE dfe.events ADD INDEX idx_raw_text _raw TYPE text(tokenizer = 'default') GRANULARITY 64;
ALTER TABLE dfe.events MATERIALIZE INDEX idx_raw_text;
```

The `MODIFY COLUMN` for `_json` only works on an empty column. For tables with existing data, the recommended path is:
1. Create new table with v2.1 schema
2. `INSERT INTO new SELECT * FROM old`
3. `RENAME TABLE old TO old_backup, new TO events`

### For New Tables

Auto-init creates tables with v2.1 schema automatically. No manual action needed.

### Config Migration

```yaml
# Before (v2.0)
metadata:
  capture_json: true
  capture_raw: true
  disable_json_tables:
    - dfe.metrics

# After (v2.1) — equivalent
metadata:
  capture_mode: full
  table_capture_modes:
    dfe.metrics: extracted_only
```

Both formats accepted during the deprecation period. Startup warning logged if old format detected.
