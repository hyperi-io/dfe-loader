<!--
  Project:      dfe-loader
  File:         docs/transport/COMMON-HEADER.md
  Purpose:      Common header schema injected into every event table
  Language:     Markdown

  License:      BUSL-1.1
  Copyright:    (c) 2026 HYPERI PTY LIMITED
-->

# Common header schema v2

Every event the loader writes -- regardless of the transport it arrived on
(Kafka, gRPC, Memory) -- gets a standardised common header injected before
insert. This is the base schema shared by all event tables in the DFE (Data
Fusion Engine) pipeline. This page documents the full field set, the DDL, and the
ORDER BY / PARTITION BY / codec / index decisions behind it.

## Overview

Every event table shares a common header that provides:

1. **Temporal ordering** -- multiple timestamp fields for different use cases.
2. **Multi-tenancy** -- organisation ID for row-level security (RLS).
3. **Deduplication** -- time-ordered UUIDs for uniqueness.
4. **Auditability** -- original payload preservation.
5. **Extensibility** -- dynamic metadata via JSON fields.

## Common header vs complete table schema

The common header is the **base** of every table's schema -- not the complete
schema itself. The loader injects these system fields into every event.

- **Default table** (`dfe.default`): schema IS just the common header. This is the
  catch-all for unrouted events and the only table auto-created by the loader.
- **Non-default tables** (e.g., `dfe.auth`, `dfe.metrics`): schema = common header
  + data-specific columns. These tables are created externally (by DBAs or IaC)
  with their own columns alongside the common header.

```text
+----------------------------------+
|   default table (dfe.default)    |   <- schema = common header ONLY
|  +----------------------------+  |
|  |      common header         |  |
|  |  (_timestamp, _org_id, ...) |  |
|  +----------------------------+  |
+----------------------------------+

+----------------------------------+
|  non-default table (dfe.auth)    |   <- schema = common header + data columns
|  +----------------------------+  |
|  |      common header         |  |
|  |  (_timestamp, _org_id, ...) |  |
|  +----------------------------+  |
|  |    data columns            |  |
|  |  (user_id, action, ip, ...) |  |
|  +----------------------------+  |
+----------------------------------+
```

The profile system (dfe-schemas `common-header/*.yaml`) controls which common
header fields are injected. The profile does NOT define data-specific columns --
those come from the source data or are defined in the table's own DDL.

## Common header DDL (default table)

The following DDL shows the common header as used for the default table.
Non-default tables would include these columns alongside their own data-specific
columns.

```sql
CREATE TABLE IF NOT EXISTS {db}.{table}
(
    -- Primary timestamps
    `_timestamp_load` DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    `_timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `_timestamp_received` Nullable(DateTime64(3)) CODEC(Delta, ZSTD(1)),

    -- Identity
    `_uuid` UUID DEFAULT generateUUIDv7(),
    `_org_id` LowCardinality(String) CODEC(ZSTD(1)),

    -- Source identifier
    `_source` LowCardinality(String) CODEC(ZSTD(1)),

    -- Payload preservation
    `_raw` Nullable(String) CODEC(ZSTD(3)),
    `_json` Nullable(JSON(max_dynamic_paths = 2048)) CODEC(ZSTD(3)),

    -- Metadata
    `_tags` Nullable(JSON) CODEC(ZSTD(3)),

    -- Indexes
    INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1,
    INDEX idx_raw_text _raw TYPE text(tokenizer = 'default') GRANULARITY 64
)
ENGINE = {engine}
ORDER BY (_org_id, _timestamp_load, _uuid)
PARTITION BY (toYYYYMM(_timestamp_load), _org_id)
SETTINGS index_granularity = 8192
```

## Column reference

### `_timestamp_load`

| Property | Value |
|----------|-------|
| Type | `DateTime64(3)` |
| Nullable | No |
| Default | `now64(3)` |
| Codec | `Delta, ZSTD(1)` |
| Source | `@generated: now64(3)` |

**Purpose:** When the data was loaded into ClickHouse.

**Rationale:**

- Primary query filter for operational dashboards ("show me events from the last hour").
- More reliable than event timestamp for time-windowed queries.
- ClickHouse generates via DEFAULT -- loader omits this field.
- All rows in a batch get the same load timestamp (acceptable trade-off).

**Why first in ORDER BY after `_org_id`:**

- Every dashboard query filters by recent time window.
- Combined with `_org_id`, provides excellent query locality.
- Enables efficient "tail -f" style queries.

### `_timestamp`

| Property | Value |
|----------|-------|
| Type | `DateTime64(3)` |
| Nullable | No |
| Default | None |
| Codec | `Delta, ZSTD(1)` |
| Source | `@source: timestamp \| now()` |

**Purpose:** When the event actually occurred (event time).

**Rationale:**

- Event time semantics for analytics and time-series analysis.
- May differ significantly from load time (buffered, delayed, replayed events).
- Fallback to `now()` ensures no NULL values in non-nullable column.
- minmax index enables efficient range scans.

**Field resolution order:**

1. `timestamp` field in source data.
2. `@timestamp` field (common in log formats).
3. `event_time` field.
4. Fallback to `now()` if all missing/invalid.

**Validation:**

- Must be within reasonable bounds (not before 1970, not far in future).
- Millisecond precision preserved.
- Auto-detection of epoch seconds/millis/micros/nanos.

### `_timestamp_received`

| Property | Value |
|----------|-------|
| Type | `Nullable(DateTime64(3))` |
| Nullable | Yes |
| Default | None |
| Codec | `Delta, ZSTD(1)` |
| Source | `@source: timestamp_received` |

**Purpose:** When the receiver/loader first received the event.

**Rationale:**

- Useful for latency analysis (time from generation to receipt).
- Only present if source includes this timestamp.
- Nullable because most sources do not provide this.
- Distinct from `_timestamp_load` which is insert time.

**Use cases:**

- Pipeline latency monitoring: `_timestamp_load - _timestamp_received`.
- Event age at receipt: `_timestamp_received - _timestamp`.
- SLA compliance tracking.

### `_uuid`

| Property | Value |
|----------|-------|
| Type | `UUID` |
| Nullable | No |
| Default | `generateUUIDv7()` |
| Codec | None (UUID is already compact) |
| Source | `@generated: generateUUIDv7()` |

**Purpose:** Unique identifier for each event.

**Rationale:**

- UUIDv7 is time-ordered (millisecond precision + random suffix).
- Sortable within timestamp -- useful for deterministic ordering.
- Generated by ClickHouse DEFAULT -- no client-side generation needed.
- Last in ORDER BY to ensure uniqueness within (org_id, timestamp_load).

**Why UUIDv7 over other options:**

| Option | Pros | Cons |
|--------|------|------|
| UUIDv4 | Widely supported | Not sortable, poor locality |
| UUIDv7 | **Time-ordered, sortable** | Requires ClickHouse 24.8+ |
| ULID | Time-ordered | Not native ClickHouse type |
| Snowflake | Compact, sortable | Requires coordination |
| xxhash64 | Fast | Not guaranteed unique |

**ClickHouse 24.8+ functions:**

```sql
-- Thread-monotonic (guarantees ordering within thread)
generateUUIDv7()
generateUUIDv7ThreadMonotonic()

-- Slightly faster, no ordering guarantee
generateUUIDv7NonMonotonic()
```

### `_org_id`

| Property | Value |
|----------|-------|
| Type | `LowCardinality(String)` |
| Nullable | No |
| Default | None |
| Codec | `ZSTD(1)` |
| Source | `@source: org_id` |

**Purpose:** Organisation/tenant identifier for multi-tenancy and RLS.

**Rationale:**

- First in ORDER BY -- every query has an org_id filter (RLS).
- LowCardinality for dictionary encoding (orgs are highly repeated).
- Required field -- cannot be NULL.
- Extracted from source and stored for query-time filtering.

**Row-Level Security (RLS):**

```sql
-- Create row policy for tenant isolation
CREATE ROW POLICY org_isolation ON common.events
FOR SELECT
USING _org_id = currentUser()
TO ALL;

-- Or with role-based access
CREATE ROW POLICY org_policy ON common.events
FOR SELECT
USING _org_id IN (
    SELECT org_id FROM system.role_mappings
    WHERE role = currentUser()
)
TO analytics_users;
```

**Why LowCardinality:**

- Typical deployment has <1000 unique orgs.
- Dictionary encoding reduces storage 10-100x for this column.
- Faster equality comparisons (dictionary index lookup).

### `_raw`

| Property | Value |
|----------|-------|
| Type | `Nullable(String)` |
| Nullable | Yes |
| Default | None |
| Codec | `ZSTD(3)` |
| Source | `@captured: raw_payload` |

**Purpose:** Original raw data as received -- the data as it would appear in a
tailed log file or a database row, BEFORE any RFC/format parsing.

**NOT the same as `_json`:** `_raw` is the original wire format (e.g., raw syslog
RFC 3164/5424 line), while `_json` is the parsed/structured result.

**Rationale:**

- Captured BEFORE any transformation or parsing.
- Enables "grep-like" full-text searches across the original payload.
- Nullable -- can be disabled globally or per-table to save storage.
- Higher ZSTD level (3) for better compression of text.

**Text search index (default, included in Common Header v2.1):**

```sql
INDEX idx_raw_text _raw TYPE text(tokenizer = 'default') GRANULARITY 64
```

The `text` index (GA in ClickHouse 26.2+) provides deterministic full-text search
with no false positives. Supported query functions: `hasToken()`,
`hasAnyTokens()`, `hasAllTokens()`, `LIKE`, `ILIKE`. Insert overhead ~50%.

`_raw` without the index has no reason to exist. To avoid the overhead, disable
`_raw` entirely via `capture_mode` -- do not keep the column without the index.

**Capture modes** control `_raw` and `_json` population:

| `capture_mode` | `_json` | `_raw` | Use case |
|---|---|---|---|
| `full` (default) | Full payload (JSON type) | Extracted from `raw_source_fields` | Full observability |
| `raw_only` | NULL | Full Kafka payload (String) | CH CPU saving -- no JSON type overhead |
| `extracted_only` | NULL | NULL | Minimal -- only promoted schema fields |

All three modes extract promoted fields to schema columns. Configurable at global,
per-table, and DDL levels. See `metadata.capture_mode` in config.

### `_json`

| Property | Value |
|----------|-------|
| Type | `Nullable(JSON(max_dynamic_paths = 2048))` |
| Nullable | Yes |
| Default | None |
| Codec | `ZSTD(3)` |
| Source | `@captured: raw_payload as JSON` |

**Purpose:** Complete Kafka message as native ClickHouse JSON type for structured
path-based queries.

**`max_dynamic_paths`:** Default raised to 2048 (from ClickHouse default 1024) to
cover multi-Beats deployments (ECS + Winlogbeat + Sysmon generates 500-1500 unique
paths). Per-column DDL only -- cannot be changed after data is inserted. Hard max
10,000. Paths exceeding the limit are stored in shared data (slower queries, data
not lost).

**NOT the same as `_raw`:** `_json` is the parsed/structured result stored as
native columnar JSON. `_raw` is the original wire format before parsing.

**Rationale:**

- Captured BEFORE any transformation (preserves original structure).
- ClickHouse JSON type -- each JSON path stored as a native subcolumn.
- Enables ad-hoc queries on any field without schema changes.
- Nullable -- can be disabled to save storage.

**ClickHouse JSON type benefits:**

- Columnar storage within JSON (not stored as a string blob).
- Type inference per JSON path.
- Efficient queries: `SELECT _json.user.id, _json.action FROM events`.
- Supports nested objects and arrays.
- ClickHouse stores each path as a dense subcolumn with compression.

**Requirements:**

- ClickHouse 26.2+ for GA JSON type + `text` skip index (hard deck).

**Configuration:**

```toml
[metadata]
capture_mode = "full"  # full (default) | raw_only | extracted_only
```

`capture_mode` controls `_json` and `_raw` population -- see the Capture Modes
section below. Legacy booleans `capture_json` / `capture_raw` are still accepted
for backwards compatibility and mapped onto `capture_mode`.

### `_tags`

| Property | Value |
|----------|-------|
| Type | `Nullable(JSON)` |
| Nullable | Yes |
| Default | None |
| Codec | `ZSTD(3)` |
| Source | `@source: first(tags/_tags/meta/metadata.tags)` |

**Purpose:** Metadata and collector/agent information.

**Rationale:**

- Config-driven source field list (first match wins).
- Stored as JSON for flexible querying.
- Contains collector metadata, enrichment tags, routing info.
- Nullable -- not all events have tags.

**Field resolution order:**

1. `tags` field.
2. `_tags` field.
3. `meta` field.
4. `metadata.tags` nested path.

**Configuration:**

```toml
[metadata]
tags_fields = ["tags", "_tags", "meta", "metadata.tags"]
tags_output = "_tags"
drop_tags = false  # Remove source tags after extraction
```

#### An array source is wrapped under `_values`

ECS sends `tags` as an array of strings, and a ClickHouse JSON column parses only
an object at its root -- an array is rejected outright with code 117
(`JSON object should start with '{'`). The loader therefore wraps a root-level
array before encoding, so what lands is deliberately not the shape that was sent.

Sent:

```json
{"tags": ["beats", "filebeat"]}
```

Stored in `_tags`:

```json
{"_values": ["beats", "filebeat"]}
```

Query the wrapped list through the `_values` subcolumn, casting the `Dynamic` to
the array type it holds:

```sql
SELECT _tags._values.:`Array(Nullable(String))` AS tags
FROM dfe.default
WHERE arrayExists(x -> x = 'filebeat', _tags._values.:`Array(Nullable(String))`)
```

Notes:

- The wrap applies to any root-level array reaching any JSON column, not to
  `_tags` alone.
- Only the root is reshaped. An array nested inside an object is stored as sent,
  because ClickHouse already parses it.
- An object source passes through untouched, so `_tags` holding
  `{"_values": [...]}` cannot be distinguished from a source that genuinely sent
  that key.
- The key is `dfe_loader::clickhouse_ext::JSON_ARRAY_WRAPPER_KEY`.

## Underscore prefix convention

All common header fields use an underscore prefix (`_timestamp`, `_org_id`,
`_uuid`, etc.).

**Rationale:**

- Avoids collision with source data fields.
- Source data often contains `timestamp`, `id`, `tags` etc.
- Clear visual distinction between system and data fields.
- Consistent with ClickHouse system columns (`_part`, `_partition_id`).

**Example collision prevention:**

```json
// Source event
{
  "timestamp": "2024-01-15T10:30:00Z",
  "id": "user-123",
  "tags": ["important", "urgent"]
}

// Stored as:
// _timestamp = 2024-01-15T10:30:00Z (from source timestamp)
// _uuid = 01234567-... (generated)
// _tags = {"_values": ["important", "urgent"]} (wrapped -- see above)
// + all original fields preserved in _json
```

## ORDER BY design

```sql
ORDER BY (_org_id, _timestamp_load, _uuid)
```

### Column order rationale

1. **`_org_id` first:**
   - Every query has an org_id filter (RLS enforcement).
   - Data physically grouped by organisation.
   - Partition pruning works with org_id in ORDER BY.

2. **`_timestamp_load` second:**
   - Primary time filter for operational queries.
   - Recent data queries are most common.
   - Good data locality for time-windowed dashboards.

3. **`_uuid` last:**
   - Ensures uniqueness within (org_id, timestamp_load).
   - Deterministic ordering for reproducible queries.
   - Enables efficient point lookups by UUID.

### Query patterns optimised

```sql
-- Pattern 1: Recent events for an org (most common)
SELECT * FROM events
WHERE _org_id = 'acme' AND _timestamp_load > now() - INTERVAL 1 HOUR
-- Uses: Full ORDER BY prefix

-- Pattern 2: Event by UUID
SELECT * FROM events
WHERE _org_id = 'acme' AND _uuid = '...'
-- Uses: org_id prefix, UUID in last position

-- Pattern 3: Time range across orgs (admin dashboard)
SELECT _org_id, count() FROM events
WHERE _timestamp_load > now() - INTERVAL 1 DAY
GROUP BY _org_id
-- Uses: Scans all orgs but time-filtered

-- Pattern 4: Event time analysis
SELECT * FROM events
WHERE _org_id = 'acme' AND _timestamp BETWEEN '...' AND '...'
-- Uses: org_id prefix, minmax index on _timestamp
```

## PARTITION BY design

```sql
PARTITION BY (toYYYYMM(_timestamp_load), _org_id)
```

### Rationale

1. **Monthly partitions by load time:**
   - Natural TTL boundary (drop old months).
   - Reasonable partition count (~12-24 active).
   - Aligns with typical retention policies.

2. **Includes `_org_id`:**
   - Enables per-org partition pruning.
   - Efficient org-level data management.
   - Only acceptable for <100 orgs (partition cardinality limit).

### Partition management

```sql
-- View partitions
SELECT partition, rows, bytes_on_disk
FROM system.parts
WHERE database = 'common' AND table = 'events'
ORDER BY partition;

-- Drop old data (by month)
ALTER TABLE common.events DROP PARTITION ('202401', 'acme');

-- Drop all data for an org
ALTER TABLE common.events DROP PARTITION ('*', 'acme');

-- TTL-based retention (alternative)
ALTER TABLE common.events MODIFY TTL _timestamp_load + INTERVAL 90 DAY;
```

### Cardinality considerations

| Orgs | Months | Partitions | Status |
|------|--------|------------|--------|
| 10 | 12 | 120 | Excellent |
| 50 | 12 | 600 | Good |
| 100 | 12 | 1,200 | Acceptable |
| 500 | 12 | 6,000 | Too many -- use shared schema |

**For >100 orgs:** use shared schema without org in partition:

```sql
PARTITION BY toYYYYMM(_timestamp_load)
-- RLS handles isolation, not partitioning
```

## Codec selection

### Delta + ZSTD(1) for timestamps

```sql
`_timestamp_load` DateTime64(3) CODEC(Delta, ZSTD(1))
```

**Rationale:**

- Delta encoding exploits the monotonic timestamp nature.
- Adjacent timestamps differ by small amounts.
- ZSTD(1) compresses the delta-encoded values.
- Level 1 balances speed and compression.

**Compression ratio:** typically 10-20x for timestamp columns.

### ZSTD(3) for text/JSON

```sql
`_raw` Nullable(String) CODEC(ZSTD(3))
`_json` Nullable(JSON) CODEC(ZSTD(3))
```

**Rationale:**

- Higher level (3) for better compression of text.
- Text/JSON has high redundancy (field names, common values).
- Slightly slower compression but faster decompression.
- Worth the trade-off for storage-heavy columns.

**Compression ratio:** typically 5-15x for JSON, 3-8x for raw text.

### No codec for UUID

```sql
`_uuid` UUID DEFAULT generateUUIDv7()
```

**Rationale:**

- UUID is already 16 bytes (compact binary format).
- UUIDv7 has some entropy that does not compress well.
- Codec overhead not worth the minimal compression gain.

## Index strategy

### Primary index (sparse)

The ORDER BY clause creates a sparse primary index:

```sql
ORDER BY (_org_id, _timestamp_load, _uuid)
```

- Index entry every `index_granularity` rows (default: 8192).
- ~1 index entry per 8KB-64KB of data.
- Extremely memory-efficient for large tables.

### minmax index on `_timestamp`

```sql
INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1
```

**Purpose:** efficient time range queries on event time (not load time).

**How it works:**

- Stores min/max `_timestamp` per granule.
- Query `WHERE _timestamp BETWEEN x AND y` skips granules outside range.
- GRANULARITY 1 = one min/max per index granularity (8192 rows).

**When it helps:**

- Queries filtering on event time, not load time.
- Analytics queries: "events that occurred in Q1".
- Audit queries: "what happened at timestamp X".

### Text index on `_raw` (default)

```sql
-- Standard: text index (GA in ClickHouse 26.2+, required by hard deck)
INDEX idx_raw_text _raw TYPE text(tokenizer = 'default') GRANULARITY 64
```

Auto-init creates this index on every `_raw` column unless `capture_mode` is
`extracted_only` (in which case `_raw` is NULL and an index would be meaningless).

**Trade-offs:**

| Index type | Pros | Cons |
|------------|------|------|
| `text` (default) | GA in 26.2+, accurate, no false positives | ~50% insert overhead on `_raw` |
| `ngrambf_v1` (legacy) | Lower storage | False positives; no reason to prefer on 26.2+ |
| None | Zero overhead | Full scan for text search -- use `capture_mode = "extracted_only"` instead |

If you do not need full-text search on the raw payload, set
`capture_mode = "extracted_only"` -- `_raw` is NULL and the text index is skipped
entirely.

**Configuration:**

```toml
[auto_init]
create_text_index = true  # Default: create text index on _raw
```

## Engine selection

The schema supports three MergeTree variants:

### SharedMergeTree (recommended)

```sql
ENGINE = SharedMergeTree()
```

**Requirements:** ClickHouse Cloud or 24.1+ with shared storage.

**Benefits:**

- Automatic replication without ZooKeeper.
- Seamless horizontal scaling.
- No replica configuration needed.

### ReplicatedMergeTree (clustered)

```sql
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{db}/{table}', '{replica}')
```

**Requirements:** ZooKeeper or ClickHouse Keeper.

**Benefits:**

- Traditional replication with explicit control.
- Well-tested, stable.
- Works with any ClickHouse version.

### MergeTree (single-node)

```sql
ENGINE = MergeTree()
```

**Use case:** development, testing, single-node deployments.

**Benefits:**

- Simplest configuration.
- No external dependencies.
- Fastest for local development.

### Auto-detection

The loader auto-detects the best engine:

```rust
pub fn best_engine(&self) -> TableEngine {
    if self.shared_merge_tree {
        TableEngine::SharedMergeTree
    } else if self.is_clustered {
        TableEngine::ReplicatedMergeTree
    } else {
        TableEngine::MergeTree
    }
}
```

## Source field mapping

Using the DDL Expression Language (see [../clickhouse/DDL-DIRECTIVES.md](../clickhouse/DDL-DIRECTIVES.md)):

| Column | Expression | Description |
|--------|------------|-------------|
| `_timestamp_load` | `@generated: now64(3)` | DB generates on insert |
| `_timestamp` | `@source: timestamp \| now()` | From source with fallback |
| `_timestamp_received` | `@source: timestamp_received` | Optional, from source |
| `_uuid` | `@generated: generateUUIDv7()` | DB generates on insert |
| `_org_id` | `@source: org_id` | Required from source |
| `_raw` | `@captured: raw_payload` | Pre-transform capture |
| `_json` | `@captured: raw_payload as JSON` | Pre-transform as JSON |
| `_tags` | `@source: first(tags/_tags/meta/metadata.tags)` | First match wins |

## Configuration reference

### Loader config

```toml
[routing]
org_id_field = "org_id"           # Field to extract for _org_id
db_fields = []                     # Empty = shared schema
table_fields = ["event_category"]  # Field for table routing
default_db = "common"
default_table = "default"

[metadata]
capture_mode = "full"              # full (default) | raw_only | extracted_only
tags_fields = ["tags", "_tags", "meta", "metadata.tags"]
tags_output = "_tags"
drop_tags = false                  # Keep tags in _json after extraction

[timestamp]
field = "timestamp"                # Primary timestamp field
fallback = "now()"                 # Fallback if missing
formats = ["rfc3339", "epoch_ms", "epoch_s", "epoch_us", "epoch_ns"]
```

### Auto-initialisation

```toml
[auto_init]
enabled = true
create_topics = true
create_database = true
create_table = true
create_text_index = true
topic_partitions = 3
topic_replication_factor = 1
```

## Implementation files

| File | Purpose |
|------|---------|
| `src/clickhouse/schema.rs` | Reads the deployed columns from `system.columns` on a TTL |
| `src/column_meta/mod.rs` | Parses the `@directive` annotations out of column comments |
| `src/schema/mod.rs` | Schema parsing, table tags, capability detection |
| `src/transform/transformer.rs` | Profile-driven field injection |

The profile YAML itself lives in
[dfe-schemas](https://github.com/hyperi-io/dfe-schemas) under `common-header/`
and reaches the loader only as deployed ClickHouse columns -- see
[SCHEMAS.md](../deployment/SCHEMAS.md).

## Migration from v1

### Breaking changes

1. **Underscore prefix:** all system fields now prefixed with `_`.
   - `timestamp` -> `_timestamp`
   - `uuid` -> `_uuid`
   - `org_id` -> `_org_id`
   - `tags` -> `_tags`

2. **New fields:**
   - `_timestamp_load` (load time, was implicit).
   - `_timestamp_received` (optional received time).
   - `_raw` (raw payload, was `logoriginal`).
   - `_json` (JSON payload, was `logjson`).

3. **ORDER BY change:**
   - v1: `ORDER BY (timestamp, uuid)`
   - v2: `ORDER BY (_org_id, _timestamp_load, _uuid)`

### Migration script

```sql
-- Create new table with v2 schema
CREATE TABLE common.events_v2 AS common.events
ENGINE = MergeTree()
ORDER BY (_org_id, _timestamp_load, _uuid)
PARTITION BY (toYYYYMM(_timestamp_load), _org_id);

-- Migrate data
INSERT INTO common.events_v2
SELECT
    now64(3) AS _timestamp_load,
    timestamp AS _timestamp,
    NULL AS _timestamp_received,
    uuid AS _uuid,
    org_id AS _org_id,
    logoriginal AS _raw,
    logjson AS _json,
    tags AS _tags
FROM common.events;

-- Swap tables
RENAME TABLE common.events TO common.events_v1,
             common.events_v2 TO common.events;
```

## Profiles

The common header is controlled by **profiles** -- named, versioned YAML
definitions that specify which system fields to inject and how to populate them.

| Profile | Fields | Use case |
|---------|--------|----------|
| `timeseries` (default) | 9 | Full common header for event ingestion |
| `minimal` | 5 | High-volume structured data (no _raw,_tags, _source) |
| `passthrough` | 4 | Transparent bridge (no _timestamp injection) |

Profiles define:

- **Field set**: which common header columns to inject.
- **Field behaviour**: source expression for each field (how it is populated).
- **DDL structure**: ORDER BY, PARTITION BY, indexes (for default table creation).

Profiles do NOT define data-specific columns. Those come from the source data or
are defined in the table's own DDL.

### Profile versioning

Tables created by auto-init are tagged with their profile name and version in the
table comment:

```sql
COMMENT '@schema_source: core | @schema_version: 2 | @profile: timeseries | @profile_version: 1'
```

The migration tooling (`ProfileDiff`) can compare a profile's current version
against an existing table's tagged version and generate safe `ALTER TABLE ADD
COLUMN` statements for any missing common header fields.

### Custom profiles

User-defined profiles can be loaded from YAML files:

```yaml
# config
profiles:
  default: timeseries
  custom_dir: /etc/dfe-loader/profiles
  table_profiles:
    dfe.metrics: minimal
```

## Version history

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-12 | Initial schema (no underscore prefix) |
| 2.0 | 2026-01 | Underscore prefix, added `_timestamp_received`, RLS support |

## References

- [../clickhouse/DDL-DIRECTIVES.md](../clickhouse/DDL-DIRECTIVES.md) -- field mapping expression language
- [ClickHouse MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree)
- [ClickHouse JSON Type](https://clickhouse.com/docs/en/sql-reference/data-types/json)
- [Row-Level Security](https://clickhouse.com/docs/en/guides/sre/user-management/row-level-security)
