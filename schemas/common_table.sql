-- Common Table Schema v2
--
-- Template variables: {db}, {table}, {engine}, {table_comment}
--
-- Table-level tags (stored in table COMMENT):
--   @schema_source: core     - Pre-supplied schema from dfe-loader
--   @schema_source: user     - User-created schema (or absent)
--   @schema_version: 2       - Schema version for migrations
--
-- Engine auto-detection priority:
--   1. SharedMergeTree (ClickHouse Cloud / 24.1+)
--   2. ReplicatedMergeTree (clustered with ZooKeeper/Keeper)
--   3. MergeTree (single-node fallback)
--
-- Requirements:
--   - ClickHouse 24.8+ for generateUUIDv7()
--   - ClickHouse 25.x+ for stable JSON type
--   - ClickHouse 25.1+ for full_text index (GA)
--
-- Field Mapping: See docs/DDL-EXPRESSION.md for expression language reference

CREATE TABLE IF NOT EXISTS {db}.{table}
(
    `_timestamp_load` DateTime64(3) DEFAULT now64(3) COMMENT '@generated: now64(3) | Load time - primary query filter' CODEC(Delta, ZSTD(1)),
    `_timestamp` DateTime64(3) COMMENT '@source: timestamp | now() | Event occurrence time (minmax indexed)' CODEC(Delta, ZSTD(1)),
    `_timestamp_received` Nullable(DateTime64(3)) COMMENT '@source: timestamp_received | When receiver/loader received the event' CODEC(Delta, ZSTD(1)),
    `_uuid` UUID DEFAULT generateUUIDv7() COMMENT '@generated: generateUUIDv7() | Unique event ID UUIDv7 (time-ordered)',
    `_org_id` LowCardinality(String) COMMENT '@source: org_id | Organisation ID for RLS (first in ORDER BY)' CODEC(ZSTD(1)),
    `_raw` Nullable(String) COMMENT '@renamed: logoriginal | Original log line (zero-copy rename from source)' CODEC(ZSTD(3)),
    `_json` Nullable(JSON) COMMENT '@captured: raw_payload as JSON | Complete Kafka message as JSON' CODEC(ZSTD(3)),
    `_source` LowCardinality(String) COMMENT '@source: first(_source) | topic_name | Destination table / data source identifier' CODEC(ZSTD(1)),
    `_tags` Nullable(JSON) COMMENT '@source: first(tags/_tags/meta/metadata.tags) | Metadata and collector/agent info' CODEC(ZSTD(3)),

    INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1
)
ENGINE = {engine}
ORDER BY (_org_id, _timestamp_load, _uuid)
PARTITION BY (toYYYYMM(_timestamp_load), _org_id)
SETTINGS index_granularity = 8192
COMMENT '{table_comment}'

-- Schema Design Notes:
--
-- ORDER BY (_org_id, _timestamp_load, _uuid):
--   - _org_id first: Every query has org_id (RLS), data grouped by org
--   - _timestamp_load second: Primary time filter within each org
--   - _uuid last: Ensures uniqueness and deterministic ordering
--
-- PARTITION BY (toYYYYMM(_timestamp_load), _org_id):
--   - Monthly partitions by load time
--   - Includes _org_id for partition pruning (<100 orgs = acceptable cardinality)
--   - Enables efficient TTL and per-org data management
--
-- LowCardinality(_org_id):
--   - Dictionary encoding for repeated org IDs
--   - Reduces storage and speeds up equality comparisons
--
-- _timestamp vs _timestamp_load:
--   - _timestamp_load: When data was loaded (query filter, ORDER BY)
--   - _timestamp: When event occurred (event time, minmax index)
--   - Most queries filter on load time for operational dashboards
--
-- Text Search (configurable, default ON):
--   - Add via ALTER TABLE when enabled:
--     ALTER TABLE {db}.{table} ADD INDEX idx_raw _raw
--       TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
--   - Or when full_text is GA (25.1+):
--     ALTER TABLE {db}.{table} ADD INDEX idx_raw _raw
--       TYPE full_text(0) GRANULARITY 1
