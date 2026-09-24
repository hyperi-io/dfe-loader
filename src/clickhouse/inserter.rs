// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/inserter.rs
// Purpose:   Batch inserter for ClickHouse with retry and salvage
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Batch inserter for `ClickHouse` with format-aware dispatch.
//!
//! Handles batch inserts with retry logic using `Vec<Map<String, Value>>`.
//! All inserts route through the clickhouse-rs fork's `Client`:
//! - `RowBinary`: `DynamicInsert` — schema-reflected binary encoding (default)
//! - `JsonEachRow`: `InsertFormatted` — NDJSON body via HTTP (fallback)
//!
//! ## Error Classification
//!
//! Errors are classified to determine retry strategy:
//! - **Transient** (Server/Protocol): Geometric backoff retry, never DLQ
//! - **Data**: Binary-split salvage to isolate bad rows, DLQ those rows only
//! - **Fatal**: Don't retry, fail immediately
//!
//! ## Batch Salvage (Data Errors Only)
//!
//! When a batch insert fails with a DATA error:
//! 1. Split the failed batch in half
//! 2. Recursively retry each half
//! 3. Continue splitting until single-row failure is identified
//! 4. Return failed rows for DLQ routing

use std::sync::Arc;
use std::time::Duration;

use backon::BackoffBuilder;
use bytes::Bytes;
use scalo::sink_stack::SinkStackConfig;
use serde_json::{Map, Value};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};

use crate::Result;
use crate::buffer::{FlushBatch, KafkaOffset};
use crate::clickhouse::client_http::escape_identifier;
use crate::clickhouse::config::InsertFormat;
use crate::clickhouse::error::{
    ErrorCategory, classify_dynamic_error, classify_insert_end_error, classify_json_insert_error,
    is_schema_drift_error,
};
use crate::clickhouse::{ClickHouseQueryClient, SchemaCache};
use crate::clickhouse_ext::{ColumnDef, json_shaping_changes, shape_json_for_type};
use crate::transform::Coercer;

/// Split "db.table" into (db, table). Panics if no dot — callers always
/// pass fully qualified names from `BufferManager`.
fn parse_db_table(table: &str) -> (&str, &str) {
    table.split_once('.').unwrap_or(("default", table))
}

/// Configuration for the inserter
pub struct InserterConfig {
    /// Maximum retries for transient errors (geometric backoff)
    pub max_retries: u32,
    /// Base delay for geometric backoff (doubles each retry), before jitter
    pub base_retry_delay_ms: u64,
    /// Cap on the geometric backoff, before jitter adds up to the same again
    pub max_retry_delay_ms: u64,
    /// Enable batch salvage on data errors
    pub enable_salvage: bool,
    /// Maximum depth for binary-split salvage (prevents infinite recursion)
    pub max_salvage_depth: u32,
    /// Maximum concurrent inserts (0 = unlimited)
    pub max_concurrent_inserts: usize,
}

impl Default for InserterConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 30_000,
            enable_salvage: true,
            max_salvage_depth: 20,
            max_concurrent_inserts: 8,
        }
    }
}

impl InserterConfig {
    /// Calculate backoff delay for a given attempt (jittered geometric backoff).
    ///
    /// Delay = `min(base * 2^attempt, max_delay)`, plus a random share of that
    /// again.
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        calc_backoff(self.base_retry_delay_ms, attempt, self.max_retry_delay_ms)
    }
}

/// Doublings past which the backoff stops growing, whatever `max_ms` allows.
const MAX_BACKOFF_DOUBLINGS: u32 = 16;

/// Jittered geometric backoff on scalo's sink schedule: `min(base_ms *
/// 2^attempt, max_ms)` plus up to the same again, so loader pods retrying one
/// recovering server do not all arrive at once.
fn calc_backoff(base_ms: u64, attempt: u32, max_ms: u64) -> Duration {
    SinkStackConfig {
        // backon takes its first step before it applies the cap.
        min_backoff_ms: base_ms.min(max_ms),
        max_backoff_ms: max_ms,
        ..SinkStackConfig::default()
    }
    .backoff()
    .without_max_times()
    .build()
    .nth(attempt.min(MAX_BACKOFF_DOUBLINGS) as usize)
    .unwrap_or(Duration::from_millis(max_ms))
}

/// What the flush path must do with a batch's Kafka offsets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchDisposition {
    /// Every row is either in `ClickHouse` or in `InsertResult::failed`.
    /// Commit once the failed rows are somewhere durable.
    Settled,
    /// Something can still clear. Withhold the offsets and let Kafka
    /// re-deliver -- the safe direction, since the payload is still in Kafka.
    Retry(String),
}

/// Result of a batch insert with salvage
#[derive(Debug)]
pub struct InsertResult {
    /// Number of rows successfully inserted
    pub inserted: usize,
    /// Rows `ClickHouse` can never accept, with their Kafka offsets and reason
    pub failed: Vec<FailedRow>,
    /// Whether the batch's offsets may be committed.
    pub disposition: BatchDisposition,
    /// The batch itself on a `Retry`, for a transport with nothing to re-deliver.
    pub unsettled: Option<FlushBatch>,
}

impl InsertResult {
    pub fn success(count: usize) -> Self {
        Self {
            inserted: count,
            failed: Vec::new(),
            disposition: BatchDisposition::Settled,
            unsettled: None,
        }
    }

    pub fn with_failures(inserted: usize, failed: Vec<FailedRow>) -> Self {
        Self {
            inserted,
            failed,
            disposition: BatchDisposition::Settled,
            unsettled: None,
        }
    }

    /// A batch whose offsets must be withheld so Kafka re-delivers it.
    pub fn retry(inserted: usize, reason: impl Into<String>) -> Self {
        Self {
            inserted,
            failed: Vec::new(),
            disposition: BatchDisposition::Retry(reason.into()),
            unsettled: None,
        }
    }

    /// Hand the batch back with a `Retry`, so a caller whose transport cannot
    /// re-deliver it can hold it and insert it again.
    #[must_use]
    pub fn returning(mut self, batch: FlushBatch) -> Self {
        self.unsettled = Some(batch);
        self
    }

    /// Whether the flush path may commit this batch's offsets once the failed
    /// rows are accepted by the DLQ.
    #[must_use]
    pub fn is_settled(&self) -> bool {
        self.disposition == BatchDisposition::Settled
    }
}

/// Running state of a binary-split salvage.
#[derive(Debug, Default)]
struct Salvage {
    inserted: usize,
    /// Rows isolated as unencodable -- these go to the DLQ.
    failed: Vec<FailedRow>,
    /// Set by the first row that failed for a reason that can still clear.
    retry_reason: Option<String>,
}

/// Split a slice held parallel to the rows, tolerating a short or empty one.
///
/// `offsets` is empty on the gRPC path and `raw_payloads` is empty in
/// `extracted_only` capture mode, so neither can be indexed blind.
fn split_parallel<T>(s: &[T], mid: usize) -> (&[T], &[T]) {
    if s.len() < mid {
        (s, &[])
    } else {
        s.split_at(mid)
    }
}

/// A row that failed to insert
#[derive(Debug)]
pub struct FailedRow {
    /// Row index in the original batch
    pub row_index: usize,
    /// Kafka offset for this row (if available)
    pub offset: Option<KafkaOffset>,
    /// Error message
    pub reason: String,
    /// The promoted row, serialised, for the DLQ to carry when the batch's
    /// parallel raw-payload slot is empty. Filled by `rejected_row_bytes`.
    pub row_json: Option<Vec<u8>>,
}

/// Whether binary-split salvage can actually isolate this failure to a row.
///
/// Only a data error belongs to a row. Anything else -- an unreachable sink, a
/// missing GRANT, a column type this build cannot encode -- is equally true of
/// every row in the slice, so splitting on it issues ~2N inserts against
/// something already struggling and isolates nothing.
fn is_salvageable(err: &crate::Error) -> bool {
    matches!(err, crate::Error::ClickHousePermanent(_))
}

/// The bytes a permanently rejected row must carry to the DLQ.
///
/// `raw_payloads[i]` is empty for every capture mode that keeps no raw bytes:
/// `raw_only` and `extracted_only`, the whole `legacy_flatten` path, and any
/// `MessagePack` payload. DLQ'ing an empty entry for those and then committing
/// the offset loses the event from `ClickHouse`, the DLQ and Kafka at once, so
/// fall back to the promoted row -- under `raw_only` it still holds the payload
/// as `_raw`, under `legacy_flatten` full capture as `_json`.
///
/// Returns `None` when the raw slot already has the bytes: the flush path reads
/// those directly and re-serialising would only duplicate them.
fn rejected_row_bytes(
    raw: Option<&Arc<[u8]>>,
    row: Option<&Map<String, Value>>,
) -> Option<Vec<u8>> {
    if raw.is_some_and(|p| !p.is_empty()) {
        return None;
    }
    row.and_then(|r| serde_json::to_vec(r).ok())
}

/// Handles batch inserts to `ClickHouse` with retry logic.
///
/// All fields are `Copy` primitives or `Arc`-wrapped — `Clone` is `O(fields)` with
/// cheap reference-count increments. Used to move a per-task inserter into `tokio::spawn`
/// without reconstructing fields manually.
///
/// ## Error Handling Strategy
///
/// - **Transient errors** (overload, network): Geometric backoff retry
/// - **Data errors** (type mismatch, corrupt): Binary-split salvage → DLQ bad rows
/// - **Fatal errors** (auth, schema): Fail immediately, no retry
#[derive(Clone)]
pub struct Inserter {
    /// HTTP client for DDL and schema queries only (no inserts)
    http_client: Arc<ClickHouseQueryClient>,
    /// The fork client -- HTTP or native TCP per config. RowBinary inserts
    /// dispatch by transport; `JSONEachRow` is HTTP-only.
    ch_client: clickhouse::Client,
    /// Schema cache for the dynamic RowBinary path, shared across
    /// `DynamicInsert` instances and invalidated on schema-mismatch recovery.
    dynamic_schema_cache: Arc<crate::clickhouse_ext::DynamicSchemaCache>,
    /// Insert format — `RowBinary` (schema-reflected) or `JSONEachRow`
    insert_format: InsertFormat,
    max_retries: u32,
    base_retry_delay_ms: u64,
    max_retry_delay_ms: u64,
    enable_salvage: bool,
    max_salvage_depth: u32,
    /// Semaphore for limiting concurrent inserts
    semaphore: Option<Arc<Semaphore>>,
    /// Schema cache for type-aware coercion (`JSONEachRow` path)
    schema_cache: Option<Arc<SchemaCache>>,
    /// Type coercer applied before each insert (`JSONEachRow` path)
    coercer: Option<Arc<Coercer>>,
    /// Whether a schema-drift insert error drops the cached schema
    /// (`schema.refresh_on_error`).
    refresh_on_error: bool,
}

impl Inserter {
    /// Create a new inserter.
    ///
    /// The `ch_client` handles ALL inserts — `RowBinary` via `DynamicInsert`,
    /// `JSONEachRow` via `InsertFormatted`. The `http_client` is used only for
    /// DDL and schema queries.
    pub fn new(
        http_client: Arc<ClickHouseQueryClient>,
        ch_client: clickhouse::Client,
        config: InserterConfig,
    ) -> Self {
        let semaphore = if config.max_concurrent_inserts > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_inserts)))
        } else {
            None
        };

        Self {
            http_client,
            ch_client,
            dynamic_schema_cache: crate::clickhouse_ext::DynamicSchemaCache::new(
                std::time::Duration::from_secs(300),
            ),
            insert_format: InsertFormat::default(),
            max_retries: config.max_retries,
            base_retry_delay_ms: config.base_retry_delay_ms,
            max_retry_delay_ms: config.max_retry_delay_ms,
            enable_salvage: config.enable_salvage,
            max_salvage_depth: config.max_salvage_depth,
            semaphore,
            schema_cache: None,
            coercer: None,
            refresh_on_error: true,
        }
    }

    /// Set the insert format.
    ///
    /// - `RowBinary` (default): schema-reflected binary via `DynamicInsert`.
    ///   `ClickHouse` skips JSON parsing — lower cluster CPU at scale.
    /// - `JsonEachRow`: NDJSON via `InsertFormatted`. Self-describing fallback.
    pub fn with_insert_format(mut self, format: InsertFormat) -> Self {
        self.insert_format = format;
        self
    }

    /// Set the schema cache for cache invalidation on schema-drift errors.
    ///
    /// When set, the inserter invalidates this cache on data errors that
    /// suggest the RowBinary encoding used a stale schema (e.g., "Cannot
    /// parse JSON", "type mismatch", "INCORRECT_DATA"). This forces a
    /// fresh schema fetch on the next insert attempt.
    pub fn with_schema_cache(mut self, schema_cache: Arc<SchemaCache>) -> Self {
        self.schema_cache = Some(schema_cache);
        self
    }

    /// Set whether a schema-drift insert error drops the cached schema so the
    /// retry re-reads it (default `true`).
    ///
    /// With `false` both the `RowBinary` encoder's cache and the loader's
    /// [`SchemaCache`] keep the stale schema until its TTL expires, so the
    /// retries run against it. A schema fetch that failed is still discarded.
    pub fn with_refresh_on_error(mut self, enabled: bool) -> Self {
        self.refresh_on_error = enabled;
        self
    }

    /// Drop the loader's cached schema for `table`, so its next reader re-fetches.
    fn drop_cached_schema(&self, table: &str) {
        if let Some(cache) = &self.schema_cache {
            cache.invalidate(table);
        }
    }

    /// Enable schema-driven type coercion before each insert.
    ///
    /// When enabled, the inserter fetches the table schema (from cache or live)
    /// and applies the coercer to each row before sending to `ClickHouse`. This
    /// covers cases that `ClickHouse`'s `JSONEachRow` server-side coercion does not
    /// handle automatically — e.g., epoch ms integers into `DateTime64` columns,
    /// UUID normalisation, string "true"/"1" into Bool, and null handling for
    /// non-nullable columns.
    pub fn with_schema_coercion(
        mut self,
        schema_cache: Arc<SchemaCache>,
        coercer: Arc<Coercer>,
    ) -> Self {
        self.schema_cache = Some(schema_cache);
        self.coercer = Some(coercer);
        self
    }

    /// Apply schema-driven coercion to a batch of rows.
    ///
    /// Fetches schema from cache (or live from `ClickHouse` on cache miss), then
    /// applies the coercer to every row. On schema fetch failure, rows are sent
    /// as-is and `ClickHouse`'s server-side coercion handles them.
    async fn coerce_batch(&self, table: &str, rows: &mut [Map<String, Value>]) {
        let (coercer, cache) = match (&self.coercer, &self.schema_cache) {
            (Some(c), Some(sc)) => (c, sc),
            _ => return, // Coercion not configured — pass through
        };

        let schema = match cache.get(table) {
            Some(s) => s,
            None => match self.http_client.fetch_table_schema(table).await {
                Ok(s) => {
                    cache.insert(table.to_string(), s.clone());
                    s
                }
                Err(e) => {
                    warn!(table = %table, error = %e, "Schema fetch failed, skipping coercion");
                    return;
                }
            },
        };

        for row in rows.iter_mut() {
            if let Err(e) = coercer.coerce_row(row, &schema) {
                warn!(table = %table, error = %e, "Row coercion failed, row sent as-is");
            }
        }
    }

    /// The table's columns that carry a JSON type at any depth, read through
    /// the same schema cache the RowBinary path fills.
    ///
    /// A schema that cannot be fetched returns none and the rows go unshaped,
    /// which is where this path stood before shaping existed.
    async fn json_columns(&self, table: &str) -> Vec<ColumnDef> {
        let (db, tbl) = parse_db_table(table);
        let schema = match self.dynamic_schema_cache.get(table) {
            Some(schema) => schema,
            None => {
                match crate::clickhouse_ext::fetch_dynamic_schema(&self.ch_client, db, tbl).await {
                    Ok(fetched) => {
                        self.dynamic_schema_cache.insert(table, fetched.clone());
                        fetched
                    }
                    Err(e) => {
                        warn!(table = %table, error = %e, "Schema fetch failed, JSON columns not shaped");
                        return Vec::new();
                    }
                }
            }
        };
        if !schema.has_json_columns() {
            return Vec::new();
        }
        schema
            .columns
            .iter()
            .filter(|c| c.ty.contains_json())
            .cloned()
            .collect()
    }

    /// Calculate backoff delay for a given attempt.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        calc_backoff(self.base_retry_delay_ms, attempt, self.max_retry_delay_ms)
    }

    /// Get connection pool stats. Currently always `None`: the hyperi-port
    /// TCP pool does not expose its status publicly yet (see
    /// `client_http::PoolStats`).
    pub fn pool_stats(&self) -> Option<crate::clickhouse::PoolStats> {
        None
    }

    /// Insert rows into a table with error-aware retry.
    ///
    /// Dispatches based on `insert_format`:
    /// - `RowBinary`: schema-reflected binary via `crate::clickhouse_ext::DynamicInsert`
    /// - `JsonEachRow`: HTTP `JSONEachRow` via `ClickHouseQueryClient`
    ///
    /// - **Transient errors**: Geometric backoff retry up to `max_retries`
    /// - **Data errors**: Returns immediately (caller should salvage)
    /// - **Fatal errors**: Returns immediately (no retry)
    ///
    /// `raw_payloads` is parallel to `rows` — used for zero-copy `_json` splice.
    /// For RowBinary: passed to `DynamicInsert::write_map_with_raw()` for direct
    /// byte encoding (no row cloning). For JSONEachRow: spliced into NDJSON body.
    pub async fn insert_rows(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
        raw_payloads: &[Arc<[u8]>],
    ) -> Result<usize> {
        match self.insert_format {
            InsertFormat::RowBinary => self.insert_rows_rowbinary(table, rows, raw_payloads).await,
            InsertFormat::JsonEachRow => self.insert_rows_json(table, rows, raw_payloads).await,
        }
    }

    /// `RowBinary` insert path — schema-reflected binary encoding via `DynamicInsert`.
    ///
    /// `ClickHouse` receives pre-columnarised data, zero server-side JSON parsing.
    /// On schema mismatch, invalidates cache and retries once with fresh schema.
    ///
    /// `raw_payloads` is parallel to `rows` — non-empty entries are passed to
    /// `DynamicInsert::write_map_with_raw()` for zero-copy `_json` encoding.
    async fn insert_rows_rowbinary(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
        raw_payloads: &[Arc<[u8]>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        debug!(
            table = %table,
            rows = rows.len(),
            format = "RowBinary",
            "Insert started"
        );

        let insert_start = std::time::Instant::now();

        let (db, tbl) = parse_db_table(table);

        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let mut insert = crate::clickhouse_ext::DynamicInsert::new(
                self.ch_client.clone(),
                db,
                tbl,
                Arc::clone(&self.dynamic_schema_cache),
            )
            .refresh_on_error(self.refresh_on_error);

            let mut write_failed = false;
            for (i, row) in rows.iter().enumerate() {
                if tracing::enabled!(tracing::Level::TRACE) {
                    trace!(
                        table = %table,
                        row_index = i,
                        columns = row.len(),
                        "Encoding row"
                    );
                }
                // Zero-copy _json: pass raw bytes directly to the encoder when
                // the extractor path provides them. No row cloning, no String
                // allocation — raw Kafka payload flows straight to RowBinary.
                let raw = raw_payloads.get(i).filter(|r| !r.is_empty());
                let write_result = if let Some(raw_bytes) = raw {
                    insert
                        .write_map_with_raw(row, &[("_json", raw_bytes)])
                        .await
                } else {
                    insert.write_map(row).await
                };
                if let Err(e) = write_result {
                    // Schema mismatch — invalidate both fork and loader caches, then retry
                    if matches!(
                        e,
                        crate::clickhouse_ext::DynamicError::SchemaMismatch { .. }
                    ) {
                        if self.refresh_on_error {
                            insert.invalidate_schema();
                            self.drop_cached_schema(table);
                        }
                        last_error =
                            Some(crate::Error::ClickHouse(format!("Schema mismatch: {e}")));
                        write_failed = true;
                        break;
                    }
                    // Check for schema-drift-indicative errors beyond SchemaMismatch
                    if is_schema_drift_error(&e.to_string()) {
                        if self.refresh_on_error {
                            insert.invalidate_schema();
                            self.drop_cached_schema(table);
                        }
                        last_error = Some(crate::Error::ClickHouse(format!(
                            "Schema drift in write: {e}"
                        )));
                        write_failed = true;
                        break;
                    }
                    // A schema fetch that never reached the server, or a table
                    // still being created, is a fault of the moment: drop the
                    // cached schema and try again with a fresh one.
                    if matches!(
                        e,
                        crate::clickhouse_ext::DynamicError::SchemaFetch { .. }
                            | crate::clickhouse_ext::DynamicError::EmptySchema { .. }
                    ) {
                        insert.invalidate_schema();
                        self.drop_cached_schema(table);
                        last_error = Some(crate::Error::ClickHouse(format!(
                            "Schema unavailable during write: {e}"
                        )));
                        write_failed = true;
                        break;
                    }
                    // Only an encoding failure is a verdict on the payload. A
                    // type this build cannot encode is a loader gap, so those
                    // offsets are withheld and the rows come back.
                    return Err(match classify_dynamic_error(&e) {
                        ErrorCategory::Data => {
                            crate::Error::ClickHousePermanent(format!("RowBinary encode: {e}"))
                        }
                        _ => crate::Error::ClickHouse(format!("RowBinary encode: {e}")),
                    });
                }
            }

            if write_failed {
                if attempt < self.max_retries {
                    let delay = self.backoff_delay(attempt);
                    debug!(
                        table = %table,
                        attempt = attempt,
                        delay_ms = delay.as_millis(),
                        "Insert retry"
                    );
                    warn!(
                        table = %table,
                        attempt,
                        delay_ms = delay.as_millis(),
                        refresh_on_error = self.refresh_on_error,
                        "Schema issue during write, retrying"
                    );
                    sleep(delay).await;
                    continue;
                }
                break;
            }

            match insert.end().await {
                Ok(count) => {
                    debug!(
                        table = %table,
                        rows = count,
                        duration_ms = insert_start.elapsed().as_millis(),
                        "Insert completed"
                    );
                    return Ok(count as usize);
                }
                Err(crate::clickhouse_ext::DynamicError::SchemaMismatch { .. }) => {
                    // Invalidate loader's schema cache alongside the fork's
                    if self.refresh_on_error {
                        self.drop_cached_schema(table);
                    }
                    if attempt < self.max_retries {
                        let delay = self.backoff_delay(attempt);
                        warn!(
                            table = %table,
                            attempt,
                            delay_ms = delay.as_millis(),
                            refresh_on_error = self.refresh_on_error,
                            "Schema mismatch on end(), retrying"
                        );
                        sleep(delay).await;
                        last_error = Some(crate::Error::ClickHouse(
                            "Schema mismatch on flush".to_string(),
                        ));
                        continue;
                    }
                    last_error = Some(crate::Error::ClickHouse(
                        "Schema mismatch after max retries".to_string(),
                    ));
                }
                Err(e) => {
                    let message = format!("RowBinary insert: {e}");
                    // insert.end() consumed the DynamicInsert, so only the
                    // loader's cache is dropped here; the retry still reads the
                    // encoder's cached schema.
                    if is_schema_drift_error(&e.to_string()) {
                        if self.refresh_on_error {
                            self.drop_cached_schema(table);
                        }
                        if attempt < self.max_retries {
                            let delay = self.backoff_delay(attempt);
                            warn!(
                                table = %table,
                                attempt,
                                error = %e,
                                delay_ms = delay.as_millis(),
                                refresh_on_error = self.refresh_on_error,
                                "Schema drift on end(), retrying"
                            );
                            sleep(delay).await;
                            last_error = Some(crate::Error::ClickHouse(message));
                            continue;
                        }
                        last_error = Some(crate::Error::ClickHouse(message));
                        break;
                    }
                    // Permanent means the payload can never encode. Drift was
                    // ruled out above, so the message classification decides.
                    return Err(match classify_insert_end_error(&message) {
                        ErrorCategory::Data => crate::Error::ClickHousePermanent(message),
                        _ => crate::Error::ClickHouse(message),
                    });
                }
            }
        }

        Err(last_error.unwrap_or_else(|| crate::Error::Buffer("Max retries exceeded".into())))
    }

    /// `JSONEachRow` insert path — NDJSON via fork's `InsertFormatted`.
    ///
    /// Serialises `Map<String, Value>` rows as NDJSON and sends via the fork's
    /// HTTP client. `raw_payloads` enables zero-copy `_json` splice.
    async fn insert_rows_json(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
        raw_payloads: &[Arc<[u8]>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        debug!(
            table = %table,
            rows = rows.len(),
            format = "JSONEachRow",
            "Insert started"
        );

        let insert_start = std::time::Instant::now();

        let (db, tbl) = parse_db_table(table);

        // This path serialises the row map as it stands, so a value bound for a
        // JSON column is shaped here — the encoder does it for RowBinary, and a
        // hoisted `tags` array reaching the column unshaped is code 117.
        let json_columns = self.json_columns(table).await;

        // Serialise rows as NDJSON — sonic_rs for SIMD-accelerated encoding.
        let estimated_size = rows.len() * 256;
        let mut body = Vec::with_capacity(estimated_size);
        for (idx, row) in rows.iter().enumerate() {
            let shaped = shape_json_row(row, &json_columns);
            let row = shaped.as_ref().unwrap_or(row);
            match raw_payloads.get(idx) {
                // Zero-copy `_json` splice; an empty entry (or none at all) is
                // a transformer-path row carrying its own `_json`, or no `_json`.
                Some(raw) if !raw.is_empty() => write_row_with_json(&mut body, row, raw)?,
                _ => {
                    sonic_rs::to_writer(&mut body, row).map_err(|e| {
                        crate::Error::ClickHousePermanent(format!("JSON serialisation error: {e}"))
                    })?;
                    body.push(b'\n');
                }
            }
        }

        // Defence-in-depth: escape db/tbl identifiers even though both are
        // operator-controlled (routing config + sanitised _source).
        let sql = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            escape_identifier(db),
            escape_identifier(tbl)
        );
        let body = Bytes::from(body); // Cheap clone for retries (ref-counted)

        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            // JSONEachRow uses the HTTP InsertFormatted path. On a TCP-only
            // client this surfaces as a transport error at send() time (the
            // client has no HTTP url); RowBinary is the default and works on
            // both transports via insert_native_with_columns.
            let mut insert = self.ch_client.insert_formatted_with(sql.clone());

            // One INSERT carries one verdict, and which call surfaces it is an
            // accident of buffering: the fork queues the body on a channel, so
            // a server rejection reaches a small batch at end() and a large one
            // at send(), where the closed channel makes it return the response
            // error instead. Classifying only one of them is what let a
            // rejected JSON batch read as retryable and come back forever.
            // Bytes clone is O(1).
            let outcome = match insert.send(body.clone()).await {
                Ok(()) => insert.end().await,
                Err(e) => Err(e),
            };

            let Err(e) = outcome else {
                debug!(
                    table = %table,
                    rows = rows.len(),
                    duration_ms = insert_start.elapsed().as_millis(),
                    "Insert completed"
                );
                return Ok(rows.len());
            };

            let message = format!("JSONEachRow insert: {e}");
            match classify_json_insert_error(&e) {
                ErrorCategory::Data => {
                    debug!(
                        table = %table,
                        error = %e,
                        "Server rejected the payload, returning for salvage"
                    );
                    return Err(crate::Error::ClickHousePermanent(message));
                }
                ErrorCategory::Fatal => {
                    error!(table = %table, error = %e, "Fatal error, not retrying");
                    return Err(crate::Error::ClickHouse(message));
                }
                ErrorCategory::Transient | ErrorCategory::Unknown => {
                    last_error = Some(crate::Error::ClickHouse(message));
                    if attempt < self.max_retries {
                        let delay = self.backoff_delay(attempt);
                        warn!(
                            table = %table,
                            attempt,
                            delay_ms = delay.as_millis(),
                            error = %e,
                            "Transient error, backing off"
                        );
                        sleep(delay).await;
                        continue;
                    }
                    error!(
                        table = %table,
                        attempts = self.max_retries + 1,
                        error = %e,
                        "Max retries exhausted for transient error"
                    );
                }
            }
        }

        Err(last_error.unwrap_or_else(|| crate::Error::Buffer("Max retries exceeded".into())))
    }

    /// Insert a `FlushBatch` with error-aware retry and salvage.
    ///
    /// Error handling strategy:
    /// - **Transient errors**: Retried with geometric backoff (handled by `insert_rows`)
    /// - **Data errors**: Binary-split salvage to isolate bad rows for DLQ
    /// - **Fatal errors**: Fail entire batch (no salvage, no DLQ)
    pub async fn insert_with_salvage(&self, batch: FlushBatch) -> InsertResult {
        let table = batch.table;
        let mut rows = batch.rows;
        let offsets = batch.offsets;
        let raw_payloads = batch.raw_payloads;
        let num_rows = rows.len();

        // Apply schema-driven coercion if configured. Done once here --
        // salvage sub-batches reuse already-coerced rows.
        self.coerce_batch(&table, &mut rows).await;

        // Pass raw_payloads for zero-copy _json splice (json_primary path);
        // salvage sub-batches keep their slice of it.
        match self.insert_rows(&table, &rows, &raw_payloads).await {
            Ok(count) => {
                return InsertResult::success(count);
            }
            Err(e) => {
                // insert_rows has already classified this, so a permanent
                // rejection is the only thing salvage can isolate. Splitting
                // anything else issues 2N inserts against a sink that is
                // already struggling, for rows that were never at fault.
                let is_data_error = is_salvageable(&e);

                // max_dynamic_paths guidance: metric always, log debounced.
                {
                    use std::sync::atomic::AtomicU64;
                    static MAX_PATHS_TS: AtomicU64 = AtomicU64::new(0);
                    let err_str = e.to_string();
                    if crate::clickhouse::error::is_max_dynamic_paths_error(&err_str) {
                        // Metric: always (dashboards need accurate count)
                        metrics::counter!(
                            "loader_json_max_paths_exceeded_total",
                            "table" => table.to_string()
                        )
                        .increment(1);
                        // Log: debounced at 5 min (operator guidance, not spam)
                        if scalo::logger::log_debounced(&MAX_PATHS_TS, 300_000) {
                            warn!(
                                table = %table,
                                "Table _json column hit max_dynamic_paths limit. \
                                 Paths beyond the limit are stored in shared data (slower queries). \
                                 Fix: ALTER TABLE {table} MODIFY COLUMN \
                                 _json JSON(max_dynamic_paths = 4096). \
                                 Note: requires empty column. \
                                 Consider capture_mode = 'raw_only' for high-cardinality tables."
                            );
                        }
                    }
                }

                if !is_data_error {
                    // A missing table or a missing grant is an operator fix,
                    // not a bad payload, so the rows wait in Kafka for it.
                    error!(
                        table = %table,
                        rows = num_rows,
                        error = %e,
                        "Insert failed, returning the batch for retry"
                    );
                    return InsertResult::retry(0, e.to_string()).returning(FlushBatch {
                        table,
                        rows,
                        offsets,
                        raw_payloads,
                    });
                }

                if !self.enable_salvage || num_rows <= 1 {
                    let reason = e.to_string();
                    let failed: Vec<FailedRow> = (0..num_rows)
                        .map(|i| FailedRow {
                            row_index: i,
                            offset: offsets.get(i).cloned(),
                            reason: reason.clone(),
                            row_json: rejected_row_bytes(raw_payloads.get(i), rows.get(i)),
                        })
                        .collect();
                    return InsertResult::with_failures(0, failed);
                }

                info!(
                    table = %table,
                    rows = num_rows,
                    error = %e,
                    "Data error, starting batch salvage to isolate bad rows"
                );
            }
        }

        // Binary-split salvage (only for data errors)
        let mut salvage = Salvage::default();

        self.salvage_batch(&table, &rows, &offsets, &raw_payloads, 0, 0, &mut salvage)
            .await;

        info!(
            table = %table,
            inserted = salvage.inserted,
            failed = salvage.failed.len(),
            "Batch salvage complete"
        );

        // One row that needs to come back drags the batch's offsets with it:
        // the offsets are committed as a block, so a partial commit would lose
        // the rows behind it.
        match salvage.retry_reason {
            // The whole batch goes back, so rows salvage already landed arrive
            // twice on a retry: duplicates, never loss.
            Some(reason) => InsertResult::retry(salvage.inserted, reason).returning(FlushBatch {
                table,
                rows,
                offsets,
                raw_payloads,
            }),
            None => InsertResult::with_failures(salvage.inserted, salvage.failed),
        }
    }

    /// Recursively salvage a batch using binary split.
    ///
    /// Splits `rows` slice in half, retries each half. Recurses until
    /// single-row failures are isolated for DLQ routing.
    ///
    /// `raw_payloads` is sliced in step with `rows` so a salvaged row keeps its
    /// zero-copy `_json`; dropping it would land the row with an empty `_json`.
    #[allow(clippy::too_many_arguments)]
    fn salvage_batch<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [Map<String, Value>],
        offsets: &'a [KafkaOffset],
        raw_payloads: &'a [Arc<[u8]>],
        start_index: usize,
        depth: u32,
        out: &'a mut Salvage,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let num_rows = rows.len();

            if depth > self.max_salvage_depth {
                error!(
                    table = %table,
                    depth = depth,
                    rows = num_rows,
                    "Max salvage depth exceeded, marking all rows as failed"
                );
                for i in 0..num_rows {
                    out.failed.push(FailedRow {
                        row_index: start_index + i,
                        offset: offsets.get(i).cloned(),
                        reason: "Max salvage depth exceeded".to_string(),
                        row_json: rejected_row_bytes(raw_payloads.get(i), rows.get(i)),
                    });
                }
                return;
            }

            // Base case: single row
            if num_rows == 1 {
                match self.insert_rows(table, rows, raw_payloads).await {
                    Ok(_) => {
                        out.inserted += 1;
                    }
                    Err(e) => {
                        if is_salvageable(&e) {
                            out.failed.push(FailedRow {
                                row_index: start_index,
                                offset: offsets.first().cloned(),
                                reason: e.to_string(),
                                row_json: rejected_row_bytes(raw_payloads.first(), rows.first()),
                            });
                        } else {
                            out.retry_reason.get_or_insert_with(|| e.to_string());
                        }
                    }
                }
                return;
            }

            // Try the whole slice first (might succeed now, e.g., transient
            // error). Skipped at depth 0: the caller has just made that exact
            // attempt, and repeating it doubles the cost of every rejection.
            //
            // Splitting is only ever right for a DATA error. A transient one
            // here means the sink is struggling, and halving on it issues ~2N
            // more inserts into that -- for rows that were never at fault. The
            // is_data_error guard at the entry point only covers depth 0, so
            // the same classification has to happen on every deeper attempt.
            if depth > 0 {
                match self.insert_rows(table, rows, raw_payloads).await {
                    Ok(count) => {
                        out.inserted += count;
                        return;
                    }
                    Err(e) if !is_salvageable(&e) => {
                        debug!(
                            table = %table,
                            depth = depth,
                            rows = num_rows,
                            error = %e,
                            "Transient failure mid-salvage, withholding instead of splitting"
                        );
                        out.retry_reason.get_or_insert_with(|| e.to_string());
                        return;
                    }
                    Err(_) => {}
                }
            }
            // Split and recurse

            // Split in half
            let mid = num_rows / 2;
            let (left_rows, right_rows) = rows.split_at(mid);
            let (left_offsets, right_offsets) = split_parallel(offsets, mid);
            let (left_raw, right_raw) = split_parallel(raw_payloads, mid);

            debug!(
                table = %table,
                depth = depth,
                left_rows = mid,
                right_rows = num_rows - mid,
                "Splitting batch for salvage"
            );

            self.salvage_batch(
                table,
                left_rows,
                left_offsets,
                left_raw,
                start_index,
                depth + 1,
                out,
            )
            .await;

            self.salvage_batch(
                table,
                right_rows,
                right_offsets,
                right_raw,
                start_index + mid,
                depth + 1,
                out,
            )
            .await;
        })
    }

    /// Insert multiple batches concurrently with salvage.
    ///
    /// Uses semaphore to limit concurrent inserts if configured.
    /// Each spawn gets a clone of `self` with `semaphore: None` — the permit
    /// is acquired in the outer scope before spawning, so the inner inserter
    /// must not try to acquire it again.
    pub async fn insert_batches_with_salvage(
        &self,
        batches: Vec<FlushBatch>,
    ) -> Vec<(String, InsertResult)> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let table = batch.table.to_string();
            let semaphore = self.semaphore.clone();
            let inserter = Self {
                semaphore: None,
                ..self.clone()
            };

            handles.push(tokio::spawn(async move {
                let _permit = match &semaphore {
                    Some(sem) => Some(sem.acquire().await.expect("Semaphore closed")),
                    None => None,
                };
                let result = inserter.insert_with_salvage(batch).await;
                (table, result)
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok((table, result)) => results.push((table, result)),
                Err(e) => {
                    // A panicked task proves nothing about the payload, so the
                    // offsets stay withheld rather than being DLQ'd blind.
                    results.push((
                        "unknown".to_string(),
                        InsertResult::retry(0, format!("Insert task panicked: {e}")),
                    ));
                }
            }
        }

        results
    }
}

/// Shape a row's JSON-column values for the JSONEachRow body, cloning the row
/// only when a value must change.
fn shape_json_row(
    row: &Map<String, Value>,
    json_columns: &[ColumnDef],
) -> Option<Map<String, Value>> {
    let changes = json_columns.iter().any(|col| {
        row.get(&col.name)
            .is_some_and(|value| json_shaping_changes(value, &col.ty))
    });
    if !changes {
        return None;
    }
    let mut shaped = row.clone();
    for col in json_columns {
        if let Some(value) = shaped.get_mut(&col.name) {
            shape_json_for_type(value, &col.ty);
        }
    }
    Some(shaped)
}

/// Serialise one promoted-column row extended with a `_json` field.
///
/// Splices the raw payload bytes directly as the `_json` value — zero-copy for
/// the fast path. No parsing of `raw` is required: `ClickHouse` receives the raw
/// JSON bytes verbatim and ingests them into the native JSON column.
///
/// Handles the empty-map edge case: `{}` + splice → `{"_json": raw}`.
/// Non-empty maps: strip trailing `}`, append `,"_json": raw}`.
fn write_row_with_json(body: &mut Vec<u8>, row: &Map<String, Value>, raw: &[u8]) -> Result<()> {
    if row.is_empty() {
        body.extend_from_slice(b"{\"_json\":");
        body.extend_from_slice(raw);
        body.push(b'}');
    } else {
        sonic_rs::to_writer(&mut *body, row).map_err(|e| {
            crate::Error::ClickHousePermanent(format!("JSON serialisation error: {e}"))
        })?;
        let last = body.len() - 1;
        debug_assert_eq!(body[last], b'}', "sonic_rs must produce a closing brace");
        body[last] = b',';
        body.extend_from_slice(b"\"_json\":");
        body.extend_from_slice(raw);
        body.push(b'}');
    }
    body.push(b'\n');
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inserter_config_default() {
        let config = InserterConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_retry_delay_ms, 100);
        assert!(config.enable_salvage);
        assert_eq!(config.max_salvage_depth, 20);
    }

    #[test]
    fn test_insert_result_success() {
        let result = InsertResult::success(100);
        assert_eq!(result.inserted, 100);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_insert_result_with_failures() {
        let failed = vec![
            FailedRow {
                row_index: 5,
                offset: None,
                reason: "Schema mismatch".to_string(),
                row_json: None,
            },
            FailedRow {
                row_index: 10,
                offset: None,
                reason: "Invalid data".to_string(),
                row_json: None,
            },
        ];
        let result = InsertResult::with_failures(98, failed);
        assert_eq!(result.inserted, 98);
        assert_eq!(result.failed.len(), 2);
        assert_eq!(result.failed[0].row_index, 5);
        assert_eq!(result.failed[1].row_index, 10);
        assert!(result.is_settled());
    }

    #[test]
    fn a_retry_disposition_carries_no_dlq_rows() {
        // Withholding is the safe direction: the payload is still in Kafka, so
        // nothing may be DLQ'd and nothing may be committed.
        let result = InsertResult::retry(0, "connection reset by peer");
        assert!(!result.is_settled());
        assert!(result.failed.is_empty());
        assert_eq!(
            result.disposition,
            BatchDisposition::Retry("connection reset by peer".to_string())
        );
    }

    #[test]
    fn split_parallel_tolerates_a_short_or_empty_companion() {
        // offsets is empty on the gRPC path and raw_payloads is empty in
        // extracted_only capture mode.
        let full = [1, 2, 3, 4];
        assert_eq!(split_parallel(&full, 2), (&full[..2], &full[2..]));

        let empty: [i32; 0] = [];
        assert_eq!(split_parallel(&empty, 2), (&empty[..], &empty[..]));

        let short = [1];
        assert_eq!(split_parallel(&short, 2), (&short[..], &empty[..]));

        // An exact-length split leaves an empty right half, not a panic.
        assert_eq!(split_parallel(&full, 4), (&full[..], &empty[..]));
    }

    #[test]
    fn test_inserter_config_semaphore() {
        let config = InserterConfig {
            max_concurrent_inserts: 4,
            ..Default::default()
        };
        assert_eq!(config.max_concurrent_inserts, 4);

        let unlimited = InserterConfig {
            max_concurrent_inserts: 0,
            ..Default::default()
        };
        assert_eq!(unlimited.max_concurrent_inserts, 0);
    }

    /// A jittered delay lies in `[step, 2 * step]`, give or take the
    /// millisecond scalo's f32 schedule can round by.
    #[track_caller]
    fn assert_jittered(delay: Duration, step_ms: u64) {
        let ms = delay.as_secs_f64() * 1000.0;
        let step = step_ms as f64;
        assert!(
            ms >= step - 1.0 && ms <= 2.0 * step + 1.0,
            "{delay:?} is outside the jittered step [{step_ms}ms, {}ms]",
            2 * step_ms
        );
    }

    #[test]
    fn test_backoff_delay_geometric() {
        let config = InserterConfig {
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 30_000,
            ..Default::default()
        };

        for (attempt, step_ms) in [(0, 100), (1, 200), (2, 400), (3, 800), (4, 1600), (5, 3200)] {
            assert_jittered(config.backoff_delay(attempt), step_ms);
        }
    }

    #[test]
    fn test_backoff_delay_capped() {
        let config = InserterConfig {
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 1000,
            ..Default::default()
        };

        assert_jittered(config.backoff_delay(10), 1000);
        assert_jittered(config.backoff_delay(20), 1000);
    }

    #[test]
    fn two_retries_of_the_same_attempt_do_not_wait_in_lockstep() {
        // Loader pods retrying one recovering server must spread out.
        let delays: std::collections::BTreeSet<Duration> =
            (0..20).map(|_| calc_backoff(100, 3, 30_000)).collect();
        assert!(
            delays.len() > 1,
            "twenty retries of one attempt all waited {delays:?}"
        );
        for delay in delays {
            assert_jittered(delay, 800);
        }
    }

    // ========================================================================
    // JSONEachRow rows are shaped for their JSON columns (#139)
    // ========================================================================

    fn tags_json_column() -> Vec<ColumnDef> {
        vec![ColumnDef::new("_tags", "JSON")]
    }

    fn row_map(value: Value) -> Map<String, Value> {
        value
            .as_object()
            .expect("test row must be an object")
            .clone()
    }

    #[test]
    fn test_shape_json_row_wraps_an_ecs_tags_array() {
        let row = row_map(
            serde_json::json!({"message": "x", "_tags": ["preserve_original_event", "forwarded"]}),
        );

        let shaped = shape_json_row(&row, &tags_json_column()).expect("the array must be shaped");

        assert_eq!(
            shaped.get("_tags").unwrap(),
            &serde_json::json!({"list": ["preserve_original_event", "forwarded"]})
        );
        assert_eq!(shaped.get("message").unwrap(), "x", "the rest is untouched");
    }

    #[test]
    fn test_shape_json_row_does_not_clone_a_row_it_would_not_change() {
        let row = row_map(serde_json::json!({"message": "x", "_tags": {"env": "prod"}}));

        assert!(
            shape_json_row(&row, &tags_json_column()).is_none(),
            "an object needs no shaping, so the row must not be cloned"
        );
        assert!(
            shape_json_row(&row, &[]).is_none(),
            "a table with no JSON column must not be cloned either"
        );
    }

    #[test]
    fn test_shape_json_row_wraps_broken_object_text() {
        let row = row_map(serde_json::json!({"_tags": "{not json"}));

        let shaped = shape_json_row(&row, &tags_json_column()).expect("broken text must be shaped");

        assert_eq!(
            shaped.get("_tags").unwrap(),
            &serde_json::json!({"value": "{not json"}),
            "text that does not parse is stored as the string it is"
        );
    }

    #[test]
    fn test_write_row_with_json_splices_raw_payload() {
        // Extractor-path row: _json is NOT in the map, raw bytes are spliced in.
        let row = serde_json::json!({"severity": "high", "_org_id": "acme"});
        let row_map = row.as_object().unwrap();
        let raw = br#"{"severity":"high","extra":"ignored"}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, row_map, raw).unwrap();

        let line = std::str::from_utf8(&body).unwrap();
        assert!(line.ends_with('\n'), "must end with newline");
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        // _json must be present and contain the raw payload
        assert_eq!(
            parsed.get("_json").unwrap(),
            &serde_json::json!({"severity":"high","extra":"ignored"})
        );
        // Original fields must still be present
        assert_eq!(parsed.get("severity").unwrap(), "high");
        assert_eq!(parsed.get("_org_id").unwrap(), "acme");
    }

    #[test]
    fn test_write_row_with_json_empty_row() {
        // Edge case: empty row map with only raw payload.
        let row_map = serde_json::Map::new();
        let raw = br#"{"event":"login"}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row_map, raw).unwrap();

        let line = std::str::from_utf8(&body).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(
            parsed.get("_json").unwrap(),
            &serde_json::json!({"event":"login"})
        );
    }

    #[test]
    fn test_json_insert_mixed_batch_raw_payloads() {
        // Simulate a mixed batch: extractor-path rows (non-empty raw) and
        // transformer-path rows (empty raw). Verifies that empty raw entries
        // produce normal JSON serialisation (no _json splice).
        let extractor_row = serde_json::json!({"severity": "high"});
        let transformer_row = serde_json::json!({"severity": "low", "_json": "{}"});

        let rows = [
            extractor_row.as_object().unwrap().clone(),
            transformer_row.as_object().unwrap().clone(),
        ];
        let raw_payloads: [Arc<[u8]>; 2] = [
            Arc::from(br#"{"severity":"high","detail":"x"}"#.as_slice()),
            Arc::from(b"".as_slice()), // empty = transformer path
        ];

        // Build NDJSON body the same way insert_rows_json does
        let mut body = Vec::new();
        for (row, raw) in rows.iter().zip(raw_payloads.iter()) {
            if raw.is_empty() {
                sonic_rs::to_writer(&mut body, row).unwrap();
                body.push(b'\n');
            } else {
                write_row_with_json(&mut body, row, raw).unwrap();
            }
        }

        let output = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 2);

        // Line 0: extractor path — _json spliced from raw
        let line0: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert!(
            line0.get("_json").is_some(),
            "extractor row must have _json"
        );

        // Line 1: transformer path — _json already in map, no splice
        let line1: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(line1.get("_json").unwrap(), "{}");
        assert_eq!(line1.get("severity").unwrap(), "low");
    }

    // ============================================================
    // parse_db_table — edge cases and fuzz-style inputs
    // ============================================================

    #[test]
    fn test_parse_db_table_standard() {
        assert_eq!(parse_db_table("mydb.events"), ("mydb", "events"));
    }

    #[test]
    fn test_parse_db_table_no_dot_falls_back_to_default() {
        // No dot — falls back to ("default", table)
        assert_eq!(parse_db_table("events"), ("default", "events"));
    }

    #[test]
    fn test_parse_db_table_empty_string() {
        // Empty input — no dot, falls back to default
        assert_eq!(parse_db_table(""), ("default", ""));
    }

    #[test]
    fn test_parse_db_table_leading_dot() {
        // Leading dot — empty db, non-empty table
        assert_eq!(parse_db_table(".events"), ("", "events"));
    }

    #[test]
    fn test_parse_db_table_trailing_dot() {
        // Trailing dot — non-empty db, empty table
        assert_eq!(parse_db_table("mydb."), ("mydb", ""));
    }

    #[test]
    fn test_parse_db_table_just_dot() {
        assert_eq!(parse_db_table("."), ("", ""));
    }

    #[test]
    fn test_parse_db_table_multiple_dots_splits_on_first() {
        // split_once uses first occurrence — trailing dots are part of table name
        assert_eq!(parse_db_table("a.b.c.d"), ("a", "b.c.d"));
    }

    #[test]
    fn test_parse_db_table_unicode() {
        // Unicode in names — should work because split_once is byte-wise on ASCII '.'
        assert_eq!(
            parse_db_table("métrics.événements"),
            ("métrics", "événements")
        );
    }

    #[test]
    fn test_parse_db_table_whitespace_preserved() {
        // Whitespace is not stripped — callers must pre-validate
        assert_eq!(parse_db_table(" db . table "), (" db ", " table "));
    }

    // ============================================================
    // calc_backoff -- geometric growth, cap, jitter, overflow protection
    // ============================================================

    #[test]
    fn test_calc_backoff_attempt_zero() {
        assert_jittered(calc_backoff(100, 0, 30_000), 100);
    }

    #[test]
    fn test_calc_backoff_geometric_progression() {
        // Each attempt doubles the step: base * 2^attempt, then jitter.
        for (attempt, step_ms) in (0..8).zip([50, 100, 200, 400, 800, 1600, 3200, 6400]) {
            assert_jittered(calc_backoff(50, attempt, 60_000), step_ms);
        }
    }

    #[test]
    fn test_calc_backoff_capped_at_max() {
        // Once base * 2^attempt exceeds max_ms, the step stays at max_ms.
        assert_jittered(calc_backoff(100, 16, 500), 500);
        assert_jittered(calc_backoff(100, 20, 1000), 1000);
    }

    #[test]
    fn test_calc_backoff_attempt_clamped_to_16() {
        // Attempts past MAX_BACKOFF_DOUBLINGS walk the schedule no further.
        assert_jittered(calc_backoff(100, 100, 10_000), 10_000);
        assert_jittered(calc_backoff(100, u32::MAX, 10_000), 10_000);
    }

    #[test]
    fn test_calc_backoff_huge_base_is_held_to_max() {
        assert_jittered(calc_backoff(u64::MAX, 16, 1000), 1000);
    }

    #[test]
    fn test_calc_backoff_zero_base() {
        // 0 * anything = 0, and so is its jitter -- valid but weird config
        assert_eq!(calc_backoff(0, 5, 30_000), Duration::ZERO);
    }

    #[test]
    fn test_calc_backoff_zero_max_clamps_to_zero() {
        // max=0 clamps every delay to 0
        assert_eq!(calc_backoff(100, 5, 0), Duration::ZERO);
    }

    #[test]
    fn test_calc_backoff_max_less_than_base() {
        // Unusual: max < base. The first step is already max.
        assert_jittered(calc_backoff(1000, 0, 100), 100);
    }

    // ============================================================
    // InserterConfig — defaults and boundary backoffs
    // ============================================================

    #[test]
    fn test_inserter_config_default_full_values() {
        let config = InserterConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.base_retry_delay_ms, 100);
        assert_eq!(config.max_retry_delay_ms, 30_000);
        assert!(config.enable_salvage);
        assert_eq!(config.max_salvage_depth, 20);
        assert_eq!(config.max_concurrent_inserts, 8);
    }

    #[test]
    fn test_inserter_config_backoff_at_default_max_retries() {
        let config = InserterConfig::default();
        // With defaults, the step at max_retries (5) is 100 * 2^5 = 3200ms
        assert_jittered(config.backoff_delay(5), 3200);
    }

    #[test]
    fn test_inserter_config_backoff_eventually_caps() {
        let config = InserterConfig::default();
        // 100 * 2^9 = 51200 > 30000, cap applies
        assert_jittered(config.backoff_delay(9), 30_000);
        assert_jittered(config.backoff_delay(100), 30_000);
    }

    // ============================================================
    // InsertResult — counts and failure lists
    // ============================================================

    #[test]
    fn test_insert_result_success_zero() {
        let result = InsertResult::success(0);
        assert_eq!(result.inserted, 0);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_insert_result_success_large() {
        let result = InsertResult::success(usize::MAX);
        assert_eq!(result.inserted, usize::MAX);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn test_insert_result_with_empty_failures() {
        // with_failures passing empty vec should yield a behaviourally-successful result
        let result = InsertResult::with_failures(50, Vec::new());
        assert_eq!(result.inserted, 50);
        assert_eq!(result.failed.len(), 0);
    }

    #[test]
    fn test_insert_result_all_failed() {
        // Zero inserted, many failed — simulates a complete batch failure
        let failed: Vec<FailedRow> = (0..100)
            .map(|i| FailedRow {
                row_index: i,
                offset: None,
                reason: format!("Error at row {i}"),
                row_json: None,
            })
            .collect();
        let result = InsertResult::with_failures(0, failed);
        assert_eq!(result.inserted, 0);
        assert_eq!(result.failed.len(), 100);
        assert_eq!(result.failed[42].row_index, 42);
        assert!(result.failed[42].reason.contains("row 42"));
    }

    // ============================================================
    // FailedRow — construction with optional offset
    // ============================================================

    #[test]
    fn test_failed_row_with_offset() {
        let offset = KafkaOffset {
            topic: Arc::from("events"),
            partition: 3,
            offset: 12345,
        };
        let row = FailedRow {
            row_index: 7,
            offset: Some(offset.clone()),
            reason: "Type mismatch".to_string(),
            row_json: None,
        };
        assert_eq!(row.row_index, 7);
        assert!(row.offset.is_some());
        let ko = row.offset.unwrap();
        assert_eq!(&*ko.topic, "events");
        assert_eq!(ko.partition, 3);
        assert_eq!(ko.offset, 12345);
    }

    #[test]
    fn test_failed_row_no_offset() {
        let row = FailedRow {
            row_index: 0,
            offset: None,
            reason: "No offset (manual insert path)".to_string(),
            row_json: None,
        };
        assert!(row.offset.is_none());
    }

    #[test]
    fn a_row_with_no_raw_payload_carries_its_own_bytes() {
        // raw_only, extracted_only, legacy_flatten and MessagePack all leave
        // raw_payloads[i] empty. Without the fallback the DLQ entry is empty
        // and the offset commits over the top of it.
        let mut row = Map::new();
        row.insert("_raw".to_string(), Value::String("{\"a\":1}".to_string()));
        let empty: Arc<[u8]> = Arc::from(&[][..]);

        let bytes = rejected_row_bytes(Some(&empty), Some(&row)).expect("row must be serialised");
        let back: Value = serde_json::from_slice(&bytes).expect("valid JSON");
        assert_eq!(back["_raw"], "{\"a\":1}");
    }

    #[test]
    fn a_row_that_still_has_its_raw_payload_is_not_re_serialised() {
        let mut row = Map::new();
        row.insert("n".to_string(), Value::from(1));
        let raw: Arc<[u8]> = Arc::from(&b"{\"n\":1}"[..]);
        assert_eq!(rejected_row_bytes(Some(&raw), Some(&row)), None);
    }

    #[test]
    fn only_a_data_error_is_worth_splitting() {
        // Splitting a transient failure issues ~2N inserts into a sink that is
        // already struggling, and isolates nothing.
        assert!(is_salvageable(&crate::Error::ClickHousePermanent(
            "code 117".into()
        )));
        assert!(!is_salvageable(&crate::Error::ClickHouse(
            "connection reset by peer".into()
        )));
        assert!(!is_salvageable(&crate::Error::Buffer("closed".into())));
    }

    #[test]
    fn test_failed_row_unicode_reason() {
        let row = FailedRow {
            row_index: 1,
            offset: None,
            reason: "Erreur: données invalides 世界 🚫".to_string(),
            row_json: None,
        };
        assert!(row.reason.contains("世界"));
        assert!(row.reason.contains("🚫"));
    }

    // ============================================================
    // InsertFormat — serde round-trip and variant coverage
    // ============================================================

    #[test]
    fn test_insert_format_default_is_row_binary() {
        assert_eq!(InsertFormat::default(), InsertFormat::RowBinary);
    }

    #[test]
    fn test_insert_format_serde_roundtrip_rowbinary() {
        let fmt = InsertFormat::RowBinary;
        let s = serde_json::to_string(&fmt).unwrap();
        assert_eq!(s, "\"rowbinary\"");
        let back: InsertFormat = serde_json::from_str(&s).unwrap();
        assert_eq!(back, fmt);
    }

    #[test]
    fn test_insert_format_serde_roundtrip_jsoneachrow() {
        let fmt = InsertFormat::JsonEachRow;
        let s = serde_json::to_string(&fmt).unwrap();
        assert_eq!(s, "\"jsoneachrow\"");
        let back: InsertFormat = serde_json::from_str(&s).unwrap();
        assert_eq!(back, fmt);
    }

    #[test]
    fn test_insert_format_serde_aliases() {
        // Aliases from the config — verified via actual YAML/JSON input
        let cases = [
            ("\"rowbinary\"", InsertFormat::RowBinary),
            ("\"row_binary\"", InsertFormat::RowBinary),
            ("\"binary\"", InsertFormat::RowBinary),
            ("\"jsoneachrow\"", InsertFormat::JsonEachRow),
            ("\"json\"", InsertFormat::JsonEachRow),
            ("\"json_each_row\"", InsertFormat::JsonEachRow),
        ];
        for (input, expected) in cases {
            let parsed: InsertFormat = serde_json::from_str(input).unwrap();
            assert_eq!(parsed, expected, "alias {input} should map to {expected:?}");
        }
    }

    #[test]
    fn test_insert_format_invalid_variant_fails() {
        // Unknown variants must error — prevents silent typos in config
        let result: std::result::Result<InsertFormat, _> = serde_json::from_str("\"arrow\"");
        assert!(result.is_err());
        // `native` is a protocol spelling, and the protocol key rejects it too.
        let result: std::result::Result<InsertFormat, _> = serde_json::from_str("\"native\"");
        assert!(result.is_err());
        let result: std::result::Result<InsertFormat, _> = serde_json::from_str("42");
        assert!(result.is_err());
    }

    // ============================================================
    // write_row_with_json — splice behaviour for complex inputs
    // ============================================================

    #[test]
    fn test_write_row_with_json_contains_escapes() {
        // Row with escaped characters: JSON must round-trip correctly after splice.
        let mut row = serde_json::Map::new();
        row.insert(
            "message".to_string(),
            serde_json::Value::String("line1\nline2\t\"quoted\"".to_string()),
        );
        row.insert(
            "path".to_string(),
            serde_json::Value::String("C:\\Users\\test".to_string()),
        );
        let raw = br#"{"raw_key":"raw \"value\""}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row, raw).unwrap();
        let line = std::str::from_utf8(&body).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(parsed.get("message").unwrap(), "line1\nline2\t\"quoted\"");
        assert_eq!(parsed.get("path").unwrap(), "C:\\Users\\test");
        assert!(parsed.get("_json").is_some());
        assert_eq!(
            parsed.get("_json").unwrap().get("raw_key").unwrap(),
            "raw \"value\""
        );
    }

    #[test]
    fn test_write_row_with_json_unicode_content() {
        // Unicode keys/values — UTF-8 should pass through unchanged.
        let mut row = serde_json::Map::new();
        row.insert(
            "ユーザー".to_string(),
            serde_json::Value::String("日本語".to_string()),
        );
        row.insert(
            "emoji".to_string(),
            serde_json::Value::String("🔥🚀".to_string()),
        );
        let raw = "{\"地域\":\"東京\",\"status\":\"✅\"}".as_bytes();

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row, raw).unwrap();
        let line = std::str::from_utf8(&body).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(parsed.get("ユーザー").unwrap(), "日本語");
        assert_eq!(parsed.get("emoji").unwrap(), "🔥🚀");
        assert_eq!(parsed.get("_json").unwrap().get("地域").unwrap(), "東京");
    }

    #[test]
    fn test_write_row_with_json_nested_structure() {
        // Deeply nested Map values — splice must still produce valid JSON.
        let row_val = serde_json::json!({
            "level1": {
                "level2": {
                    "level3": {
                        "deep": [1, 2, [3, [4, 5]]]
                    }
                }
            },
            "arr": [null, true, false, 3.14, "string"]
        });
        let row = row_val.as_object().unwrap();
        let raw = br#"{"outer":{"inner":42}}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, row, raw).unwrap();
        let line = std::str::from_utf8(&body).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        assert_eq!(
            parsed["level1"]["level2"]["level3"]["deep"][2][1][1],
            serde_json::json!(5)
        );
        assert_eq!(parsed["arr"][3], serde_json::json!(3.14));
        assert_eq!(parsed["_json"]["outer"]["inner"], serde_json::json!(42));
    }

    #[test]
    fn test_write_row_with_json_large_map() {
        // Large map: 1000 keys. Splice must handle large allocations correctly.
        let mut row = serde_json::Map::new();
        for i in 0..1000 {
            row.insert(format!("key_{i}"), serde_json::Value::from(i));
        }
        let raw = br#"{"summary":"lots"}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row, raw).unwrap();

        let line = std::str::from_utf8(&body).unwrap();
        assert!(line.ends_with('\n'));
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();
        assert_eq!(parsed.as_object().unwrap().len(), 1001); // 1000 + _json
        assert_eq!(parsed.get("key_500").unwrap(), 500);
        assert_eq!(parsed.get("_json").unwrap().get("summary").unwrap(), "lots");
    }

    #[test]
    fn test_write_row_with_json_raw_is_array() {
        // The raw payload can be any valid JSON value, including arrays or scalars.
        // This is a fuzz-style input: verify splicing doesn't break on non-object raw.
        let mut row = serde_json::Map::new();
        row.insert("id".to_string(), serde_json::Value::from(1));
        let raw = b"[1,2,3]";

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row, raw).unwrap();
        let line = std::str::from_utf8(&body).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).unwrap();

        // _json is the literal raw bytes parsed as JSON — here an array.
        assert!(parsed.get("_json").unwrap().is_array());
        assert_eq!(parsed["_json"][0], serde_json::json!(1));
    }

    #[test]
    fn test_write_row_with_json_empty_raw_produces_invalid_json() {
        // Edge case: empty raw bytes. The splice produces {"_json":} which is NOT
        // valid JSON — callers must filter empty raw (insert_rows_json does this).
        // We verify the observable byte output matches the documented behaviour.
        let row = serde_json::Map::new();
        let raw: &[u8] = b"";

        let mut body = Vec::new();
        write_row_with_json(&mut body, &row, raw).unwrap();
        let line = std::str::from_utf8(&body).unwrap();
        // Empty map path: {"_json": + empty raw + } = {"_json":}
        assert_eq!(line.trim_end(), r#"{"_json":}"#);
        // This is INTENTIONALLY invalid — confirms caller must filter empty raw.
        assert!(serde_json::from_str::<serde_json::Value>(line.trim()).is_err());
    }

    #[test]
    fn test_write_row_with_json_appends_newline() {
        // NDJSON format requires trailing newline — check both paths (empty + non-empty map).
        let empty_row = serde_json::Map::new();
        let raw = br#"{"k":1}"#;
        let mut body1 = Vec::new();
        write_row_with_json(&mut body1, &empty_row, raw).unwrap();
        assert_eq!(body1.last(), Some(&b'\n'));

        let mut nonempty_row = serde_json::Map::new();
        nonempty_row.insert("x".to_string(), serde_json::Value::from(1));
        let mut body2 = Vec::new();
        write_row_with_json(&mut body2, &nonempty_row, raw).unwrap();
        assert_eq!(body2.last(), Some(&b'\n'));
    }

    #[test]
    fn test_write_row_with_json_null_and_bool_values() {
        // Row with null, true, false — common for timestamps/flags.
        let row_val = serde_json::json!({
            "deleted_at": null,
            "active": true,
            "hidden": false,
            "count": 0
        });
        let row = row_val.as_object().unwrap();
        let raw = br#"{"note":"ok"}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, row, raw).unwrap();
        let parsed: serde_json::Value =
            serde_json::from_str(std::str::from_utf8(&body).unwrap().trim()).unwrap();

        assert!(parsed.get("deleted_at").unwrap().is_null());
        assert_eq!(parsed.get("active").unwrap(), true);
        assert_eq!(parsed.get("hidden").unwrap(), false);
        assert_eq!(parsed.get("count").unwrap(), 0);
    }

    #[test]
    fn test_write_row_with_json_splice_position_correct() {
        // Whitebox check: the last byte before newline should be '}',
        // and the splice should not leave the body malformed (e.g. trailing comma).
        let row_val = serde_json::json!({"a": 1});
        let row = row_val.as_object().unwrap();
        let raw = br#"{"b":2}"#;

        let mut body = Vec::new();
        write_row_with_json(&mut body, row, raw).unwrap();

        // Last byte is newline, second-to-last is '}' (the final brace of the object).
        assert_eq!(body[body.len() - 1], b'\n');
        assert_eq!(body[body.len() - 2], b'}');
    }
}
