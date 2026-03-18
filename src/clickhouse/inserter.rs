// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/inserter.rs
// Purpose:   Batch inserter for ClickHouse with retry and salvage
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Batch inserter for ClickHouse via JSONEachRow HTTP.
//!
//! Handles batch inserts with retry logic using `Vec<Map<String, Value>>`.
//! Uses `HttpClickHouseClient` for JSONEachRow inserts.
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

use serde_json::{Map, Value};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::Result;
use crate::buffer::{FlushBatch, KafkaOffset};
use crate::clickhouse::circuit_breaker::CircuitBreaker;
use crate::clickhouse::config::InsertFormat;
use crate::clickhouse::error::ErrorCategory;
use crate::clickhouse::{HttpClickHouseClient, SchemaCache};
use crate::transform::Coercer;

/// Split "db.table" into (db, table). Panics if no dot — callers always
/// pass fully qualified names from BufferManager.
fn parse_db_table(table: &str) -> (&str, &str) {
    table.split_once('.').unwrap_or(("default", table))
}

/// Configuration for the inserter
pub struct InserterConfig {
    /// Maximum retries for transient errors (geometric backoff)
    pub max_retries: u32,
    /// Base delay for geometric backoff (doubles each retry)
    pub base_retry_delay_ms: u64,
    /// Maximum delay cap for backoff (prevents excessive waits)
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
    /// Calculate backoff delay for a given attempt (geometric backoff).
    ///
    /// Delay = min(base * 2^attempt, max_delay)
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        calc_backoff(self.base_retry_delay_ms, attempt, self.max_retry_delay_ms)
    }
}

/// Geometric backoff: `min(base_ms * 2^attempt, max_ms)`.
fn calc_backoff(base_ms: u64, attempt: u32, max_ms: u64) -> Duration {
    let delay_ms = base_ms.saturating_mul(1 << attempt.min(16));
    Duration::from_millis(delay_ms.min(max_ms))
}

/// Result of a batch insert with salvage
#[derive(Debug)]
pub struct InsertResult {
    /// Number of rows successfully inserted
    pub inserted: usize,
    /// Failed rows with their Kafka offsets and error reason
    pub failed: Vec<FailedRow>,
}

impl InsertResult {
    pub fn success(count: usize) -> Self {
        Self {
            inserted: count,
            failed: Vec::new(),
        }
    }

    pub fn with_failures(inserted: usize, failed: Vec<FailedRow>) -> Self {
        Self { inserted, failed }
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
}

/// Handles batch inserts to ClickHouse with retry logic.
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
    /// HTTP client for JSONEachRow inserts and DDL
    http_client: Arc<HttpClickHouseClient>,
    /// clickhouse-rs Client for DynamicInsert (RowBinary path)
    ch_client: Option<clickhouse::Client>,
    /// Insert format — RowBinary (schema-reflected) or JSONEachRow
    insert_format: InsertFormat,
    max_retries: u32,
    base_retry_delay_ms: u64,
    max_retry_delay_ms: u64,
    enable_salvage: bool,
    max_salvage_depth: u32,
    /// Semaphore for limiting concurrent inserts
    semaphore: Option<Arc<Semaphore>>,
    /// Circuit breaker for per-table failure detection
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    /// Schema cache for type-aware coercion (JSONEachRow path)
    schema_cache: Option<Arc<SchemaCache>>,
    /// Type coercer applied before each insert (JSONEachRow path)
    coercer: Option<Arc<Coercer>>,
}

impl Inserter {
    /// Create a new inserter.
    ///
    /// When `insert_format` is `RowBinary`, a `clickhouse::Client` must be provided
    /// via `with_ch_client()`. Otherwise inserts use the HTTP JSONEachRow path.
    pub fn new(http_client: Arc<HttpClickHouseClient>, config: InserterConfig) -> Self {
        let semaphore = if config.max_concurrent_inserts > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_inserts)))
        } else {
            None
        };

        Self {
            http_client,
            ch_client: None,
            insert_format: InsertFormat::default(),
            max_retries: config.max_retries,
            base_retry_delay_ms: config.base_retry_delay_ms,
            max_retry_delay_ms: config.max_retry_delay_ms,
            enable_salvage: config.enable_salvage,
            max_salvage_depth: config.max_salvage_depth,
            semaphore,
            circuit_breaker: None,
            schema_cache: None,
            coercer: None,
        }
    }

    /// Set the insert format and clickhouse-rs Client for RowBinary inserts.
    ///
    /// When `InsertFormat::RowBinary`, the inserter uses `DynamicInsert` from the
    /// clickhouse-rs fork — schema-reflected binary encoding. ClickHouse skips
    /// JSON parsing entirely, significantly reducing cluster CPU at scale.
    pub fn with_insert_format(
        mut self,
        format: InsertFormat,
        ch_client: Option<clickhouse::Client>,
    ) -> Self {
        self.insert_format = format;
        self.ch_client = ch_client;
        self
    }

    /// Enable schema-driven type coercion before each insert.
    ///
    /// When enabled, the inserter fetches the table schema (from cache or live)
    /// and applies the coercer to each row before sending to ClickHouse. This
    /// covers cases that ClickHouse's JSONEachRow server-side coercion does not
    /// handle automatically — e.g., epoch ms integers into DateTime64 columns,
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
    /// Fetches schema from cache (or live from ClickHouse on cache miss), then
    /// applies the coercer to every row. On schema fetch failure, rows are sent
    /// as-is and ClickHouse's server-side coercion handles them.
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

    /// Calculate backoff delay for a given attempt.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        calc_backoff(self.base_retry_delay_ms, attempt, self.max_retry_delay_ms)
    }

    /// Set the circuit breaker for per-table failure detection
    pub fn with_circuit_breaker(mut self, cb: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(cb);
        self
    }

    /// Insert rows into a table with error-aware retry.
    ///
    /// Dispatches based on `insert_format`:
    /// - `RowBinary`: schema-reflected binary via `DynamicInsert` (fork)
    /// - `JsonEachRow`: HTTP JSONEachRow via `HttpClickHouseClient`
    ///
    /// - **Transient errors**: Geometric backoff retry up to max_retries
    /// - **Data errors**: Returns immediately (caller should salvage)
    /// - **Fatal errors**: Returns immediately (no retry)
    ///
    /// `raw_payloads` is parallel to `rows` — used by JSONEachRow path for
    /// zero-copy `_json` splice. Ignored by RowBinary path.
    pub async fn insert_rows(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
        raw_payloads: &[Arc<[u8]>],
    ) -> Result<usize> {
        match self.insert_format {
            InsertFormat::RowBinary => self.insert_rows_rowbinary(table, rows).await,
            InsertFormat::JsonEachRow => self.insert_rows_json(table, rows, raw_payloads).await,
        }
    }

    /// RowBinary insert path — schema-reflected binary encoding via DynamicInsert.
    ///
    /// ClickHouse receives pre-columnarised data, zero server-side JSON parsing.
    /// On schema mismatch, invalidates cache and retries once with fresh schema.
    async fn insert_rows_rowbinary(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let ch_client = self.ch_client.as_ref().ok_or_else(|| {
            crate::Error::Config(
                "RowBinary insert requires clickhouse::Client (set via with_insert_format)"
                    .to_string(),
            )
        })?;

        let (db, tbl) = parse_db_table(table);

        let mut last_error = None;
        for attempt in 0..=self.max_retries {
            let mut insert = ch_client.dynamic_insert(db, tbl);

            let mut write_failed = false;
            for row in rows {
                if let Err(e) = insert.write_map(row).await {
                    // Schema mismatch — invalidate and retry
                    if matches!(e, clickhouse::dynamic::DynamicError::SchemaMismatch { .. }) {
                        insert.invalidate_schema();
                        last_error =
                            Some(crate::Error::ClickHouse(format!("Schema mismatch: {e}")));
                        write_failed = true;
                        break;
                    }
                    return Err(crate::Error::ClickHouse(format!("RowBinary encode: {e}")));
                }
            }

            if write_failed {
                if attempt < self.max_retries {
                    let delay = self.backoff_delay(attempt);
                    warn!(
                        table = %table,
                        attempt,
                        delay_ms = delay.as_millis(),
                        "Schema mismatch, re-fetching and retrying"
                    );
                    sleep(delay).await;
                    continue;
                }
                break;
            }

            match insert.end().await {
                Ok(count) => {
                    debug!(table = %table, rows = count, "RowBinary insert successful");
                    return Ok(count as usize);
                }
                Err(clickhouse::dynamic::DynamicError::SchemaMismatch { .. }) => {
                    if attempt < self.max_retries {
                        let delay = self.backoff_delay(attempt);
                        warn!(
                            table = %table,
                            attempt,
                            delay_ms = delay.as_millis(),
                            "Schema mismatch on end(), re-fetching and retrying"
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
                    return Err(crate::Error::ClickHouse(format!("RowBinary insert: {e}")));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| crate::Error::Buffer("Max retries exceeded".into())))
    }

    /// JSONEachRow insert path — HTTP POST with NDJSON body.
    ///
    /// The existing path: serialize Map<String, Value> to JSON, ClickHouse
    /// parses server-side. `raw_payloads` enables zero-copy `_json` splice.
    async fn insert_rows_json(
        &self,
        table: &str,
        rows: &[Map<String, Value>],
        raw_payloads: &[Arc<[u8]>],
    ) -> Result<usize> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match self
                .http_client
                .insert_json_rows(table, rows, raw_payloads)
                .await
            {
                Ok(count) => {
                    debug!(table = %table, rows = count, "JSONEachRow insert successful");
                    return Ok(count);
                }
                Err(e) => {
                    let category = e.category();

                    match category {
                        ErrorCategory::Transient | ErrorCategory::Unknown => {
                            if attempt < self.max_retries {
                                let delay = self.backoff_delay(attempt);
                                warn!(
                                    table = %table,
                                    attempt = attempt,
                                    delay_ms = delay.as_millis(),
                                    error = %e,
                                    "Transient error, backing off"
                                );
                                sleep(delay).await;
                                last_error = Some(e);
                                continue;
                            }
                            error!(
                                table = %table,
                                attempts = self.max_retries + 1,
                                error = %e,
                                "Max retries exhausted for transient error"
                            );
                            last_error = Some(e);
                        }

                        ErrorCategory::Data => {
                            debug!(
                                table = %table,
                                error = %e,
                                "Data error, returning for salvage"
                            );
                            return Err(e.into());
                        }

                        ErrorCategory::Fatal => {
                            error!(
                                table = %table,
                                error = %e,
                                "Fatal error, not retrying"
                            );
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        Err(match last_error {
            Some(e) => e.into(),
            None => crate::Error::Buffer("Max retries exceeded".into()),
        })
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

        // Apply schema-driven coercion if configured (Phase 5.6).
        // Done once here — salvage sub-batches reuse already-coerced rows.
        self.coerce_batch(&table, &mut rows).await;

        // Pass raw_payloads for zero-copy _json splice (json_primary path).
        // Salvage sub-batches use &[] — the salvage path is error recovery only.
        match self.insert_rows(&table, &rows, &raw_payloads).await {
            Ok(count) => {
                return InsertResult::success(count);
            }
            Err(e) => {
                let is_data_error = matches!(
                    e,
                    crate::Error::ClickHouse(ref msg) if {
                        let ch_err = crate::clickhouse::ClickHouseError::Insert(msg.clone());
                        ch_err.is_data_error()
                    }
                ) || e.to_string().to_lowercase().contains("type mismatch")
                    || e.to_string().to_lowercase().contains("incorrect data")
                    || e.to_string().to_lowercase().contains("cannot parse");

                if !self.enable_salvage || num_rows <= 1 || !is_data_error {
                    let reason = e.to_string();
                    let is_fatal = e.to_string().to_lowercase().contains("unknown table")
                        || e.to_string().to_lowercase().contains("access denied");

                    if is_fatal {
                        error!(
                            table = %table,
                            rows = num_rows,
                            error = %e,
                            "Fatal error, batch failed (not sending to DLQ)"
                        );
                        return InsertResult::with_failures(0, Vec::new());
                    }

                    let failed: Vec<FailedRow> = (0..num_rows)
                        .map(|i| FailedRow {
                            row_index: i,
                            offset: offsets.get(i).cloned(),
                            reason: reason.clone(),
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
        let mut inserted = 0;
        let mut failed = Vec::new();

        self.salvage_batch(&table, &rows, &offsets, 0, 0, &mut inserted, &mut failed)
            .await;

        info!(
            table = %table,
            inserted = inserted,
            failed = failed.len(),
            "Batch salvage complete"
        );

        InsertResult::with_failures(inserted, failed)
    }

    /// Recursively salvage a batch using binary split.
    ///
    /// Splits `rows` slice in half, retries each half. Recurses until
    /// single-row failures are isolated for DLQ routing.
    #[allow(clippy::too_many_arguments)]
    fn salvage_batch<'a>(
        &'a self,
        table: &'a str,
        rows: &'a [Map<String, Value>],
        offsets: &'a [KafkaOffset],
        start_index: usize,
        depth: u32,
        inserted: &'a mut usize,
        failed: &'a mut Vec<FailedRow>,
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
                    failed.push(FailedRow {
                        row_index: start_index + i,
                        offset: offsets.get(i).cloned(),
                        reason: "Max salvage depth exceeded".to_string(),
                    });
                }
                return;
            }

            // Base case: single row
            if num_rows == 1 {
                match self.insert_rows(table, rows, &[]).await {
                    Ok(_) => {
                        *inserted += 1;
                    }
                    Err(e) => {
                        failed.push(FailedRow {
                            row_index: start_index,
                            offset: offsets.first().cloned(),
                            reason: e.to_string(),
                        });
                    }
                }
                return;
            }

            // Try the whole slice first (might succeed now, e.g., transient error)
            match self.insert_rows(table, rows, &[]).await {
                Ok(count) => {
                    *inserted += count;
                    return;
                }
                Err(_) => {
                    // Split and recurse
                }
            }

            // Split in half
            let mid = num_rows / 2;
            let (left_rows, right_rows) = rows.split_at(mid);
            let left_offsets = if offsets.len() >= mid {
                &offsets[..mid]
            } else {
                offsets
            };
            let right_offsets = if offsets.len() > mid {
                &offsets[mid..]
            } else {
                &[]
            };

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
                start_index,
                depth + 1,
                inserted,
                failed,
            )
            .await;

            self.salvage_batch(
                table,
                right_rows,
                right_offsets,
                start_index + mid,
                depth + 1,
                inserted,
                failed,
            )
            .await;
        })
    }

    /// Insert a `FlushBatch` — simple version without salvage.
    pub async fn insert_batch(&self, batch: FlushBatch) -> Result<usize> {
        self.insert_rows(&batch.table, &batch.rows, &batch.raw_payloads)
            .await
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
                    results.push((
                        "unknown".to_string(),
                        InsertResult::with_failures(
                            0,
                            vec![FailedRow {
                                row_index: 0,
                                offset: None,
                                reason: format!("Insert task panicked: {e}"),
                            }],
                        ),
                    ));
                }
            }
        }

        results
    }

    /// Insert multiple batches concurrently (simple version).
    pub async fn insert_batches(&self, batches: Vec<FlushBatch>) -> Vec<Result<usize>> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
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
                inserter.insert_batch(batch).await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(crate::Error::Buffer(format!(
                    "Insert task panicked: {e}"
                )))),
            }
        }

        results
    }
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
            },
            FailedRow {
                row_index: 10,
                offset: None,
                reason: "Invalid data".to_string(),
            },
        ];
        let result = InsertResult::with_failures(98, failed);
        assert_eq!(result.inserted, 98);
        assert_eq!(result.failed.len(), 2);
        assert_eq!(result.failed[0].row_index, 5);
        assert_eq!(result.failed[1].row_index, 10);
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

    #[test]
    fn test_backoff_delay_geometric() {
        let config = InserterConfig {
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 30_000,
            ..Default::default()
        };

        assert_eq!(config.backoff_delay(0), Duration::from_millis(100));
        assert_eq!(config.backoff_delay(1), Duration::from_millis(200));
        assert_eq!(config.backoff_delay(2), Duration::from_millis(400));
        assert_eq!(config.backoff_delay(3), Duration::from_millis(800));
        assert_eq!(config.backoff_delay(4), Duration::from_millis(1600));
        assert_eq!(config.backoff_delay(5), Duration::from_millis(3200));
    }

    #[test]
    fn test_backoff_delay_capped() {
        let config = InserterConfig {
            base_retry_delay_ms: 100,
            max_retry_delay_ms: 1000,
            ..Default::default()
        };

        assert_eq!(config.backoff_delay(10), Duration::from_millis(1000));
        assert_eq!(config.backoff_delay(20), Duration::from_millis(1000));
    }
}
