//! Arrow-based batch inserter for ClickHouse
//!
//! Handles batch inserts with retry logic using Arrow RecordBatch.
//! Uses clickhouse-arrow for native Arrow protocol inserts.
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

use arrow::array::RecordBatch;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::buffer::arrow::KafkaOffset;
use crate::buffer::FlushBatch;
use crate::clickhouse::circuit_breaker::CircuitBreaker;
use crate::clickhouse::error::ErrorCategory;
use crate::clickhouse::ArrowClickHouseClient;
use crate::Result;

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
            base_retry_delay_ms: 100,   // Start at 100ms
            max_retry_delay_ms: 30_000, // Cap at 30 seconds
            enable_salvage: true,
            max_salvage_depth: 20,     // 2^20 = 1M rows max batch size
            max_concurrent_inserts: 8, // Reasonable default for ClickHouse
        }
    }
}

impl InserterConfig {
    /// Calculate backoff delay for a given attempt (geometric backoff).
    ///
    /// Delay = min(base * 2^attempt, max_delay)
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay_ms = self
            .base_retry_delay_ms
            .saturating_mul(1 << attempt.min(16));
        Duration::from_millis(delay_ms.min(self.max_retry_delay_ms))
    }
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

/// Handles batch inserts to ClickHouse with retry logic
///
/// Uses native Arrow protocol via clickhouse-arrow for efficient columnar inserts.
///
/// ## Error Handling Strategy
///
/// - **Transient errors** (overload, network): Geometric backoff retry
/// - **Data errors** (type mismatch, corrupt): Binary-split salvage → DLQ bad rows
/// - **Fatal errors** (auth, schema): Fail immediately, no retry
pub struct Inserter {
    /// Arrow client for native protocol inserts
    arrow_client: Arc<ArrowClickHouseClient>,
    max_retries: u32,
    base_retry_delay_ms: u64,
    max_retry_delay_ms: u64,
    enable_salvage: bool,
    max_salvage_depth: u32,
    /// Semaphore for limiting concurrent inserts
    semaphore: Option<Arc<Semaphore>>,
    /// Circuit breaker for per-table failure detection
    circuit_breaker: Option<Arc<CircuitBreaker>>,
}

impl Inserter {
    /// Create a new inserter with Arrow client
    pub fn new(arrow_client: Arc<ArrowClickHouseClient>, config: InserterConfig) -> Self {
        let semaphore = if config.max_concurrent_inserts > 0 {
            Some(Arc::new(Semaphore::new(config.max_concurrent_inserts)))
        } else {
            None
        };

        Self {
            arrow_client,
            max_retries: config.max_retries,
            base_retry_delay_ms: config.base_retry_delay_ms,
            max_retry_delay_ms: config.max_retry_delay_ms,
            enable_salvage: config.enable_salvage,
            max_salvage_depth: config.max_salvage_depth,
            semaphore,
            circuit_breaker: None,
        }
    }

    /// Calculate backoff delay for a given attempt.
    fn backoff_delay(&self, attempt: u32) -> Duration {
        let delay_ms = self
            .base_retry_delay_ms
            .saturating_mul(1 << attempt.min(16));
        Duration::from_millis(delay_ms.min(self.max_retry_delay_ms))
    }

    /// Set the circuit breaker for per-table failure detection
    pub fn with_circuit_breaker(mut self, cb: Arc<CircuitBreaker>) -> Self {
        self.circuit_breaker = Some(cb);
        self
    }

    /// Insert an Arrow RecordBatch into a table with error-aware retry.
    ///
    /// - **Transient errors**: Geometric backoff retry up to max_retries
    /// - **Data errors**: Returns immediately (caller should salvage)
    /// - **Fatal errors**: Returns immediately (no retry)
    ///
    /// Does not perform salvage - use `insert_with_salvage` for that.
    pub async fn insert_arrow(&self, table: &str, batch: RecordBatch) -> Result<usize> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            match self.arrow_client.insert(table, batch.clone()).await {
                Ok(count) => {
                    debug!(
                        table = %table,
                        rows = count,
                        "Arrow insert successful"
                    );
                    return Ok(count);
                }
                Err(e) => {
                    let category = e.category();

                    match category {
                        ErrorCategory::Transient | ErrorCategory::Unknown => {
                            // Transient error - backoff and retry
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
                            // Max retries exhausted
                            error!(
                                table = %table,
                                attempts = self.max_retries + 1,
                                error = %e,
                                "Max retries exhausted for transient error"
                            );
                            last_error = Some(e);
                        }

                        ErrorCategory::Data => {
                            // Data error - don't retry, caller should salvage
                            debug!(
                                table = %table,
                                error = %e,
                                "Data error, returning for salvage"
                            );
                            return Err(e.into());
                        }

                        ErrorCategory::Fatal => {
                            // Fatal error - don't retry
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

        // Convert ClickHouseError to crate::Error
        Err(match last_error {
            Some(e) => e.into(),
            None => crate::Error::Buffer("Max retries exceeded".into()),
        })
    }

    /// Insert a FlushBatch with error-aware retry and salvage.
    ///
    /// Error handling strategy:
    /// - **Transient errors**: Retried with geometric backoff (handled by `insert_arrow`)
    /// - **Data errors**: Binary-split salvage to isolate bad rows for DLQ
    /// - **Fatal errors**: Fail entire batch (no salvage, no DLQ)
    ///
    /// Returns successfully inserted count and list of failed rows for DLQ routing.
    pub async fn insert_with_salvage(&self, batch: FlushBatch) -> InsertResult {
        let table = batch.table;
        let record_batch = batch.batch;
        let offsets = batch.offsets;
        let num_rows = record_batch.num_rows();

        // Try initial insert (handles transient retries internally)
        match self.insert_arrow(&table, record_batch.clone()).await {
            Ok(count) => {
                return InsertResult::success(count);
            }
            Err(e) => {
                // Check if this is a data error (salvageable) or something else
                let is_data_error = matches!(
                    e,
                    crate::Error::ClickHouse(ref msg) if {
                        // Check the underlying error category
                        let ch_err = crate::clickhouse::ClickHouseError::Insert(msg.clone());
                        ch_err.is_data_error()
                    }
                ) || e.to_string().to_lowercase().contains("type mismatch")
                    || e.to_string().to_lowercase().contains("incorrect data")
                    || e.to_string().to_lowercase().contains("cannot parse");

                if !self.enable_salvage || num_rows <= 1 || !is_data_error {
                    // Salvage disabled, single row, or non-data error - fail all rows
                    let reason = e.to_string();
                    let is_fatal = e.to_string().to_lowercase().contains("unknown table")
                        || e.to_string().to_lowercase().contains("access denied");

                    if is_fatal {
                        // Fatal error - don't send to DLQ, just fail
                        error!(
                            table = %table,
                            rows = num_rows,
                            error = %e,
                            "Fatal error, batch failed (not sending to DLQ)"
                        );
                        return InsertResult::with_failures(0, Vec::new());
                    }

                    // Transient error after max retries - fail all rows but mark for DLQ
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

        self.salvage_batch(
            &table,
            record_batch,
            &offsets,
            0, // start offset in original batch
            0, // depth
            &mut inserted,
            &mut failed,
        )
        .await;

        info!(
            table = %table,
            inserted = inserted,
            failed = failed.len(),
            "Batch salvage complete"
        );

        InsertResult::with_failures(inserted, failed)
    }

    /// Recursively salvage a batch using binary split
    fn salvage_batch<'a>(
        &'a self,
        table: &'a str,
        batch: RecordBatch,
        offsets: &'a [KafkaOffset],
        start_index: usize,
        depth: u32,
        inserted: &'a mut usize,
        failed: &'a mut Vec<FailedRow>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            let num_rows = batch.num_rows();

            // Safety check: prevent infinite recursion
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
                match self.arrow_client.insert(table, batch).await {
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

            // Try the whole batch first (it might work now, e.g., transient error)
            match self.arrow_client.insert(table, batch.clone()).await {
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
            let left = batch.slice(0, mid);
            let right = batch.slice(mid, num_rows - mid);

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

            // Recurse on left half
            self.salvage_batch(
                table,
                left,
                left_offsets,
                start_index,
                depth + 1,
                inserted,
                failed,
            )
            .await;

            // Recurse on right half
            self.salvage_batch(
                table,
                right,
                right_offsets,
                start_index + mid,
                depth + 1,
                inserted,
                failed,
            )
            .await;
        })
    }

    /// Insert a FlushBatch (Arrow-native) - simple version without salvage
    pub async fn insert_batch(&self, batch: FlushBatch) -> Result<usize> {
        self.insert_arrow(&batch.table, batch.batch).await
    }

    /// Insert multiple batches concurrently with salvage
    ///
    /// Uses semaphore to limit concurrent inserts if configured.
    /// All batches are spawned as tasks, but only `max_concurrent_inserts`
    /// will execute simultaneously.
    pub async fn insert_batches_with_salvage(
        &self,
        batches: Vec<FlushBatch>,
    ) -> Vec<(String, InsertResult)> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let arrow_client = self.arrow_client.clone();
            let max_retries = self.max_retries;
            let base_retry_delay_ms = self.base_retry_delay_ms;
            let max_retry_delay_ms = self.max_retry_delay_ms;
            let enable_salvage = self.enable_salvage;
            let max_salvage_depth = self.max_salvage_depth;
            let table = batch.table.to_string();
            let semaphore = self.semaphore.clone();

            handles.push(tokio::spawn(async move {
                // Acquire semaphore permit if configured (limits concurrent inserts)
                let _permit = match &semaphore {
                    Some(sem) => Some(sem.acquire().await.expect("Semaphore closed")),
                    None => None,
                };

                let inserter = Inserter {
                    arrow_client,
                    max_retries,
                    base_retry_delay_ms,
                    max_retry_delay_ms,
                    enable_salvage,
                    max_salvage_depth,
                    semaphore: None, // Child inserter doesn't need its own semaphore
                    circuit_breaker: None,
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
                    // Task panicked - create a failed result
                    results.push((
                        "unknown".to_string(),
                        InsertResult::with_failures(
                            0,
                            vec![FailedRow {
                                row_index: 0,
                                offset: None,
                                reason: format!("Insert task panicked: {}", e),
                            }],
                        ),
                    ));
                }
            }
        }

        results
    }

    /// Insert multiple batches concurrently (simple version)
    ///
    /// Uses semaphore to limit concurrent inserts if configured.
    pub async fn insert_batches(&self, batches: Vec<FlushBatch>) -> Vec<Result<usize>> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let arrow_client = self.arrow_client.clone();
            let max_retries = self.max_retries;
            let base_retry_delay_ms = self.base_retry_delay_ms;
            let max_retry_delay_ms = self.max_retry_delay_ms;
            let enable_salvage = self.enable_salvage;
            let max_salvage_depth = self.max_salvage_depth;
            let semaphore = self.semaphore.clone();

            handles.push(tokio::spawn(async move {
                // Acquire semaphore permit if configured (limits concurrent inserts)
                let _permit = match &semaphore {
                    Some(sem) => Some(sem.acquire().await.expect("Semaphore closed")),
                    None => None,
                };

                let inserter = Inserter {
                    arrow_client,
                    max_retries,
                    base_retry_delay_ms,
                    max_retry_delay_ms,
                    enable_salvage,
                    max_salvage_depth,
                    semaphore: None,
                    circuit_breaker: None,
                };
                inserter.insert_batch(batch).await
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(crate::Error::Buffer(format!(
                    "Insert task panicked: {}",
                    e
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

        // Test with 0 (unlimited)
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

        // Geometric backoff: 100, 200, 400, 800, 1600, 3200, ...
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
            max_retry_delay_ms: 1000, // Cap at 1 second
            ..Default::default()
        };

        // Should cap at 1000ms
        assert_eq!(config.backoff_delay(10), Duration::from_millis(1000));
        assert_eq!(config.backoff_delay(20), Duration::from_millis(1000));
    }
}
