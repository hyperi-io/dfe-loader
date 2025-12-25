//! Arrow-based batch inserter for ClickHouse
//!
//! Handles batch inserts with retry logic using Arrow RecordBatch.
//! Uses clickhouse-arrow for native Arrow protocol inserts.
//!
//! ## Batch Salvage
//!
//! When a batch insert fails, the inserter uses binary-split salvage:
//! 1. Split the failed batch in half
//! 2. Recursively retry each half
//! 3. Continue splitting until single-row failure is identified
//! 4. Return failed rows for DLQ routing

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::buffer::arrow::KafkaOffset;
use crate::buffer::FlushBatch;
use crate::clickhouse::ArrowClickHouseClient;
use crate::Result;

/// Configuration for the inserter
pub struct InserterConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    /// Enable batch salvage on insert failure
    pub enable_salvage: bool,
    /// Maximum depth for binary-split salvage (prevents infinite recursion)
    pub max_salvage_depth: u32,
}

impl Default for InserterConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 1000,
            enable_salvage: true,
            max_salvage_depth: 20, // 2^20 = 1M rows max batch size
        }
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
/// Supports batch salvage for recovering from partial failures.
pub struct Inserter {
    /// Arrow client for native protocol inserts
    arrow_client: Arc<ArrowClickHouseClient>,
    max_retries: u32,
    retry_delay: Duration,
    enable_salvage: bool,
    max_salvage_depth: u32,
}

impl Inserter {
    /// Create a new inserter with Arrow client
    pub fn new(arrow_client: Arc<ArrowClickHouseClient>, config: InserterConfig) -> Self {
        Self {
            arrow_client,
            max_retries: config.max_retries,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
            enable_salvage: config.enable_salvage,
            max_salvage_depth: config.max_salvage_depth,
        }
    }

    /// Insert an Arrow RecordBatch into a table with retry
    ///
    /// Does not perform salvage - use `insert_with_salvage` for that.
    pub async fn insert_arrow(&self, table: &str, batch: RecordBatch) -> Result<usize> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                warn!(table = %table, attempt = attempt, "Retrying insert");
                sleep(self.retry_delay).await;
            }

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
                    error!(table = %table, attempt = attempt, error = %e, "Insert failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            crate::Error::Buffer("Max retries exceeded".into())
        }))
    }

    /// Insert a FlushBatch with batch salvage on failure
    ///
    /// If the batch fails to insert, uses binary-split salvage to identify
    /// and isolate failing rows. Returns successfully inserted count and
    /// list of failed rows for DLQ routing.
    pub async fn insert_with_salvage(&self, batch: FlushBatch) -> InsertResult {
        let table = batch.table;
        let record_batch = batch.batch;
        let offsets = batch.offsets;
        let num_rows = record_batch.num_rows();

        // Try initial insert
        match self.insert_arrow(&table, record_batch.clone()).await {
            Ok(count) => {
                return InsertResult::success(count);
            }
            Err(e) => {
                if !self.enable_salvage || num_rows <= 1 {
                    // Salvage disabled or single row - all rows failed
                    let reason = e.to_string();
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
                    "Insert failed, starting batch salvage"
                );
            }
        }

        // Binary-split salvage
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
    pub async fn insert_batches_with_salvage(
        &self,
        batches: Vec<FlushBatch>,
    ) -> Vec<(String, InsertResult)> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let arrow_client = self.arrow_client.clone();
            let max_retries = self.max_retries;
            let retry_delay = self.retry_delay;
            let enable_salvage = self.enable_salvage;
            let max_salvage_depth = self.max_salvage_depth;
            let table = batch.table.clone();

            handles.push(tokio::spawn(async move {
                let inserter = Inserter {
                    arrow_client,
                    max_retries,
                    retry_delay,
                    enable_salvage,
                    max_salvage_depth,
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
    pub async fn insert_batches(&self, batches: Vec<FlushBatch>) -> Vec<Result<usize>> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let arrow_client = self.arrow_client.clone();
            let max_retries = self.max_retries;
            let retry_delay = self.retry_delay;
            let enable_salvage = self.enable_salvage;
            let max_salvage_depth = self.max_salvage_depth;

            handles.push(tokio::spawn(async move {
                let inserter = Inserter {
                    arrow_client,
                    max_retries,
                    retry_delay,
                    enable_salvage,
                    max_salvage_depth,
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
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
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
}
