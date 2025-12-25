//! Arrow-based batch inserter for ClickHouse
//!
//! Handles batch inserts with retry logic using Arrow RecordBatch.
//! Uses clickhouse-arrow for native Arrow protocol inserts.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use tokio::time::sleep;
use tracing::{debug, error, warn};

use crate::buffer::FlushBatch;
use crate::clickhouse::ArrowClickHouseClient;
use crate::Result;

/// Configuration for the inserter
pub struct InserterConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    /// Use native Arrow protocol (requires clickhouse-arrow)
    pub use_arrow_native: bool,
}

impl Default for InserterConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 1000,
            use_arrow_native: true, // Always true - native Arrow is the only path
        }
    }
}

/// Handles batch inserts to ClickHouse with retry logic
///
/// Uses native Arrow protocol via clickhouse-arrow for efficient columnar inserts.
pub struct Inserter {
    /// Arrow client for native protocol inserts
    arrow_client: Arc<ArrowClickHouseClient>,
    max_retries: u32,
    retry_delay: Duration,
}

impl Inserter {
    /// Create a new inserter with Arrow client
    pub fn new(arrow_client: Arc<ArrowClickHouseClient>, config: InserterConfig) -> Self {
        Self {
            arrow_client,
            max_retries: config.max_retries,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
        }
    }

    /// Insert an Arrow RecordBatch into a table with retry
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

    /// Insert a FlushBatch (Arrow-native)
    pub async fn insert_batch(&self, batch: FlushBatch) -> Result<usize> {
        self.insert_arrow(&batch.table, batch.batch).await
    }

    /// Insert multiple batches concurrently
    pub async fn insert_batches(&self, batches: Vec<FlushBatch>) -> Vec<Result<usize>> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let arrow_client = self.arrow_client.clone();
            let max_retries = self.max_retries;
            let retry_delay = self.retry_delay;

            handles.push(tokio::spawn(async move {
                let inserter = Inserter {
                    arrow_client,
                    max_retries,
                    retry_delay,
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
        assert!(config.use_arrow_native);
    }
}
