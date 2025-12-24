//! Batch inserter for ClickHouse
//!
//! Handles batch inserts with retry logic.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::time::sleep;
use tracing::{debug, error, warn};

use crate::buffer::FlushBatch;
use crate::clickhouse::ClickHouseClient;
use crate::Result;

/// Configuration for the inserter
pub struct InserterConfig {
    pub max_retries: u32,
    pub retry_delay_ms: u64,
}

impl Default for InserterConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 1000,
        }
    }
}

/// Handles batch inserts to ClickHouse with retry logic
pub struct Inserter {
    client: Arc<ClickHouseClient>,
    max_retries: u32,
    retry_delay: Duration,
}

impl Inserter {
    /// Create a new inserter
    pub fn new(client: Arc<ClickHouseClient>, config: InserterConfig) -> Self {
        Self {
            client,
            max_retries: config.max_retries,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
        }
    }

    /// Insert a batch of rows into a table with retry
    pub async fn insert(&self, table: &str, rows: Vec<Map<String, Value>>) -> Result<usize> {
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                warn!(table = %table, attempt = attempt, "Retrying insert");
                sleep(self.retry_delay).await;
            }

            match self.client.insert_json(table, rows.clone()).await {
                Ok(count) => {
                    debug!(table = %table, rows = count, "Insert successful");
                    return Ok(count);
                }
                Err(e) => {
                    error!(table = %table, attempt = attempt, error = %e, "Insert failed");
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            crate::Error::ClickHouse(klickhouse::KlickhouseError::ProtocolError(
                "Max retries exceeded".into(),
            ))
        }))
    }

    /// Insert a FlushBatch
    pub async fn insert_batch(&self, batch: FlushBatch) -> Result<usize> {
        self.insert(&batch.table, batch.rows).await
    }

    /// Insert multiple batches concurrently
    pub async fn insert_batches(&self, batches: Vec<FlushBatch>) -> Vec<Result<usize>> {
        let mut handles = Vec::with_capacity(batches.len());

        for batch in batches {
            let client = self.client.clone();
            let max_retries = self.max_retries;
            let retry_delay = self.retry_delay;

            handles.push(tokio::spawn(async move {
                let inserter = Inserter {
                    client,
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
    }
}
