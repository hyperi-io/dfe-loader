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
use crate::clickhouse::{ArrowClickHouseClient, ClickHouseClient};
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
            use_arrow_native: true, // Prefer native Arrow by default
        }
    }
}

/// Handles batch inserts to ClickHouse with retry logic
///
/// Supports two modes:
/// - Native Arrow protocol (via clickhouse-arrow) - preferred
/// - JSON bridge (via klickhouse) - fallback
pub struct Inserter {
    /// Arrow client for native protocol inserts
    arrow_client: Option<Arc<ArrowClickHouseClient>>,
    /// klickhouse client for JSON bridge inserts
    json_client: Arc<ClickHouseClient>,
    max_retries: u32,
    retry_delay: Duration,
}

impl Inserter {
    /// Create a new inserter with both clients
    pub fn new(json_client: Arc<ClickHouseClient>, config: InserterConfig) -> Self {
        Self {
            arrow_client: None,
            json_client,
            max_retries: config.max_retries,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
        }
    }

    /// Create a new inserter with Arrow client for native inserts
    pub fn with_arrow_client(
        arrow_client: Arc<ArrowClickHouseClient>,
        json_client: Arc<ClickHouseClient>,
        config: InserterConfig,
    ) -> Self {
        Self {
            arrow_client: Some(arrow_client),
            json_client,
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

            // Try native Arrow insert first if available
            let result = if let Some(ref arrow_client) = self.arrow_client {
                arrow_client.insert(table, batch.clone()).await
            } else {
                // Fall back to JSON bridge
                self.insert_arrow_via_json(table, &batch).await
            };

            match result {
                Ok(count) => {
                    debug!(
                        table = %table,
                        rows = count,
                        native = self.arrow_client.is_some(),
                        "Insert successful"
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
            crate::Error::ClickHouse(klickhouse::KlickhouseError::ProtocolError(
                "Max retries exceeded".into(),
            ))
        }))
    }

    /// Fallback: Convert Arrow batch to JSON for insert
    ///
    /// Used when clickhouse-arrow native client is not available.
    async fn insert_arrow_via_json(&self, table: &str, batch: &RecordBatch) -> Result<usize> {
        use serde_json::Map;

        let schema = batch.schema();
        let mut rows = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            let mut row_map = Map::new();

            for (col_idx, field) in schema.fields().iter().enumerate() {
                let column = batch.column(col_idx);
                let value = arrow_value_to_json(column, row_idx);
                row_map.insert(field.name().clone(), value);
            }

            rows.push(row_map);
        }

        self.json_client.insert_json(table, rows).await
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
            let json_client = self.json_client.clone();
            let max_retries = self.max_retries;
            let retry_delay = self.retry_delay;

            handles.push(tokio::spawn(async move {
                let inserter = Inserter {
                    arrow_client,
                    json_client,
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

/// Convert an Arrow array value at an index to JSON
fn arrow_value_to_json(array: &dyn arrow::array::Array, idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    use serde_json::Value;

    if array.is_null(idx) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            Value::Number(arr.value(idx).into())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Value::String(arr.value(idx).to_string())
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            // Try to parse as UTF-8 string, otherwise use base64
            match std::str::from_utf8(arr.value(idx)) {
                Ok(s) => Value::String(s.to_string()),
                Err(_) => Value::String(base64_encode(arr.value(idx))),
            }
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(arr.value(idx))
        }
        _ => {
            // Fallback: try to format as string
            Value::String(format!("{:?}", array))
        }
    }
}

/// Simple base64 encoding for binary data
fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(data.len() * 4 / 3 + 4);
    for chunk in data.chunks(3) {
        match chunk.len() {
            3 => {
                let n = (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8 | chunk[2] as u32;
                write!(
                    &mut result,
                    "{}{}{}{}",
                    BASE64_CHARS[(n >> 18 & 0x3f) as usize],
                    BASE64_CHARS[(n >> 12 & 0x3f) as usize],
                    BASE64_CHARS[(n >> 6 & 0x3f) as usize],
                    BASE64_CHARS[(n & 0x3f) as usize]
                )
                .unwrap();
            }
            2 => {
                let n = (chunk[0] as u32) << 16 | (chunk[1] as u32) << 8;
                write!(
                    &mut result,
                    "{}{}{}=",
                    BASE64_CHARS[(n >> 18 & 0x3f) as usize],
                    BASE64_CHARS[(n >> 12 & 0x3f) as usize],
                    BASE64_CHARS[(n >> 6 & 0x3f) as usize]
                )
                .unwrap();
            }
            1 => {
                let n = (chunk[0] as u32) << 16;
                write!(
                    &mut result,
                    "{}{}==",
                    BASE64_CHARS[(n >> 18 & 0x3f) as usize],
                    BASE64_CHARS[(n >> 12 & 0x3f) as usize]
                )
                .unwrap();
            }
            _ => {}
        }
    }
    result
}

const BASE64_CHARS: &[char] = &[
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '+', '/',
];

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
