//! Dead Letter Queue producer

use crate::Result;

/// Produces failed messages to DLQ topics
pub struct DlqProducer {
    // TODO: rdkafka FutureProducer
}

impl DlqProducer {
    /// Create a new DLQ producer
    pub fn new() -> Result<Self> {
        Ok(Self {})
    }

    /// Send a message to the DLQ
    pub async fn send(&self, _topic: &str, _payload: &[u8]) -> Result<()> {
        // TODO: Implement
        Ok(())
    }
}
