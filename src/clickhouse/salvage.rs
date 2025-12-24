//! Row salvage for failed inserts

use crate::Result;

/// Attempts to salvage rows from a failed batch insert
pub struct RowSalvager {
    // TODO: Add salvage config
}

impl RowSalvager {
    /// Create a new row salvager
    pub fn new() -> Self {
        Self {}
    }

    /// Attempt to salvage valid rows from a failed batch
    pub fn salvage(&self, _failed_batch: &[u8]) -> Result<Vec<Vec<u8>>> {
        // TODO: Implement binary search salvage
        Ok(Vec::new())
    }
}

impl Default for RowSalvager {
    fn default() -> Self {
        Self::new()
    }
}
