//! IP/domain reputation enrichment

/// Reputation lookup result
#[derive(Debug, Clone)]
pub struct ReputationResult {
    pub score: f64,
    pub categories: Vec<String>,
}

/// Reputation enricher
pub struct ReputationEnricher {
    // TODO: Reputation database
}

impl ReputationEnricher {
    /// Create a new reputation enricher
    pub fn new() -> Self {
        Self {}
    }

    /// Look up reputation for an IP or domain
    pub fn lookup(&self, _target: &str) -> Option<ReputationResult> {
        // TODO: Implement
        None
    }
}

impl Default for ReputationEnricher {
    fn default() -> Self {
        Self::new()
    }
}
