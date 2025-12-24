//! Risk scoring

/// Risk score result
#[derive(Debug, Clone)]
pub struct RiskScore {
    pub score: f64,
    pub factors: Vec<String>,
}

/// Risk scorer
pub struct RiskScorer {
    // TODO: Risk rules
}

impl RiskScorer {
    /// Create a new risk scorer
    pub fn new() -> Self {
        Self {}
    }

    /// Calculate risk score for an event
    pub fn score(&self, _event: &[u8]) -> RiskScore {
        RiskScore {
            score: 0.0,
            factors: Vec::new(),
        }
    }
}

impl Default for RiskScorer {
    fn default() -> Self {
        Self::new()
    }
}
