//! GeoIP enrichment

/// GeoIP lookup result
#[derive(Debug, Clone)]
pub struct GeoIpResult {
    pub country: Option<String>,
    pub city: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
}

/// GeoIP enricher
pub struct GeoIpEnricher {
    // TODO: MaxMind reader
}

impl GeoIpEnricher {
    /// Create a new GeoIP enricher
    pub fn new() -> Self {
        Self {}
    }

    /// Look up an IP address
    pub fn lookup(&self, _ip: &str) -> Option<GeoIpResult> {
        // TODO: Implement
        None
    }
}

impl Default for GeoIpEnricher {
    fn default() -> Self {
        Self::new()
    }
}
