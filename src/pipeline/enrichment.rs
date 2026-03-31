// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Enrichment pipeline: GeoIP + IP reputation + risk scoring.
//!
//! All components are optional — each is only active if configured and
//! initialised successfully. Failures are non-fatal (logged as warnings).
//!
//! All lookups are in-memory (`&self`), safe for rayon `par_iter`.
//! GeoIP uses `parking_lot::RwLock` for internal caching (Sync-safe).

use serde_json::Value;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::enrich::geoip::{GeoIpEnricher, GeoIpResult};
use crate::enrich::reputation::{ReputationEnricher, ReputationResult, ThreatSource, ThreatType};
use crate::enrich::risk::{RiskInput, RiskOutput, RiskPreset, RiskScorer};

/// Active enrichment pipeline (GeoIP + reputation + risk scoring).
///
/// All components are optional. Lookups are `&self` — safe for parallel processing.
pub(crate) struct EnrichmentPipeline {
    /// IP field names to check in event data (first match wins).
    pub ip_fields: Vec<String>,
    /// GeoIP lookup (city + ASN).
    pub geoip: Option<GeoIpEnricher>,
    /// IP reputation lookup (VPN, Tor, proxy, botnet, ...).
    pub reputation: Option<ReputationEnricher>,
    /// Risk scorer (weighted composite of geo + reputation data).
    pub risk: Option<RiskScorer>,
}

impl EnrichmentPipeline {
    /// Initialise enrichment pipeline from config.
    ///
    /// All failures are non-fatal — the pipeline continues with whatever
    /// components were successfully initialised.
    pub async fn init(config: &Config) -> Self {
        let geoip = if config.geoip.enabled {
            let enricher = GeoIpEnricher::from_config(&config.geoip).await;
            if enricher.is_available() {
                info!(provider = ?config.geoip.provider, "GeoIP enrichment enabled");
                Some(enricher)
            } else {
                warn!(provider = ?config.geoip.provider, "GeoIP enabled but no databases loaded");
                None
            }
        } else {
            None
        };

        let reputation = if config.enrichment.reputation.enabled {
            let enricher = ReputationEnricher::new()
                .with_cache_capacity(config.enrichment.reputation.cache_capacity);

            let mut loaded = 0usize;
            for path in &config.enrichment.reputation.blocklist_files {
                match std::fs::read_to_string(path) {
                    Ok(content) => {
                        enricher.load_plain_list(&content, ThreatType::None, ThreatSource::Custom);
                        loaded += 1;
                        debug!(path = %path, "Loaded reputation blocklist");
                    }
                    Err(e) => {
                        warn!(path = %path, error = %e, "Failed to load reputation blocklist");
                    }
                }
            }

            if enricher.is_available() {
                info!(blocklists = loaded, "Reputation enrichment enabled");
                Some(enricher)
            } else if !config.enrichment.reputation.blocklist_files.is_empty() {
                warn!("Reputation enabled but no blocklists loaded");
                None
            } else {
                info!("Reputation enrichment enabled (no blocklist files configured)");
                Some(enricher)
            }
        } else {
            None
        };

        let risk = if config.enrichment.risk_scoring.enabled {
            let preset = match config.enrichment.risk_scoring.preset.as_str() {
                "us_enterprise" => RiskPreset::UsEnterprise,
                "eu_enterprise" => RiskPreset::EuEnterprise,
                "apac_enterprise" => RiskPreset::ApacEnterprise,
                "high_security" => RiskPreset::HighSecurity,
                _ => RiskPreset::Global,
            };
            info!(preset = %config.enrichment.risk_scoring.preset, "Risk scoring enabled");
            Some(RiskScorer::from_preset(preset))
        } else {
            None
        };

        Self {
            ip_fields: config.enrichment.ip_fields.clone(),
            geoip,
            reputation,
            risk,
        }
    }

    /// Returns true if any enrichment is active.
    pub fn is_active(&self) -> bool {
        self.geoip.is_some() || self.reputation.is_some() || self.risk.is_some()
    }

    /// Run the full enrichment pipeline on a data map.
    ///
    /// Pure `&self` — safe for rayon `par_iter`. Modifies `data` in place.
    pub fn enrich(&self, data: &mut serde_json::Map<String, Value>) {
        if !self.is_active() {
            return;
        }

        let ip = match extract_enrich_ip(data, &self.ip_fields) {
            Some(ip) => ip,
            None => return,
        };

        let geo_result = self.geoip.as_ref().and_then(|g| g.lookup(ip));
        let rep_result = self.reputation.as_ref().and_then(|r| r.lookup(ip));

        if let Some(ref geo) = geo_result {
            inject_geo(data, geo);
        }
        if let Some(ref rep) = rep_result {
            inject_reputation(data, rep);
        }
        if let Some(ref scorer) = self.risk
            && (geo_result.is_some() || rep_result.is_some())
        {
            let input = RiskInput::from_enrichment(geo_result.as_ref(), rep_result.as_ref());
            let output = scorer.score(&input);
            inject_risk(data, &output);
        }
    }
}

// =============================================================================
// Enrichment helpers (module-private)
// =============================================================================

/// Extract the first IP string found in `data` by checking `ip_fields` in order.
fn extract_enrich_ip<'a>(
    data: &'a serde_json::Map<String, Value>,
    ip_fields: &[String],
) -> Option<&'a str> {
    for field in ip_fields {
        if let Some(Value::String(s)) = data.get(field.as_str())
            && !s.is_empty()
        {
            return Some(s.as_str());
        }
    }
    None
}

/// Inject GeoIP result fields as `geo_*` prefixed fields.
fn inject_geo(data: &mut serde_json::Map<String, Value>, result: &GeoIpResult) {
    macro_rules! insert_if_absent {
        ($key:expr, $val:expr) => {
            if !data.contains_key($key) {
                data.insert($key.to_string(), $val);
            }
        };
    }
    if let Some(ref v) = result.continent_code {
        insert_if_absent!("geo_continent_code", Value::String(v.clone()));
    }
    if let Some(ref v) = result.country_code {
        insert_if_absent!("geo_country_code", Value::String(v.clone()));
    }
    if let Some(ref v) = result.country_name {
        insert_if_absent!("geo_country", Value::String(v.clone()));
    }
    if let Some(ref v) = result.city {
        insert_if_absent!("geo_city", Value::String(v.clone()));
    }
    if let Some(v) = result.latitude {
        insert_if_absent!("geo_latitude", serde_json::json!(v));
    }
    if let Some(v) = result.longitude {
        insert_if_absent!("geo_longitude", serde_json::json!(v));
    }
    if let Some(ref v) = result.timezone {
        insert_if_absent!("geo_timezone", Value::String(v.clone()));
    }
    if let Some(ref v) = result.subdivision {
        insert_if_absent!("geo_region", Value::String(v.clone()));
    }
    if let Some(ref v) = result.subdivision_code {
        insert_if_absent!("geo_region_code", Value::String(v.clone()));
    }
    if let Some(v) = result.asn {
        insert_if_absent!("geo_asn", serde_json::json!(v));
    }
    if let Some(ref v) = result.asn_org {
        insert_if_absent!("geo_asn_org", Value::String(v.clone()));
    }
    insert_if_absent!("geo_is_private", Value::Bool(result.is_private));
}

/// Inject reputation result fields as `rep_*` prefixed fields.
fn inject_reputation(data: &mut serde_json::Map<String, Value>, result: &ReputationResult) {
    macro_rules! insert_if_absent {
        ($key:expr, $val:expr) => {
            if !data.contains_key($key) {
                data.insert($key.to_string(), $val);
            }
        };
    }
    insert_if_absent!("rep_is_vpn", Value::Bool(result.is_vpn));
    insert_if_absent!("rep_is_proxy", Value::Bool(result.is_proxy));
    insert_if_absent!("rep_is_tor", Value::Bool(result.is_tor));
    insert_if_absent!("rep_is_relay", Value::Bool(result.is_relay));
    insert_if_absent!("rep_is_datacenter", Value::Bool(result.is_datacenter));
    insert_if_absent!("rep_is_botnet", Value::Bool(result.is_botnet));
    insert_if_absent!("rep_is_spam", Value::Bool(result.is_spam));
    insert_if_absent!("rep_is_scanner", Value::Bool(result.is_scanner));
    insert_if_absent!("rep_is_malicious", Value::Bool(result.is_malicious));
    insert_if_absent!(
        "rep_threat_type",
        Value::String(format!("{:?}", result.threat_type).to_lowercase())
    );
    if result.abuse_score > 0 {
        insert_if_absent!("rep_abuse_score", serde_json::json!(result.abuse_score));
    }
}

/// Inject risk scoring output as `risk_*` prefixed fields.
fn inject_risk(data: &mut serde_json::Map<String, Value>, output: &RiskOutput) {
    if !data.contains_key("risk_score") {
        data.insert(
            "risk_score".to_string(),
            serde_json::json!(output.risk_score),
        );
    }
    if !data.contains_key("risk_level") {
        data.insert(
            "risk_level".to_string(),
            Value::String(output.risk_level.as_str().to_string()),
        );
    }
    if !output.risk_factors.is_empty() && !data.contains_key("risk_factors") {
        let factors: Vec<Value> = output
            .risk_factors
            .iter()
            .map(|s| Value::String(s.to_string()))
            .collect();
        data.insert("risk_factors".to_string(), Value::Array(factors));
    }
}
