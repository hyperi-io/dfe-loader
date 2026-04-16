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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, json};

    // ========================================================================
    // extract_enrich_ip
    // ========================================================================

    #[test]
    fn extract_ip_first_matching_field() {
        let mut data = Map::new();
        data.insert("src_ip".into(), json!("10.0.0.1"));
        data.insert("dst_ip".into(), json!("192.168.1.1"));

        let fields = vec!["src_ip".to_string(), "dst_ip".to_string()];
        let ip = extract_enrich_ip(&data, &fields);
        assert_eq!(ip, Some("10.0.0.1"));
    }

    #[test]
    fn extract_ip_skips_empty_strings() {
        let mut data = Map::new();
        data.insert("src_ip".into(), json!(""));
        data.insert("dst_ip".into(), json!("8.8.8.8"));

        let fields = vec!["src_ip".to_string(), "dst_ip".to_string()];
        let ip = extract_enrich_ip(&data, &fields);
        assert_eq!(ip, Some("8.8.8.8"));
    }

    #[test]
    fn extract_ip_returns_none_when_no_match() {
        let mut data = Map::new();
        data.insert("hostname".into(), json!("server-01"));
        data.insert("port".into(), json!(8080));

        let fields = vec!["src_ip".to_string(), "dst_ip".to_string()];
        assert!(extract_enrich_ip(&data, &fields).is_none());
    }

    #[test]
    fn extract_ip_returns_none_for_empty_field_list() {
        let mut data = Map::new();
        data.insert("src_ip".into(), json!("10.0.0.1"));

        let fields: Vec<String> = vec![];
        assert!(extract_enrich_ip(&data, &fields).is_none());
    }

    #[test]
    fn extract_ip_ignores_non_string_values() {
        let mut data = Map::new();
        data.insert("src_ip".into(), json!(192)); // number, not string
        data.insert("dst_ip".into(), json!(true)); // bool, not string
        data.insert("real_ip".into(), json!("1.2.3.4"));

        let fields = vec![
            "src_ip".to_string(),
            "dst_ip".to_string(),
            "real_ip".to_string(),
        ];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("1.2.3.4"));
    }

    #[test]
    fn extract_ip_returns_none_for_empty_data() {
        let data = Map::new();
        let fields = vec!["src_ip".to_string()];
        assert!(extract_enrich_ip(&data, &fields).is_none());
    }

    // ========================================================================
    // inject_geo
    // ========================================================================

    #[test]
    fn inject_geo_populates_all_available_fields() {
        let mut data = Map::new();
        let result = GeoIpResult {
            continent_code: Some("OC".into()),
            country_code: Some("AU".into()),
            country_name: Some("Australia".into()),
            city: Some("Sydney".into()),
            latitude: Some(-33.8688),
            longitude: Some(151.2093),
            timezone: Some("Australia/Sydney".into()),
            subdivision: Some("New South Wales".into()),
            subdivision_code: Some("NSW".into()),
            asn: Some(13335),
            asn_org: Some("Cloudflare Inc".into()),
            is_private: false,
            ..Default::default()
        };

        inject_geo(&mut data, &result);

        assert_eq!(data["geo_continent_code"], json!("OC"));
        assert_eq!(data["geo_country_code"], json!("AU"));
        assert_eq!(data["geo_country"], json!("Australia"));
        assert_eq!(data["geo_city"], json!("Sydney"));
        assert_eq!(data["geo_latitude"], json!(-33.8688));
        assert_eq!(data["geo_longitude"], json!(151.2093));
        assert_eq!(data["geo_timezone"], json!("Australia/Sydney"));
        assert_eq!(data["geo_region"], json!("New South Wales"));
        assert_eq!(data["geo_region_code"], json!("NSW"));
        assert_eq!(data["geo_asn"], json!(13335));
        assert_eq!(data["geo_asn_org"], json!("Cloudflare Inc"));
        assert_eq!(data["geo_is_private"], json!(false));
    }

    #[test]
    fn inject_geo_handles_minimal_result() {
        let mut data = Map::new();
        let result = GeoIpResult {
            country_code: Some("US".into()),
            is_private: false,
            ..Default::default()
        };

        inject_geo(&mut data, &result);

        assert_eq!(data["geo_country_code"], json!("US"));
        assert_eq!(data["geo_is_private"], json!(false));
        // Fields with None should NOT be present
        assert!(!data.contains_key("geo_city"));
        assert!(!data.contains_key("geo_latitude"));
        assert!(!data.contains_key("geo_asn"));
    }

    #[test]
    fn inject_geo_private_ip_result() {
        let mut data = Map::new();
        let result = GeoIpResult {
            is_private: true,
            ..Default::default()
        };

        inject_geo(&mut data, &result);

        assert_eq!(data["geo_is_private"], json!(true));
        assert!(!data.contains_key("geo_country_code"));
    }

    #[test]
    fn inject_geo_does_not_overwrite_existing_fields() {
        let mut data = Map::new();
        data.insert("geo_country_code".into(), json!("EXISTING"));
        data.insert("geo_is_private".into(), json!("EXISTING"));

        let result = GeoIpResult {
            country_code: Some("AU".into()),
            is_private: false,
            ..Default::default()
        };

        inject_geo(&mut data, &result);

        // Pre-existing values must NOT be overwritten
        assert_eq!(data["geo_country_code"], json!("EXISTING"));
        assert_eq!(data["geo_is_private"], json!("EXISTING"));
    }

    // ========================================================================
    // inject_reputation
    // ========================================================================

    #[test]
    fn inject_reputation_tor_node() {
        let mut data = Map::new();
        let result = ReputationResult {
            is_tor: true,
            is_malicious: true,
            threat_type: ThreatType::Tor,
            abuse_score: 85,
            ..Default::default()
        };

        inject_reputation(&mut data, &result);

        assert_eq!(data["rep_is_tor"], json!(true));
        assert_eq!(data["rep_is_malicious"], json!(true));
        assert_eq!(data["rep_is_vpn"], json!(false));
        assert_eq!(data["rep_is_proxy"], json!(false));
        assert_eq!(data["rep_threat_type"], json!("tor"));
        assert_eq!(data["rep_abuse_score"], json!(85));
    }

    #[test]
    fn inject_reputation_clean_ip() {
        let mut data = Map::new();
        let result = ReputationResult::default();

        inject_reputation(&mut data, &result);

        assert_eq!(data["rep_is_vpn"], json!(false));
        assert_eq!(data["rep_is_tor"], json!(false));
        assert_eq!(data["rep_is_botnet"], json!(false));
        assert_eq!(data["rep_is_scanner"], json!(false));
        // abuse_score == 0 → should NOT be inserted
        assert!(!data.contains_key("rep_abuse_score"));
        // threat_type None → "none" string
        assert_eq!(data["rep_threat_type"], json!("none"));
    }

    #[test]
    fn inject_reputation_does_not_overwrite_existing() {
        let mut data = Map::new();
        data.insert("rep_is_vpn".into(), json!("MANUAL_OVERRIDE"));

        let result = ReputationResult {
            is_vpn: true,
            ..Default::default()
        };

        inject_reputation(&mut data, &result);

        // Existing value must survive
        assert_eq!(data["rep_is_vpn"], json!("MANUAL_OVERRIDE"));
    }

    #[test]
    fn inject_reputation_multi_threat() {
        let mut data = Map::new();
        let result = ReputationResult {
            is_vpn: true,
            is_proxy: true,
            is_datacenter: true,
            is_spam: true,
            threat_type: ThreatType::Vpn,
            abuse_score: 42,
            ..Default::default()
        };

        inject_reputation(&mut data, &result);

        assert_eq!(data["rep_is_vpn"], json!(true));
        assert_eq!(data["rep_is_proxy"], json!(true));
        assert_eq!(data["rep_is_datacenter"], json!(true));
        assert_eq!(data["rep_is_spam"], json!(true));
        assert_eq!(data["rep_abuse_score"], json!(42));
    }

    // ========================================================================
    // inject_risk
    // ========================================================================

    #[test]
    fn inject_risk_high_score_with_factors() {
        let mut data = Map::new();
        let output = RiskOutput {
            risk_score: 85,
            risk_level: crate::enrich::risk::RiskLevel::Critical,
            risk_factors: vec!["high_risk_country", "tor_detected"],
            ..Default::default()
        };

        inject_risk(&mut data, &output);

        assert_eq!(data["risk_score"], json!(85));
        assert_eq!(data["risk_level"], json!("critical"));
        let factors = data["risk_factors"]
            .as_array()
            .expect("risk_factors should be array");
        assert_eq!(factors.len(), 2);
        assert_eq!(factors[0], json!("high_risk_country"));
        assert_eq!(factors[1], json!("tor_detected"));
    }

    #[test]
    fn inject_risk_zero_score_no_factors() {
        let mut data = Map::new();
        let output = RiskOutput {
            risk_score: 0,
            risk_level: crate::enrich::risk::RiskLevel::Minimal,
            risk_factors: vec![],
            ..Default::default()
        };

        inject_risk(&mut data, &output);

        assert_eq!(data["risk_score"], json!(0));
        assert_eq!(data["risk_level"], json!("minimal"));
        // Empty factors should NOT be inserted
        assert!(!data.contains_key("risk_factors"));
    }

    #[test]
    fn inject_risk_does_not_overwrite_existing() {
        let mut data = Map::new();
        data.insert("risk_score".into(), json!(999));
        data.insert("risk_level".into(), json!("override"));
        data.insert("risk_factors".into(), json!(["manual"]));

        let output = RiskOutput {
            risk_score: 50,
            risk_level: crate::enrich::risk::RiskLevel::Medium,
            risk_factors: vec!["should_not_appear"],
            ..Default::default()
        };

        inject_risk(&mut data, &output);

        assert_eq!(data["risk_score"], json!(999));
        assert_eq!(data["risk_level"], json!("override"));
        assert_eq!(data["risk_factors"], json!(["manual"]));
    }

    // ========================================================================
    // EnrichmentPipeline: is_active and enrich (no-op path)
    // ========================================================================

    #[test]
    fn pipeline_no_components_is_inactive() {
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: None,
            risk: None,
        };
        assert!(!pipeline.is_active());
    }

    #[test]
    fn pipeline_with_risk_only_is_active() {
        let scorer = RiskScorer::from_preset(RiskPreset::Global);
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: None,
            risk: Some(scorer),
        };
        assert!(pipeline.is_active());
    }

    #[test]
    fn enrich_noop_when_no_enrichers() {
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: None,
            risk: None,
        };
        let mut data = Map::new();
        data.insert("src_ip".into(), json!("8.8.8.8"));
        data.insert("hostname".into(), json!("test"));

        pipeline.enrich(&mut data);

        // Data should be completely untouched
        assert_eq!(data.len(), 2);
        assert!(!data.contains_key("geo_country_code"));
        assert!(!data.contains_key("rep_is_vpn"));
        assert!(!data.contains_key("risk_score"));
    }

    #[test]
    fn enrich_noop_when_no_ip_found() {
        let scorer = RiskScorer::from_preset(RiskPreset::Global);
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: None,
            risk: Some(scorer),
        };
        let mut data = Map::new();
        data.insert("hostname".into(), json!("no-ip-here"));

        pipeline.enrich(&mut data);

        // No IP found → no enrichment
        assert_eq!(data.len(), 1);
        assert!(!data.contains_key("risk_score"));
    }

    #[test]
    fn enrich_reputation_only_pipeline() {
        // Create a reputation enricher and load a known malicious IP
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "203.0.113.50".parse().expect("valid IP");
        enricher.add_ip(addr, ThreatType::Botnet, ThreatSource::AbuseCh);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        let mut data = Map::new();
        data.insert("src_ip".into(), json!("203.0.113.50"));

        pipeline.enrich(&mut data);

        assert_eq!(data["rep_is_botnet"], json!(true));
        assert_eq!(data["rep_is_vpn"], json!(false));
        // No GeoIP → no geo fields
        assert!(!data.contains_key("geo_country_code"));
        // No risk scorer → no risk fields
        assert!(!data.contains_key("risk_score"));
    }

    #[test]
    fn enrich_reputation_plus_risk_pipeline() {
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "198.51.100.99".parse().expect("valid IP");
        enricher.add_ip(addr, ThreatType::Tor, ThreatSource::TorProject);

        let scorer = RiskScorer::from_preset(RiskPreset::HighSecurity);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["client_ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: Some(scorer),
        };

        let mut data = Map::new();
        data.insert("client_ip".into(), json!("198.51.100.99"));

        pipeline.enrich(&mut data);

        // Reputation fields should be present
        assert_eq!(data["rep_is_tor"], json!(true));
        // Risk scoring should run because reputation result is Some
        assert!(data.contains_key("risk_score"));
        assert!(data.contains_key("risk_level"));
        // Score should be non-trivial due to Tor detection
        let score = data["risk_score"].as_u64().expect("score should be u64");
        assert!(
            score > 0,
            "Tor IP should produce non-zero risk score, got {score}"
        );
    }

    #[test]
    fn enrich_unknown_ip_returns_clean_reputation() {
        // Enricher with no data loaded — clean IP returns all-false result
        let enricher = ReputationEnricher::new();

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        let mut data = Map::new();
        data.insert("ip".into(), json!("192.0.2.1"));

        pipeline.enrich(&mut data);

        assert_eq!(data["rep_is_vpn"], json!(false));
        assert_eq!(data["rep_is_tor"], json!(false));
        assert_eq!(data["rep_is_botnet"], json!(false));
    }

    #[test]
    fn enrich_invalid_ip_string_skips_enrichment() {
        // ReputationEnricher.lookup parses the IP — "not_an_ip" returns None
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "10.0.0.1".parse().expect("valid");
        enricher.add_ip(addr, ThreatType::Scanner, ThreatSource::Custom);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        let mut data = Map::new();
        data.insert("ip".into(), json!("not_an_ip_address"));

        pipeline.enrich(&mut data);

        // lookup returns None for invalid IP → no rep fields injected
        // (ReputationEnricher.lookup parses the IP; parse failure → returns None)
        assert!(!data.contains_key("rep_is_scanner"));
    }

    // ========================================================================
    // IP field precedence — order matters
    // ========================================================================

    #[test]
    fn extract_ip_precedence_order_matters() {
        // When multiple IP fields are present, the FIRST one listed in ip_fields wins.
        let mut data = Map::new();
        data.insert("client_ip".into(), json!("1.1.1.1"));
        data.insert("src_ip".into(), json!("2.2.2.2"));
        data.insert("dst_ip".into(), json!("3.3.3.3"));

        // Order: src_ip first
        let fields = vec![
            "src_ip".to_string(),
            "client_ip".to_string(),
            "dst_ip".to_string(),
        ];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("2.2.2.2"));

        // Order: client_ip first
        let fields = vec![
            "client_ip".to_string(),
            "src_ip".to_string(),
            "dst_ip".to_string(),
        ];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("1.1.1.1"));

        // Order: dst_ip first
        let fields = vec![
            "dst_ip".to_string(),
            "src_ip".to_string(),
            "client_ip".to_string(),
        ];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("3.3.3.3"));
    }

    #[test]
    fn extract_ip_skips_null_values() {
        let mut data = Map::new();
        data.insert("src_ip".into(), serde_json::Value::Null);
        data.insert("dst_ip".into(), json!("10.0.0.5"));

        let fields = vec!["src_ip".to_string(), "dst_ip".to_string()];
        // null is not a string, so src_ip is skipped and dst_ip is picked
        assert_eq!(extract_enrich_ip(&data, &fields), Some("10.0.0.5"));
    }

    #[test]
    fn extract_ip_skips_array_and_object_values() {
        let mut data = Map::new();
        data.insert("src_ip".into(), json!(["1.1.1.1", "2.2.2.2"]));
        data.insert("dst_ip".into(), json!({"ip": "3.3.3.3"}));
        data.insert("real_ip".into(), json!("4.4.4.4"));

        let fields = vec![
            "src_ip".to_string(),
            "dst_ip".to_string(),
            "real_ip".to_string(),
        ];
        // Only real_ip is a string
        assert_eq!(extract_enrich_ip(&data, &fields), Some("4.4.4.4"));
    }

    #[test]
    fn extract_ip_whitespace_not_empty() {
        // Whitespace-only string is not empty, so it IS returned
        // (caller's responsibility to validate — extract is pure field lookup)
        let mut data = Map::new();
        data.insert("src_ip".into(), json!("   "));

        let fields = vec!["src_ip".to_string()];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("   "));
    }

    // ========================================================================
    // inject_geo / inject_reputation / inject_risk — edge cases
    // ========================================================================

    #[test]
    fn inject_geo_partial_only_overwrites_missing_fields() {
        // Mixed case: some fields pre-existing, others not → inject only missing
        let mut data = Map::new();
        data.insert("geo_country_code".into(), json!("PRE_EXISTING"));
        // geo_city NOT set

        let result = GeoIpResult {
            country_code: Some("NEW".into()),
            city: Some("Melbourne".into()),
            latitude: Some(-37.8136),
            is_private: false,
            ..Default::default()
        };

        inject_geo(&mut data, &result);

        // Pre-existing preserved
        assert_eq!(data["geo_country_code"], json!("PRE_EXISTING"));
        // Missing fields were injected
        assert_eq!(data["geo_city"], json!("Melbourne"));
        assert_eq!(data["geo_latitude"], json!(-37.8136));
        assert_eq!(data["geo_is_private"], json!(false));
    }

    #[test]
    fn inject_reputation_does_not_overwrite_mixed_fields() {
        let mut data = Map::new();
        data.insert("rep_is_vpn".into(), json!("manual_string"));
        data.insert("rep_abuse_score".into(), json!(-999));
        // rep_is_tor NOT set

        let result = ReputationResult {
            is_vpn: true,
            is_tor: true,
            abuse_score: 50,
            ..Default::default()
        };

        inject_reputation(&mut data, &result);

        // Pre-existing values (even wrong-typed) are preserved
        assert_eq!(data["rep_is_vpn"], json!("manual_string"));
        assert_eq!(data["rep_abuse_score"], json!(-999));
        // Missing field gets injected
        assert_eq!(data["rep_is_tor"], json!(true));
    }

    #[test]
    fn inject_risk_partial_existing() {
        let mut data = Map::new();
        data.insert("risk_score".into(), json!(99)); // pre-existing
        // risk_level and risk_factors NOT set

        let output = RiskOutput {
            risk_score: 42,
            risk_level: crate::enrich::risk::RiskLevel::Low,
            risk_factors: vec!["factor_a", "factor_b"],
            ..Default::default()
        };

        inject_risk(&mut data, &output);

        // Pre-existing preserved
        assert_eq!(data["risk_score"], json!(99));
        // Missing injected
        assert_eq!(data["risk_level"], json!("low"));
        let factors = data["risk_factors"].as_array().unwrap();
        assert_eq!(factors.len(), 2);
        assert_eq!(factors[0], json!("factor_a"));
    }

    #[test]
    fn inject_geo_empty_result_only_adds_is_private() {
        // All-None GeoIpResult still sets geo_is_private (which is non-Option)
        let mut data = Map::new();
        let result = GeoIpResult::default();

        inject_geo(&mut data, &result);

        assert_eq!(data.len(), 1);
        assert_eq!(data["geo_is_private"], json!(false));
    }

    #[test]
    fn inject_reputation_every_threat_flag() {
        // Exercise every boolean flag once — ensures no missed fields
        let mut data = Map::new();
        let result = ReputationResult {
            is_vpn: true,
            is_proxy: true,
            is_tor: true,
            is_relay: true,
            is_datacenter: true,
            is_botnet: true,
            is_spam: true,
            is_scanner: true,
            is_malicious: true,
            threat_type: ThreatType::Proxy,
            abuse_score: 100,
            ..Default::default()
        };

        inject_reputation(&mut data, &result);

        // Every flag set to true
        for key in &[
            "rep_is_vpn",
            "rep_is_proxy",
            "rep_is_tor",
            "rep_is_relay",
            "rep_is_datacenter",
            "rep_is_botnet",
            "rep_is_spam",
            "rep_is_scanner",
            "rep_is_malicious",
        ] {
            assert_eq!(data[*key], json!(true), "field {key} should be true");
        }
        assert_eq!(data["rep_threat_type"], json!("proxy"));
        assert_eq!(data["rep_abuse_score"], json!(100));
    }

    // ========================================================================
    // Full pipeline: all three enrichers configured
    // ========================================================================

    #[test]
    fn enrich_all_three_components_configured() {
        // GeoIP is skipped (requires MMDB files), but reputation + risk cover
        // the combined path. This test demonstrates the full pipeline with
        // both non-None reputation and risk scorer.
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "203.0.113.100".parse().expect("valid");
        enricher.add_ip(addr, ThreatType::Botnet, ThreatSource::AbuseCh);

        let scorer = RiskScorer::from_preset(RiskPreset::HighSecurity);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into(), "client_ip".into()],
            geoip: None, // GeoIP requires MMDB files — skip in unit tests
            reputation: Some(enricher),
            risk: Some(scorer),
        };

        assert!(pipeline.is_active());

        let mut data = Map::new();
        data.insert("src_ip".into(), json!("203.0.113.100"));
        data.insert("action".into(), json!("login"));
        data.insert("user".into(), json!("alice"));

        pipeline.enrich(&mut data);

        // Reputation results injected
        assert_eq!(data["rep_is_botnet"], json!(true));
        assert_eq!(data["rep_is_malicious"], json!(true)); // Botnet → malicious
        // Risk score present
        assert!(data.contains_key("risk_score"));
        assert!(data.contains_key("risk_level"));
        let score = data["risk_score"].as_u64().unwrap();
        // Botnet contributes substantial threat points; require > 20 to confirm
        // the risk scorer was invoked and produced a non-trivial score.
        assert!(
            score > 20,
            "Botnet IP should produce non-trivial risk score, got {score}"
        );
        // Original fields preserved
        assert_eq!(data["action"], json!("login"));
        assert_eq!(data["user"], json!("alice"));
        // GeoIP wasn't configured, so no geo fields
        assert!(!data.contains_key("geo_country_code"));
    }

    #[test]
    fn enrich_risk_does_not_overwrite_manually_set_risk_score() {
        // User has pre-computed a risk score — enrichment must not clobber
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "203.0.113.200".parse().expect("valid");
        enricher.add_ip(addr, ThreatType::Malware, ThreatSource::AbuseCh);

        let scorer = RiskScorer::from_preset(RiskPreset::Global);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: Some(scorer),
        };

        let mut data = Map::new();
        data.insert("ip".into(), json!("203.0.113.200"));
        data.insert("risk_score".into(), json!(42)); // manually set
        data.insert("risk_level".into(), json!("manual"));

        pipeline.enrich(&mut data);

        // User's manual values preserved despite high-threat reputation
        assert_eq!(data["risk_score"], json!(42));
        assert_eq!(data["risk_level"], json!("manual"));
        // But reputation fields (not pre-set) are injected
        assert_eq!(data["rep_is_malicious"], json!(true));
    }

    #[test]
    fn enrich_second_ip_field_used_when_first_missing() {
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "10.10.10.10".parse().expect("valid");
        enricher.add_ip(addr, ThreatType::Vpn, ThreatSource::AbuseIpdb);

        let pipeline = EnrichmentPipeline {
            // src_ip is first, then client_ip
            ip_fields: vec!["src_ip".into(), "client_ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        // Only client_ip present
        let mut data = Map::new();
        data.insert("client_ip".into(), json!("10.10.10.10"));
        data.insert("session".into(), json!("abc123"));

        pipeline.enrich(&mut data);

        // Fallback to client_ip worked — VPN detected
        assert_eq!(data["rep_is_vpn"], json!(true));
    }

    #[test]
    fn enrich_empty_ip_fields_list_skips_enrichment() {
        // Configuration oversight: reputation enabled but no IP fields listed
        let enricher = ReputationEnricher::new();
        use std::net::IpAddr;
        let addr: IpAddr = "1.2.3.4".parse().expect("valid");
        enricher.add_ip(addr, ThreatType::Scanner, ThreatSource::Custom);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec![], // empty
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        let mut data = Map::new();
        data.insert("src_ip".into(), json!("1.2.3.4"));

        pipeline.enrich(&mut data);

        // No IP fields configured → no enrichment occurred
        assert!(!data.contains_key("rep_is_scanner"));
        assert_eq!(data.len(), 1, "Data should be untouched");
    }

    #[test]
    fn enrich_risk_only_without_geo_or_rep_skips_scoring() {
        // Risk scorer alone can't produce meaningful output without GeoIP or reputation.
        // Implementation: risk only runs when geo_result.is_some() || rep_result.is_some().
        let scorer = RiskScorer::from_preset(RiskPreset::Global);
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["ip".into()],
            geoip: None,
            reputation: None,
            risk: Some(scorer),
        };

        let mut data = Map::new();
        data.insert("ip".into(), json!("8.8.8.8"));

        pipeline.enrich(&mut data);

        // No geo or rep data → risk scoring is skipped
        assert!(!data.contains_key("risk_score"));
        assert!(!data.contains_key("risk_level"));
    }

    // ========================================================================
    // EnrichmentPipeline::init async constructor paths
    // ========================================================================

    #[tokio::test]
    async fn init_all_disabled_produces_inactive_pipeline() {
        // All enrichment components disabled → init should yield an inactive pipeline.
        let mut config = crate::config::Config::default();
        config.geoip.enabled = false;
        config.enrichment.reputation.enabled = false;
        config.enrichment.risk_scoring.enabled = false;

        let pipeline = EnrichmentPipeline::init(&config).await;
        assert!(!pipeline.is_active());
        assert!(pipeline.geoip.is_none());
        assert!(pipeline.reputation.is_none());
        assert!(pipeline.risk.is_none());
    }

    #[tokio::test]
    async fn init_reputation_enabled_no_blocklists_still_active() {
        let mut config = crate::config::Config::default();
        config.geoip.enabled = false;
        config.enrichment.reputation.enabled = true;
        config.enrichment.reputation.blocklist_files = Vec::new();
        config.enrichment.risk_scoring.enabled = false;

        let pipeline = EnrichmentPipeline::init(&config).await;
        // Reputation without blocklists is still considered active (ready to add IPs)
        assert!(pipeline.reputation.is_some(), "reputation should be Some");
        assert!(pipeline.is_active());
    }

    #[tokio::test]
    async fn init_reputation_with_nonexistent_blocklist_warns_but_continues() {
        let mut config = crate::config::Config::default();
        config.geoip.enabled = false;
        config.enrichment.reputation.enabled = true;
        config.enrichment.reputation.blocklist_files =
            vec!["/nonexistent/path/blocklist.txt".to_string()];
        config.enrichment.risk_scoring.enabled = false;

        let pipeline = EnrichmentPipeline::init(&config).await;
        // Bad path is warned + skipped. If no other blocklists loaded and
        // blocklist_files was non-empty, reputation becomes None.
        // Based on impl: if loaded=0 and blocklist_files non-empty, reputation=None
        assert!(
            pipeline.reputation.is_none(),
            "reputation should be None when all blocklists failed to load"
        );
    }

    #[tokio::test]
    async fn init_reputation_with_valid_blocklist() {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(
            tmp.path(),
            "192.0.2.1\n203.0.113.5\n# comment\n198.51.100.1\n",
        )
        .expect("write");

        let mut config = crate::config::Config::default();
        config.geoip.enabled = false;
        config.enrichment.reputation.enabled = true;
        config.enrichment.reputation.blocklist_files =
            vec![tmp.path().to_string_lossy().into_owned()];
        config.enrichment.risk_scoring.enabled = false;

        let pipeline = EnrichmentPipeline::init(&config).await;
        // Blocklist loaded successfully → reputation is active
        assert!(pipeline.reputation.is_some());
        assert!(pipeline.is_active());
    }

    #[tokio::test]
    async fn init_risk_scoring_each_preset() {
        // Exercise every preset branch
        for preset_str in &[
            "us_enterprise",
            "eu_enterprise",
            "apac_enterprise",
            "high_security",
            "global",
            "unknown_preset_falls_to_global",
        ] {
            let mut config = crate::config::Config::default();
            config.geoip.enabled = false;
            config.enrichment.reputation.enabled = false;
            config.enrichment.risk_scoring.enabled = true;
            config.enrichment.risk_scoring.preset = preset_str.to_string();

            let pipeline = EnrichmentPipeline::init(&config).await;
            assert!(
                pipeline.risk.is_some(),
                "risk should be Some for preset '{preset_str}'"
            );
            assert!(pipeline.is_active());
        }
    }

    #[tokio::test]
    async fn init_all_components_combined() {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(tmp.path(), "192.0.2.1\n").expect("write");

        let mut config = crate::config::Config::default();
        // GeoIP: enabled but no MMDB → no enricher
        config.geoip.enabled = true;
        config.geoip.provider = crate::config::GeoIpProvider::Custom;
        // Ensure no download attempt
        config.geoip.auto_download.enabled = false;

        config.enrichment.reputation.enabled = true;
        config.enrichment.reputation.blocklist_files =
            vec![tmp.path().to_string_lossy().into_owned()];

        config.enrichment.risk_scoring.enabled = true;
        config.enrichment.risk_scoring.preset = "global".to_string();

        let pipeline = EnrichmentPipeline::init(&config).await;
        // GeoIP probably None (no MMDB). Reputation and risk should be active.
        assert!(pipeline.reputation.is_some());
        assert!(pipeline.risk.is_some());
        assert!(pipeline.is_active());
    }

    #[tokio::test]
    async fn init_geoip_disabled_no_enricher() {
        let mut config = crate::config::Config::default();
        config.geoip.enabled = false;
        let pipeline = EnrichmentPipeline::init(&config).await;
        assert!(pipeline.geoip.is_none());
    }

    // ========================================================================
    // extract_enrich_ip: whitespace IPs and numeric fields
    // ========================================================================

    #[test]
    fn extract_ip_with_only_invalid_fields() {
        let mut data = Map::new();
        data.insert("src".into(), json!(42));
        data.insert("dst".into(), serde_json::Value::Null);
        data.insert("other".into(), json!({"nested": "object"}));

        let fields = vec!["src".into(), "dst".into(), "other".into()];
        assert!(extract_enrich_ip(&data, &fields).is_none());
    }

    #[test]
    fn extract_ip_single_field_match() {
        let mut data = Map::new();
        data.insert("ip".into(), json!("127.0.0.1"));

        let fields = vec!["ip".to_string()];
        assert_eq!(extract_enrich_ip(&data, &fields), Some("127.0.0.1"));
    }

    // ========================================================================
    // Full pipeline: reputation handles empty IP gracefully
    // ========================================================================

    #[test]
    fn enrich_empty_data_map_is_noop() {
        let scorer = RiskScorer::from_preset(RiskPreset::Global);
        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: None,
            risk: Some(scorer),
        };

        let mut data = Map::new();
        pipeline.enrich(&mut data);
        // No data → no enrichment; data remains empty
        assert!(data.is_empty());
    }

    #[test]
    fn enrich_with_additional_non_ip_fields_preserves_them() {
        use crate::enrich::reputation::{ReputationEnricher, ThreatSource, ThreatType};
        use std::net::IpAddr;

        let enricher = ReputationEnricher::new();
        let addr: IpAddr = "203.0.113.5".parse().expect("ip");
        enricher.add_ip(addr, ThreatType::Proxy, ThreatSource::AbuseIpdb);

        let pipeline = EnrichmentPipeline {
            ip_fields: vec!["src_ip".into()],
            geoip: None,
            reputation: Some(enricher),
            risk: None,
        };

        let mut data = Map::new();
        data.insert("src_ip".into(), json!("203.0.113.5"));
        data.insert("timestamp".into(), json!("2024-01-01"));
        data.insert("event".into(), json!("login"));
        data.insert("user".into(), json!("alice"));

        pipeline.enrich(&mut data);

        // Original fields preserved
        assert_eq!(data["src_ip"], json!("203.0.113.5"));
        assert_eq!(data["timestamp"], json!("2024-01-01"));
        assert_eq!(data["event"], json!("login"));
        assert_eq!(data["user"], json!("alice"));
        // Reputation data added
        assert_eq!(data["rep_is_proxy"], json!(true));
    }
}
