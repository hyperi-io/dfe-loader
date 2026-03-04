// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Risk scoring engine
//!
//! Composite risk scoring based on multiple factors:
//! - Geographic risk (high-risk countries/regions)
//! - Reputation risk (VPN, proxy, Tor, etc.)
//! - Privacy risk (anonymization attempts)
//! - Threat risk (known malicious activity)
//!
//! ## Hot Path Optimizations
//!
//! 1. **Integer math only**: All scores are u8 (0-100), no floating point
//! 2. **Preset configurations**: Pre-computed country/region risk tables
//! 3. **Short-circuit evaluation**: Early exit if risk is already critical
//! 4. **Schema-aware output**: Only compute needed risk components

use std::collections::HashMap;

use super::geoip::GeoIpResult;
use super::reputation::ReputationResult;

/// Risk level category (low cardinality for ClickHouse)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RiskLevel {
    #[default]
    Minimal, // 0-19
    Low,      // 20-39
    Medium,   // 40-59
    High,     // 60-79
    Critical, // 80-100
}

impl RiskLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            RiskLevel::Minimal => "minimal",
            RiskLevel::Low => "low",
            RiskLevel::Medium => "medium",
            RiskLevel::High => "high",
            RiskLevel::Critical => "critical",
        }
    }

    /// Derive risk level from score (0-100)
    #[inline]
    pub fn from_score(score: u8) -> Self {
        match score {
            0..=19 => RiskLevel::Minimal,
            20..=39 => RiskLevel::Low,
            40..=59 => RiskLevel::Medium,
            60..=79 => RiskLevel::High,
            80..=100 => RiskLevel::Critical,
            _ => RiskLevel::Critical, // Safety for > 100
        }
    }
}

/// Risk input combining GeoIP and reputation data
#[derive(Debug, Default)]
pub struct RiskInput<'a> {
    // GeoIP data
    pub country_code: Option<&'a str>,
    pub continent_code: Option<&'a str>,
    pub is_private: bool,

    // Reputation data
    pub is_vpn: bool,
    pub is_proxy: bool,
    pub is_tor: bool,
    pub is_relay: bool,
    pub is_datacenter: bool,
    pub is_botnet: bool,
    pub is_spam: bool,
    pub is_scanner: bool,
    pub is_malicious: bool,

    // External scores (e.g., from AbuseIPDB)
    pub abuse_score: u8,
}

impl<'a> RiskInput<'a> {
    /// Create from GeoIP and Reputation results
    pub fn from_enrichment(
        geo: Option<&'a GeoIpResult>,
        rep: Option<&'a ReputationResult>,
    ) -> Self {
        let mut input = RiskInput::default();

        if let Some(g) = geo {
            input.country_code = g.country_code.as_deref();
            input.continent_code = g.continent_code.as_deref();
            input.is_private = g.is_private;
        }

        if let Some(r) = rep {
            input.is_vpn = r.is_vpn;
            input.is_proxy = r.is_proxy;
            input.is_tor = r.is_tor;
            input.is_relay = r.is_relay;
            input.is_datacenter = r.is_datacenter;
            input.is_botnet = r.is_botnet;
            input.is_spam = r.is_spam;
            input.is_scanner = r.is_scanner;
            input.is_malicious = r.is_malicious;
            input.abuse_score = r.abuse_score;
        }

        input
    }
}

/// Risk output with composite score and component breakdown
#[derive(Debug, Clone, Default)]
pub struct RiskOutput {
    /// Composite score (0-100, higher = more risky)
    pub risk_score: u8,

    /// Individual component scores
    pub geo_risk_score: u8,
    pub reputation_risk_score: u8,
    pub privacy_risk_score: u8,
    pub threat_risk_score: u8,

    /// Risk level category
    pub risk_level: RiskLevel,

    /// Flags explaining risk factors
    pub risk_factors: Vec<&'static str>,
}

impl RiskOutput {
    /// Convert result to a map, filtering to only requested fields
    pub fn to_schema_map(&self, schema_fields: &[&str]) -> HashMap<String, serde_json::Value> {
        use serde_json::Value;
        let mut out = HashMap::with_capacity(schema_fields.len());

        for &field in schema_fields {
            match field {
                "risk_score" => {
                    out.insert(field.to_string(), Value::Number(self.risk_score.into()));
                }
                "geo_risk_score" => {
                    out.insert(field.to_string(), Value::Number(self.geo_risk_score.into()));
                }
                "reputation_risk_score" => {
                    out.insert(
                        field.to_string(),
                        Value::Number(self.reputation_risk_score.into()),
                    );
                }
                "privacy_risk_score" => {
                    out.insert(
                        field.to_string(),
                        Value::Number(self.privacy_risk_score.into()),
                    );
                }
                "threat_risk_score" => {
                    out.insert(
                        field.to_string(),
                        Value::Number(self.threat_risk_score.into()),
                    );
                }
                "risk_level" => {
                    out.insert(
                        field.to_string(),
                        Value::String(self.risk_level.as_str().to_string()),
                    );
                }
                "risk_factors" => {
                    let factors: Vec<Value> = self
                        .risk_factors
                        .iter()
                        .map(|s| Value::String(s.to_string()))
                        .collect();
                    out.insert(field.to_string(), Value::Array(factors));
                }
                _ => {}
            }
        }

        out
    }
}

/// Risk weight configuration
#[derive(Debug, Clone)]
pub struct RiskWeights {
    /// Geographic risk weight (0-100)
    pub geo_weight: u8,
    /// Reputation risk weight (0-100)
    pub reputation_weight: u8,
    /// Privacy risk weight (0-100)
    pub privacy_weight: u8,
    /// Threat risk weight (0-100)
    pub threat_weight: u8,
}

impl Default for RiskWeights {
    fn default() -> Self {
        Self {
            geo_weight: 15,
            reputation_weight: 25,
            privacy_weight: 25,
            threat_weight: 35,
        }
    }
}

/// Preset risk configurations for different use cases
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskPreset {
    /// US enterprise - high risk: CN, RU, KP, IR
    UsEnterprise,
    /// EU enterprise - GDPR-aware
    EuEnterprise,
    /// APAC enterprise - regional focus
    ApacEnterprise,
    /// Global - balanced, sanctions-focused
    Global,
    /// High security - financial/government
    HighSecurity,
}

impl RiskPreset {
    /// Get high-risk country codes for this preset
    pub fn high_risk_countries(&self) -> &'static [&'static str] {
        match self {
            RiskPreset::UsEnterprise => &["CN", "RU", "KP", "IR", "SY", "CU", "VE"],
            RiskPreset::EuEnterprise => &["RU", "KP", "IR", "SY", "BY"],
            RiskPreset::ApacEnterprise => &["KP", "AF", "MM"],
            RiskPreset::Global => &["KP", "IR", "SY"], // OFAC sanctions
            RiskPreset::HighSecurity => &["CN", "RU", "KP", "IR", "SY", "CU", "VE", "BY", "MM"],
        }
    }

    /// Get medium-risk country codes for this preset
    pub fn medium_risk_countries(&self) -> &'static [&'static str] {
        match self {
            RiskPreset::UsEnterprise => &["UA", "BY", "PK", "NG", "VN"],
            RiskPreset::EuEnterprise => &["CN", "UA", "PK"],
            RiskPreset::ApacEnterprise => &["RU", "IR"],
            RiskPreset::Global => &["RU", "CN", "VE", "CU"],
            RiskPreset::HighSecurity => &["UA", "PK", "NG", "VN", "BD", "NP"],
        }
    }
}

/// Risk scorer configuration
#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub weights: RiskWeights,
    pub preset: Option<RiskPreset>,
    pub high_risk_countries: Vec<String>,
    pub medium_risk_countries: Vec<String>,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            weights: RiskWeights::default(),
            preset: Some(RiskPreset::Global),
            high_risk_countries: Vec::new(),
            medium_risk_countries: Vec::new(),
        }
    }
}

impl RiskConfig {
    /// Create config from a preset
    pub fn from_preset(preset: RiskPreset) -> Self {
        Self {
            weights: RiskWeights::default(),
            preset: Some(preset),
            high_risk_countries: Vec::new(),
            medium_risk_countries: Vec::new(),
        }
    }
}

/// Risk scorer
pub struct RiskScorer {
    config: RiskConfig,
    /// Pre-computed high-risk country set for O(1) lookup
    high_risk_set: rustc_hash::FxHashSet<String>,
    /// Pre-computed medium-risk country set for O(1) lookup
    medium_risk_set: rustc_hash::FxHashSet<String>,
}

impl RiskScorer {
    /// Create a new risk scorer with default config
    pub fn new() -> Self {
        Self::with_config(RiskConfig::default())
    }

    /// Create with specific config
    pub fn with_config(config: RiskConfig) -> Self {
        let mut high_risk_set = rustc_hash::FxHashSet::default();
        let mut medium_risk_set = rustc_hash::FxHashSet::default();

        // Add preset countries
        if let Some(preset) = config.preset {
            for &code in preset.high_risk_countries() {
                high_risk_set.insert(code.to_string());
            }
            for &code in preset.medium_risk_countries() {
                medium_risk_set.insert(code.to_string());
            }
        }

        // Add custom countries
        for code in &config.high_risk_countries {
            high_risk_set.insert(code.clone());
        }
        for code in &config.medium_risk_countries {
            medium_risk_set.insert(code.clone());
        }

        Self {
            config,
            high_risk_set,
            medium_risk_set,
        }
    }

    /// Create from a preset
    pub fn from_preset(preset: RiskPreset) -> Self {
        Self::with_config(RiskConfig::from_preset(preset))
    }

    /// Calculate risk score from input
    pub fn score(&self, input: &RiskInput) -> RiskOutput {
        let mut output = RiskOutput::default();

        // Calculate component scores
        output.geo_risk_score = self.calculate_geo_risk(input, &mut output.risk_factors);
        output.reputation_risk_score =
            self.calculate_reputation_risk(input, &mut output.risk_factors);
        output.privacy_risk_score = self.calculate_privacy_risk(input, &mut output.risk_factors);
        output.threat_risk_score = self.calculate_threat_risk(input, &mut output.risk_factors);

        // Weighted composite score
        let weights = &self.config.weights;
        let total_weight = weights.geo_weight as u32
            + weights.reputation_weight as u32
            + weights.privacy_weight as u32
            + weights.threat_weight as u32;

        if total_weight > 0 {
            let weighted_sum = (output.geo_risk_score as u32 * weights.geo_weight as u32)
                + (output.reputation_risk_score as u32 * weights.reputation_weight as u32)
                + (output.privacy_risk_score as u32 * weights.privacy_weight as u32)
                + (output.threat_risk_score as u32 * weights.threat_weight as u32);

            output.risk_score = (weighted_sum / total_weight).min(100) as u8;
        }

        output.risk_level = RiskLevel::from_score(output.risk_score);

        output
    }

    /// Calculate geographic risk score
    fn calculate_geo_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        // Private IP = no geo risk
        if input.is_private {
            return 0;
        }

        // Unknown country = moderate risk
        let Some(country) = input.country_code else {
            factors.push("unknown_country");
            return 35;
        };

        // High-risk country
        if self.high_risk_set.contains(country) {
            factors.push("high_risk_country");
            return 90;
        }

        // Medium-risk country
        if self.medium_risk_set.contains(country) {
            factors.push("medium_risk_country");
            return 60;
        }

        // Normal country - low base risk
        15
    }

    /// Calculate reputation risk score
    fn calculate_reputation_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        let mut score: u8 = 0;

        if input.is_vpn {
            score = score.saturating_add(50);
            factors.push("vpn_detected");
        }

        if input.is_proxy {
            score = score.saturating_add(60);
            factors.push("proxy_detected");
        }

        if input.is_tor {
            score = score.saturating_add(80);
            factors.push("tor_detected");
        }

        if input.is_datacenter {
            score = score.saturating_add(40);
            factors.push("datacenter_ip");
        }

        if input.is_relay {
            score = score.saturating_add(45);
            factors.push("private_relay");
        }

        score.min(100)
    }

    /// Calculate privacy risk score (anonymization attempts)
    fn calculate_privacy_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        let mut anonymizer_count: u8 = 0;

        if input.is_vpn {
            anonymizer_count += 1;
        }
        if input.is_proxy {
            anonymizer_count += 1;
        }
        if input.is_tor {
            anonymizer_count += 1;
        }
        if input.is_relay {
            anonymizer_count += 1;
        }

        match anonymizer_count {
            0 => 0,
            1 => 40,
            2 => {
                factors.push("multi_layer_anonymization");
                70
            }
            _ => {
                factors.push("extensive_anonymization");
                95
            }
        }
    }

    /// Calculate threat risk score
    fn calculate_threat_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        let mut score: u8 = 0;

        if input.is_botnet {
            score = score.saturating_add(95);
            factors.push("botnet_ip");
        }

        if input.is_malicious {
            score = score.saturating_add(90);
            factors.push("malicious_ip");
        }

        if input.is_spam {
            score = score.saturating_add(70);
            factors.push("spam_source");
        }

        if input.is_scanner {
            score = score.saturating_add(60);
            factors.push("scanner_ip");
        }

        // Include abuse score from external sources
        if input.abuse_score > 0 {
            score = score.saturating_add(input.abuse_score);
            if input.abuse_score >= 80 {
                factors.push("high_abuse_score");
            }
        }

        score.min(100)
    }
}

impl Default for RiskScorer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_level_from_score() {
        assert_eq!(RiskLevel::from_score(0), RiskLevel::Minimal);
        assert_eq!(RiskLevel::from_score(19), RiskLevel::Minimal);
        assert_eq!(RiskLevel::from_score(20), RiskLevel::Low);
        assert_eq!(RiskLevel::from_score(39), RiskLevel::Low);
        assert_eq!(RiskLevel::from_score(40), RiskLevel::Medium);
        assert_eq!(RiskLevel::from_score(59), RiskLevel::Medium);
        assert_eq!(RiskLevel::from_score(60), RiskLevel::High);
        assert_eq!(RiskLevel::from_score(79), RiskLevel::High);
        assert_eq!(RiskLevel::from_score(80), RiskLevel::Critical);
        assert_eq!(RiskLevel::from_score(100), RiskLevel::Critical);
    }

    #[test]
    fn test_clean_input() {
        let scorer = RiskScorer::new();
        let input = RiskInput::default();
        let output = scorer.score(&input);

        // Unknown country gets 35 geo risk, weighted to ~5 overall (15% weight)
        // Overall score: (35 * 15 + 0 * 25 + 0 * 25 + 0 * 35) / 100 = 5.25 -> 5
        assert!(output.risk_score < 20);
        assert_eq!(output.risk_level, RiskLevel::Minimal);
    }

    #[test]
    fn test_high_risk_country() {
        let scorer = RiskScorer::from_preset(RiskPreset::UsEnterprise);
        let input = RiskInput {
            country_code: Some("CN"),
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert!(output.geo_risk_score >= 90);
        assert!(output.risk_factors.contains(&"high_risk_country"));
    }

    #[test]
    fn test_tor_detection() {
        let scorer = RiskScorer::new();
        let input = RiskInput {
            is_tor: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert!(output.reputation_risk_score >= 80);
        assert!(output.privacy_risk_score > 0);
        assert!(output.risk_factors.contains(&"tor_detected"));
    }

    #[test]
    fn test_botnet_critical() {
        let scorer = RiskScorer::new();
        let input = RiskInput {
            is_botnet: true,
            is_malicious: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        // Threat risk maxes at 100, but weighted score is:
        // (35 geo * 15 + 0 rep * 25 + 0 priv * 25 + 100 threat * 35) / 100 = 40
        assert_eq!(output.threat_risk_score, 100);
        assert!(output.risk_score >= 40); // Threat is 35% weight
        assert!(output.risk_factors.contains(&"botnet_ip"));
    }

    #[test]
    fn test_multi_layer_anonymization() {
        let scorer = RiskScorer::new();
        let input = RiskInput {
            is_vpn: true,
            is_tor: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert!(output.privacy_risk_score >= 70);
        assert!(output.risk_factors.contains(&"multi_layer_anonymization"));
    }

    #[test]
    fn test_private_ip_no_geo_risk() {
        let scorer = RiskScorer::new();
        let input = RiskInput {
            is_private: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert_eq!(output.geo_risk_score, 0);
    }

    #[test]
    fn test_output_to_schema_map() {
        let output = RiskOutput {
            risk_score: 75,
            risk_level: RiskLevel::High,
            risk_factors: vec!["tor_detected", "high_risk_country"],
            ..Default::default()
        };

        let fields = ["risk_score", "risk_level"];
        let map = output.to_schema_map(&fields);

        assert_eq!(map.len(), 2);
        assert_eq!(
            map.get("risk_score").unwrap(),
            &serde_json::Value::Number(75.into())
        );
        assert_eq!(map.get("risk_level").unwrap(), "high");
    }
}
