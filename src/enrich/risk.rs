// SPDX-License-Identifier: BUSL-1.1
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
//! 2. **Pre-computed country sets**: the configured preset's lists, built once
//! 3. **Schema-aware output**: Only compute needed risk components
//!
//! Every country list, weight, score and level band comes from
//! [`RiskScoringConfig`].

use std::collections::HashMap;

use super::geoip::GeoIpResult;
use super::reputation::ReputationResult;
use crate::config::{RiskLevelBands, RiskScores, RiskScoringConfig, RiskWeights};

/// Risk level category (low cardinality for `ClickHouse`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RiskLevel {
    #[default]
    Minimal,
    Low,
    Medium,
    High,
    Critical,
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

    /// The level `score` falls in under `bands`.
    #[inline]
    #[must_use]
    pub fn from_score(score: u8, bands: &RiskLevelBands) -> Self {
        if score >= bands.critical {
            RiskLevel::Critical
        } else if score >= bands.high {
            RiskLevel::High
        } else if score >= bands.medium {
            RiskLevel::Medium
        } else if score >= bands.low {
            RiskLevel::Low
        } else {
            RiskLevel::Minimal
        }
    }
}

/// Risk input combining `GeoIP` and reputation data
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
    /// Create from `GeoIP` and Reputation results
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

/// Risk scorer
pub struct RiskScorer {
    weights: RiskWeights,
    scores: RiskScores,
    levels: RiskLevelBands,
    /// The preset's high-risk countries, for O(1) lookup
    high_risk_set: rustc_hash::FxHashSet<String>,
    /// The preset's medium-risk countries, for O(1) lookup
    medium_risk_set: rustc_hash::FxHashSet<String>,
}

impl RiskScorer {
    /// A scorer for `config`. A preset `config.presets` does not hold scores
    /// every country as `other_country`. [`RiskScoringConfig::validate`]
    /// refuses one at load.
    #[must_use]
    pub fn new(config: &RiskScoringConfig) -> Self {
        let countries = config.countries();
        let set = |codes: Option<&Vec<String>>| -> rustc_hash::FxHashSet<String> {
            codes.into_iter().flatten().cloned().collect()
        };
        Self {
            weights: config.weights.clone(),
            scores: config.scores.clone(),
            levels: config.levels.clone(),
            high_risk_set: set(countries.map(|c| &c.high)),
            medium_risk_set: set(countries.map(|c| &c.medium)),
        }
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
        let weights = &self.weights;
        let total_weight = weights.geo as u32
            + weights.reputation as u32
            + weights.privacy as u32
            + weights.threat as u32;

        let weighted_sum = (output.geo_risk_score as u32 * weights.geo as u32)
            + (output.reputation_risk_score as u32 * weights.reputation as u32)
            + (output.privacy_risk_score as u32 * weights.privacy as u32)
            + (output.threat_risk_score as u32 * weights.threat as u32);

        if let Some(divided) = weighted_sum.checked_div(total_weight) {
            output.risk_score = divided.min(100) as u8;
        }

        output.risk_level = RiskLevel::from_score(output.risk_score, &self.levels);

        output
    }

    /// Calculate geographic risk score
    fn calculate_geo_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        // Private IP = no geo risk
        if input.is_private {
            return 0;
        }

        let Some(country) = input.country_code else {
            factors.push("unknown_country");
            return self.scores.unknown_country;
        };

        if self.high_risk_set.contains(country) {
            factors.push("high_risk_country");
            return self.scores.high_risk_country;
        }

        if self.medium_risk_set.contains(country) {
            factors.push("medium_risk_country");
            return self.scores.medium_risk_country;
        }

        self.scores.other_country
    }

    /// Calculate reputation risk score
    fn calculate_reputation_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        let s = &self.scores;
        let signals = [
            (input.is_vpn, s.vpn, "vpn_detected"),
            (input.is_proxy, s.proxy, "proxy_detected"),
            (input.is_tor, s.tor, "tor_detected"),
            (input.is_datacenter, s.datacenter, "datacenter_ip"),
            (input.is_relay, s.relay, "private_relay"),
        ];
        add_signals(&signals, factors)
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
            1 => self.scores.one_anonymiser,
            2 => {
                factors.push("multi_layer_anonymization");
                self.scores.two_anonymisers
            }
            _ => {
                factors.push("extensive_anonymization");
                self.scores.three_anonymisers
            }
        }
    }

    /// Calculate threat risk score
    fn calculate_threat_risk(&self, input: &RiskInput, factors: &mut Vec<&'static str>) -> u8 {
        let s = &self.scores;
        let signals = [
            (input.is_botnet, s.botnet, "botnet_ip"),
            (input.is_malicious, s.malicious, "malicious_ip"),
            (input.is_spam, s.spam, "spam_source"),
            (input.is_scanner, s.scanner, "scanner_ip"),
        ];
        let score = add_signals(&signals, factors);

        // Include abuse score from external sources
        if input.abuse_score > 0 && input.abuse_score >= s.high_abuse_score {
            factors.push("high_abuse_score");
        }
        score.saturating_add(input.abuse_score).min(100)
    }
}

/// The sum of the scores of the signals that are set, capped at 100, with each
/// one's factor pushed.
fn add_signals(signals: &[(bool, u8, &'static str)], factors: &mut Vec<&'static str>) -> u8 {
    let mut score: u8 = 0;
    for &(set, points, factor) in signals {
        if set {
            score = score.saturating_add(points);
            factors.push(factor);
        }
    }
    score.min(100)
}

impl Default for RiskScorer {
    /// A scorer for the shipped default config.
    fn default() -> Self {
        Self::new(&RiskScoringConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scorer for the shipped preset `name`.
    fn scorer_for(name: &str) -> RiskScorer {
        RiskScorer::new(&RiskScoringConfig {
            preset: name.to_string(),
            ..RiskScoringConfig::default()
        })
    }

    /// A scorer for the default config with `weights`.
    fn weighted(geo: u8, reputation: u8, privacy: u8, threat: u8) -> RiskScorer {
        RiskScorer::new(&RiskScoringConfig {
            weights: RiskWeights {
                geo,
                reputation,
                privacy,
                threat,
            },
            ..RiskScoringConfig::default()
        })
    }

    #[test]
    fn test_clean_input() {
        let scorer = RiskScorer::default();
        let input = RiskInput::default();
        let output = scorer.score(&input);

        // Unknown country gets 35 geo risk, weighted to ~5 overall (15% weight)
        // Overall score: (35 * 15 + 0 * 25 + 0 * 25 + 0 * 35) / 100 = 5.25 -> 5
        assert!(output.risk_score < 20);
        assert_eq!(output.risk_level, RiskLevel::Minimal);
    }

    #[test]
    fn test_high_risk_country() {
        let scorer = scorer_for("us_enterprise");
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
        let scorer = RiskScorer::default();
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
        let scorer = RiskScorer::default();
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
        let scorer = RiskScorer::default();
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
        let scorer = RiskScorer::default();
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

    // ========================================================================
    // All presets: score a known IP through each
    // ========================================================================

    #[test]
    fn test_preset_us_enterprise_high_risk_countries() {
        let scorer = scorer_for("us_enterprise");

        // CN is high-risk for UsEnterprise
        let input = RiskInput {
            country_code: Some("CN"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(output.geo_risk_score >= 90);
        assert!(output.risk_factors.contains(&"high_risk_country"));

        // UA is medium-risk for UsEnterprise
        let input = RiskInput {
            country_code: Some("UA"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 60);
        assert!(output.risk_factors.contains(&"medium_risk_country"));

        // AU is normal — low base risk
        let input = RiskInput {
            country_code: Some("AU"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 15);
    }

    #[test]
    fn test_preset_eu_enterprise() {
        let scorer = scorer_for("eu_enterprise");

        // RU is high-risk for EuEnterprise
        let input = RiskInput {
            country_code: Some("RU"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(output.geo_risk_score >= 90);

        // CN is medium-risk for EuEnterprise (not high)
        let input = RiskInput {
            country_code: Some("CN"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 60);
    }

    #[test]
    fn test_preset_apac_enterprise() {
        let scorer = scorer_for("apac_enterprise");

        // KP is high-risk for APAC
        let input = RiskInput {
            country_code: Some("KP"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(output.geo_risk_score >= 90);

        // RU is medium-risk for APAC
        let input = RiskInput {
            country_code: Some("RU"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 60);

        // CN is neither high nor medium in APAC
        let input = RiskInput {
            country_code: Some("CN"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 15);
    }

    #[test]
    fn test_preset_global() {
        let scorer = scorer_for("global");

        // KP, IR, SY are high-risk (OFAC sanctions)
        for code in &["KP", "IR", "SY"] {
            let input = RiskInput {
                country_code: Some(code),
                ..Default::default()
            };
            let output = scorer.score(&input);
            assert!(
                output.geo_risk_score >= 90,
                "{code} should be high-risk in Global preset"
            );
        }

        // RU is medium in Global
        let input = RiskInput {
            country_code: Some("RU"),
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 60);
    }

    #[test]
    fn test_preset_high_security() {
        let scorer = scorer_for("high_security");

        // HighSecurity has the largest high-risk set
        for code in &["CN", "RU", "KP", "IR", "SY", "CU", "VE", "BY", "MM"] {
            let input = RiskInput {
                country_code: Some(code),
                ..Default::default()
            };
            let output = scorer.score(&input);
            assert!(
                output.geo_risk_score >= 90,
                "{code} should be high-risk in HighSecurity preset"
            );
        }

        // Medium risk countries
        for code in &["UA", "PK", "NG"] {
            let input = RiskInput {
                country_code: Some(code),
                ..Default::default()
            };
            let output = scorer.score(&input);
            assert_eq!(
                output.geo_risk_score, 60,
                "{code} should be medium-risk in HighSecurity preset"
            );
        }
    }

    // ========================================================================
    // Component score saturation
    // ========================================================================

    #[test]
    fn test_reputation_risk_saturates_at_100() {
        let scorer = RiskScorer::default();
        // VPN (50) + Proxy (60) + Tor (80) + Datacenter (40) + Relay (45) = 275, capped at 100
        let input = RiskInput {
            is_vpn: true,
            is_proxy: true,
            is_tor: true,
            is_datacenter: true,
            is_relay: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(
            output.reputation_risk_score, 100,
            "Reputation score should cap at 100"
        );
    }

    #[test]
    fn test_threat_risk_saturates_at_100() {
        let scorer = RiskScorer::default();
        // Botnet (95) + Malicious (90) + Spam (70) + Scanner (60) = 315, capped at 100
        let input = RiskInput {
            is_botnet: true,
            is_malicious: true,
            is_spam: true,
            is_scanner: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(
            output.threat_risk_score, 100,
            "Threat score should cap at 100"
        );
    }

    #[test]
    fn test_threat_risk_with_high_abuse_score() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            abuse_score: 95,
            is_botnet: true, // 95 + 95 + 90 (malicious implied by botnet? no) = 95 + 95 = 190 -> cap 100
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.threat_risk_score, 100);
        assert!(output.risk_factors.contains(&"high_abuse_score"));
        assert!(output.risk_factors.contains(&"botnet_ip"));
    }

    // ========================================================================
    // Zero-score inputs
    // ========================================================================

    #[test]
    fn test_completely_clean_private_ip() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            country_code: Some("AU"),
            is_private: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert_eq!(
            output.geo_risk_score, 0,
            "Private IP should have 0 geo risk"
        );
        assert_eq!(output.reputation_risk_score, 0);
        assert_eq!(output.privacy_risk_score, 0);
        assert_eq!(output.threat_risk_score, 0);
        assert_eq!(output.risk_score, 0);
        assert_eq!(output.risk_level, RiskLevel::Minimal);
        assert_eq!(output.risk_factors, [] as [&str; 0]);
    }

    #[test]
    fn test_clean_known_country_minimal_risk() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            country_code: Some("US"),
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert_eq!(output.geo_risk_score, 15);
        assert_eq!(output.reputation_risk_score, 0);
        assert_eq!(output.privacy_risk_score, 0);
        assert_eq!(output.threat_risk_score, 0);
        // Weighted: (15 * 15 + 0 + 0 + 0) / 100 = 2.25 -> 2
        assert!(output.risk_score < 20);
        assert_eq!(output.risk_level, RiskLevel::Minimal);
    }

    // ========================================================================
    // All risk levels
    // ========================================================================

    #[test]
    fn test_risk_level_minimal() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            country_code: Some("AU"),
            is_private: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.risk_level, RiskLevel::Minimal);
        assert_eq!(output.risk_level.as_str(), "minimal");
    }

    #[test]
    fn test_risk_level_low() {
        let scorer = RiskScorer::default();
        // Unknown country (35 geo) + single VPN (50 rep, 40 priv)
        // Weighted: (35*15 + 50*25 + 40*25 + 0*35) / 100 = (525 + 1250 + 1000) / 100 = 27.75 -> 27
        let input = RiskInput {
            is_vpn: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(
            output.risk_score >= 20 && output.risk_score < 40,
            "Expected Low range (20-39), got: {}",
            output.risk_score
        );
        assert_eq!(output.risk_level, RiskLevel::Low);
    }

    #[test]
    fn test_risk_level_medium() {
        let scorer = scorer_for("us_enterprise");
        // Weighted: (60 geo * 15 + 90 rep * 25 + 40 priv * 25 + 0 threat * 35) / 100
        // = (900 + 2250 + 1000) / 100 = 41 -> Medium
        let input = RiskInput {
            country_code: Some("UA"), // medium risk = 60 geo
            is_vpn: true,             // 50 rep, 40 priv
            is_datacenter: true,      // +40 rep = 90 rep
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(
            output.risk_score >= 40 && output.risk_score < 60,
            "Expected Medium range (40-59), got: {} (geo={}, rep={}, priv={}, threat={})",
            output.risk_score,
            output.geo_risk_score,
            output.reputation_risk_score,
            output.privacy_risk_score,
            output.threat_risk_score
        );
        assert_eq!(output.risk_level, RiskLevel::Medium);
    }

    #[test]
    fn test_risk_level_high() {
        let scorer = scorer_for("us_enterprise");
        // High risk country (90 geo) + Tor (80 rep, 40 priv) + botnet (95 threat)
        // Weighted: (90*15 + 80*25 + 40*25 + 100*35) / 100 = (1350+2000+1000+3500)/100 = 78.5 -> 78
        let input = RiskInput {
            country_code: Some("CN"), // 90 geo
            is_tor: true,             // 80 rep, 40 priv
            is_botnet: true,          // 95 threat
            is_malicious: true,       // +90 threat (capped at 100)
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(
            output.risk_score >= 60 && output.risk_score < 80,
            "Expected High range (60-79), got: {}",
            output.risk_score
        );
        assert_eq!(output.risk_level, RiskLevel::High);
    }

    #[test]
    fn test_risk_level_critical() {
        let scorer = scorer_for("high_security");
        // All signals maxed out
        let input = RiskInput {
            country_code: Some("KP"), // 90 geo
            is_vpn: true,
            is_proxy: true,
            is_tor: true,       // 100 rep, 95 priv (3 anonymizers -> extensive)
            is_botnet: true,    // 95 threat
            is_malicious: true, // +90 threat (capped 100)
            is_spam: true,
            is_scanner: true,
            abuse_score: 100,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(
            output.risk_score >= 80,
            "Expected Critical (>=80), got: {}",
            output.risk_score
        );
        assert_eq!(output.risk_level, RiskLevel::Critical);
    }

    // ========================================================================
    // Custom weights
    // ========================================================================

    #[test]
    fn test_custom_weights_threat_only() {
        let scorer = weighted(0, 0, 0, 100);

        let input = RiskInput {
            country_code: Some("KP"), // Would be 90 geo, but weight=0
            is_tor: true,             // Would be 80 rep, but weight=0
            is_botnet: true,          // 95 threat
            is_malicious: true,       // +90 threat (capped 100)
            ..Default::default()
        };
        let output = scorer.score(&input);

        // Only threat contributes: 100 * 100 / 100 = 100
        assert_eq!(
            output.risk_score, 100,
            "With 100% threat weight, score should be 100"
        );
    }

    #[test]
    fn test_custom_weights_geo_only() {
        let scorer = weighted(100, 0, 0, 0);

        let input = RiskInput {
            country_code: Some("KP"),
            is_botnet: true, // Should not affect score
            ..Default::default()
        };
        let output = scorer.score(&input);

        assert_eq!(output.risk_score, 90, "With 100% geo weight, should be 90");
    }

    #[test]
    fn test_zero_total_weight() {
        let scorer = weighted(0, 0, 0, 0);

        let input = RiskInput {
            country_code: Some("KP"),
            is_botnet: true,
            ..Default::default()
        };
        let output = scorer.score(&input);

        // total_weight is 0, so the if-guard skips the division => risk_score stays 0
        assert_eq!(output.risk_score, 0);
    }

    // ========================================================================
    // Edge case countries
    // ========================================================================

    #[test]
    fn test_unknown_country_code() {
        let scorer = scorer_for("us_enterprise");
        let input = RiskInput {
            country_code: Some("ZZ"), // Not a real country
            ..Default::default()
        };
        let output = scorer.score(&input);
        // Not in high or medium risk sets, so base risk 15
        assert_eq!(output.geo_risk_score, 15);
    }

    #[test]
    fn test_empty_string_country_code() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            country_code: Some(""),
            ..Default::default()
        };
        let output = scorer.score(&input);
        // Empty string won't match any set, so base risk 15
        assert_eq!(output.geo_risk_score, 15);
    }

    #[test]
    fn test_no_country_code() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            country_code: None,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.geo_risk_score, 35);
        assert!(output.risk_factors.contains(&"unknown_country"));
    }

    // ========================================================================
    // Combined high risk (Critical scenario)
    // ========================================================================

    #[test]
    fn test_tor_vpn_botnet_high_risk_country_critical() {
        let scorer = scorer_for("us_enterprise");
        let input = RiskInput {
            country_code: Some("IR"),
            is_tor: true,
            is_vpn: true,
            is_botnet: true,
            is_malicious: true,
            abuse_score: 99,
            ..Default::default()
        };
        let output = scorer.score(&input);

        // Geo: 90 (high risk)
        assert_eq!(output.geo_risk_score, 90);
        // Rep: VPN 50 + Tor 80 = 130 -> 100
        assert_eq!(output.reputation_risk_score, 100);
        // Privacy: 2 anonymizers -> 70
        assert_eq!(output.privacy_risk_score, 70);
        // Threat: botnet 95 + malicious 90 + abuse 99 -> 100
        assert_eq!(output.threat_risk_score, 100);

        // Weighted: (90*15 + 100*25 + 70*25 + 100*35) / 100
        // = (1350 + 2500 + 1750 + 3500) / 100 = 91
        assert!(
            output.risk_score >= 80,
            "Combined high risk should be Critical, got: {}",
            output.risk_score
        );
        assert_eq!(output.risk_level, RiskLevel::Critical);

        // Should have multiple risk factors
        assert!(output.risk_factors.contains(&"high_risk_country"));
        assert!(output.risk_factors.contains(&"tor_detected"));
        assert!(output.risk_factors.contains(&"vpn_detected"));
        assert!(output.risk_factors.contains(&"botnet_ip"));
        assert!(output.risk_factors.contains(&"malicious_ip"));
        assert!(output.risk_factors.contains(&"high_abuse_score"));
        assert!(output.risk_factors.contains(&"multi_layer_anonymization"));
    }

    // ========================================================================
    // Privacy risk levels
    // ========================================================================

    #[test]
    fn test_privacy_risk_zero_anonymizers() {
        let scorer = RiskScorer::default();
        let input = RiskInput::default();
        let output = scorer.score(&input);
        assert_eq!(output.privacy_risk_score, 0);
    }

    #[test]
    fn test_privacy_risk_single_anonymizer() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            is_vpn: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.privacy_risk_score, 40);
    }

    #[test]
    fn test_privacy_risk_two_anonymizers() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            is_vpn: true,
            is_proxy: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.privacy_risk_score, 70);
        assert!(output.risk_factors.contains(&"multi_layer_anonymization"));
    }

    #[test]
    fn test_privacy_risk_three_plus_anonymizers() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            is_vpn: true,
            is_proxy: true,
            is_tor: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.privacy_risk_score, 95);
        assert!(output.risk_factors.contains(&"extensive_anonymization"));
    }

    #[test]
    fn test_privacy_risk_all_four_anonymizers() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            is_vpn: true,
            is_proxy: true,
            is_tor: true,
            is_relay: true,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.privacy_risk_score, 95);
        assert!(output.risk_factors.contains(&"extensive_anonymization"));
    }

    // ========================================================================
    // Schema map comprehensive
    // ========================================================================

    #[test]
    fn test_output_schema_map_all_fields() {
        let output = RiskOutput {
            risk_score: 85,
            geo_risk_score: 90,
            reputation_risk_score: 100,
            privacy_risk_score: 70,
            threat_risk_score: 95,
            risk_level: RiskLevel::Critical,
            risk_factors: vec!["tor_detected", "botnet_ip"],
        };

        let fields = [
            "risk_score",
            "geo_risk_score",
            "reputation_risk_score",
            "privacy_risk_score",
            "threat_risk_score",
            "risk_level",
            "risk_factors",
        ];
        let map = output.to_schema_map(&fields);
        assert_eq!(map.len(), 7);
        assert_eq!(
            map.get("risk_score").unwrap(),
            &serde_json::Value::Number(85.into())
        );
        assert_eq!(
            map.get("geo_risk_score").unwrap(),
            &serde_json::Value::Number(90.into())
        );
        assert_eq!(map.get("risk_level").unwrap(), "critical");

        let factors = map.get("risk_factors").unwrap().as_array().unwrap();
        assert_eq!(factors.len(), 2);
    }

    #[test]
    fn test_output_schema_map_unknown_field_ignored() {
        let output = RiskOutput::default();
        let fields = ["nonexistent", "risk_score"];
        let map = output.to_schema_map(&fields);
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key("nonexistent"));
    }

    // ========================================================================
    // RiskInput::from_enrichment
    // ========================================================================

    #[test]
    fn test_risk_input_from_enrichment_both_none() {
        let input = RiskInput::from_enrichment(None, None);
        assert!(input.country_code.is_none());
        assert!(!input.is_vpn);
        assert!(!input.is_private);
        assert_eq!(input.abuse_score, 0);
    }

    #[test]
    fn test_risk_input_from_enrichment_with_geo() {
        let geo = GeoIpResult {
            country_code: Some("AU".to_string()),
            continent_code: Some("OC".to_string()),
            is_private: false,
            ..Default::default()
        };
        let input = RiskInput::from_enrichment(Some(&geo), None);
        assert_eq!(input.country_code, Some("AU"));
        assert_eq!(input.continent_code, Some("OC"));
        assert!(!input.is_private);
    }

    #[test]
    fn test_risk_input_from_enrichment_with_reputation() {
        let rep = ReputationResult {
            is_vpn: true,
            is_tor: true,
            is_botnet: true,
            abuse_score: 85,
            ..Default::default()
        };
        let input = RiskInput::from_enrichment(None, Some(&rep));
        assert!(input.is_vpn);
        assert!(input.is_tor);
        assert!(input.is_botnet);
        assert_eq!(input.abuse_score, 85);
    }

    // ========================================================================
    // Abuse score thresholds
    // ========================================================================

    #[test]
    fn test_abuse_score_below_threshold_no_factor() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            abuse_score: 79, // Below 80 threshold
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(!output.risk_factors.contains(&"high_abuse_score"));
        assert!(
            output.threat_risk_score > 0,
            "Should still contribute to score"
        );
    }

    #[test]
    fn test_abuse_score_at_threshold() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            abuse_score: 80,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert!(output.risk_factors.contains(&"high_abuse_score"));
    }

    #[test]
    fn test_abuse_score_zero_no_contribution() {
        let scorer = RiskScorer::default();
        let input = RiskInput {
            abuse_score: 0,
            ..Default::default()
        };
        let output = scorer.score(&input);
        assert_eq!(output.threat_risk_score, 0);
    }

    // ========================================================================
    // Configured lists, scores and bands
    // ========================================================================

    /// Every value the scorer uses comes from config: a deployment's own
    /// preset, scores and bands change the output.
    #[test]
    fn configured_presets_scores_and_bands_drive_the_score() {
        let mut config = RiskScoringConfig {
            preset: "mine".to_string(),
            ..RiskScoringConfig::default()
        };
        config.presets.insert(
            "mine".to_string(),
            crate::config::RiskCountries {
                high: vec!["XX".to_string()],
                medium: vec!["ZZ".to_string()],
            },
        );
        config.scores.high_risk_country = 70;
        config.scores.medium_risk_country = 30;
        config.scores.vpn = 10;
        config.levels.low = 5;
        let scorer = RiskScorer::new(&config);

        // (country, vpn, geo score, reputation score, level)
        let cases = [
            (Some("XX"), false, 70, 0, RiskLevel::Low),
            (Some("ZZ"), false, 30, 0, RiskLevel::Minimal),
            // KP is high risk in the shipped presets, not in this one.
            (Some("KP"), false, 15, 0, RiskLevel::Minimal),
            (Some("AU"), true, 15, 10, RiskLevel::Low),
        ];
        for (country, is_vpn, geo, reputation, level) in cases {
            let output = scorer.score(&RiskInput {
                country_code: country,
                is_vpn,
                ..Default::default()
            });
            assert_eq!(output.geo_risk_score, geo, "{country:?}");
            assert_eq!(output.reputation_risk_score, reputation, "{country:?}");
            assert_eq!(output.risk_level, level, "{country:?}");
        }
    }

    #[test]
    fn test_risk_level_boundary_values_exhaustive() {
        let shipped = RiskLevelBands::default();
        let custom = RiskLevelBands {
            low: 10,
            medium: 50,
            high: 70,
            critical: 95,
        };
        // (bands, score, level)
        let cases = [
            (&shipped, 0, RiskLevel::Minimal),
            (&shipped, 19, RiskLevel::Minimal),
            (&shipped, 20, RiskLevel::Low),
            (&shipped, 39, RiskLevel::Low),
            (&shipped, 40, RiskLevel::Medium),
            (&shipped, 59, RiskLevel::Medium),
            (&shipped, 60, RiskLevel::High),
            (&shipped, 79, RiskLevel::High),
            (&shipped, 80, RiskLevel::Critical),
            (&shipped, 100, RiskLevel::Critical),
            (&shipped, 255, RiskLevel::Critical),
            (&custom, 9, RiskLevel::Minimal),
            (&custom, 10, RiskLevel::Low),
            (&custom, 69, RiskLevel::Medium),
            (&custom, 94, RiskLevel::High),
            (&custom, 95, RiskLevel::Critical),
        ];
        for (bands, score, level) in cases {
            assert_eq!(RiskLevel::from_score(score, bands), level, "{score}");
        }
    }

    #[test]
    fn test_risk_level_as_str_all_variants() {
        assert_eq!(RiskLevel::Minimal.as_str(), "minimal");
        assert_eq!(RiskLevel::Low.as_str(), "low");
        assert_eq!(RiskLevel::Medium.as_str(), "medium");
        assert_eq!(RiskLevel::High.as_str(), "high");
        assert_eq!(RiskLevel::Critical.as_str(), "critical");
    }
}
