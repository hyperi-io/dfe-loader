// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Kafka topic auto-discovery with load-over-land fallback and regex filtering.
//!
//! ## Auto-Discovery Logic
//!
//! When `topics` config is empty, `TopicResolver` discovers all `*_land` and
//! `*_load` topics from the broker, then applies:
//!
//! 1. **Load-over-land fallback**: if `{source}_load` exists, suppress `{source}_land`
//!    (the `_load` topic is the promoted, schema-validated version).
//! 2. **Include filter**: if `topic_include` patterns are set, only matching topics pass.
//! 3. **Exclude filter**: topics matching any `topic_exclude` pattern are dropped.
//!
//! ## Note
//!
//! This logic belongs in hyperi-rustlib alongside `KafkaAdmin`. Pending migration
//! once rustlib CI is restored.

use hyperi_rustlib::transport::kafka::{KafkaAdmin, KafkaConfig as TransportKafkaConfig};
use regex::Regex;
use rustc_hash::FxHashSet;
use tracing::{debug, info};

use crate::Result;
use crate::kafka::transport::TransportAdapter;

/// Resolves the active Kafka topic list from broker metadata.
///
/// Used when `KafkaConfig.topics` is empty (auto-discovery mode).
pub struct TopicResolver {
    admin: KafkaAdmin,
    include_patterns: Vec<Regex>,
    exclude_patterns: Vec<Regex>,
}

impl TopicResolver {
    /// Create a new resolver from the transport config and filter patterns.
    ///
    /// Compiles all include/exclude patterns up front — errors out on invalid regex.
    pub fn new(
        transport_config: &TransportKafkaConfig,
        include: &[String],
        exclude: &[String],
    ) -> Result<Self> {
        let admin = KafkaAdmin::new(transport_config)
            .map_err(|e| crate::Error::Kafka(format!("KafkaAdmin init error: {e}")))?;

        let include_patterns = compile_patterns(include)?;
        let exclude_patterns = compile_patterns(exclude)?;

        Ok(Self {
            admin,
            include_patterns,
            exclude_patterns,
        })
    }

    /// Discover topics from the broker and return the resolved list.
    ///
    /// Fetches all broker topics, filters to `*_land` / `*_load` only, applies
    /// load-over-land fallback, then applies include/exclude filters.
    pub fn resolve(&self) -> Result<Vec<String>> {
        let all_topics = self
            .admin
            .list_topics()
            .map_err(|e| crate::Error::Kafka(format!("Failed to list topics: {e}")))?;

        debug!(total = all_topics.len(), "Fetched broker topic list");

        let dfe_topics: Vec<String> = all_topics
            .into_iter()
            .filter(|t| t.ends_with("_land") || t.ends_with("_load"))
            .collect();

        let after_fallback = apply_load_over_land(dfe_topics);

        let resolved: Vec<String> = after_fallback
            .into_iter()
            .filter(|t| passes_filters(t, &self.include_patterns, &self.exclude_patterns))
            .collect();

        info!(topics = ?resolved, "Auto-discovered Kafka topics");

        Ok(resolved)
    }
}

/// Suppress `*_land` topics when a corresponding `*_load` topic exists.
///
/// The `_load` topic is the promoted, schema-validated variant — when both
/// exist for the same source, consuming `_load` is always preferred.
pub(crate) fn apply_load_over_land(topics: Vec<String>) -> Vec<String> {
    // Collect source names (without suffix) for all _load topics.
    // Use owned Strings so the borrow does not outlive `topics`.
    let load_sources: FxHashSet<String> = topics
        .iter()
        .filter_map(|t| {
            t.strip_suffix("_load")
                .map(std::string::ToString::to_string)
        })
        .collect();

    topics
        .into_iter()
        .filter(|t| {
            if let Some(source) = t.strip_suffix("_land") {
                // Suppress this _land topic if a _load exists for the same source.
                !load_sources.contains(source)
            } else {
                true
            }
        })
        .collect()
}

/// Return true if `topic` passes the include/exclude filters.
pub(crate) fn passes_filters(topic: &str, include: &[Regex], exclude: &[Regex]) -> bool {
    if !include.is_empty() && !include.iter().any(|r| r.is_match(topic)) {
        return false;
    }
    if exclude.iter().any(|r| r.is_match(topic)) {
        return false;
    }
    true
}

/// Create a `TopicResolver` from a local dfe-loader `KafkaConfig`.
///
/// Convenience constructor used by `TransportAdapter`.
pub fn resolver_from_config(config: &crate::config::KafkaConfig) -> Result<TopicResolver> {
    let transport_config = TransportAdapter::convert_config(config);
    TopicResolver::new(
        &transport_config,
        &config.topic_include,
        &config.topic_exclude,
    )
}

fn compile_patterns(patterns: &[String]) -> Result<Vec<Regex>> {
    patterns
        .iter()
        .map(|p| {
            Regex::new(p)
                .map_err(|e| crate::Error::Config(format!("Invalid topic filter regex '{p}': {e}")))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_over_land_suppression() {
        let topics = vec![
            "auth_land".to_string(),
            "auth_load".to_string(),
            "events_land".to_string(),
            "syslog_load".to_string(),
        ];
        let result = apply_load_over_land(topics);
        // auth_land suppressed because auth_load exists
        assert!(!result.contains(&"auth_land".to_string()));
        assert!(result.contains(&"auth_load".to_string()));
        assert!(result.contains(&"events_land".to_string()));
        assert!(result.contains(&"syslog_load".to_string()));
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_load_over_land_no_load_topics() {
        let topics = vec!["auth_land".to_string(), "events_land".to_string()];
        let result = apply_load_over_land(topics.clone());
        // No _load topics — nothing suppressed
        assert_eq!(result, topics);
    }

    #[test]
    fn test_load_over_land_all_load() {
        let topics = vec!["auth_load".to_string(), "events_load".to_string()];
        let result = apply_load_over_land(topics.clone());
        assert_eq!(result, topics);
    }

    #[test]
    fn test_passes_filters_empty() {
        let include = vec![];
        let exclude = vec![];
        assert!(passes_filters("auth_land", &include, &exclude));
        assert!(passes_filters("events_load", &include, &exclude));
    }

    #[test]
    fn test_passes_filters_include() {
        let include = vec![Regex::new("^auth").unwrap()];
        let exclude = vec![];
        assert!(passes_filters("auth_land", &include, &exclude));
        assert!(!passes_filters("events_land", &include, &exclude));
    }

    #[test]
    fn test_passes_filters_exclude() {
        let include = vec![];
        let exclude = vec![Regex::new("^test_").unwrap()];
        assert!(passes_filters("auth_land", &include, &exclude));
        assert!(!passes_filters("test_land", &include, &exclude));
    }

    #[test]
    fn test_passes_filters_include_and_exclude() {
        let include = vec![Regex::new("_land$").unwrap()];
        let exclude = vec![Regex::new("^test_").unwrap()];
        assert!(passes_filters("auth_land", &include, &exclude));
        assert!(!passes_filters("test_land", &include, &exclude)); // excluded
        assert!(!passes_filters("auth_load", &include, &exclude)); // not included
    }

    #[test]
    fn test_compile_patterns_invalid_regex() {
        let result = compile_patterns(&["[invalid".to_string()]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid topic filter regex")
        );
    }
}
