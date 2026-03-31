// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Per-table capture override resolution.
//!
//! Resolves `_json` and `_raw` disable flags from two sources:
//! 1. Config lists (`disable_json_tables`, `disable_raw_tables`) — applied immediately
//! 2. DDL comment tags (`@no_capture_json`, `@no_capture_raw`) — applied after async fetch
//!
//! DDL tags take precedence over config lists.
//!
//! Key design: [`CaptureOverrides::derive_config`] is a pure `&self` method — safe
//! for rayon `par_iter`. The mutable operations (`mark_pending`, `ensure_cached`,
//! `update_from_comment`) are called only in the sequential phase.

use rustc_hash::{FxHashMap, FxHashSet};

use crate::config::{MetadataConfig, TableCaptureConfig};
use crate::schema::TableTags;

/// Per-table capture override resolution cache.
pub struct CaptureOverrides {
    /// Resolved per-table configs (populated by `ensure_cached` or `update_from_comment`).
    configs: FxHashMap<String, TableCaptureConfig>,
    /// Tables with `_json` disabled (from config, O(1) lookup).
    disable_json_tables: FxHashSet<String>,
    /// Tables with `_raw` disabled (from config, O(1) lookup).
    disable_raw_tables: FxHashSet<String>,
    /// Tables needing async DDL tag resolution.
    pending_tables: Vec<String>,
}

impl CaptureOverrides {
    /// Create from metadata config.
    pub fn new(metadata_config: &MetadataConfig) -> Self {
        Self {
            configs: FxHashMap::default(),
            disable_json_tables: metadata_config
                .disable_json_tables
                .iter()
                .cloned()
                .collect(),
            disable_raw_tables: metadata_config.disable_raw_tables.iter().cloned().collect(),
            pending_tables: Vec::new(),
        }
    }

    /// Pure derivation — no mutation, safe for `par_iter`.
    ///
    /// Checks DDL-resolved overrides first (takes precedence), falls back to
    /// config-list defaults. Two `HashSet` lookups, O(1) each.
    pub fn derive_config(&self, table: &str) -> TableCaptureConfig {
        // DDL-resolved overrides take precedence
        if let Some(config) = self.configs.get(table) {
            return config.clone();
        }
        // Fall back to config-list defaults
        TableCaptureConfig {
            disable_json: self.disable_json_tables.contains(table),
            disable_raw: self.disable_raw_tables.contains(table),
        }
    }

    /// Populate the config cache for a table if absent.
    ///
    /// Called in the sequential phase after `derive_config` was used in the
    /// parallel phase. Ensures `mark_pending` sees the table as resolved
    /// and does not re-add it to the pending list on every batch.
    pub fn ensure_cached(&mut self, table: &str) {
        if !self.configs.contains_key(table) {
            self.configs.insert(
                table.to_string(),
                TableCaptureConfig {
                    disable_json: self.disable_json_tables.contains(table),
                    disable_raw: self.disable_raw_tables.contains(table),
                },
            );
        }
    }

    /// Mark a table for async DDL tag resolution (first time seen).
    ///
    /// Only adds to the pending list if the table has no resolved config yet.
    /// Called in the sequential phase by `BatchCoordinator`.
    pub fn mark_pending(&mut self, table: &str) {
        if !self.configs.contains_key(table) {
            self.pending_tables.push(table.to_string());
        }
    }

    /// Take pending tables for async resolution.
    pub fn take_pending(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_tables)
    }

    /// Update capture config from DDL table comment tags.
    ///
    /// DDL tags override config list settings (only to disable, not re-enable).
    pub fn update_from_comment(&mut self, table: &str, comment: &str) {
        if comment.is_empty() {
            return;
        }

        let tags = TableTags::from_comment(comment);

        let entry = self
            .configs
            .entry(table.to_string())
            .or_insert_with(|| TableCaptureConfig {
                disable_json: self.disable_json_tables.contains(table),
                disable_raw: self.disable_raw_tables.contains(table),
            });

        // DDL tags override config (only to disable)
        if tags.get("no_capture_json").is_some_and(|v| v == "true") {
            entry.disable_json = true;
        }
        if tags.get("no_capture_raw").is_some_and(|v| v == "true") {
            entry.disable_raw = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_config_default() {
        let metadata_config = MetadataConfig::default();
        let overrides = CaptureOverrides::new(&metadata_config);

        let config = overrides.derive_config("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }

    #[test]
    fn test_derive_config_with_disabled_tables() {
        let mut metadata_config = MetadataConfig::default();
        metadata_config
            .disable_json_tables
            .push("common.metrics".to_string());
        metadata_config
            .disable_raw_tables
            .push("common.health".to_string());

        let overrides = CaptureOverrides::new(&metadata_config);

        let config = overrides.derive_config("common.metrics");
        assert!(config.disable_json);
        assert!(!config.disable_raw);

        let config = overrides.derive_config("common.health");
        assert!(!config.disable_json);
        assert!(config.disable_raw);

        let config = overrides.derive_config("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }

    #[test]
    fn test_ddl_tag_precedence() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // DDL tag overrides even when config didn't disable
        overrides.update_from_comment(
            "common.events",
            "@schema_source: core | @no_capture_json: true",
        );

        let config = overrides.derive_config("common.events");
        assert!(config.disable_json);
        assert!(!config.disable_raw);
    }

    #[test]
    fn test_ensure_cached_prevents_duplicate_pending() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // First time: should be marked pending
        overrides.mark_pending("common.events");
        overrides.mark_pending("common.metrics");
        let pending = overrides.take_pending();
        assert_eq!(pending.len(), 2);

        // Ensure cached for these tables
        overrides.ensure_cached("common.events");
        overrides.ensure_cached("common.metrics");

        // Now they have configs — should not be pending again
        overrides.mark_pending("common.events");
        let pending = overrides.take_pending();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_empty_comment_is_noop() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);
        overrides.update_from_comment("common.events", "");
        let config = overrides.derive_config("common.events");
        assert!(!config.disable_json);
        assert!(!config.disable_raw);
    }
}
