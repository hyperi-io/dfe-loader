// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Per-table capture mode resolution.
//!
//! Resolves `CaptureMode` (full/raw_only/json_only/extracted_only) from three sources:
//! 1. DDL comment tag `@capture_mode` — highest priority
//! 2. Config `table_capture_modes` map — per-table override
//! 3. Config `capture_mode` — global default
//!
//! Key design: [`CaptureOverrides::derive_config`] is a pure `&self` method — safe
//! for rayon `par_iter`. The mutable operations (`mark_pending`, `ensure_cached`,
//! `update_from_comment`) are called only in the sequential phase.

use rustc_hash::FxHashMap;
use tracing::warn;

use crate::config::{CaptureMode, MetadataConfig, TableCaptureConfig};
use crate::schema::TableTags;

/// Per-table capture mode resolution cache.
pub struct CaptureOverrides {
    /// Global default capture mode.
    global_mode: CaptureMode,
    /// Per-table capture mode overrides from config.
    table_modes: FxHashMap<String, CaptureMode>,
    /// Resolved per-table configs (populated by DDL tag resolution).
    configs: FxHashMap<String, TableCaptureConfig>,
    /// Tables needing async DDL tag resolution.
    pending_tables: Vec<String>,
}

impl CaptureOverrides {
    /// Create from metadata config.
    pub fn new(metadata_config: &MetadataConfig) -> Self {
        Self {
            global_mode: metadata_config.capture_mode,
            table_modes: metadata_config
                .table_capture_modes
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
            configs: FxHashMap::default(),
            pending_tables: Vec::new(),
        }
    }

    /// Pure derivation — no mutation, safe for `par_iter`.
    ///
    /// Resolution order (highest priority first):
    /// 1. DDL-resolved config (from `update_from_comment`)
    /// 2. Per-table config (`table_capture_modes`)
    /// 3. Global `capture_mode`
    pub fn derive_config(&self, table: &str) -> TableCaptureConfig {
        // DDL-resolved overrides take highest precedence
        if let Some(config) = self.configs.get(table) {
            return config.clone();
        }
        self.configured(table)
    }

    /// The table's policy from config alone: steps 2 and 3 of
    /// [`Self::derive_config`].
    fn configured(&self, table: &str) -> TableCaptureConfig {
        let mode = self
            .table_modes
            .get(table)
            .copied()
            .unwrap_or(self.global_mode);
        TableCaptureConfig { mode }
    }

    /// Populate the config cache for a table if absent.
    pub fn ensure_cached(&mut self, table: &str) {
        if !self.configs.contains_key(table) {
            let config = self.derive_config(table);
            self.configs.insert(table.to_string(), config);
        }
    }

    /// Mark a table for async DDL tag resolution (first time seen).
    pub fn mark_pending(&mut self, table: &str) {
        if !self.configs.contains_key(table) {
            self.pending_tables.push(table.to_string());
        }
    }

    /// Take pending tables for async resolution.
    pub fn take_pending(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_tables)
    }

    /// Update capture config from DDL table comment tags, read successfully.
    ///
    /// `@capture_mode: <mode>` overrides the configured policy. A comment
    /// with no `@capture_mode` tag, empty included, drops the table's
    /// override, so the configured policy applies again.
    pub fn update_from_comment(&mut self, table: &str, comment: &str) {
        let tags = TableTags::from_comment(comment);

        if let Some(mode_str) = tags.get("capture_mode") {
            let mode = match mode_str {
                "full" => CaptureMode::Full,
                "raw_only" => CaptureMode::RawOnly,
                "json_only" => CaptureMode::JsonOnly,
                "extracted_only" => CaptureMode::ExtractedOnly,
                other => {
                    warn!(
                        table = %table,
                        value = %other,
                        "Unknown @capture_mode value, ignoring"
                    );
                    return;
                }
            };
            self.configs
                .insert(table.to_string(), TableCaptureConfig { mode });
            return;
        }

        // Cached rather than removed: an absent entry is what marks a
        // table for resolution, and the next message would queue it again.
        let configured = self.configured(table);
        self.configs.insert(table.to_string(), configured);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_derive_config_default_is_full() {
        let metadata_config = MetadataConfig::default();
        let overrides = CaptureOverrides::new(&metadata_config);

        let config = overrides.derive_config("dfe.events");
        assert_eq!(config.mode, CaptureMode::Full);
    }

    #[test]
    fn test_global_capture_mode() {
        let metadata_config = MetadataConfig {
            capture_mode: CaptureMode::RawOnly,
            ..Default::default()
        };
        let overrides = CaptureOverrides::new(&metadata_config);

        let config = overrides.derive_config("dfe.events");
        assert_eq!(config.mode, CaptureMode::RawOnly);
    }

    #[test]
    fn test_per_table_overrides_global() {
        let metadata_config = MetadataConfig {
            table_capture_modes: HashMap::from([(
                "dfe.metrics".to_string(),
                CaptureMode::ExtractedOnly,
            )]),
            ..Default::default()
        };
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.metrics").mode,
            CaptureMode::ExtractedOnly
        );
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    #[test]
    fn test_ddl_capture_mode_overrides_config() {
        let metadata_config = MetadataConfig {
            table_capture_modes: HashMap::from([("dfe.events".to_string(), CaptureMode::Full)]),
            ..Default::default()
        };
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // DDL says extracted_only — should win over config
        overrides.update_from_comment(
            "dfe.events",
            "@schema_source: core | @capture_mode: extracted_only",
        );

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::ExtractedOnly
        );
    }

    #[test]
    fn test_ddl_capture_mode_raw_only() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.raw_logs", "@capture_mode: raw_only");

        assert_eq!(
            overrides.derive_config("dfe.raw_logs").mode,
            CaptureMode::RawOnly
        );
    }

    // ---- json_only resolves at each of the three layers ----

    #[test]
    fn test_global_capture_mode_json_only() {
        let metadata_config = MetadataConfig {
            capture_mode: CaptureMode::JsonOnly,
            ..Default::default()
        };
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::JsonOnly
        );
    }

    #[test]
    fn test_per_table_json_only_overrides_global() {
        let metadata_config = MetadataConfig {
            table_capture_modes: HashMap::from([("dfe.events".to_string(), CaptureMode::JsonOnly)]),
            ..Default::default()
        };
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::JsonOnly
        );
        assert_eq!(
            overrides.derive_config("dfe.metrics").mode,
            CaptureMode::Full
        );
    }

    #[test]
    fn test_ddl_capture_mode_json_only_overrides_config() {
        let metadata_config = MetadataConfig {
            table_capture_modes: HashMap::from([("dfe.events".to_string(), CaptureMode::RawOnly)]),
            ..Default::default()
        };
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@capture_mode: json_only");

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::JsonOnly
        );
    }

    #[test]
    fn test_ensure_cached_prevents_duplicate_pending() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.mark_pending("dfe.events");
        overrides.mark_pending("dfe.metrics");
        let pending = overrides.take_pending();
        assert_eq!(pending.len(), 2);

        overrides.ensure_cached("dfe.events");
        overrides.ensure_cached("dfe.metrics");

        overrides.mark_pending("dfe.events");
        let pending = overrides.take_pending();
        assert_eq!(pending, [] as [std::string::String; 0]);
    }

    #[test]
    fn test_empty_comment_is_noop() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);
        overrides.update_from_comment("dfe.events", "");
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    /// A capture tag taken off the table's DDL takes its override with it,
    /// whether the comment is now empty or carries other tags, and the
    /// table's `table_capture_modes` entry applies again ahead of the global
    /// mode.
    #[test]
    fn removing_the_capture_tag_restores_the_configured_policy() {
        for (table_modes, expected) in [
            (HashMap::new(), CaptureMode::Full),
            (
                HashMap::from([("dfe.events".to_string(), CaptureMode::JsonOnly)]),
                CaptureMode::JsonOnly,
            ),
        ] {
            for later in ["", "@retention: 30d"] {
                let metadata_config = MetadataConfig {
                    table_capture_modes: table_modes.clone(),
                    ..Default::default()
                };
                let mut overrides = CaptureOverrides::new(&metadata_config);
                overrides.update_from_comment("dfe.events", "@capture_mode: extracted_only");
                assert_eq!(
                    overrides.derive_config("dfe.events").mode,
                    CaptureMode::ExtractedOnly
                );
                overrides.update_from_comment("dfe.events", later);
                assert_eq!(
                    overrides.derive_config("dfe.events").mode,
                    expected,
                    "after {later:?}"
                );
                overrides.mark_pending("dfe.events");
                assert!(
                    overrides.take_pending().is_empty(),
                    "a resolved table is not queued to resolve again"
                );
            }
        }
    }

    #[test]
    fn test_unknown_capture_mode_ignored() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);
        overrides.update_from_comment("dfe.events", "@capture_mode: nonsense");
        // Should fall through to global default
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    #[test]
    fn test_ddl_update_full_capture_mode() {
        // Ensure the "full" variant in update_from_comment is exercised
        let metadata_config = MetadataConfig {
            capture_mode: CaptureMode::RawOnly,
            ..Default::default()
        };
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@capture_mode: full");

        // DDL override wins — goes from RawOnly (global) to Full
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    #[test]
    fn test_take_pending_empty_initially() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);
        assert_eq!(overrides.take_pending(), [] as [std::string::String; 0]);
    }

    #[test]
    fn test_mark_pending_duplicates_allowed_until_cached() {
        // mark_pending checks configs map, not pending vec — so it can add duplicates
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.mark_pending("dfe.events");
        overrides.mark_pending("dfe.events");
        overrides.mark_pending("dfe.events");

        let pending = overrides.take_pending();
        // Duplicates permitted — dedup happens upstream
        assert_eq!(pending.len(), 3);
    }

    #[test]
    fn test_unknown_capture_mode_preserves_existing_config() {
        // If update_from_comment gets an unknown value AFTER a valid one,
        // the valid config should remain (unknown → early return).
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@capture_mode: raw_only");
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::RawOnly
        );

        // Now apply a bad value — should not clobber
        overrides.update_from_comment("dfe.events", "@capture_mode: bogus");
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::RawOnly
        );
    }

    #[test]
    fn test_ensure_cached_is_idempotent() {
        let metadata_config = MetadataConfig {
            capture_mode: CaptureMode::RawOnly,
            ..Default::default()
        };
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.ensure_cached("dfe.events");
        let first = overrides.derive_config("dfe.events").mode;
        overrides.ensure_cached("dfe.events");
        overrides.ensure_cached("dfe.events");
        let second = overrides.derive_config("dfe.events").mode;

        assert_eq!(first, second);
        assert_eq!(first, CaptureMode::RawOnly);
    }
}
