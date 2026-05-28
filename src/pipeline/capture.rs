// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Per-table capture mode resolution.
//!
//! Resolves `CaptureMode` (full/raw_only/extracted_only) from three sources:
//! 1. DDL comment tag `@capture_mode` — highest priority
//! 2. Config `table_capture_modes` map — per-table override
//! 3. Config `capture_mode` — global default
//!
//! Legacy fields (`disable_json_tables`, `@no_capture_json`, etc.) are deprecated
//! but supported for backward compatibility during migration.
//!
//! Key design: [`CaptureOverrides::derive_config`] is a pure `&self` method — safe
//! for rayon `par_iter`. The mutable operations (`mark_pending`, `ensure_cached`,
//! `update_from_comment`) are called only in the sequential phase.

use rustc_hash::{FxHashMap, FxHashSet};
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
    /// Legacy: tables with `_json` disabled (deprecated, from config lists).
    disable_json_tables: FxHashSet<String>,
    /// Legacy: tables with `_raw` disabled (deprecated, from config lists).
    disable_raw_tables: FxHashSet<String>,
    /// Tables needing async DDL tag resolution.
    pending_tables: Vec<String>,
}

impl CaptureOverrides {
    /// Create from metadata config.
    pub fn new(metadata_config: &MetadataConfig) -> Self {
        // Warn on deprecated config fields
        if !metadata_config.disable_json_tables.is_empty()
            || !metadata_config.disable_raw_tables.is_empty()
        {
            warn!(
                "disable_json_tables/disable_raw_tables are deprecated. \
                 Use table_capture_modes with capture_mode values instead."
            );
        }

        Self {
            global_mode: metadata_config.capture_mode,
            table_modes: metadata_config
                .table_capture_modes
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect(),
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
    /// Resolution order (highest priority first):
    /// 1. DDL-resolved config (from `update_from_comment`)
    /// 2. Per-table config (`table_capture_modes`)
    /// 3. Legacy per-table disable lists (deprecated)
    /// 4. Global `capture_mode`
    pub fn derive_config(&self, table: &str) -> TableCaptureConfig {
        // DDL-resolved overrides take highest precedence
        if let Some(config) = self.configs.get(table) {
            return config.clone();
        }

        // Per-table config map (new style)
        if let Some(&mode) = self.table_modes.get(table) {
            return TableCaptureConfig { mode };
        }

        // Legacy per-table disable lists (deprecated, backward compat)
        let json_disabled = self.disable_json_tables.contains(table);
        let raw_disabled = self.disable_raw_tables.contains(table);
        if json_disabled || raw_disabled {
            let mode = match (json_disabled, raw_disabled) {
                (true, true) => CaptureMode::ExtractedOnly,
                (true, false) => CaptureMode::RawOnly,
                (false, true) => self.global_mode, // _raw disabled but _json enabled → use global
                (false, false) => unreachable!(),
            };
            return TableCaptureConfig { mode };
        }

        // Global default
        TableCaptureConfig {
            mode: self.global_mode,
        }
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

    /// Update capture config from DDL table comment tags.
    ///
    /// `@capture_mode: <mode>` takes precedence over legacy tags.
    /// Legacy `@no_capture_json` / `@no_capture_raw` tags are supported
    /// for backward compatibility but superseded by `@capture_mode`.
    pub fn update_from_comment(&mut self, table: &str, comment: &str) {
        if comment.is_empty() {
            return;
        }

        let tags = TableTags::from_comment(comment);

        // New-style: @capture_mode tag wins over everything
        if let Some(mode_str) = tags.get("capture_mode") {
            let mode = match mode_str {
                "full" => CaptureMode::Full,
                "raw_only" => CaptureMode::RawOnly,
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

        // Legacy tags: @no_capture_json / @no_capture_raw (deprecated)
        let json_disabled = tags.get("no_capture_json").is_some_and(|v| v == "true");
        let raw_disabled = tags.get("no_capture_raw").is_some_and(|v| v == "true");

        if json_disabled || raw_disabled {
            let mode = match (json_disabled, raw_disabled) {
                (true, true) => CaptureMode::ExtractedOnly,
                (true, false) => CaptureMode::RawOnly,
                (false, true) => self.global_mode,
                (false, false) => return, // Neither set, no override
            };
            self.configs
                .insert(table.to_string(), TableCaptureConfig { mode });
        }
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

    #[test]
    fn test_legacy_disable_json_maps_to_raw_only() {
        let mut metadata_config = MetadataConfig::default();
        metadata_config
            .disable_json_tables
            .push("dfe.metrics".to_string());
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.metrics").mode,
            CaptureMode::RawOnly
        );
    }

    #[test]
    fn test_legacy_disable_both_maps_to_extracted_only() {
        let mut metadata_config = MetadataConfig::default();
        metadata_config
            .disable_json_tables
            .push("dfe.health".to_string());
        metadata_config
            .disable_raw_tables
            .push("dfe.health".to_string());
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.health").mode,
            CaptureMode::ExtractedOnly
        );
    }

    #[test]
    fn test_legacy_ddl_no_capture_json_maps_to_raw_only() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@no_capture_json: true");

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::RawOnly
        );
    }

    #[test]
    fn test_ddl_capture_mode_wins_over_legacy_tags() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        // Both old and new tags present — new wins
        overrides.update_from_comment("dfe.events", "@no_capture_json: true | @capture_mode: full");

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
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
        assert!(pending.is_empty());
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

    // ---- Coverage gap: legacy raw_disabled only falls through to global mode ----

    #[test]
    fn test_legacy_disable_raw_only_falls_through_to_global_full() {
        // disable_raw_tables has the table, disable_json_tables does not
        // Global mode is Full (default) — (false, true) match arm → global_mode
        let mut metadata_config = MetadataConfig::default();
        metadata_config
            .disable_raw_tables
            .push("dfe.events".to_string());
        let overrides = CaptureOverrides::new(&metadata_config);

        // Hits the (false, true) legacy match arm → global_mode (Full)
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    #[test]
    fn test_legacy_disable_raw_only_falls_through_to_global_extracted() {
        // Same path but different global mode to prove the arm delegates to global
        let mut metadata_config = MetadataConfig {
            capture_mode: CaptureMode::ExtractedOnly,
            ..Default::default()
        };
        metadata_config
            .disable_raw_tables
            .push("dfe.events".to_string());
        let overrides = CaptureOverrides::new(&metadata_config);

        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::ExtractedOnly
        );
    }

    // ---- Coverage gap: DDL @no_capture_raw alone falls through to global ----

    #[test]
    fn test_ddl_no_capture_raw_only_uses_global_mode() {
        // Global is RawOnly — DDL @no_capture_raw alone should yield global (RawOnly)
        let metadata_config = MetadataConfig {
            capture_mode: CaptureMode::RawOnly,
            ..Default::default()
        };
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@no_capture_raw: true");

        // Hits (false, true) → global_mode (RawOnly)
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::RawOnly
        );
    }

    #[test]
    fn test_ddl_no_capture_raw_only_default_global_is_full() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment("dfe.events", "@no_capture_raw: true");

        // Hits (false, true) → global_mode (Full)
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::Full
        );
    }

    // ---- Coverage gap: DDL legacy no_capture_json and no_capture_raw together ----

    #[test]
    fn test_ddl_legacy_both_no_capture_maps_to_extracted_only() {
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment(
            "dfe.events",
            "@no_capture_json: true | @no_capture_raw: true",
        );

        // Hits (true, true) → ExtractedOnly
        assert_eq!(
            overrides.derive_config("dfe.events").mode,
            CaptureMode::ExtractedOnly
        );
    }

    #[test]
    fn test_ddl_legacy_tags_false_values_noop() {
        // @no_capture_json: false → is_some_and(|v| v == "true") returns false
        // Both legacy flags false → update does nothing, global default applies
        let metadata_config = MetadataConfig::default();
        let mut overrides = CaptureOverrides::new(&metadata_config);

        overrides.update_from_comment(
            "dfe.events",
            "@no_capture_json: false | @no_capture_raw: false",
        );

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
        assert!(overrides.take_pending().is_empty());
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
    fn test_deprecation_warning_emitted_on_legacy_config() {
        // Just ensures Self::new doesn't panic when deprecated fields are populated.
        // The warn! is a side effect; we don't capture logs here.
        let mut metadata_config = MetadataConfig::default();
        metadata_config.disable_json_tables.push("t1".to_string());
        metadata_config.disable_raw_tables.push("t2".to_string());
        let _overrides = CaptureOverrides::new(&metadata_config);
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
