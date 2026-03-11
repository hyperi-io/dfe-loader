// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Builds per-table field mappings by merging all sources with correct precedence
//! and filtering against the destination schema.
//!
//! Precedence (highest wins for same destination):
//! 1. ClickHouse column comments (`@renamed` directives)
//! 2. Config per-field overrides
//! 3. External remap files (later files override earlier)
//! 4. Built-in presets

use rustc_hash::{FxHashMap, FxHashSet};
use tracing::debug;

use crate::clickhouse::TableSchema;
use crate::config::FieldMappingConfig;

use super::field_mapping::{FieldMappingRule, MappingAction, RuleOrigin, TableFieldMapping};
use super::remap_loader::{load_builtin, load_file, BuiltinPreset};

/// Builds per-table field mappings from all configured sources.
///
/// Created once at startup from config. Used to build per-table mappings
/// as new tables are encountered.
pub struct MappingBuilder {
    /// Base rules from built-in presets + external files (lowest precedence).
    base_rules: Vec<FieldMappingRule>,
    /// Default action for rules without explicit action.
    default_action: MappingAction,
    /// Per-field action overrides from config.
    overrides: FxHashMap<String, MappingAction>,
}

impl MappingBuilder {
    /// Create from config, loading all built-in presets and external files.
    ///
    /// Loads in precedence order (lowest first):
    /// 1. Built-in preset (if configured)
    /// 2. External files (in order)
    pub fn from_config(config: &FieldMappingConfig) -> crate::Result<Self> {
        let default_action = MappingAction::parse(&config.default_action);
        let mut base_rules = Vec::new();

        // Load built-in preset (lowest precedence)
        if let Some(preset) = BuiltinPreset::parse(&config.builtin) {
            match load_builtin(preset, default_action) {
                Ok(rules) => {
                    debug!(preset = %config.builtin, rules = rules.len(), "Loaded built-in field mapping preset");
                    base_rules.extend(rules);
                }
                Err(e) => {
                    return Err(crate::Error::Config(format!(
                        "Failed to load built-in preset '{}': {}",
                        config.builtin, e
                    )));
                }
            }
        }

        // Load external files (later files override earlier for same destination)
        for path in &config.files {
            match load_file(path, default_action) {
                Ok(rules) => {
                    debug!(path = %path, rules = rules.len(), "Loaded external field mapping file");
                    base_rules.extend(rules);
                }
                Err(e) => {
                    return Err(crate::Error::Config(format!(
                        "Failed to load remap file '{}': {}",
                        path, e
                    )));
                }
            }
        }

        // Parse config overrides
        let overrides: FxHashMap<String, MappingAction> = config
            .overrides
            .iter()
            .map(|(dest, ov)| (dest.clone(), MappingAction::parse(&ov.action)))
            .collect();

        Ok(Self {
            base_rules,
            default_action,
            overrides,
        })
    }

    /// Build mapping for a specific table.
    ///
    /// Merges all sources with precedence, filters against destination schema.
    ///
    /// Precedence (highest wins):
    /// 1. `ColumnMetaCache::renamed_for_table` — from DDL comments + config cascade
    /// 2. Base rules from built-in presets and external remap files
    pub fn build_for_table(
        &self,
        schema: &TableSchema,
        col_meta: &crate::column_meta::ColumnMetaCache,
    ) -> TableFieldMapping {
        let table = format!("{}.{}", schema.database, schema.table);

        // Build set of valid destination columns for filtering
        let schema_columns: FxHashSet<&str> =
            schema.columns.iter().map(|c| c.name.as_str()).collect();

        // Key: destination field name → rule
        let mut merged: FxHashMap<String, FieldMappingRule> = FxHashMap::default();

        // Base rules from presets/files (lowest precedence)
        for rule in &self.base_rules {
            merged.insert(rule.destination.clone(), rule.clone());
        }

        // ColumnMetaCache renamed directives (highest precedence, config wins over DDL)
        for (col, source_fields) in col_meta.renamed_for_table(&table) {
            merged.insert(
                col.clone(),
                FieldMappingRule {
                    source_fields,
                    destination: col,
                    action: self.default_action,
                    origin: RuleOrigin::ColumnComment,
                },
            );
        }

        // Apply per-field action overrides from field_mapping config
        for (dest, action) in &self.overrides {
            if let Some(rule) = merged.get_mut(dest) {
                rule.action = *action;
            }
        }

        // Filter: only keep rules whose destination exists in the table schema
        let filtered: Vec<FieldMappingRule> = merged
            .into_values()
            .filter(|rule| schema_columns.contains(rule.destination.as_str()))
            .collect();

        debug!(
            table = %table,
            rules = filtered.len(),
            "Built field mapping for table"
        );

        TableFieldMapping::new(filtered)
    }

    /// Number of base rules loaded.
    pub fn base_rule_count(&self) -> usize {
        self.base_rules.len()
    }
}

// ============================================================================
// Field Mapping Cache (for orchestrator)
// ============================================================================

/// Per-table field mapping cache with lazy resolution.
///
/// Follows the same pattern as `CaptureOverrides` in the orchestrator:
/// - First encounter marks table as pending for async resolution
/// - Async resolution fetches column comments and builds mapping
/// - Subsequent lookups use cached mapping
pub struct FieldMappingCache {
    /// Pre-computed per-table mappings.
    mappings: FxHashMap<String, TableFieldMapping>,
    /// Tables pending async column comment fetch.
    pending_tables: Vec<String>,
    /// Builder for creating new mappings.
    builder: MappingBuilder,
}

impl FieldMappingCache {
    /// Create from a `MappingBuilder`.
    pub fn new(builder: MappingBuilder) -> Self {
        Self {
            mappings: FxHashMap::default(),
            pending_tables: Vec::new(),
            builder,
        }
    }

    /// Get cached mapping for a table (returns None if not yet resolved).
    #[inline]
    pub fn get(&self, table: &str) -> Option<&TableFieldMapping> {
        self.mappings.get(table)
    }

    /// Mark a table for async column comment resolution (first time seen).
    pub fn mark_pending(&mut self, table: &str) {
        if !self.mappings.contains_key(table) && !self.pending_tables.contains(&table.to_string()) {
            self.pending_tables.push(table.to_string());
        }
    }

    /// Take pending tables for async resolution.
    pub fn take_pending(&mut self) -> Vec<String> {
        std::mem::take(&mut self.pending_tables)
    }

    /// Build and cache mapping for a table using the unified `ColumnMetaCache`.
    pub fn build_and_cache(
        &mut self,
        table: &str,
        schema: &TableSchema,
        col_meta: &crate::column_meta::ColumnMetaCache,
    ) {
        let mapping = self.builder.build_for_table(schema, col_meta);
        self.mappings.insert(table.to_string(), mapping);
    }

    /// Invalidate cached mapping for a table (e.g., on schema refresh).
    pub fn invalidate(&mut self, table: &str) {
        self.mappings.remove(table);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::types::{ColumnInfo, ParsedType};

    fn make_schema(columns: &[&str]) -> TableSchema {
        TableSchema {
            database: "test_db".to_string(),
            table: "test_table".to_string(),
            columns: columns
                .iter()
                .enumerate()
                .map(|(i, name)| ColumnInfo {
                    name: name.to_string(),
                    type_name: "String".to_string(),
                    parsed_type: ParsedType::parse("String"),
                    position: (i as u64) + 1,
                    default_kind: String::new(),
                    default_expression: String::new(),
                    comment: String::new(),
                    is_in_primary_key: false,
                    is_in_sorting_key: false,
                })
                .collect(),
            comment: String::new(),
        }
    }

    #[test]
    fn test_schema_filters_irrelevant_rules() {
        let builder = MappingBuilder {
            base_rules: vec![
                FieldMappingRule {
                    source_fields: vec!["src_ip".to_string()],
                    destination: "source.ip".to_string(),
                    action: MappingAction::Rename,
                    origin: RuleOrigin::Builtin("ecs".to_string()),
                },
                FieldMappingRule {
                    source_fields: vec!["dst_ip".to_string()],
                    destination: "destination.ip".to_string(),
                    action: MappingAction::Rename,
                    origin: RuleOrigin::Builtin("ecs".to_string()),
                },
            ],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        // Schema only has source.ip, not destination.ip
        let schema = make_schema(&["source.ip", "other_field"]);
        let col_meta = crate::column_meta::ColumnMetaCache::new(Default::default());

        let mapping = builder.build_for_table(&schema, &col_meta);
        assert_eq!(mapping.len(), 1);
        assert_eq!(mapping.rules()[0].destination, "source.ip");
    }

    #[test]
    fn test_comment_overrides_file_rule() {
        use crate::column_meta::{ColumnDirectivesConfig, ColumnDirectivesEntry, ColumnMetaCache};
        let builder = MappingBuilder {
            base_rules: vec![FieldMappingRule {
                source_fields: vec!["src_ip".to_string()],
                destination: "source.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::ExternalFile("test.csv".to_string()),
            }],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        let schema = make_schema(&["source.ip"]);

        // Provide renamed directive via ColumnMetaCache config
        let mut config = ColumnDirectivesConfig::default();
        let mut table_cols = FxHashMap::default();
        table_cols.insert(
            "source.ip".to_string(),
            ColumnDirectivesEntry {
                renamed: Some("first(custom_src/custom_source)".to_string()),
                ..Default::default()
            },
        );
        config
            .tables
            .insert("test_db.test_table".to_string(), table_cols);
        let col_meta = ColumnMetaCache::new(config);

        let mapping = builder.build_for_table(&schema, &col_meta);
        assert_eq!(mapping.len(), 1);
        // Should use col_meta sources, not file sources
        assert_eq!(
            mapping.rules()[0].source_fields,
            vec!["custom_src", "custom_source"]
        );
        assert_eq!(mapping.rules()[0].origin, RuleOrigin::ColumnComment);
    }

    #[test]
    fn test_config_override_action() {
        let mut overrides = FxHashMap::default();
        overrides.insert("source.ip".to_string(), MappingAction::Copy);

        let builder = MappingBuilder {
            base_rules: vec![FieldMappingRule {
                source_fields: vec!["src_ip".to_string()],
                destination: "source.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::Builtin("ecs".to_string()),
            }],
            default_action: MappingAction::Rename,
            overrides,
        };

        let schema = make_schema(&["source.ip"]);
        let col_meta = crate::column_meta::ColumnMetaCache::new(Default::default());

        let mapping = builder.build_for_table(&schema, &col_meta);
        assert_eq!(mapping.rules()[0].action, MappingAction::Copy);
    }

    #[test]
    fn test_empty_schema_empty_mapping() {
        let builder = MappingBuilder {
            base_rules: vec![FieldMappingRule {
                source_fields: vec!["src_ip".to_string()],
                destination: "source.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::Builtin("ecs".to_string()),
            }],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        let schema = make_schema(&[]);
        let col_meta = crate::column_meta::ColumnMetaCache::new(Default::default());

        let mapping = builder.build_for_table(&schema, &col_meta);
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_cache_mark_pending() {
        let builder = MappingBuilder {
            base_rules: vec![],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        let mut cache = FieldMappingCache::new(builder);
        cache.mark_pending("common.events");
        cache.mark_pending("common.events"); // duplicate should be ignored
        cache.mark_pending("common.auth");

        let pending = cache.take_pending();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&"common.events".to_string()));
        assert!(pending.contains(&"common.auth".to_string()));

        // After take, pending should be empty
        assert!(cache.take_pending().is_empty());
    }

    #[test]
    fn test_cache_build_and_get() {
        let builder = MappingBuilder {
            base_rules: vec![FieldMappingRule {
                source_fields: vec!["src_ip".to_string()],
                destination: "source.ip".to_string(),
                action: MappingAction::Rename,
                origin: RuleOrigin::Builtin("ecs".to_string()),
            }],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        let mut cache = FieldMappingCache::new(builder);
        let schema = make_schema(&["source.ip"]);
        let col_meta = crate::column_meta::ColumnMetaCache::new(Default::default());

        assert!(cache.get("common.events").is_none());
        cache.build_and_cache("common.events", &schema, &col_meta);
        assert!(cache.get("common.events").is_some());
        assert_eq!(cache.get("common.events").unwrap().len(), 1);
    }

    #[test]
    fn test_cache_invalidate() {
        let builder = MappingBuilder {
            base_rules: vec![],
            default_action: MappingAction::Rename,
            overrides: FxHashMap::default(),
        };

        let mut cache = FieldMappingCache::new(builder);
        let schema = make_schema(&[]);
        let col_meta = crate::column_meta::ColumnMetaCache::new(Default::default());
        cache.build_and_cache("common.events", &schema, &col_meta);
        assert!(cache.get("common.events").is_some());

        cache.invalidate("common.events");
        assert!(cache.get("common.events").is_none());
    }
}
