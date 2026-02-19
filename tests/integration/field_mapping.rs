// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Integration tests for the field mapping feature.
//!
//! Tests cover: apply semantics, builder schema filtering, builtin preset
//! smoke tests, cache lifecycle, and ClickHouse `@renamed` comment parsing.

use std::collections::HashMap;

use rustc_hash::FxHashMap;
use serde_json::{json, Map};

use dfe_loader::clickhouse::types::{ColumnInfo, ParsedType};
use dfe_loader::clickhouse::TableSchema;
use dfe_loader::config::{FieldMappingConfig, FieldMappingOverride};
use dfe_loader::transform::remap_loader::load_builtin;
use dfe_loader::transform::{
    BuiltinPreset, FieldMappingCache, FieldMappingRule, MappingAction, MappingBuilder, RuleOrigin,
    TableFieldMapping,
};

// ============================================================================
// Helpers
// ============================================================================

/// Build a test TableSchema with String columns.
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

/// Build a single rename rule.
fn rename_rule(sources: &[&str], destination: &str) -> FieldMappingRule {
    FieldMappingRule {
        source_fields: sources.iter().map(|s| s.to_string()).collect(),
        destination: destination.to_string(),
        action: MappingAction::Rename,
        origin: RuleOrigin::Builtin("test".to_string()),
    }
}

/// Build a single copy rule.
fn copy_rule(sources: &[&str], destination: &str) -> FieldMappingRule {
    FieldMappingRule {
        source_fields: sources.iter().map(|s| s.to_string()).collect(),
        destination: destination.to_string(),
        action: MappingAction::Copy,
        origin: RuleOrigin::Builtin("test".to_string()),
    }
}

/// Build a FieldMappingConfig with ECS preset enabled.
fn ecs_config() -> FieldMappingConfig {
    FieldMappingConfig {
        enabled: true,
        builtin: "ecs".to_string(),
        ..Default::default()
    }
}

// ============================================================================
// 1. Apply Semantics
// ============================================================================

#[test]
fn test_apply_rename() {
    let mapping = TableFieldMapping::new(vec![rename_rule(&["src_ip"], "source.ip")]);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("other".to_string(), json!("keep"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert!(
        !data.contains_key("src_ip"),
        "source field should be removed"
    );
    assert_eq!(data.get("other").unwrap(), "keep");
}

#[test]
fn test_apply_copy() {
    let mapping = TableFieldMapping::new(vec![copy_rule(&["src_ip"], "source.ip")]);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(
        data.get("src_ip").unwrap(),
        "10.0.0.1",
        "copy should retain source"
    );
}

#[test]
fn test_apply_first_match_wins() {
    let mapping = TableFieldMapping::new(vec![rename_rule(
        &["src_ip", "srcip", "source_ip"],
        "source.ip",
    )]);

    // Only second source present
    let mut data = Map::new();
    data.insert("srcip".to_string(), json!("192.168.1.1"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "192.168.1.1");
    assert!(!data.contains_key("srcip"));
}

#[test]
fn test_apply_first_match_priority() {
    let mapping = TableFieldMapping::new(vec![rename_rule(&["src_ip", "srcip"], "source.ip")]);

    // Both present — first listed source wins
    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("srcip".to_string(), json!("192.168.1.1"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert!(!data.contains_key("src_ip"));
    // Second source untouched (only first match is consumed)
    assert!(data.contains_key("srcip"));
}

#[test]
fn test_apply_skip_existing_destination() {
    let mapping = TableFieldMapping::new(vec![rename_rule(&["src_ip"], "source.ip")]);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("source.ip".to_string(), json!("already.set"));

    mapping.apply(&mut data);

    assert_eq!(
        data.get("source.ip").unwrap(),
        "already.set",
        "existing dest should not be overwritten"
    );
    assert!(
        data.contains_key("src_ip"),
        "source should not be consumed when dest exists"
    );
}

#[test]
fn test_apply_missing_source() {
    let mapping = TableFieldMapping::new(vec![rename_rule(&["nonexistent"], "source.ip")]);

    let mut data = Map::new();
    data.insert("other".to_string(), json!("value"));

    mapping.apply(&mut data);

    assert!(!data.contains_key("source.ip"));
    assert_eq!(data.len(), 1);
}

#[test]
fn test_apply_empty_mapping_is_noop() {
    let mapping = TableFieldMapping::empty();

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));

    mapping.apply(&mut data);

    assert_eq!(data.len(), 1);
    assert_eq!(data.get("src_ip").unwrap(), "10.0.0.1");
}

#[test]
fn test_apply_multiple_rules() {
    let mapping = TableFieldMapping::new(vec![
        rename_rule(&["src_ip"], "source.ip"),
        rename_rule(&["dst_ip"], "destination.ip"),
        copy_rule(&["username"], "user.name"),
    ]);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("dst_ip".to_string(), json!("10.0.0.2"));
    data.insert("username".to_string(), json!("alice"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
    assert_eq!(data.get("user.name").unwrap(), "alice");
    assert!(!data.contains_key("src_ip"));
    assert!(!data.contains_key("dst_ip"));
    assert!(data.contains_key("username"), "copy retains source");
}

#[test]
fn test_apply_preserves_value_types() {
    let mapping = TableFieldMapping::new(vec![
        rename_rule(&["count"], "event.count"),
        rename_rule(&["active"], "event.active"),
        rename_rule(&["tags"], "event.tags"),
    ]);

    let mut data = Map::new();
    data.insert("count".to_string(), json!(42));
    data.insert("active".to_string(), json!(true));
    data.insert("tags".to_string(), json!(["a", "b"]));

    mapping.apply(&mut data);

    assert_eq!(data.get("event.count").unwrap(), &json!(42));
    assert_eq!(data.get("event.active").unwrap(), &json!(true));
    assert_eq!(data.get("event.tags").unwrap(), &json!(["a", "b"]));
}

// ============================================================================
// 2. MappingBuilder Schema Filtering
// ============================================================================

#[test]
fn test_builder_schema_filters_to_matching_columns() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    // ECS preset has 40+ rules, but schema only has these two destinations
    let schema = make_schema(&["source.ip", "destination.ip"]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    assert_eq!(
        mapping.len(),
        2,
        "only matching columns should produce rules"
    );

    let rules = mapping.rules();
    let dests: Vec<&str> = rules.iter().map(|r| r.destination.as_str()).collect();
    assert!(dests.contains(&"source.ip"));
    assert!(dests.contains(&"destination.ip"));
}

#[test]
fn test_builder_empty_schema_produces_empty_mapping() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    let schema = make_schema(&[]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    assert!(mapping.is_empty());
    assert_eq!(mapping.len(), 0);
}

#[test]
fn test_builder_no_preset_no_rules() {
    let config = FieldMappingConfig::default(); // builtin: "none"
    let builder = MappingBuilder::from_config(&config).unwrap();

    assert_eq!(builder.base_rule_count(), 0);

    let schema = make_schema(&["source.ip"]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());
    assert!(mapping.is_empty());
}

#[test]
fn test_builder_column_comment_overrides_builtin() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    let schema = make_schema(&["source.ip"]);

    // Column comment provides a different source field for source.ip
    let mut comments = FxHashMap::default();
    comments.insert(
        "source.ip".to_string(),
        "@renamed: custom_source_field".to_string(),
    );

    let mapping = builder.build_for_table(&schema, &comments);
    assert_eq!(mapping.len(), 1);

    let rule = &mapping.rules()[0];
    assert_eq!(rule.destination, "source.ip");
    assert_eq!(
        rule.source_fields,
        vec!["custom_source_field"],
        "column comment should override builtin rule"
    );
    assert_eq!(rule.origin, RuleOrigin::ColumnComment);
}

#[test]
fn test_builder_column_comment_first_directive() {
    let config = FieldMappingConfig::default();
    let builder = MappingBuilder::from_config(&config).unwrap();

    let schema = make_schema(&["source.ip"]);

    let mut comments = FxHashMap::default();
    comments.insert(
        "source.ip".to_string(),
        "@renamed: first(src_ip/srcip/source_address)".to_string(),
    );

    let mapping = builder.build_for_table(&schema, &comments);
    assert_eq!(mapping.len(), 1);

    let rule = &mapping.rules()[0];
    assert_eq!(
        rule.source_fields,
        vec!["src_ip", "srcip", "source_address"]
    );
}

#[test]
fn test_builder_config_override_action() {
    let config = FieldMappingConfig {
        enabled: true,
        builtin: "ecs".to_string(),
        overrides: HashMap::from([(
            "source.ip".to_string(),
            FieldMappingOverride {
                action: "copy".to_string(),
            },
        )]),
        ..Default::default()
    };

    let builder = MappingBuilder::from_config(&config).unwrap();
    let schema = make_schema(&["source.ip"]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    assert_eq!(mapping.len(), 1);
    let rule = &mapping.rules()[0];
    assert_eq!(rule.action, MappingAction::Copy, "config override to copy");
}

#[test]
fn test_builder_unrelated_columns_ignored() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    // Schema has columns that are not destinations in any ECS rule
    let schema = make_schema(&["custom_field_1", "custom_field_2", "my_data"]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    assert!(mapping.is_empty(), "no ECS rule targets these columns");
}

// ============================================================================
// 3. Builtin Preset Smoke Tests
// ============================================================================

#[test]
fn test_builtin_ecs_loads_and_has_source_ip() {
    let rules = load_builtin(BuiltinPreset::Ecs, MappingAction::Rename).unwrap();

    assert!(rules.len() > 30, "ECS preset should have 30+ rules");

    let source_ip = rules.iter().find(|r| r.destination == "source.ip");
    assert!(source_ip.is_some(), "ECS must map source.ip");

    let rule = source_ip.unwrap();
    assert!(
        rule.source_fields.contains(&"src_ip".to_string()),
        "src_ip should be a source for source.ip"
    );
}

#[test]
fn test_builtin_cim_loads() {
    let rules = load_builtin(BuiltinPreset::Cim, MappingAction::Rename).unwrap();
    assert!(rules.len() > 10, "CIM preset should have 10+ rules");
}

#[test]
fn test_builtin_beats_loads_and_has_agent_type() {
    let rules = load_builtin(BuiltinPreset::Beats, MappingAction::Rename).unwrap();
    assert!(rules.len() > 3, "Beats preset should have 3+ rules");

    let agent_type = rules.iter().find(|r| r.destination == "agent.type");
    assert!(agent_type.is_some(), "Beats must map agent.type");

    let rule = agent_type.unwrap();
    assert!(rule.source_fields.contains(&"beat.name".to_string()));
}

#[test]
fn test_builtin_ecs_apply_renames_event() {
    let rules = load_builtin(BuiltinPreset::Ecs, MappingAction::Rename).unwrap();
    let mapping = TableFieldMapping::new(rules);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("dst_ip".to_string(), json!("10.0.0.2"));
    data.insert("username".to_string(), json!("alice"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
    assert_eq!(data.get("user.name").unwrap(), "alice");
    assert!(!data.contains_key("src_ip"));
    assert!(!data.contains_key("dst_ip"));
    assert!(!data.contains_key("username"));
}

#[test]
fn test_builtin_cim_apply_renames_event() {
    let rules = load_builtin(BuiltinPreset::Cim, MappingAction::Rename).unwrap();
    let mapping = TableFieldMapping::new(rules);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("dest_ip".to_string(), json!("10.0.0.2"));
    data.insert("action".to_string(), json!("login"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
    assert_eq!(data.get("event.action").unwrap(), "login");
}

// ============================================================================
// 4. Cache Lifecycle
// ============================================================================

#[test]
fn test_cache_get_before_build_returns_none() {
    let config = FieldMappingConfig::default();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let cache = FieldMappingCache::new(builder);

    assert!(cache.get("test.table").is_none());
}

#[test]
fn test_cache_mark_pending_dedup() {
    let config = FieldMappingConfig::default();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    cache.mark_pending("test.table");
    cache.mark_pending("test.table");
    cache.mark_pending("test.table");

    let pending = cache.take_pending();
    assert_eq!(pending.len(), 1, "duplicate marks should be deduped");
    assert_eq!(pending[0], "test.table");
}

#[test]
fn test_cache_take_pending_drains() {
    let config = FieldMappingConfig::default();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    cache.mark_pending("table_a");
    cache.mark_pending("table_b");

    let first = cache.take_pending();
    assert_eq!(first.len(), 2);

    let second = cache.take_pending();
    assert!(second.is_empty(), "take_pending should drain the list");
}

#[test]
fn test_cache_build_and_get() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    let schema = make_schema(&["source.ip", "destination.ip"]);
    cache.build_and_cache("test.events", &schema, &FxHashMap::default());

    let mapping = cache.get("test.events");
    assert!(mapping.is_some());
    assert_eq!(mapping.unwrap().len(), 2);
}

#[test]
fn test_cache_build_no_comments() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    let schema = make_schema(&["source.ip"]);
    cache.build_and_cache_no_comments("test.events", &schema);

    let mapping = cache.get("test.events");
    assert!(mapping.is_some());
    assert_eq!(mapping.unwrap().len(), 1);
}

#[test]
fn test_cache_invalidate() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    let schema = make_schema(&["source.ip"]);
    cache.build_and_cache_no_comments("test.events", &schema);
    assert!(cache.get("test.events").is_some());

    cache.invalidate("test.events");
    assert!(cache.get("test.events").is_none());
}

#[test]
fn test_cache_mark_pending_skips_already_cached() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();
    let mut cache = FieldMappingCache::new(builder);

    let schema = make_schema(&["source.ip"]);
    cache.build_and_cache_no_comments("test.events", &schema);

    // Marking a cached table should not add to pending
    cache.mark_pending("test.events");
    let pending = cache.take_pending();
    assert!(
        pending.is_empty(),
        "already-cached tables should not be marked pending"
    );
}

// ============================================================================
// 5. ClickHouse Column Comments (requires ClickHouse)
// ============================================================================

#[cfg(test)]
mod clickhouse_tests {
    use super::*;

    use crate::common::{create_test_client, drop_test_table, load_dotenv, unique_table_name};

    #[tokio::test]
    async fn test_fetch_column_comments_with_renamed() {
        load_dotenv();
        let client = match create_test_client().await {
            Some(c) => c,
            None => {
                eprintln!("Skipping: ClickHouse not reachable");
                return;
            }
        };

        let table_name = unique_table_name("fm_comments");
        let full_name = format!("default.{}", table_name);

        // Create table with @renamed directives in column comments
        let ddl = format!(
            "CREATE TABLE default.{} (\
                _timestamp DateTime64(3),\
                source_ip String COMMENT '@renamed: first(src_ip/srcip)',\
                dest_ip String COMMENT '@renamed: dst_ip',\
                user_name String\
            ) ENGINE = Memory",
            table_name
        );
        client.query(&ddl).await.expect("DDL failed");

        // Fetch column comments
        let comments = client
            .fetch_column_comments(&full_name)
            .await
            .expect("fetch_column_comments failed");

        assert!(
            comments.contains_key("source_ip"),
            "should have source_ip comment"
        );
        assert_eq!(
            comments.get("source_ip").unwrap(),
            "@renamed: first(src_ip/srcip)"
        );
        assert_eq!(comments.get("dest_ip").unwrap(), "@renamed: dst_ip");
        // user_name has no comment, should not appear
        assert!(!comments.contains_key("user_name"));

        // Build mapping from these comments
        let config = FieldMappingConfig::default();
        let builder = MappingBuilder::from_config(&config).unwrap();

        let schema = client
            .fetch_table_schema(&full_name)
            .await
            .expect("fetch_table_schema failed");

        let mapping = builder.build_for_table(&schema, &comments);

        // source_ip and dest_ip have @renamed directives
        assert_eq!(mapping.len(), 2);

        // Apply to test data
        let mut data = Map::new();
        data.insert("src_ip".to_string(), json!("10.0.0.1"));
        data.insert("dst_ip".to_string(), json!("10.0.0.2"));
        data.insert("user_name".to_string(), json!("alice"));

        mapping.apply(&mut data);

        assert_eq!(data.get("source_ip").unwrap(), "10.0.0.1");
        assert_eq!(data.get("dest_ip").unwrap(), "10.0.0.2");
        assert!(
            !data.contains_key("src_ip"),
            "renamed source should be removed"
        );

        drop_test_table(&client, &full_name).await;
    }
}

// ============================================================================
// 6. End-to-End Preset Apply
// ============================================================================

#[test]
fn test_e2e_ecs_preset_with_schema_filter() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    // Schema with a subset of ECS destination columns
    let schema = make_schema(&[
        "source.ip",
        "destination.ip",
        "user.name",
        "event.action",
        "source.port",
        "destination.port",
    ]);

    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    // Only rules matching these columns should be present
    assert!(mapping.len() >= 4, "should have at least 4 matching rules");
    assert!(mapping.len() <= 6, "should not exceed schema column count");

    // Apply to event with raw field names
    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("dst_ip".to_string(), json!("10.0.0.2"));
    data.insert("username".to_string(), json!("alice"));
    data.insert("action".to_string(), json!("login"));
    data.insert("src_port".to_string(), json!(12345));
    data.insert("dst_port".to_string(), json!(443));
    data.insert("untouched_field".to_string(), json!("stays"));

    mapping.apply(&mut data);

    // Verify renames
    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
    assert_eq!(data.get("user.name").unwrap(), "alice");
    assert_eq!(data.get("event.action").unwrap(), "login");
    assert_eq!(data.get("source.port").unwrap(), 12345);
    assert_eq!(data.get("destination.port").unwrap(), 443);

    // Original fields removed (rename action)
    assert!(!data.contains_key("src_ip"));
    assert!(!data.contains_key("dst_ip"));
    assert!(!data.contains_key("username"));
    assert!(!data.contains_key("action"));

    // Unrelated fields untouched
    assert_eq!(data.get("untouched_field").unwrap(), "stays");
}

#[test]
fn test_e2e_cim_preset_with_schema_filter() {
    let config = FieldMappingConfig {
        enabled: true,
        builtin: "cim".to_string(),
        ..Default::default()
    };
    let builder = MappingBuilder::from_config(&config).unwrap();

    let schema = make_schema(&["source.ip", "destination.ip", "event.action"]);
    let mapping = builder.build_for_table(&schema, &FxHashMap::default());

    assert_eq!(mapping.len(), 3);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("dest_ip".to_string(), json!("10.0.0.2"));
    data.insert("action".to_string(), json!("allow"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("destination.ip").unwrap(), "10.0.0.2");
    assert_eq!(data.get("event.action").unwrap(), "allow");
}

#[test]
fn test_e2e_disabled_config_no_rules() {
    let config = FieldMappingConfig {
        enabled: false,
        builtin: "ecs".to_string(),
        ..Default::default()
    };

    // Even with ECS preset named, from_config still loads rules
    // (enabled flag is checked by orchestrator, not builder)
    let builder = MappingBuilder::from_config(&config).unwrap();
    assert!(
        builder.base_rule_count() > 0,
        "builder loads rules regardless of enabled flag"
    );
}

#[test]
fn test_e2e_comment_plus_builtin_merged() {
    let config = ecs_config();
    let builder = MappingBuilder::from_config(&config).unwrap();

    let schema = make_schema(&["source.ip", "custom_field"]);

    // Column comment adds a rule for custom_field (not in ECS preset)
    let mut comments = FxHashMap::default();
    comments.insert(
        "custom_field".to_string(),
        "@renamed: raw_custom".to_string(),
    );

    let mapping = builder.build_for_table(&schema, &comments);

    // Should have ECS rule for source.ip + comment rule for custom_field
    assert_eq!(mapping.len(), 2);

    let mut data = Map::new();
    data.insert("src_ip".to_string(), json!("10.0.0.1"));
    data.insert("raw_custom".to_string(), json!("custom_value"));

    mapping.apply(&mut data);

    assert_eq!(data.get("source.ip").unwrap(), "10.0.0.1");
    assert_eq!(data.get("custom_field").unwrap(), "custom_value");
}
