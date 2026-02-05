// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Property-based tests using proptest
//!
//! Tests routing, transformation, buffer, and timestamp logic with generated inputs

use proptest::prelude::*;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use dfe_loader::buffer::BufferManager;
use dfe_loader::config::{BufferConfig, DlqConfig, RoutingConfig};
use dfe_loader::routing::{RouteResult, Router};
use dfe_loader::transform::Transformer;

// ============================================================================
// Routing Property Tests
// ============================================================================

proptest! {
    /// Test that routing handles arbitrary org_ids without panicking
    #[test]
    fn prop_routing_handles_arbitrary_org_ids(
        org_id in "[a-zA-Z0-9_-]{1,100}",
        category in "[a-zA-Z0-9_-]{1,50}"
    ) {
        let router = Router::default();
        let event = json!({
            "org_id": org_id,
            "event_category": category,
        });

        let payload = serde_json::to_vec(&event).unwrap();
        let result = router.route(&payload);

        // Should always route to a table (shared schema default)
        prop_assert!(matches!(result, RouteResult::Table(_)));
    }

    /// Test routing with nested fields
    #[test]
    fn prop_routing_nested_fields(
        org_id in "[a-zA-Z0-9_-]{1,50}",
        nested_category in "[a-zA-Z0-9_-]{1,30}"
    ) {
        let config = RoutingConfig {
            db_fields: vec![],
            table_fields: vec!["tags.event.category".to_string(), "event_category".to_string()],
            default_db: "common".to_string(),
            default_table: "events".to_string(),
            org_id_field: Some("org_id".to_string()),
            routed_orgs: vec![],
            route_all_by_org: false,
            category_to_table: HashMap::new(),
            mapping_file: None,
            dlq: DlqConfig::default(),
        };

        let router = Router::new(&config);
        let event = json!({
            "org_id": org_id,
            "tags": {
                "event": {
                    "category": nested_category
                }
            }
        });

        let payload = serde_json::to_vec(&event).unwrap();
        let result = router.route(&payload);

        prop_assert!(matches!(result, RouteResult::Table(ref s) if s.contains(&nested_category)));
    }

    /// Test that org_id extraction handles various formats
    #[test]
    fn prop_org_id_extraction(
        org_id in "[a-zA-Z0-9_-]{1,100}"
    ) {
        let router = Router::default();
        let event = json!({
            "org_id": org_id.clone(),
            "event_category": "test"
        });

        let value: serde_json::Value = event;
        let extracted = router.extract_org_id_from_value(&value);

        prop_assert_eq!(extracted, Some(org_id.as_str()));
    }
}

// ============================================================================
// Transformation Property Tests
// ============================================================================

proptest! {
    /// Test that transformer preserves underscore-prefixed fields
    #[test]
    fn prop_transformer_preserves_underscore_fields(
        field_name in "_[a-z]{1,20}",
        value in ".*"
    ) {
        let transformer = Transformer::default();
        let event = json!({ field_name.clone(): value.clone() });

        let result = transformer.transform(event);
        prop_assert!(result.is_ok());

        let data = result.unwrap().data;

        // System fields should be preserved
        if field_name == "_org_id" || field_name == "_tags" || field_name == "_uuid" {
            prop_assert!(data.contains_key(&field_name),
                "System field {} should be preserved", field_name);
        }
    }

    /// Test flattening with arbitrary nesting depth
    #[test]
    fn prop_transformer_flattens_nested_objects(
        depth in 1..5_usize,
        value in "[a-z]{1,20}"
    ) {
        let mut event = json!(value.clone());

        // Build nested structure
        for i in 0..depth {
            event = json!({ format!("level_{}", i): event });
        }

        let transformer = Transformer::default();
        let result = transformer.transform(event);

        prop_assert!(result.is_ok());

        let data = result.unwrap().data;

        // Check that deeply nested value is accessible via flattened key
        let expected_key = (0..depth)
            .rev()
            .map(|i| format!("level_{}", i))
            .collect::<Vec<_>>()
            .join(".");

        prop_assert!(data.contains_key(&expected_key),
            "Flattened key {} should exist", expected_key);
        prop_assert_eq!(data.get(&expected_key).and_then(|v| v.as_str()), Some(value.as_str()));
    }

    /// Test that transformation handles various JSON types
    #[test]
    fn prop_transformer_handles_types(
        string_val in ".*",
        int_val in -1000..1000_i64,
        bool_val in proptest::bool::ANY,
    ) {
        let event = json!({
            "string_field": string_val,
            "int_field": int_val,
            "bool_field": bool_val,
        });

        let transformer = Transformer::default();
        let result = transformer.transform(event);

        prop_assert!(result.is_ok());

        let data = result.unwrap().data;
        prop_assert!(data.contains_key("string_field"));
        prop_assert!(data.contains_key("int_field"));
        prop_assert!(data.contains_key("bool_field"));
    }
}

// ============================================================================
// Buffer Property Tests
// ============================================================================

proptest! {
    /// Test buffer accumulation with random sequences
    #[test]
    fn prop_buffer_accumulation(
        flush_rows in 10..1000_usize,
        message_count in 1..500_usize,
    ) {
        let config = BufferConfig {
            flush_rows,
            flush_bytes: 1_000_000, // High enough to not trigger
            flush_age_secs: 3600,   // High enough to not trigger
        };

        let mut buffer = BufferManager::new(&config);
        let table = "test.table";

        // Push messages
        for i in 0..message_count {
            let data = json!({
                "id": i,
                "value": "test",
            });

            let map: Map<String, Value> = serde_json::from_value(data).unwrap();
            buffer.push(table, map, None);
        }

        let stats = buffer.stats();

        // Buffer accumulates all messages (doesn't auto-flush)
        prop_assert_eq!(stats.pending_rows, message_count,
            "Buffer should contain all pushed messages");

        // Should have 1 table
        prop_assert_eq!(stats.table_count, 1,
            "Should have 1 table buffer");
    }

    /// Test buffer with multiple tables
    #[test]
    fn prop_buffer_multiple_tables(
        table_count in 1..20_usize,
        rows_per_table in 1..100_usize,
    ) {
        let config = BufferConfig {
            flush_rows: 10000,
            flush_bytes: 10_000_000,
            flush_age_secs: 3600,
        };

        let mut buffer = BufferManager::new(&config);

        // Push to multiple tables
        for table_idx in 0..table_count {
            let table = format!("test.table_{}", table_idx);

            for row_idx in 0..rows_per_table {
                let data = json!({
                    "id": row_idx,
                    "table": table_idx,
                });

                let map: Map<String, Value> = serde_json::from_value(data).unwrap();
                buffer.push(&table, map, None);
            }
        }

        let stats = buffer.stats();
        prop_assert_eq!(stats.table_count, table_count,
            "Should have {} separate table buffers", table_count);
        prop_assert_eq!(stats.pending_rows, table_count * rows_per_table,
            "Pending rows should match pushed count");
    }
}

// ============================================================================
// Timestamp Validation Property Tests
// ============================================================================

proptest! {
    /// Test timestamp validation with edge cases
    #[test]
    fn prop_timestamp_validation(
        year in 1970..3000_i32,
        month in 1..13_u32,
        day in 1..29_u32,  // Use 28 to avoid month-specific logic
    ) {
        use chrono::{NaiveDate, Utc};

        // Create a valid date
        let date = NaiveDate::from_ymd_opt(year, month, day);

        if let Some(d) = date {
            let datetime = d.and_hms_opt(12, 0, 0).unwrap();
            let timestamp = datetime.and_utc().timestamp_millis();

            let event = json!({
                "timestamp": timestamp,
                "event": "test",
            });

            let transformer = Transformer::default();
            let result = transformer.transform(event);

            prop_assert!(result.is_ok(),
                "Should handle timestamp from {}-{:02}-{:02}", year, month, day);

            let data = result.unwrap().data;
            prop_assert!(data.contains_key("_timestamp"),
                "Timestamp should be preserved as _timestamp");
        }
    }

    /// Test timestamp as RFC3339 string
    #[test]
    fn prop_timestamp_rfc3339(
        year in 2000..2030_i32,
        month in 1..13_u32,
        day in 1..29_u32,
    ) {
        use chrono::NaiveDate;

        let date = NaiveDate::from_ymd_opt(year, month, day);

        if let Some(d) = date {
            let datetime = d.and_hms_opt(12, 0, 0).unwrap();
            let rfc3339 = datetime.and_utc().to_rfc3339();

            let event = json!({
                "timestamp": rfc3339,
                "event": "test",
            });

            let transformer = Transformer::default();
            let result = transformer.transform(event);

            prop_assert!(result.is_ok(),
                "Should handle RFC3339 timestamp");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proptest_runs() {
        // Dummy test to ensure proptest module compiles
        assert!(true);
    }
}
