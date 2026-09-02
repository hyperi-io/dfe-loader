// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Schema cache integration tests
//!
//! Tests for schema introspection, caching, and refresh

use std::time::Duration;

use dfe_loader::clickhouse::schema::{SchemaCache, SchemaCacheConfig};
use dfe_loader::clickhouse::{ColumnInfo, ParsedType, TableSchema};

use crate::common::{
    create_http_test_client, create_native_test_client, drop_http_test_table, unique_table_name,
};
use crate::skip_if_no_clickhouse;

// ============================================================================
// Helper to create test ColumnInfo
// ============================================================================

fn test_column(name: &str, type_name: &str) -> ColumnInfo {
    ColumnInfo {
        name: name.to_string(),
        type_name: type_name.to_string(),
        parsed_type: ParsedType::parse(type_name),
        position: 0,
        default_kind: String::new(),
        default_expression: String::new(),
        comment: String::new(),
        is_in_primary_key: false,
        is_in_sorting_key: false,
    }
}

fn test_schema(columns: Vec<(&str, &str)>) -> TableSchema {
    TableSchema {
        database: "default".to_string(),
        table: "test".to_string(),
        columns: columns
            .into_iter()
            .map(|(n, t)| test_column(n, t))
            .collect(),
        comment: String::new(),
    }
}

// ============================================================================
// Schema Cache Unit Tests
// ============================================================================

#[test]
fn test_schema_cache_basic() {
    let cache = SchemaCache::new(60); // 60 second TTL

    // Create a test schema
    let schema = test_schema(vec![("id", "UInt64"), ("name", "String")]);

    // Cache should be empty initially
    assert!(cache.get("test_table").is_none());

    // Insert schema
    cache.insert("test_table".to_string(), schema.clone());

    // Should be able to retrieve it
    let cached = cache.get("test_table");
    assert!(cached.is_some());
    let cached_schema = cached.unwrap();
    assert_eq!(cached_schema.columns.len(), 2);
    assert_eq!(cached_schema.columns[0].name, "id");

    eprintln!("✓ Schema cache basic operations work");
}

#[test]
fn test_schema_cache_multiple_tables() {
    let cache = SchemaCache::new(60);

    let schema1 = test_schema(vec![("id", "UInt64")]);
    let schema2 = test_schema(vec![("event", "String"), ("timestamp", "DateTime64(3)")]);

    cache.insert("table1".to_string(), schema1);
    cache.insert("table2".to_string(), schema2);

    let cached1 = cache.get("table1").unwrap();
    let cached2 = cache.get("table2").unwrap();

    assert_eq!(cached1.columns.len(), 1);
    assert_eq!(cached2.columns.len(), 2);
    assert_eq!(cached1.columns[0].name, "id");
    assert_eq!(cached2.columns[0].name, "event");

    eprintln!("✓ Multiple table schemas cached correctly");
}

#[test]
fn test_schema_cache_invalidation() {
    let cache = SchemaCache::new(60);

    let schema = test_schema(vec![("id", "UInt64")]);

    cache.insert("test_table".to_string(), schema);
    assert!(cache.get("test_table").is_some());

    // Invalidate
    cache.invalidate("test_table");
    assert!(cache.get("test_table").is_none());

    eprintln!("✓ Schema invalidation works");
}

#[test]
fn test_schema_cache_stats() {
    let cache = SchemaCache::new(60);

    let schema = test_schema(vec![("id", "UInt64")]);

    // Insert some schemas
    cache.insert("table1".to_string(), schema.clone());
    cache.insert("table2".to_string(), schema.clone());
    cache.insert("table3".to_string(), schema.clone());

    // Access them to generate hit stats
    cache.get("table1");
    cache.get("table1");
    cache.get("table2");
    cache.get("nonexistent"); // Returns None but doesn't track as miss (entry doesn't exist)

    let stats = cache.stats();
    assert_eq!(stats.total, 3);
    assert!(stats.hits >= 3);
    // Note: misses are only counted for expired entries, not non-existent ones

    eprintln!("✓ Schema cache stats: {stats:?}");
}

#[test]
fn test_schema_cache_with_config() {
    let config = SchemaCacheConfig {
        ttl_secs: 300,
        auto_refresh: true,
        refresh_interval_secs: 60,
        refresh_headroom_secs: 30,
    };

    let cache = SchemaCache::with_config(config);

    let schema = test_schema(vec![("id", "UInt64")]);

    cache.insert("test".to_string(), schema);
    assert!(cache.get("test").is_some());

    eprintln!("✓ Schema cache with config works");
}

#[test]
fn test_schema_cache_ttl_expiry() {
    // Use a very short TTL for testing
    let config = SchemaCacheConfig {
        ttl_secs: 1, // 1 second TTL
        auto_refresh: false,
        refresh_interval_secs: 60,
        refresh_headroom_secs: 0,
    };

    let cache = SchemaCache::with_config(config);

    let schema = test_schema(vec![("id", "UInt64")]);

    cache.insert("test".to_string(), schema);
    assert!(cache.get("test").is_some());

    // Wait for TTL to expire
    std::thread::sleep(Duration::from_millis(1100));

    // Should return None after expiry
    assert!(cache.get("test").is_none());

    eprintln!("✓ Schema cache TTL expiry works");
}

#[test]
fn test_schema_cache_needs_refresh() {
    let config = SchemaCacheConfig {
        ttl_secs: 5,
        auto_refresh: true,
        refresh_interval_secs: 60,
        refresh_headroom_secs: 3, // Refresh when 3 seconds left
    };

    let cache = SchemaCache::with_config(config);

    // Empty cache needs refresh
    assert!(cache.needs_refresh("nonexistent"));

    let schema = test_schema(vec![("id", "UInt64")]);

    cache.insert("test".to_string(), schema);

    // Fresh entry doesn't need refresh
    assert!(!cache.needs_refresh("test"));

    eprintln!("✓ Schema cache needs_refresh works");
}

// ============================================================================
// Integration Tests with ClickHouse
// ============================================================================

#[tokio::test]
async fn test_schema_introspection_from_clickhouse() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("schema_test");
    let oc = crate::common::on_cluster_clause();

    // Create test table with various types
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String,
            score Float64,
            created DateTime64(3),
            active Bool,
            tags Array(String)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Get schema from ClickHouse
    let schema_result = client.fetch_table_schema(&table_name).await;
    assert!(
        schema_result.is_ok(),
        "Failed to get schema: {:?}",
        schema_result.err()
    );

    let schema = schema_result.unwrap();
    assert_eq!(schema.columns.len(), 6);

    // Verify column types
    let id_col = schema.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.type_name, "UInt64");

    let name_col = schema.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.type_name, "String");

    eprintln!("✓ Schema introspection: {} columns", schema.columns.len());

    drop_http_test_table(&client, &table_name).await;
}

/// The same reads over the NATIVE transport, which is the gap that shipped:
/// a native deployment could not fetch a schema at all, because the fork's
/// client had no URL and the query died in `Url::parse` before it opened a
/// socket. Nothing here can reach HTTP, so every read is over TCP or it fails.
#[tokio::test]
async fn schema_reads_work_over_the_native_transport() {
    skip_if_no_clickhouse!();

    let Some(client) = create_native_test_client() else {
        return;
    };

    let table_name = unique_table_name("native_schema");
    let oc = crate::common::on_cluster_clause();
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String,
            score Float64
        ) ENGINE = MergeTree() ORDER BY tuple() COMMENT 'native path'"
    );
    client.execute(&ddl).await.expect("DDL over native");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("system.columns over native");
    assert_eq!(schema.column_names(), ["id", "name", "score"]);
    assert_eq!(
        schema
            .column("score")
            .expect("declared above")
            .type_name
            .as_str(),
        "Float64"
    );
    assert_eq!(schema.comment, "native path");

    assert!(client.list_tables().await.expect("system.tables over native").iter().any(|t| t == table_name.split('.').next_back().unwrap_or(&table_name)));
    assert_eq!(
        client
            .query_count(&table_name, None)
            .await
            .expect("count over native"),
        0
    );
    client.health_check().await.expect("ping over native");

    drop_http_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_schema_cache_with_clickhouse() {
    skip_if_no_clickhouse!();

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("cache_test");
    let oc = crate::common::on_cluster_clause();

    // Create test table
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            data String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Create cache and fetch schema
    let cache = SchemaCache::new(300);

    // Get schema from ClickHouse and cache it
    let schema = client.fetch_table_schema(&table_name).await.unwrap();
    cache.insert(table_name.clone(), schema.clone());

    // Subsequent access should hit cache
    let cached = cache.get(&table_name);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().columns.len(), 2);

    let stats = cache.stats();
    assert!(stats.hits >= 1);

    eprintln!("✓ Schema cache with ClickHouse: {} hits", stats.hits);

    drop_http_test_table(&client, &table_name).await;
}

/// Regression test for issue #25 — schema cache must NOT expire when the
/// background refresh task is running. Without this, the orchestrator falls
/// back from the extractor (json_primary) path to the transformer path,
/// silently dropping `@renamed` directive mappings.
///
/// Test strategy:
/// - Live ClickHouse table (so `fetch_table_schema()` actually works)
/// - Short TTL (2s) + headroom (1s) + interval (500ms) — total runtime ~5s
/// - Spawn background refresh, insert schema once
/// - Wait past TTL — schema must still be retrievable (background kept it warm)
#[tokio::test]
async fn test_schema_cache_background_refresh_keeps_warm() {
    skip_if_no_clickhouse!();

    use std::sync::Arc;

    let client = if let Some(c) = create_http_test_client() {
        Arc::new(c)
    } else {
        eprintln!("Could not create HTTP client");
        return;
    };

    let table_name = unique_table_name("test_bg_refresh");
    let oc = crate::common::on_cluster_clause();

    // Create a real table so fetch_table_schema() can succeed
    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            name String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    // Short timings — proves the background refresh actually ran
    let config = SchemaCacheConfig {
        ttl_secs: 2,
        auto_refresh: true,
        refresh_interval_secs: 1, // wake every 1s
        refresh_headroom_secs: 1, // refresh when 1s left → triggers around t=1s
    };
    let cache = Arc::new(SchemaCache::with_config(config));

    // Pre-populate the cache so background refresh has something to work on
    let schema = client.fetch_table_schema(&table_name).await.unwrap();
    cache.insert(table_name.clone(), schema);
    assert!(
        cache.get(&table_name).is_some(),
        "Cache should be populated"
    );

    // Start the background refresh task (the bit that was missing)
    let _handle = cache.start_background_refresh(Arc::clone(&client));

    // Wait past TTL. Without background refresh, get() would return None here.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    // The bug fix: schema must still be available because the background task
    // refreshed it before TTL expired.
    let cached = cache.get(&table_name);
    assert!(
        cached.is_some(),
        "Schema cache should stay warm via background refresh. \
         If this fails, issue #25 has regressed and the extractor path will \
         silently fall back to the transformer path after TTL expiry."
    );
    assert_eq!(cached.unwrap().columns.len(), 2);

    // At least one refresh should have happened
    let stats = cache.stats();
    assert!(
        stats.refreshes >= 1,
        "Background refresh should have run at least once (got {})",
        stats.refreshes
    );

    eprintln!(
        "✓ Background refresh kept schema warm past TTL ({} refreshes)",
        stats.refreshes
    );

    // Cleanup
    cache.shutdown();
    drop_http_test_table(&client, &table_name).await;
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_schema_cache_config_defaults() {
    let config = SchemaCacheConfig::default();
    assert_eq!(config.ttl_secs, 300);
    // auto_refresh on by default — see issue #25 (silent extractor→transformer fallback on TTL expiry).
    assert!(config.auto_refresh);
    assert_eq!(config.refresh_interval_secs, 60);
    assert_eq!(config.refresh_headroom_secs, 30);
}
