//! Schema cache integration tests
//!
//! Tests for schema introspection, caching, and refresh

use std::sync::Arc;
use std::time::Duration;

use dfe_loader::clickhouse::schema::{SchemaCache, SchemaCacheConfig};
use dfe_loader::clickhouse::{ColumnInfo, ParsedType, TableSchema};

use crate::common::{create_test_client, drop_test_table, unique_table_name};
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
        columns: columns.into_iter().map(|(n, t)| test_column(n, t)).collect(),
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

    eprintln!("✓ Schema cache stats: {:?}", stats);
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

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };

    let table_name = unique_table_name("schema_test");

    // Create test table with various types
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            name String,
            score Float64,
            created DateTime64(3),
            active Bool,
            tags Array(String)
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    // Get schema from ClickHouse
    let schema_result = client.fetch_table_schema(&table_name).await;
    assert!(schema_result.is_ok(), "Failed to get schema: {:?}", schema_result.err());

    let schema = schema_result.unwrap();
    assert_eq!(schema.columns.len(), 6);

    // Verify column types
    let id_col = schema.columns.iter().find(|c| c.name == "id").unwrap();
    assert_eq!(id_col.type_name, "UInt64");

    let name_col = schema.columns.iter().find(|c| c.name == "name").unwrap();
    assert_eq!(name_col.type_name, "String");

    eprintln!("✓ Schema introspection: {} columns", schema.columns.len());

    drop_test_table(&client, &table_name).await;
}

#[tokio::test]
async fn test_schema_cache_with_clickhouse() {
    skip_if_no_clickhouse!();

    let client = match create_test_client().await {
        Some(c) => Arc::new(c),
        None => return,
    };

    let table_name = unique_table_name("cache_test");

    // Create test table
    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            data String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

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

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Config Tests
// ============================================================================

#[test]
fn test_schema_cache_config_defaults() {
    let config = SchemaCacheConfig::default();
    assert_eq!(config.ttl_secs, 300);
    assert!(!config.auto_refresh); // Default is false
    assert_eq!(config.refresh_interval_secs, 60);
    assert_eq!(config.refresh_headroom_secs, 30);
}
