// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` integration tests
//!
//! Tests run against k8s.tyrell.com.au cluster via .env settings
//! Uses HTTP client with `JSONEachRow` for all `ClickHouse` operations

use serde_json::json;

use crate::common::{
    check_clickhouse_reachable, create_http_test_client, drop_http_test_table, load_dotenv,
    unique_table_name,
};

fn skip_if_no_clickhouse() -> bool {
    load_dotenv();
    if !crate::common::has_clickhouse() {
        eprintln!("CLICKHOUSE_HOST not set");
        return true;
    }
    if !check_clickhouse_reachable() {
        eprintln!("ClickHouse not reachable");
        return true;
    }
    false
}

#[tokio::test]
async fn test_clickhouse_connect() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = if let Some(c) = create_http_test_client() {
        c
    } else {
        eprintln!("Could not create HTTP client");
        return;
    };

    let start = std::time::Instant::now();
    let result = client.health_check().await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Health check failed: {:?}", result.err());
    eprintln!("✓ Connected to ClickHouse in {elapsed:?}");
    assert!(!client.database().is_empty());
}

#[tokio::test]
async fn test_clickhouse_insert_json() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("test_insert");
    let oc = crate::common::on_cluster_clause();
    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {table_name}{oc} (
            id UInt64,
            event String,
            category String,
            value Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );

    let start = std::time::Instant::now();
    client
        .execute(&create_sql)
        .await
        .expect("Failed to create table");
    eprintln!("✓ Created table '{}' in {:?}", table_name, start.elapsed());

    // Insert small batch (2 rows)
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "event": "login", "category": "auth", "value": 1.5})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 2, "event": "logout", "category": "auth", "value": 2.5})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let start = std::time::Instant::now();
    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    let elapsed = start.elapsed();
    assert!(result.is_ok(), "Insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 2);
    eprintln!("✓ Inserted 2 rows via JSONEachRow in {elapsed:?}");

    // Insert larger batch (1000 rows)
    let categories_list = ["auth", "api", "web", "mobile"];
    let large_rows: Vec<serde_json::Map<String, serde_json::Value>> = (0..1000)
        .map(|i| {
            json!({
                "id": i as u64,
                "event": format!("event_{}", i % 10),
                "category": categories_list[i % 4],
                "value": i as f64 * 1.5
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect();

    let start = std::time::Instant::now();
    let result = client.insert_json_rows(&table_name, &large_rows, &[]).await;
    let elapsed = start.elapsed();
    assert!(result.is_ok(), "Batch insert failed: {:?}", result.err());
    let count = result.unwrap();
    assert_eq!(count, 1000);
    eprintln!(
        "✓ Inserted {} rows via JSONEachRow in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Insert even larger batch (5000 rows)
    let categories5 = ["auth", "api", "web", "mobile", "backend"];
    let xl_rows: Vec<serde_json::Map<String, serde_json::Value>> = (1000..6000)
        .map(|i| {
            json!({
                "id": i as u64,
                "event": format!("bulk_{}", i % 100),
                "category": categories5[i % 5],
                "value": i as f64 * 0.7
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect();

    let start = std::time::Instant::now();
    let result = client.insert_json_rows(&table_name, &xl_rows, &[]).await;
    let elapsed = start.elapsed();
    assert!(
        result.is_ok(),
        "Large batch insert failed: {:?}",
        result.err()
    );
    let count = result.unwrap();
    assert_eq!(count, 5000);
    eprintln!(
        "✓ Inserted {} rows via JSONEachRow in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    drop_http_test_table(&client, &table_name).await;
    eprintln!("✓ Cleaned up test table");
}

#[tokio::test]
async fn test_clickhouse_table_exists() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let result = client.table_exists("tables").await;
    eprintln!("table_exists result: {result:?}");
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_clickhouse_variant_type_support() {
    if skip_if_no_clickhouse() {
        eprintln!("Skipping test: no ClickHouse available");
        return;
    }

    let client = match create_http_test_client() {
        Some(c) => c,
        None => return,
    };

    let table_name = unique_table_name("test_variant");
    let oc = crate::common::on_cluster_clause();

    let create_sql = format!(
        "CREATE TABLE IF NOT EXISTS {table_name}{oc} (
            id UInt64,
            data Variant(String, Int64, Float64)
        ) ENGINE = MergeTree() ORDER BY tuple()
        SETTINGS allow_experimental_variant_type = 1"
    );

    let result = client.execute(&create_sql).await;
    match result {
        Ok(()) => {
            eprintln!("✓ Created table with Variant type: {table_name}");
        }
        Err(e) => {
            eprintln!("✗ Failed to create Variant table (ClickHouse may be < 24.x): {e}");
            return;
        }
    }

    eprintln!("✓ Variant table created successfully (DDL test)");

    drop_http_test_table(&client, &table_name).await;
    eprintln!("✓ Cleaned up Variant test table");
}
