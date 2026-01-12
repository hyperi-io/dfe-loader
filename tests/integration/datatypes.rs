//! Data types integration tests
//!
//! Tests for various ClickHouse data types via Arrow inserts

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, RecordBatch, StringArray, TimestampMillisecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::common::{create_test_client, drop_test_table, unique_table_name};
use crate::skip_if_no_clickhouse;

// ============================================================================
// Integer Types
// ============================================================================

#[tokio::test]
async fn test_integer_types() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_integers");

    let ddl = format!(
        "CREATE TABLE {} (
            i8_col Int8,
            i16_col Int16,
            i32_col Int32,
            i64_col Int64,
            u8_col UInt8,
            u16_col UInt16,
            u32_col UInt32,
            u64_col UInt64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("i8_col", DataType::Int8, false),
        Field::new("i16_col", DataType::Int16, false),
        Field::new("i32_col", DataType::Int32, false),
        Field::new("i64_col", DataType::Int64, false),
        Field::new("u8_col", DataType::UInt8, false),
        Field::new("u16_col", DataType::UInt16, false),
        Field::new("u32_col", DataType::UInt32, false),
        Field::new("u64_col", DataType::UInt64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int8Array::from(vec![-128, 0, 127])) as ArrayRef,
            Arc::new(Int16Array::from(vec![-32768, 0, 32767])) as ArrayRef,
            Arc::new(Int32Array::from(vec![-2147483648, 0, 2147483647])) as ArrayRef,
            Arc::new(Int64Array::from(vec![-9223372036854775808i64, 0, 9223372036854775807i64])) as ArrayRef,
            Arc::new(UInt8Array::from(vec![0, 128, 255])) as ArrayRef,
            Arc::new(UInt16Array::from(vec![0, 32768, 65535])) as ArrayRef,
            Arc::new(UInt32Array::from(vec![0, 2147483648, 4294967295])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![0u64, 9223372036854775808u64, 18446744073709551615u64])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "Integer insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Integer types insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Float Types
// ============================================================================

#[tokio::test]
async fn test_float_types() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_floats");

    let ddl = format!(
        "CREATE TABLE {} (
            f32_col Float32,
            f64_col Float64
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("f32_col", DataType::Float32, false),
        Field::new("f64_col", DataType::Float64, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Float32Array::from(vec![0.0f32, 3.14159f32, -1.5e10f32])) as ArrayRef,
            Arc::new(Float64Array::from(vec![0.0f64, 3.141592653589793f64, -1.5e100f64])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "Float insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Float types insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// String Types
// ============================================================================

#[tokio::test]
async fn test_string_types() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_strings");

    let ddl = format!(
        "CREATE TABLE {} (
            str_col String,
            fixed_col FixedString(10)
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("str_col", DataType::Utf8, false),
        Field::new("fixed_col", DataType::Utf8, false), // Arrow sends as Utf8
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["hello", "world", "test string with unicode: 日本語"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["0123456789", "abc", "short"])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "String insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ String types insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// DateTime Types
// ============================================================================

#[tokio::test]
async fn test_datetime_types() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_datetime");

    let ddl = format!(
        "CREATE TABLE {} (
            dt64_ms DateTime64(3),
            dt64_us DateTime64(6),
            date_col Date
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let now_ms = chrono::Utc::now().timestamp_millis();
    let now_us = chrono::Utc::now().timestamp_micros();
    let today = (chrono::Utc::now().timestamp() / 86400) as i32; // Days since epoch

    let schema = Arc::new(Schema::new(vec![
        Field::new("dt64_ms", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("dt64_us", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("date_col", DataType::Date32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![now_ms, now_ms + 1000, now_ms + 2000])) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(vec![now_us, now_us + 1000000, now_us + 2000000])) as ArrayRef,
            Arc::new(Date32Array::from(vec![today, today + 1, today + 2])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "DateTime insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ DateTime types insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Boolean Type
// ============================================================================

#[tokio::test]
async fn test_boolean_type() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_bool");

    let ddl = format!(
        "CREATE TABLE {} (
            bool_col Bool
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("bool_col", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(BooleanArray::from(vec![true, false, true, false])) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "Boolean insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 4);

    eprintln!("✓ Boolean type insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Nullable Types
// ============================================================================

#[tokio::test]
async fn test_nullable_types() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_nullable");

    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            nullable_str Nullable(String),
            nullable_int Nullable(Int64),
            nullable_float Nullable(Float64)
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("nullable_str", DataType::Utf8, true),
        Field::new("nullable_int", DataType::Int64, true),
        Field::new("nullable_float", DataType::Float64, true),
    ]));

    // Create arrays with nulls
    let str_values: Vec<Option<&str>> = vec![Some("a"), None, Some("c"), None, Some("e")];
    let int_values: Vec<Option<i64>> = vec![Some(1), Some(2), None, Some(4), None];
    let float_values: Vec<Option<f64>> = vec![None, Some(2.0), Some(3.0), None, Some(5.0)];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef,
            Arc::new(StringArray::from(str_values)) as ArrayRef,
            Arc::new(Int64Array::from(int_values)) as ArrayRef,
            Arc::new(Float64Array::from(float_values)) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "Nullable insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 5);

    eprintln!("✓ Nullable types insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// LowCardinality Type
// ============================================================================

#[tokio::test]
async fn test_low_cardinality_type() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_lowcard");

    let ddl = format!(
        "CREATE TABLE {} (
            id UInt64,
            category LowCardinality(String)
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("category", DataType::Utf8, false), // Arrow sends as Utf8
    ]));

    // Create data with low cardinality (repeated values)
    let categories = vec!["auth", "api", "web", "auth", "api", "auth", "web", "api", "auth", "web"];

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from((0..10).collect::<Vec<u64>>())) as ArrayRef,
            Arc::new(StringArray::from(categories)) as ArrayRef,
        ],
    )
    .unwrap();

    let result = client.insert(&table_name, batch).await;
    assert!(result.is_ok(), "LowCardinality insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 10);

    eprintln!("✓ LowCardinality type insert succeeded");

    drop_test_table(&client, &table_name).await;
}

// ============================================================================
// Mixed Types (realistic event table)
// ============================================================================

#[tokio::test]
async fn test_realistic_event_table() {
    skip_if_no_clickhouse!();

    let client = create_test_client().await.unwrap();
    let table_name = unique_table_name("test_events");

    let ddl = format!(
        "CREATE TABLE {} (
            timestamp DateTime64(3),
            event_id UInt64,
            org_id String,
            event_type LowCardinality(String),
            user_id Nullable(UInt64),
            action String,
            value Float64,
            success Bool,
            metadata String
        ) ENGINE = Memory",
        table_name
    );
    client.query(&ddl).await.expect("Failed to create table");

    let row_count: usize = 1000;
    let now_ms = chrono::Utc::now().timestamp_millis();
    let event_types = ["login", "logout", "purchase", "view", "click"];
    let orgs = ["org1", "org2", "org3"];

    let timestamps: Vec<i64> = (0..row_count).map(|i| now_ms + i as i64 * 100).collect();
    let event_ids: Vec<u64> = (0..row_count as u64).collect();
    let org_ids: Vec<&str> = (0..row_count).map(|i| orgs[i % orgs.len()]).collect();
    let types: Vec<&str> = (0..row_count).map(|i| event_types[i % event_types.len()]).collect();
    let user_ids: Vec<Option<u64>> = (0..row_count).map(|i| if i % 10 == 0 { None } else { Some(i as u64 * 100) }).collect();
    let actions: Vec<String> = (0..row_count).map(|i| format!("action_{}", i % 20)).collect();
    let values: Vec<f64> = (0..row_count).map(|i| i as f64 * 0.1).collect();
    let successes: Vec<bool> = (0..row_count).map(|i| i % 5 != 0).collect();
    let metadata: Vec<String> = (0..row_count).map(|i| format!("{{\"key\": \"value_{}\"}}", i)).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("event_id", DataType::UInt64, false),
        Field::new("org_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("user_id", DataType::UInt64, true),
        Field::new("action", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("success", DataType::Boolean, false),
        Field::new("metadata", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef,
            Arc::new(UInt64Array::from(event_ids)) as ArrayRef,
            Arc::new(StringArray::from(org_ids)) as ArrayRef,
            Arc::new(StringArray::from(types)) as ArrayRef,
            Arc::new(UInt64Array::from(user_ids)) as ArrayRef,
            Arc::new(StringArray::from(actions)) as ArrayRef,
            Arc::new(Float64Array::from(values)) as ArrayRef,
            Arc::new(BooleanArray::from(successes)) as ArrayRef,
            Arc::new(StringArray::from(metadata)) as ArrayRef,
        ],
    )
    .unwrap();

    let start = std::time::Instant::now();
    let result = client.insert(&table_name, batch).await;
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Event insert failed: {:?}", result.err());
    let count = result.unwrap();
    assert_eq!(count, row_count);

    eprintln!(
        "✓ Realistic event table: {} rows in {:?} ({:.0} rows/sec)",
        count,
        elapsed,
        count as f64 / elapsed.as_secs_f64()
    );

    // Query back to verify row count
    let count_sql = format!("SELECT COUNT(*) as count FROM {}", table_name);
    let count_result = client.select(&count_sql).await.expect("Count query failed");
    let count_batch = &count_result[0];
    let count_col = count_batch.column(0).as_any().downcast_ref::<UInt64Array>().expect("Count should be UInt64");
    assert_eq!(count_col.value(0), row_count as u64, "Should have {} rows", row_count);
    eprintln!("✓ Query verification: confirmed {} rows", row_count);

    // Verify org_id distribution
    let org_sql = format!("SELECT org_id, COUNT(*) as count FROM {} GROUP BY org_id ORDER BY org_id", table_name);
    let org_result = client.select(&org_sql).await.expect("Org query failed");
    let org_batch = &org_result[0];
    assert_eq!(org_batch.num_rows(), orgs.len(), "Should have {} distinct orgs", orgs.len());
    eprintln!("✓ Query verification: confirmed {} distinct orgs", orgs.len());

    // Verify nullable field handling
    let null_sql = format!("SELECT COUNT(*) as count FROM {} WHERE user_id IS NULL", table_name);
    let null_result = client.select(&null_sql).await.expect("Null query failed");
    let null_batch = &null_result[0];
    let null_col = null_batch.column(0).as_any().downcast_ref::<UInt64Array>().expect("Count should be UInt64");
    let expected_nulls = (row_count / 10) as u64;
    assert_eq!(null_col.value(0), expected_nulls, "Should have {} NULL user_ids", expected_nulls);
    eprintln!("✓ Query verification: confirmed {} NULL user_ids", expected_nulls);

    drop_test_table(&client, &table_name).await;
}
