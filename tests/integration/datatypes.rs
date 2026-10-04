// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Data types integration tests
//!
//! Tests for various `ClickHouse` data types via `JSONEachRow` inserts, and
//! every column type the dynamic encoder supports through the `RowBinary`
//! insert path. Includes coercion tests: verify that the Coercer correctly
//! transforms ambiguous input values before they reach `ClickHouse`.
//!
//! Each test runs against the `ClickHouse` the environment configures when one
//! answers, else a testcontainer of its own.

#![allow(clippy::approx_constant)]

use std::sync::Arc;

use serde_json::{Map, Value, json};

use dfe_loader::clickhouse::config::{ClickHouseConfig, InsertFormat, Transport};
use dfe_loader::clickhouse::{ClickHouseQueryClient, Inserter, InserterConfig};

use crate::common::containers::TestInfrastructure;
use crate::common::{
    ClickHouseTestConfig, TestMode, create_http_test_client, on_cluster_clause, unique_table_name,
};
use crate::test_name;

/// The `ClickHouse` a datatypes test runs against.
struct Target {
    client: Arc<ClickHouseQueryClient>,
    url: String,
    user: String,
    password: String,
    database: String,
    on_cluster: &'static str,
    /// Holds the testcontainer, when there is one, for the test's lifetime.
    _infra: Option<TestInfrastructure>,
}

impl Target {
    /// The configured external `ClickHouse` when one is named and answers,
    /// else a testcontainer. In CI a missing Docker daemon fails the test
    /// instead of skipping it.
    async fn new(test: &str) -> Self {
        let ch = ClickHouseTestConfig::from_env();
        let configured =
            TestMode::detect() == TestMode::Docker || std::env::var_os("CLICKHOUSE_HOST").is_some();
        if configured && ch.is_reachable() {
            let client = create_http_test_client().expect("a client for the configured ClickHouse");
            return Self {
                client: Arc::new(client),
                url: ch.http_url(),
                user: ch.user,
                password: ch.password,
                database: ch.database,
                on_cluster: on_cluster_clause(),
                _infra: None,
            };
        }

        let infra = TestInfrastructure::new(test, true, false).await;
        let container = infra.clickhouse.as_ref().expect("ClickHouse container");
        let host = container
            .get_host()
            .await
            .expect("container host")
            .to_string();
        let port = container
            .get_host_port_ipv4(8123)
            .await
            .expect("HTTP port mapping");
        let client = ClickHouseQueryClient::new(&ClickHouseConfig {
            hosts: vec![format!("{host}:{port}")],
            transport: Transport::Http,
            database: "default".to_string(),
            username: "default".to_string(),
            password: String::new(),
            tls: false,
            ..Default::default()
        })
        .expect("a client for the ClickHouse container");
        for _ in 0..50 {
            if client.health_check().await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        Self {
            client: Arc::new(client),
            url: format!("http://{host}:{port}"),
            user: "default".to_string(),
            password: String::new(),
            database: "default".to_string(),
            on_cluster: "",
            _infra: Some(infra),
        }
    }

    /// A clickhouse-rs client for the same server, for inserts and reads.
    fn rs_client(&self) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(&self.url)
            .with_user(&self.user)
            .with_password(&self.password)
            .with_database(&self.database)
    }

    /// Drop `table`, on the cluster where there is one.
    async fn drop(&self, table: &str) {
        let _ = self
            .client
            .execute(&format!("DROP TABLE IF EXISTS {table}{}", self.on_cluster))
            .await;
    }
}

#[tokio::test]
async fn test_integer_types() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_integers");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            i8_col Int8,
            i16_col Int16,
            i32_col Int32,
            i64_col Int64,
            u8_col UInt8,
            u16_col UInt16,
            u32_col UInt32,
            u64_col UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"i8_col": -128, "i16_col": -32768, "i32_col": -2147483648_i64, "i64_col": -9223372036854775808_i64, "u8_col": 0, "u16_col": 0, "u32_col": 0, "u64_col": 0}).as_object().unwrap().clone(),
        json!({"i8_col": 0, "i16_col": 0, "i32_col": 0, "i64_col": 0, "u8_col": 128, "u16_col": 32768, "u32_col": 2147483648_u64, "u64_col": 9223372036854775808_u64}).as_object().unwrap().clone(),
        json!({"i8_col": 127, "i16_col": 32767, "i32_col": 2147483647, "i64_col": 9223372036854775807_i64, "u8_col": 255, "u16_col": 65535, "u32_col": 4294967295_u64, "u64_col": 18446744073709551615_u64}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Integer insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Integer types insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_float_types() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_floats");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            f32_col Float32,
            f64_col Float64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"f32_col": 0.0, "f64_col": 0.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"f32_col": 3.14159, "f64_col": 3.141592653589793})
            .as_object()
            .unwrap()
            .clone(),
        json!({"f32_col": -1.5e10, "f64_col": -1.5e100})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Float insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ Float types insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_string_types() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_strings");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            str_col String,
            fixed_col FixedString(10)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"str_col": "hello", "fixed_col": "0123456789"})
            .as_object()
            .unwrap()
            .clone(),
        json!({"str_col": "world", "fixed_col": "abc"})
            .as_object()
            .unwrap()
            .clone(),
        json!({"str_col": "test string with unicode: 日本語", "fixed_col": "short"})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "String insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ String types insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_datetime_types() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_datetime");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            dt64_ms DateTime64(3),
            date_col Date
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let now = chrono::Utc::now();
    // DateTime64(3) accepts milliseconds integer or 'YYYY-MM-DD HH:MM:SS.mmm' string.
    // RFC3339 with timezone and sub-ms precision is not reliably supported.
    let now_ms = now.timestamp_millis();
    let now_str = now.format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let today = now.format("%Y-%m-%d").to_string();

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"dt64_ms": now_ms, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
        json!({"dt64_ms": &now_str, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
        json!({"dt64_ms": now_ms, "date_col": &today})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "DateTime insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 3);

    eprintln!("✓ DateTime types insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_boolean_type() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_bool");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            bool_col Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
        json!({"bool_col": true}).as_object().unwrap().clone(),
        json!({"bool_col": false}).as_object().unwrap().clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Boolean insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 4);

    eprintln!("✓ Boolean type insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_nullable_types() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_nullable");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            nullable_str Nullable(String),
            nullable_int Nullable(Int64),
            nullable_float Nullable(Float64)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = vec![
        json!({"id": 1, "nullable_str": "a", "nullable_int": 1, "nullable_float": null})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 2, "nullable_str": null, "nullable_int": 2, "nullable_float": 2.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 3, "nullable_str": "c", "nullable_int": null, "nullable_float": 3.0})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 4, "nullable_str": null, "nullable_int": 4, "nullable_float": null})
            .as_object()
            .unwrap()
            .clone(),
        json!({"id": 5, "nullable_str": "e", "nullable_int": null, "nullable_float": 5.0})
            .as_object()
            .unwrap()
            .clone(),
    ];

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(result.is_ok(), "Nullable insert failed: {:?}", result.err());
    assert_eq!(result.unwrap(), 5);

    eprintln!("✓ Nullable types insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_low_cardinality_type() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_lowcard");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UInt64,
            category LowCardinality(String)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let categories = [
        "auth", "api", "web", "auth", "api", "auth", "web", "api", "auth", "web",
    ];
    let rows: Vec<serde_json::Map<String, serde_json::Value>> = categories
        .iter()
        .enumerate()
        .map(|(i, cat)| {
            json!({"id": i as u64, "category": cat})
                .as_object()
                .unwrap()
                .clone()
        })
        .collect();

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "LowCardinality insert failed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 10);

    eprintln!("✓ LowCardinality type insert succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_realistic_event_table() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_events");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            timestamp DateTime64(3),
            event_id UInt64,
            org_id String,
            event_type LowCardinality(String),
            user_id Nullable(UInt64),
            action String,
            value Float64,
            success Bool,
            metadata String
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let row_count: usize = 1000;
    let now = chrono::Utc::now();
    let event_types = ["login", "logout", "purchase", "view", "click"];
    let orgs = ["org1", "org2", "org3"];

    let rows: Vec<serde_json::Map<String, serde_json::Value>> = (0..row_count)
        .map(|i| {
            let ts = now.timestamp_millis() + (i as i64 * 100);
            let user_id: serde_json::Value = if i % 10 == 0 {
                serde_json::Value::Null
            } else {
                json!(i as u64 * 100)
            };
            json!({
                "timestamp": ts,
                "event_id": i as u64,
                "org_id": orgs[i % orgs.len()],
                "event_type": event_types[i % event_types.len()],
                "user_id": user_id,
                "action": format!("action_{}", i % 20),
                "value": i as f64 * 0.1,
                "success": i % 5 != 0,
                "metadata": format!("{{\"key\": \"value_{}\"}}", i)
            })
            .as_object()
            .unwrap()
            .clone()
        })
        .collect();

    let start = std::time::Instant::now();
    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
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

    target.drop(&table_name).await;
}

// ============================================================================
// Every supported column type through the RowBinary insert path
// ============================================================================

/// One column type sent through the `RowBinary` insert: the value sent, the
/// SQL expression over `v` that reads it back as text, and the text expected.
struct TypeCase {
    ty: &'static str,
    value: Value,
    read: &'static str,
    expected: &'static str,
}

/// A value read back as text.
#[derive(clickhouse::Row, serde::Deserialize)]
struct Text {
    s: String,
}

fn case(ty: &'static str, value: Value, read: &'static str, expected: &'static str) -> TypeCase {
    TypeCase {
        ty,
        value,
        read,
        expected,
    }
}

/// Every column type the dynamic encoder writes, with a value and how it reads
/// back. 1790416800 is 2026-09-26 10:00:00 UTC.
fn supported_type_cases() -> Vec<TypeCase> {
    const TEXT: &str = "toString(v)";
    const OR_NULL: &str = "ifNull(toString(v), 'NULL')";
    vec![
        case("String", json!("hello"), TEXT, "hello"),
        case("FixedString(5)", json!("abc"), "hex(v)", "6162630000"),
        case("UInt8", json!(255), TEXT, "255"),
        case("UInt16", json!(65535), TEXT, "65535"),
        case("UInt32", json!(4_294_967_295_u64), TEXT, "4294967295"),
        case("UInt64", json!(u64::MAX), TEXT, "18446744073709551615"),
        case(
            "UInt128",
            json!("340282366920938463463374607431768211455"),
            TEXT,
            "340282366920938463463374607431768211455",
        ),
        case("UInt256", json!(5), TEXT, "5"),
        case("Int8", json!(-128), TEXT, "-128"),
        case("Int16", json!(-32768), TEXT, "-32768"),
        case("Int32", json!(-2_147_483_648_i64), TEXT, "-2147483648"),
        case("Int64", json!(i64::MIN), TEXT, "-9223372036854775808"),
        case(
            "Int128",
            json!("-170141183460469231731687303715884105728"),
            TEXT,
            "-170141183460469231731687303715884105728",
        ),
        case("Int256", json!(-5), TEXT, "-5"),
        case("Float32", json!(1.5), TEXT, "1.5"),
        case(
            "Float64",
            json!(3.141592653589793),
            TEXT,
            "3.141592653589793",
        ),
        case("Bool", json!(true), TEXT, "true"),
        case("Date", json!("2026-09-26"), TEXT, "2026-09-26"),
        case("Date32", json!("1960-01-01"), TEXT, "1960-01-01"),
        case(
            "DateTime",
            json!(1_790_416_800),
            "toString(toUnixTimestamp(v))",
            "1790416800",
        ),
        case(
            "DateTime('UTC')",
            json!("2026-09-26 10:00:00"),
            TEXT,
            "2026-09-26 10:00:00",
        ),
        case(
            "Nullable(DateTime('UTC'))",
            json!(1_790_416_800),
            OR_NULL,
            "2026-09-26 10:00:00",
        ),
        case(
            "DateTime('Australia/Sydney')",
            json!(1_790_416_800),
            "toString(toUnixTimestamp(v))",
            "1790416800",
        ),
        case(
            "DateTime64(3)",
            json!(1_790_416_800_123_i64),
            "toString(toUnixTimestamp64Milli(v))",
            "1790416800123",
        ),
        case(
            "DateTime64(6, 'UTC')",
            json!("2026-09-26 10:00:00.123456"),
            TEXT,
            "2026-09-26 10:00:00.123456",
        ),
        case(
            "DateTime64(3, 'Australia/Sydney')",
            json!(1_790_416_800_123_i64),
            "toString(toUnixTimestamp64Milli(v))",
            "1790416800123",
        ),
        case("Decimal(9, 2)", json!(123.45), TEXT, "123.45"),
        case("Decimal(18, 4)", json!(12345.6789), TEXT, "12345.6789"),
        case("Decimal(38, 6)", json!(1.5), TEXT, "1.5"),
        case("Decimal(76, 10)", json!(2.5), TEXT, "2.5"),
        case(
            "UUID",
            json!("550e8400-e29b-41d4-a716-446655440000"),
            TEXT,
            "550e8400-e29b-41d4-a716-446655440000",
        ),
        case("IPv4", json!("192.168.1.1"), TEXT, "192.168.1.1"),
        case("IPv6", json!("2001:db8::1"), TEXT, "2001:db8::1"),
        case(
            "Nullable(IPv6)",
            json!("10.0.0.1"),
            OR_NULL,
            "::ffff:10.0.0.1",
        ),
        case("Enum8('a' = 1, 'b' = 2)", json!("b"), TEXT, "b"),
        case("Enum16('x' = 1000, 'y' = 2000)", json!("y"), TEXT, "y"),
        case("Enum8('a' = 1, 'b' = 2)", json!(2), TEXT, "b"),
        case(
            "Nullable(Enum8('on' = 1, 'off' = 2))",
            json!("off"),
            OR_NULL,
            "off",
        ),
        case("Array(String)", json!(["a", "b"]), TEXT, "['a','b']"),
        case("Array(UInt32)", json!([1, 2, 3]), TEXT, "[1,2,3]"),
        case("Array(Nullable(Int64))", json!([1, null]), TEXT, "[1,NULL]"),
        case(
            "Map(String, UInt64)",
            json!({"a": 1, "b": 2}),
            TEXT,
            "{'a':1,'b':2}",
        ),
        case(
            "Map(LowCardinality(String), String)",
            json!({"k": "v"}),
            TEXT,
            "{'k':'v'}",
        ),
        case("LowCardinality(String)", json!("x"), TEXT, "x"),
        case(
            "LowCardinality(Nullable(String))",
            Value::Null,
            OR_NULL,
            "NULL",
        ),
        case("Nullable(String)", Value::Null, OR_NULL, "NULL"),
        case("Nullable(Int64)", json!(7), OR_NULL, "7"),
        case(
            "Nullable(DateTime64(3, 'UTC'))",
            json!(1_790_416_800_123_i64),
            "toString(toUnixTimestamp64Milli(assumeNotNull(v)))",
            "1790416800123",
        ),
        case(
            "JSON",
            json!({"a": 1, "b": "x"}),
            TEXT,
            r#"{"a":1,"b":"x"}"#,
        ),
        case(
            "JSON(max_dynamic_paths = 2048)",
            json!({"a": 1}),
            TEXT,
            r#"{"a":1}"#,
        ),
    ]
}

/// Each supported column type lands through the `RowBinaryWithNamesAndTypes`
/// header and reads back as sent. A type whose `system.columns` string the
/// server did not accept back in the header would fail here with code 117.
#[tokio::test]
async fn every_supported_column_type_lands_through_the_row_binary_header() {
    let target = Target::new(test_name!()).await;
    let reader = target.rs_client();
    let inserter = Inserter::new(
        Arc::clone(&target.client),
        target.rs_client(),
        InserterConfig {
            max_retries: 1,
            base_retry_delay_ms: 10,
            max_retry_delay_ms: 100,
            ..InserterConfig::default()
        },
    )
    .with_insert_format(InsertFormat::RowBinary);

    let mut failures = Vec::new();
    for (index, case) in supported_type_cases().into_iter().enumerate() {
        let table = format!(
            "{}.{}",
            target.database,
            unique_table_name(&format!("rb_type_{index}"))
        );
        let outcome = async {
            target
                .client
                .execute(&format!(
                    "CREATE TABLE {table}{} (id UInt64, v {}) ENGINE = MergeTree() ORDER BY id",
                    target.on_cluster, case.ty
                ))
                .await
                .map_err(|e| format!("create: {e}"))?;
            let mut row = Map::new();
            row.insert("id".to_string(), json!(1));
            row.insert("v".to_string(), case.value.clone());
            inserter
                .insert_rows(&table, &[row], &[])
                .await
                .map_err(|e| format!("insert: {e}"))?;
            let read: Vec<Text> = reader
                .query(&format!("SELECT {} AS s FROM {table}", case.read))
                .fetch_all()
                .await
                .map_err(|e| format!("read: {e}"))?;
            match read.as_slice() {
                [only] if only.s == case.expected => Ok(()),
                [only] => Err(format!(
                    "read back {:?}, expected {:?}",
                    only.s, case.expected
                )),
                rows => Err(format!("{} rows landed", rows.len())),
            }
        }
        .await;
        match &outcome {
            Ok(()) => eprintln!("ROWBINARY TYPE {}: PASS", case.ty),
            Err(e) => eprintln!("ROWBINARY TYPE {}: FAIL {e}", case.ty),
        }
        if let Err(e) = outcome {
            failures.push(format!("{}: {e}", case.ty));
        }
        target.drop(&table).await;
    }
    assert!(
        failures.is_empty(),
        "{} column types failed through the RowBinary header:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// A column type the encoder has no writer for is refused before anything is
/// sent, never written as some other type's bytes -- whether the row carries a
/// value for it or leaves it to a default.
#[tokio::test]
async fn an_unsupported_column_type_is_refused_before_it_is_sent() {
    let target = Target::new(test_name!()).await;
    let inserter = Inserter::new(
        Arc::clone(&target.client),
        target.rs_client(),
        InserterConfig {
            max_retries: 0,
            ..InserterConfig::default()
        },
    )
    .with_insert_format(InsertFormat::RowBinary);

    for (index, (ty, value)) in [
        ("Tuple(String, UInt8)", json!(["a", 1])),
        ("Tuple(String, UInt8)", Value::Null),
    ]
    .into_iter()
    .enumerate()
    {
        let table = format!(
            "{}.{}",
            target.database,
            unique_table_name(&format!("rb_unsupported_{index}"))
        );
        target
            .client
            .execute(&format!(
                "CREATE TABLE {table}{} (id UInt64, v {ty}) ENGINE = MergeTree() ORDER BY id",
                target.on_cluster
            ))
            .await
            .expect("create table");
        let mut row = Map::new();
        row.insert("id".to_string(), json!(1));
        row.insert("v".to_string(), value);
        let outcome = inserter.insert_rows(&table, &[row], &[]).await;
        eprintln!("ROWBINARY TYPE {ty}: {outcome:?}");
        let count = target
            .client
            .query_count(&table, None)
            .await
            .expect("count");
        target.drop(&table).await;
        assert!(
            matches!(&outcome, Err(e) if e.to_string().contains("unsupported type")),
            "{ty} was not refused as unsupported: {outcome:?}"
        );
        assert_eq!(count, 0, "{ty} wrote a row");
    }
}

// ============================================================================
// Type Coercion tests
//
// Each test verifies one coercion case against real ClickHouse:
//   1. Build a table with a target column type
//   2. Apply the Coercer to a row with ambiguous/raw input
//   3. Insert via insert_json_rows
//   4. Query back to confirm the value landed correctly
// ============================================================================

/// Helper: create a Coercer with default config
fn default_coercer() -> dfe_loader::transform::Coercer {
    use dfe_loader::config::CoercionConfig;
    dfe_loader::transform::Coercer::new(CoercionConfig::default())
}

#[tokio::test]
async fn test_coerce_datetime64_from_epoch_ms() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_dt64_epoch");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ts DateTime64(3)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Epoch ms integer — coercer converts to "YYYY-MM-DD HH:MM:SS.mmm" string
    let epoch_ms: i64 = 1735084800000; // 2024-12-25 00:00:00.000 UTC
    let mut row = json!({"ts": epoch_ms}).as_object().unwrap().clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ DateTime64 from epoch ms coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_datetime64_from_iso_string() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_dt64_iso");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ts DateTime64(3)
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // ISO8601 string with Z suffix — coercer normalises to CH-accepted format
    let mut row = json!({"ts": "2024-12-25T10:30:00.123Z"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ DateTime64 from ISO string coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_bool_from_string() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_bool_str");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            b Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // String representations of bool — coercer converts to JSON bool
    let string_trues = ["true", "1", "yes", "on", "t", "y"];
    let string_falses = ["false", "0", "no", "off"];

    let mut rows = Vec::new();
    for s in &string_trues {
        let mut row = json!({"b": s}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        // After coercion, "b" must be a JSON bool
        assert_eq!(row["b"], json!(true), "Expected true for input {s:?}");
        rows.push(row);
    }
    for s in &string_falses {
        let mut row = json!({"b": s}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        assert_eq!(row["b"], json!(false), "Expected false for input {s:?}");
        rows.push(row);
    }

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), string_trues.len() + string_falses.len());

    eprintln!("✓ Bool from string coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_bool_from_int() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_bool_int");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            b Bool
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    let cases = [(1i64, true), (0i64, false), (42i64, true), (-1i64, true)];
    let mut rows = Vec::new();
    for (int_val, expected_bool) in &cases {
        let mut row = json!({"b": int_val}).as_object().unwrap().clone();
        coercer
            .coerce_row(&mut row, &schema)
            .expect("Coercion failed");
        assert_eq!(
            row["b"],
            json!(expected_bool),
            "Expected {expected_bool} for int input {int_val}"
        );
        rows.push(row);
    }

    let result = client.insert_json_rows(&table_name, &rows, &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), cases.len());

    eprintln!("✓ Bool from int coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_uuid_normalisation() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_uuid");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            id UUID
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Hex without hyphens — coercer normalises to RFC 4122 format
    let mut row = json!({"id": "550e8400e29b41d4a716446655440000"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(row["id"], json!("550e8400-e29b-41d4-a716-446655440000"));

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after UUID coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ UUID normalisation coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_ipv4_from_integer() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_ipv4");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            ip IPv4
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Integer representation of 192.168.1.1 = 3232235777
    let mut row = json!({"ip": 3232235777u64}).as_object().unwrap().clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(
        row["ip"],
        json!("192.168.1.1"),
        "Expected dotted-decimal IPv4"
    );

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after IPv4 coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ IPv4 from integer coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_null_non_nullable_defaults_to_empty() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_null_nonnullable");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            name String,
            score UInt64
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Null for non-nullable columns — coercer substitutes type defaults
    let mut row = json!({"name": null, "score": null})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");
    assert_eq!(
        row["name"],
        json!(""),
        "Expected empty string default for String"
    );
    assert_eq!(row["score"], json!(0), "Expected 0 default for UInt64");

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after null coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ Null → non-nullable default coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_array_datetime64_from_epoch_ms() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_arr_dt64");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            timestamps Array(DateTime64(3))
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // Array of epoch ms integers — coercer applies inner DateTime64 coercion
    let epoch_values = json!([1735084800000i64, 1735085000000i64, 1735085200000i64]);
    let mut row = json!({"timestamps": epoch_values})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row, &schema)
        .expect("Coercion failed");

    // After coercion, all elements should be strings (CH datetime format)
    let arr = row["timestamps"].as_array().expect("Expected array");
    assert_eq!(arr.len(), 3);
    for elem in arr {
        assert!(
            elem.is_string(),
            "Expected string datetime after coercion, got: {elem:?}"
        );
    }

    let result = client.insert_json_rows(&table_name, &[row], &[]).await;
    assert!(
        result.is_ok(),
        "Insert failed after array DateTime64 coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 1);

    eprintln!("✓ Array(DateTime64) from epoch ms coercion succeeded");
    target.drop(&table_name).await;
}

#[tokio::test]
async fn test_coerce_json_column_accepts_string_and_object() {
    let target = Target::new(test_name!()).await;
    let client = &target.client;
    let table_name = unique_table_name("test_coerce_json_col");
    let oc = target.on_cluster;

    let ddl = format!(
        "CREATE TABLE {table_name}{oc} (
            data JSON
        ) ENGINE = MergeTree() ORDER BY tuple()"
    );
    client.execute(&ddl).await.expect("Failed to create table");

    let schema = client
        .fetch_table_schema(&table_name)
        .await
        .expect("Schema fetch failed");
    let coercer = default_coercer();

    // JSON object value — passes through as-is (already valid JSON)
    let mut row1 = json!({"data": {"key": "value", "num": 42}})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row1, &schema)
        .expect("Coercion failed for object");

    // JSON string value — coercer validates it is parseable JSON
    let mut row2 = json!({"data": "{\"key\": \"from_string\", \"num\": 99}"})
        .as_object()
        .unwrap()
        .clone();
    coercer
        .coerce_row(&mut row2, &schema)
        .expect("Coercion failed for string");

    let result = client
        .insert_json_rows(&table_name, &[row1, row2], &[])
        .await;
    assert!(
        result.is_ok(),
        "Insert failed after JSON coercion: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap(), 2);

    eprintln!("✓ JSON column coercion (object + string) succeeded");
    target.drop(&table_name).await;
}
