// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Every value coercion the RowBinary insert owes a source record, run
//! through the loader's own `Inserter` by `shared_services::clickhouse_case`
//! on each protocol the shared ClickHouse offers.
//!
//! A case lands as the value the source meant, or is refused as the rows'
//! own fault (`ClickHousePermanent`, so salvage sends it to the DLQ). A
//! refusal that came back retryable would hold its partition for good.

use serde_json::{Map, Value, json};

use dfe_loader::clickhouse::{ClickHouseQueryClient, Inserter};

#[derive(clickhouse::Row, serde::Deserialize)]
struct Text {
    s: String,
}

struct Case {
    name: &'static str,
    /// Column definitions after `id UInt64`.
    cols: &'static str,
    rows: Vec<Value>,
    /// A String expression over the table, read in `id` order.
    read: &'static str,
    /// `None`: the rows must be refused as their own fault.
    expect: Option<Vec<&'static str>>,
}

#[allow(clippy::too_many_lines)]
fn cases() -> Vec<Case> {
    let c = |name, cols, rows, read, expect| Case {
        name,
        cols,
        rows,
        read,
        expect,
    };
    let one = |v: Value| vec![json!({"id": 1, "v": v})];
    vec![
        c(
            "a defaulted column the first row omits",
            "a UInt32 DEFAULT 7, b String DEFAULT 'd'",
            vec![json!({"id": 1}), json!({"id": 2, "a": 5, "b": "x"})],
            "concat(toString(a), '/', b)",
            Some(vec!["7/d", "5/x"]),
        ),
        c("UInt8 256", "v UInt8", one(json!(256)), "toString(v)", None),
        c("Int8 200", "v Int8", one(json!(200)), "toString(v)", None),
        c("UInt32 -1", "v UInt32", one(json!(-1)), "toString(v)", None),
        c(
            "UInt64 1e30",
            "v UInt64",
            one(json!(1e30)),
            "toString(v)",
            None,
        ),
        c(
            "Decimal text past 2^53",
            "v Decimal(38, 0)",
            one(json!("9007199254740993")),
            "toString(v)",
            Some(vec!["9007199254740993"]),
        ),
        c(
            "Decimal number past 2^53",
            "v Decimal(38, 0)",
            one(json!(9_007_199_254_740_993_u64)),
            "toString(v)",
            Some(vec!["9007199254740993"]),
        ),
        c(
            "Decimal(38, 10) with 29 digits",
            "v Decimal(38, 10)",
            one(json!("1234567890123456789.0123456789")),
            "toString(v)",
            Some(vec!["1234567890123456789.0123456789"]),
        ),
        c(
            "Decimal(9, 2) past Int32",
            "v Decimal(9, 2)",
            one(json!("99999999.99")),
            "toString(v)",
            None,
        ),
        c(
            "DateTime64 at +10:00",
            "v DateTime64(3, 'UTC')",
            one(json!("2026-09-26T20:00:00+10:00")),
            "toString(v)",
            Some(vec!["2026-09-26 10:00:00.000"]),
        ),
        c(
            "DateTime at +10:00",
            "v DateTime('UTC')",
            one(json!("2026-09-26T20:00:00+10:00")),
            "toString(v)",
            Some(vec!["2026-09-26 10:00:00"]),
        ),
        c(
            "DateTime64(6) from epoch milliseconds",
            "v DateTime64(6, 'UTC')",
            one(json!(1_790_416_800_123_i64)),
            "toString(v)",
            Some(vec!["2026-09-26 10:00:00.123000"]),
        ),
        c(
            "IPv6 from IPv4 text",
            "v IPv6",
            one(json!("10.0.0.1")),
            "toString(v)",
            Some(vec!["::ffff:10.0.0.1"]),
        ),
        c(
            "JSON root array",
            "v JSON",
            one(json!(["a", "b"])),
            "toString(v)",
            Some(vec![r#"{"list":["a","b"]}"#]),
        ),
        c(
            "JSON scalar",
            "v JSON",
            one(json!(42)),
            "toString(v)",
            Some(vec![r#"{"value":42}"#]),
        ),
        c(
            "JSON text that is not JSON",
            "v JSON",
            one(json!("not json")),
            "toString(v)",
            Some(vec![r#"{"value":"not json"}"#]),
        ),
        c(
            "Point as 'lat,lon'",
            "v Point",
            one(json!("41.12,-71.34")),
            "toString(v)",
            Some(vec!["(-71.34,41.12)"]),
        ),
        c(
            "Point as WKT",
            "v Point",
            one(json!("POINT (-71.34 41.12)")),
            "toString(v)",
            Some(vec!["(-71.34,41.12)"]),
        ),
        c(
            "Point at latitude 100",
            "v Point",
            one(json!({"lat": 100, "lon": 0})),
            "toString(v)",
            None,
        ),
        c(
            "Ring",
            "v Ring",
            one(json!([[1, 2], [3, 4]])),
            "toString(v)",
            Some(vec!["[(1,2),(3,4)]"]),
        ),
        c(
            "Array given one value",
            "v Array(String)",
            one(json!("one")),
            "toString(v)",
            Some(vec!["['one']"]),
        ),
        c(
            "Enum8 given an undeclared number",
            "v Enum8('a' = 1, 'b' = 2)",
            one(json!(5)),
            "toString(CAST(v, 'Int8'))",
            None,
        ),
        c(
            "Nullable Enum8 by name",
            "v Nullable(Enum8('on' = 1, 'off' = 2))",
            one(json!("off")),
            "ifNull(toString(v), 'NULL')",
            Some(vec!["off"]),
        ),
    ]
}

/// Run every case through `inserter` into tables of `db`, reading back with
/// `reader`, and name each one that lands wrong or is refused the wrong way.
/// `on_cluster` is the pet cluster's `ON CLUSTER` clause, empty on a single
/// server, so a read through any replica sees the insert.
pub(super) async fn coercion_failures(
    admin: &ClickHouseQueryClient,
    inserter: &Inserter,
    reader: &clickhouse::Client,
    db: &str,
    engine: &str,
    on_cluster: &str,
    label: &str,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for (index, case) in cases().into_iter().enumerate() {
        let table = format!("{db}.coerce_{label}_{index}");
        if let Err(e) = admin
            .execute(&format!(
                "CREATE TABLE {table} (id UInt64, {}) ENGINE = {engine} ORDER BY id",
                case.cols
            ))
            .await
        {
            wrong.push(format!("{}: create: {e}", case.name));
            continue;
        }
        let rows: Vec<Map<String, Value>> = case
            .rows
            .iter()
            .map(|r| r.as_object().cloned().unwrap_or_default())
            .collect();
        let inserted = Box::pin(inserter.insert_rows(&table, &rows, &[])).await;
        match (&case.expect, inserted) {
            (None, Err(dfe_loader::Error::ClickHousePermanent(_))) => {}
            (Some(want), Ok(_)) => {
                if !on_cluster.is_empty() {
                    let _ = admin
                        .execute(&format!("SYSTEM SYNC REPLICA{on_cluster} {table}"))
                        .await;
                }
                let got: Vec<String> = match reader
                    .query(&format!(
                        "SELECT {} AS s FROM {table} ORDER BY id",
                        case.read
                    ))
                    .fetch_all::<Text>()
                    .await
                {
                    Ok(rows) => rows.into_iter().map(|t| t.s).collect(),
                    Err(e) => {
                        wrong.push(format!("{}: read back: {e}", case.name));
                        continue;
                    }
                };
                if !got.iter().map(String::as_str).eq(want.iter().copied()) {
                    wrong.push(format!("{}: landed {got:?}", case.name));
                }
            }
            (_, outcome) => wrong.push(format!("{}: {outcome:?}", case.name)),
        }
    }
    wrong
}

/// A column type changed after the loader cached the table's schema: the rows
/// must land as their values or come back for a retry, never as other values.
/// RowBinary is positional, so bytes encoded for `UInt8` read against a
/// `UInt16` column parse as different rows unless something refuses them. The
/// batch sizes cover the counts whose old bytes are a whole number of new rows.
pub(super) async fn type_change_failures(
    admin: &ClickHouseQueryClient,
    inserter: &Inserter,
    reader: &clickhouse::Client,
    db: &str,
    engine: &str,
    label: &str,
) -> Vec<String> {
    let mut wrong = Vec::new();
    for batch in [10_u64, 11, 12, 13, 22, 23, 24] {
        if let Err(e) = Box::pin(type_change_case(
            admin, inserter, reader, db, engine, label, batch,
        ))
        .await
        {
            wrong.push(format!("type change, {batch} rows: {e}"));
        }
    }
    wrong
}

async fn type_change_case(
    admin: &ClickHouseQueryClient,
    inserter: &Inserter,
    reader: &clickhouse::Client,
    db: &str,
    engine: &str,
    label: &str,
    batch: u64,
) -> Result<(), String> {
    let table = format!("{db}.retyped_{label}_{batch}");
    let row = |id: u64| {
        json!({"id": id, "v": 7})
            .as_object()
            .cloned()
            .unwrap_or_default()
    };
    admin
        .execute(&format!(
            "CREATE TABLE {table} (id UInt64, v UInt8) ENGINE = {engine} ORDER BY id"
        ))
        .await
        .map_err(|e| format!("create: {e}"))?;
    Box::pin(inserter.insert_rows(&table, &[row(1)], &[]))
        .await
        .map_err(|e| format!("first insert: {e}"))?;
    admin
        .execute(&format!("ALTER TABLE {table} MODIFY COLUMN v UInt16"))
        .await
        .map_err(|e| format!("alter: {e}"))?;
    let rows: Vec<Map<String, Value>> = (2..2 + batch).map(row).collect();
    // A retry against the fresh schema is fine, a silent misread is not.
    let _ = Box::pin(inserter.insert_rows(&table, &rows, &[])).await;
    let got: Vec<String> = reader
        .query(&format!(
            "SELECT concat(toString(id), '=', toString(v)) AS s FROM {table} ORDER BY id"
        ))
        .fetch_all::<Text>()
        .await
        .map_err(|e| format!("read back: {e}"))?
        .into_iter()
        .map(|t| t.s)
        .collect();
    let want: Vec<String> = (1..2 + batch).map(|id| format!("{id}=7")).collect();
    if got.iter().all(|s| want.contains(s)) {
        Ok(())
    } else {
        Err(format!("a retyped column read other values: {got:?}"))
    }
}
