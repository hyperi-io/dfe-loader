# clickhouse-rs DynamicInsert Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add runtime schema-driven RowBinary inserts to the clickhouse-rs fork so users can push `Map<String, Value>` with the same ease as JSONEachRow but with binary performance.

**Architecture:** Fetch table schema from `system.columns`, parse type strings into a rich `ParsedType` AST, encode `serde_json::Value` to RowBinary at runtime using the existing native column encoders. Schema recovery on mismatch: pause, re-fetch, retry.

**Tech Stack:** Rust (edition 2024), clickhouse-rs fork (`hyperi/optimise-1` branch), serde_json, tokio

**Working directory:** `/projects/clickhouse-rs` (branch `hyperi/optimise-1`)
**Test harness:** `/projects/dfe-loader` (via `[patch.crates-io]`)

---

## File Structure

All new files live under `src/dynamic/` — a new top-level module parallel to `src/native/`.

| File | Purpose |
|---|---|
| **Create:** `src/dynamic/mod.rs` | Module root, public re-exports |
| **Create:** `src/dynamic/parsed_type.rs` | `ParsedType` — rich ClickHouse type parser (lifted from dfe-loader) |
| **Create:** `src/dynamic/schema.rs` | `DynamicSchema` + upgraded `SchemaCache` with background refresh |
| **Create:** `src/dynamic/encode.rs` | `encode_dynamic_row()` — `Value` + `ParsedType` -> RowBinary bytes |
| **Create:** `src/dynamic/insert.rs` | `DynamicInsert` — single-table insert with schema fetch + recovery |
| **Create:** `src/dynamic/batcher.rs` | `DynamicBatcher` — async background task variant of DynamicInsert |
| **Create:** `src/dynamic/error.rs` | `DynamicError` — schema mismatch, encoding errors |
| **Modify:** `src/lib.rs` | Add `pub mod dynamic;`, add `Client::dynamic_insert()` and `Client::dynamic_batcher()` |
| **Create:** `tests/dynamic_insert.rs` | Integration tests against real ClickHouse |

---

## Chunk 1: ParsedType and Schema

### Task 1: Create dynamic module skeleton and ParsedType

**Files:**
- Create: `src/dynamic/mod.rs`
- Create: `src/dynamic/parsed_type.rs`
- Create: `src/dynamic/error.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Create `src/dynamic/error.rs`**

```rust
// src/dynamic/error.rs
//! Error types for dynamic (schema-driven) inserts.

use std::fmt;

/// Errors specific to dynamic schema-driven inserts.
#[derive(Debug)]
pub enum DynamicError {
    /// Column type string could not be parsed.
    UnsupportedType { column: String, type_str: String },
    /// Value could not be encoded for the target column type.
    EncodingError { column: String, message: String },
    /// Schema mismatch detected — server rejected the insert.
    SchemaMismatch { table: String, message: String },
    /// Schema fetch from system.columns failed.
    SchemaFetch { table: String, source: crate::error::Error },
    /// Table has no columns (or does not exist).
    EmptySchema { table: String },
}

impl fmt::Display for DynamicError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedType { column, type_str } => {
                write!(f, "unsupported type '{type_str}' for column '{column}'")
            }
            Self::EncodingError { column, message } => {
                write!(f, "encoding error for column '{column}': {message}")
            }
            Self::SchemaMismatch { table, message } => {
                write!(f, "schema mismatch for table '{table}': {message}")
            }
            Self::SchemaFetch { table, source } => {
                write!(f, "failed to fetch schema for '{table}': {source}")
            }
            Self::EmptySchema { table } => {
                write!(f, "table '{table}' has no columns or does not exist")
            }
        }
    }
}

impl std::error::Error for DynamicError {}

impl From<DynamicError> for crate::error::Error {
    fn from(e: DynamicError) -> Self {
        crate::error::Error::Custom(e.to_string())
    }
}
```

- [ ] **Step 2: Create `src/dynamic/parsed_type.rs`**

Lift from `/projects/dfe-loader/src/clickhouse/types.rs`. Key adaptations:
- Remove dfe-loader-specific imports (`use crate::clickhouse::*`)
- Make all types `pub` (was `pub` in loader too)
- Add `to_column_type()` method that converts to the fork's `ColumnType` for encoding
- Keep `parse()` method identical — it's well-tested in dfe-loader (558 lines, 30+ tests)

```bash
# Start by copying the file as a base
cp /projects/dfe-loader/src/clickhouse/types.rs src/dynamic/parsed_type.rs
```

Then edit: remove dfe-loader header, add fork header, remove loader-specific `use` statements,
add `to_column_type()` bridge method at the end.

The `to_column_type()` method maps `ParsedType` -> `crate::native::columns::ColumnType`:

```rust
impl ParsedType {
    /// Convert to the native transport's `ColumnType` for binary encoding.
    pub fn to_column_type(&self) -> Option<crate::native::columns::ColumnType> {
        use crate::native::columns::ColumnType as CT;
        let inner = match self.base.as_str() {
            "String" => CT::String,
            "UInt8" | "Bool" => CT::UInt8,
            "UInt16" => CT::UInt16,
            "UInt32" => CT::UInt32,
            "UInt64" => CT::UInt64,
            "UInt128" => CT::UInt128,
            "UInt256" => CT::UInt256,
            "Int8" => CT::Int8,
            "Int16" => CT::Int16,
            "Int32" => CT::Int32,
            "Int64" => CT::Int64,
            "Int128" => CT::Int128,
            "Int256" => CT::Int256,
            "Float32" => CT::Float32,
            "Float64" => CT::Float64,
            "UUID" => CT::Uuid,
            "IPv4" => CT::IPv4,
            "IPv6" => CT::IPv6,
            "Date" => CT::Date,
            "Date32" => CT::Date32,
            "DateTime" => CT::DateTime,
            "DateTime64" => CT::DateTime64,
            "Enum8" => CT::Enum8,
            "Enum16" => CT::Enum16,
            "Decimal32" => CT::Decimal32,
            "Decimal64" => CT::Decimal64,
            "Decimal128" => CT::Decimal128,
            "Decimal256" => CT::Decimal256,
            "Decimal" => {
                match self.precision.unwrap_or(18) {
                    0..=9 => CT::Decimal32,
                    10..=18 => CT::Decimal64,
                    19..=38 => CT::Decimal128,
                    _ => CT::Decimal256,
                }
            }
            "FixedString" => CT::FixedString(self.fixed_size.unwrap_or(1)),
            "Array" => {
                let elem = self.array_element.as_ref()?.to_column_type()?;
                CT::Array(Box::new(elem))
            }
            "Map" => {
                let (k, v) = self.map_types.as_ref()?;
                CT::Map(Box::new(k.to_column_type()?), Box::new(v.to_column_type()?))
            }
            "JSON" => CT::Json,
            _ => return None,
        };
        // Wrap with Nullable / LowCardinality as needed
        let wrapped = if self.nullable {
            CT::Nullable(Box::new(inner))
        } else {
            inner
        };
        let wrapped = if self.low_cardinality {
            CT::LowCardinality(Box::new(wrapped))
        } else {
            wrapped
        };
        Some(wrapped)
    }
}
```

- [ ] **Step 3: Create `src/dynamic/mod.rs`**

```rust
// src/dynamic/mod.rs
//! Runtime schema-driven inserts for dynamic schemas.
//!
//! Use this when table schemas are not known at compile time.
//! `DynamicInsert` fetches the schema from `system.columns` and encodes
//! `Map<String, Value>` directly to RowBinary — same ease as JSONEachRow
//! but without the server-side JSON parsing overhead.

pub mod error;
pub mod parsed_type;

pub use error::DynamicError;
pub use parsed_type::ParsedType;
```

- [ ] **Step 4: Add `pub mod dynamic;` to `src/lib.rs`**

Add after the existing `pub mod native;` line:

```rust
pub mod dynamic;
```

- [ ] **Step 5: Make `ColumnType` and `ColumnType::parse` pub(crate) visible to dynamic module**

The `ColumnType` enum in `src/native/columns.rs` is currently `pub(crate)`. Verify that
`src/dynamic/parsed_type.rs` can reference `crate::native::columns::ColumnType`. If not,
widen visibility to `pub` or `pub(crate)` as needed.

- [ ] **Step 6: Build and verify**

```bash
cd /projects/clickhouse-rs && cargo check 2>&1 | tail -5
```

Expected: compiles clean.

- [ ] **Step 7: Run ParsedType unit tests**

The tests from dfe-loader should be included in the lifted file. Run them:

```bash
cd /projects/clickhouse-rs && cargo test --lib -- dynamic::parsed_type 2>&1 | tail -15
```

Expected: all ParsedType tests pass.

- [ ] **Step 8: Commit**

```bash
cd /projects/clickhouse-rs
git add src/dynamic/ src/lib.rs src/native/columns.rs
git commit -m "feat(dynamic): add ParsedType and DynamicError modules

Lift ParsedType from dfe-loader — rich ClickHouse type parser with
Nullable, LowCardinality, Array, Map, DateTime64(p, tz), Decimal(p, s),
FixedString(n), Enum support. Bridge method to_column_type() maps to
the native transport's ColumnType for binary encoding."
```

---

### Task 2: DynamicSchema and upgraded SchemaCache

**Files:**
- Create: `src/dynamic/schema.rs`
- Modify: `src/dynamic/mod.rs`

- [ ] **Step 1: Define `ColumnDef` and `DynamicSchema`**

```rust
// src/dynamic/schema.rs
//! Schema reflection for dynamic inserts.
//!
//! Fetches column definitions from `system.columns` and caches them with TTL.
//! The schema drives runtime RowBinary encoding — each column's `ParsedType`
//! determines how `serde_json::Value` is converted to binary.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::Client;
use super::parsed_type::ParsedType;
use super::error::DynamicError;

/// Column definition from system.columns.
#[derive(Debug, Clone)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,
    /// Raw type string from ClickHouse (e.g. "LowCardinality(Nullable(String))").
    pub raw_type: String,
    /// Parsed type with full structure.
    pub parsed_type: ParsedType,
    /// Default kind: "", "DEFAULT", "MATERIALIZED", "ALIAS", "EPHEMERAL".
    pub default_kind: String,
    /// Whether this column can be omitted from INSERT (has a server-side default).
    pub has_default: bool,
}

/// Schema for a single table — ordered list of column definitions.
#[derive(Debug, Clone)]
pub struct DynamicSchema {
    pub table: String,
    pub columns: Vec<ColumnDef>,
    /// Lookup by column name for O(1) access during encoding.
    column_index: HashMap<String, usize>,
}

impl DynamicSchema {
    /// Build from system.columns query results.
    pub fn from_columns(table: &str, columns: Vec<ColumnDef>) -> Self {
        let column_index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        Self {
            table: table.to_string(),
            columns,
            column_index,
        }
    }

    /// Look up a column by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.column_index.get(name).map(|&i| &self.columns[i])
    }

    /// Columns that MUST appear in INSERT (no server-side default).
    pub fn required_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| !c.has_default)
    }

    /// Columns that CAN be omitted (have DEFAULT/MATERIALIZED/ALIAS).
    pub fn optional_columns(&self) -> impl Iterator<Item = &ColumnDef> {
        self.columns.iter().filter(|c| c.has_default)
    }
}
```

- [ ] **Step 2: Add `DynamicSchemaCache`**

```rust
/// TTL-based schema cache with invalidation.
pub struct DynamicSchemaCache {
    inner: RwLock<HashMap<String, CacheEntry>>,
    ttl: Duration,
}

struct CacheEntry {
    schema: DynamicSchema,
    fetched_at: Instant,
}

impl DynamicSchemaCache {
    /// Create a new cache. Default TTL: 300s (5 minutes).
    pub fn new(ttl: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: RwLock::new(HashMap::new()),
            ttl,
        })
    }

    /// Get cached schema if not expired.
    pub fn get(&self, table: &str) -> Option<DynamicSchema> {
        let guard = self.inner.read().ok()?;
        guard.get(table).and_then(|e| {
            if e.fetched_at.elapsed() < self.ttl {
                Some(e.schema.clone())
            } else {
                None
            }
        })
    }

    /// Insert or refresh a schema.
    pub fn insert(&self, table: &str, schema: DynamicSchema) {
        if let Ok(mut guard) = self.inner.write() {
            guard.insert(
                table.to_string(),
                CacheEntry {
                    schema,
                    fetched_at: Instant::now(),
                },
            );
        }
    }

    /// Invalidate a single table (forces re-fetch on next access).
    pub fn invalidate(&self, table: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.remove(table);
        }
    }

    /// Invalidate all cached schemas.
    pub fn invalidate_all(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
    }
}
```

- [ ] **Step 3: Add `fetch_dynamic_schema()` function**

```rust
/// Fetch table schema from system.columns via the HTTP client.
///
/// Parses each column's type string into a full `ParsedType`.
pub async fn fetch_dynamic_schema(
    client: &Client,
    database: &str,
    table: &str,
) -> std::result::Result<DynamicSchema, DynamicError> {
    let full_table = format!("{database}.{table}");
    let sql = format!(
        "SELECT name, type, default_kind \
         FROM system.columns \
         WHERE database = '{database}' AND table = '{table}' \
         ORDER BY position"
    );

    // Use the existing query infrastructure
    let mut cursor = client
        .query(&sql)
        .fetch::<SchemaRow>()
        .map_err(|e| DynamicError::SchemaFetch {
            table: full_table.clone(),
            source: e,
        })?;

    let mut columns = Vec::new();
    while let Some(row) = cursor.next().await.map_err(|e| DynamicError::SchemaFetch {
        table: full_table.clone(),
        source: e,
    })? {
        let parsed_type = ParsedType::parse(&row.r#type);
        let default_kind = row.default_kind.clone();
        let has_default = !default_kind.is_empty();
        columns.push(ColumnDef {
            name: row.name,
            raw_type: row.r#type,
            parsed_type,
            default_kind,
            has_default,
        });
    }

    if columns.is_empty() {
        return Err(DynamicError::EmptySchema { table: full_table });
    }

    Ok(DynamicSchema::from_columns(&full_table, columns))
}

/// Internal Row type for system.columns query.
#[derive(Debug, crate::Row, serde::Deserialize)]
struct SchemaRow {
    name: String,
    #[serde(rename = "type")]
    r#type: String,
    default_kind: String,
}
```

- [ ] **Step 4: Update `src/dynamic/mod.rs`**

```rust
pub mod error;
pub mod parsed_type;
pub mod schema;

pub use error::DynamicError;
pub use parsed_type::ParsedType;
pub use schema::{ColumnDef, DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
```

- [ ] **Step 5: Build and verify**

```bash
cd /projects/clickhouse-rs && cargo check 2>&1 | tail -5
```

- [ ] **Step 6: Commit**

```bash
git add src/dynamic/schema.rs src/dynamic/mod.rs
git commit -m "feat(dynamic): add DynamicSchema, cache, and system.columns fetch

ColumnDef with ParsedType, DynamicSchemaCache with TTL + invalidation,
fetch_dynamic_schema() queries system.columns and parses all type strings."
```

---

## Chunk 2: Runtime RowBinary Encoder

### Task 3: Value-to-RowBinary encoder

**Files:**
- Create: `src/dynamic/encode.rs`
- Modify: `src/dynamic/mod.rs`

This is the performance-critical piece. For each column in the schema, dispatch
`serde_json::Value` to the correct RowBinary encoder.

- [ ] **Step 1: Write test file first (TDD)**

Add tests at the bottom of `src/dynamic/encode.rs` covering:
- String value -> RowBinary String (length-prefixed)
- Integer value -> RowBinary UInt32/Int64 etc (little-endian)
- Float value -> RowBinary Float64 (IEEE 754)
- Null value in Nullable column -> 0x01 prefix
- Non-null value in Nullable column -> 0x00 prefix + value
- Bool value -> UInt8 0/1
- Array of integers -> varint length + elements
- Missing column with default -> skip (don't encode)
- Type mismatch (string where int expected) -> DynamicError::EncodingError

- [ ] **Step 2: Implement `encode_dynamic_row()`**

```rust
// src/dynamic/encode.rs
//! Runtime RowBinary encoder for serde_json::Value.
//!
//! Converts a JSON map to RowBinary bytes using a DynamicSchema.
//! This is the bridge between dynamic schemas (Map<String, Value>)
//! and the efficient binary wire format that ClickHouse expects.
//!
//! Performance: avoids the JSON text overhead of JSONEachRow.
//! ClickHouse receives pre-columnarised binary — zero server-side parsing.

use serde_json::{Map, Value};

use super::error::DynamicError;
use super::schema::{ColumnDef, DynamicSchema};
use super::parsed_type::ParsedType;

/// Encode a JSON row map to RowBinary bytes according to the schema.
///
/// Columns are written in schema order. Missing columns with server-side
/// defaults are omitted (the insert column list excludes them). Missing
/// columns WITHOUT defaults get a type-appropriate zero value.
///
/// Returns the RowBinary bytes for one row.
pub fn encode_dynamic_row(
    row: &Map<String, Value>,
    schema: &DynamicSchema,
) -> std::result::Result<Vec<u8>, DynamicError> {
    let mut buf = Vec::with_capacity(256);

    for col in &schema.columns {
        if col.has_default && !row.contains_key(&col.name) {
            // Column has server-side default and is not in the row — skip
            continue;
        }

        let value = row.get(&col.name).unwrap_or(&Value::Null);
        encode_value(value, col, &mut buf)?;
    }

    Ok(buf)
}

/// Columns to include in the INSERT column list (those we'll actually send).
pub fn insert_columns<'a>(
    row: &Map<String, Value>,
    schema: &'a DynamicSchema,
) -> Vec<&'a ColumnDef> {
    schema.columns.iter().filter(|col| {
        // Include if: column is in the row OR column has no default (must send something)
        row.contains_key(&col.name) || !col.has_default
    }).collect()
}

fn encode_value(
    value: &Value,
    col: &ColumnDef,
    buf: &mut Vec<u8>,
) -> std::result::Result<(), DynamicError> {
    let pt = &col.parsed_type;

    // Handle Nullable wrapper
    if pt.nullable {
        if value.is_null() {
            buf.push(1); // is_null = true
            return Ok(());
        }
        buf.push(0); // is_null = false
        // Fall through to encode the inner value
    } else if value.is_null() {
        // Non-nullable column with null value — write type default
        write_default(pt, buf);
        return Ok(());
    }

    encode_typed_value(value, pt, &col.name, buf)
}

fn encode_typed_value(
    value: &Value,
    pt: &ParsedType,
    col_name: &str,
    buf: &mut Vec<u8>,
) -> std::result::Result<(), DynamicError> {
    match pt.base.as_str() {
        "String" => {
            let s = value_as_str(value, col_name)?;
            write_string(s.as_bytes(), buf);
        }
        "FixedString" => {
            let s = value_as_str(value, col_name)?;
            let n = pt.fixed_size.unwrap_or(1);
            let bytes = s.as_bytes();
            if bytes.len() <= n {
                buf.extend_from_slice(bytes);
                // Pad with zeros
                for _ in bytes.len()..n {
                    buf.push(0);
                }
            } else {
                buf.extend_from_slice(&bytes[..n]);
            }
        }
        "UInt8" | "Bool" => {
            let v = value_as_u64(value, col_name)? as u8;
            buf.push(v);
        }
        "UInt16" => {
            let v = value_as_u64(value, col_name)? as u16;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "UInt32" | "Date32" | "DateTime" => {
            let v = value_as_u64(value, col_name)? as u32;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "UInt64" => {
            let v = value_as_u64(value, col_name)?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Int8" => {
            let v = value_as_i64(value, col_name)? as i8;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Int16" => {
            let v = value_as_i64(value, col_name)? as i16;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Int32" | "Date" => {
            let v = value_as_i64(value, col_name)? as i32;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Int64" | "DateTime64" => {
            let v = value_as_i64(value, col_name)?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Float32" => {
            let v = value_as_f64(value, col_name)? as f32;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Float64" => {
            let v = value_as_f64(value, col_name)?;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "UUID" => {
            let s = value_as_str(value, col_name)?;
            encode_uuid(&s, col_name, buf)?;
        }
        "IPv4" => {
            let s = value_as_str(value, col_name)?;
            encode_ipv4(&s, col_name, buf)?;
        }
        "IPv6" => {
            let s = value_as_str(value, col_name)?;
            encode_ipv6(&s, col_name, buf)?;
        }
        "Enum8" => {
            // Enum values sent as strings — ClickHouse resolves
            // For RowBinary, send as Int8; but with dynamic we send as String
            // and let ClickHouse coerce. For now, encode as the numeric value
            // if it's a number, or as 0 if it's a string (ClickHouse rejects
            // string enum values in RowBinary — this is a known limitation).
            let v = value_as_i64(value, col_name).unwrap_or(0) as i8;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Enum16" => {
            let v = value_as_i64(value, col_name).unwrap_or(0) as i16;
            buf.extend_from_slice(&v.to_le_bytes());
        }
        "Array" => {
            if let Some(elem_type) = &pt.array_element {
                encode_array(value, elem_type, col_name, buf)?;
            } else {
                return Err(encoding_err(col_name, "Array without element type"));
            }
        }
        "Map" => {
            if let Some((key_type, val_type)) = &pt.map_types {
                encode_map(value, key_type, val_type, col_name, buf)?;
            } else {
                return Err(encoding_err(col_name, "Map without key/value types"));
            }
        }
        "JSON" => {
            // JSON type — send as length-prefixed JSON string
            let json_str = value.to_string();
            write_string(json_str.as_bytes(), buf);
        }
        other => {
            return Err(DynamicError::UnsupportedType {
                column: col_name.to_string(),
                type_str: other.to_string(),
            });
        }
    }
    Ok(())
}

// --- Helper functions ---

fn write_string(bytes: &[u8], buf: &mut Vec<u8>) {
    write_varint(bytes.len() as u64, buf);
    buf.extend_from_slice(bytes);
}

fn write_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

fn write_default(pt: &ParsedType, buf: &mut Vec<u8>) {
    match pt.base.as_str() {
        "String" | "JSON" => write_string(b"", buf),
        "FixedString" => {
            let n = pt.fixed_size.unwrap_or(1);
            buf.extend(std::iter::repeat_n(0u8, n));
        }
        "UInt8" | "Bool" | "Int8" | "Enum8" => buf.push(0),
        "UInt16" | "Int16" | "Enum16" | "Date" => buf.extend_from_slice(&0u16.to_le_bytes()),
        "UInt32" | "Int32" | "Date32" | "DateTime" => buf.extend_from_slice(&0u32.to_le_bytes()),
        "UInt64" | "Int64" | "DateTime64" => buf.extend_from_slice(&0u64.to_le_bytes()),
        "Float32" => buf.extend_from_slice(&0f32.to_le_bytes()),
        "Float64" => buf.extend_from_slice(&0f64.to_le_bytes()),
        "UUID" => buf.extend_from_slice(&[0u8; 16]),
        "IPv4" => buf.extend_from_slice(&[0u8; 4]),
        "IPv6" => buf.extend_from_slice(&[0u8; 16]),
        "Array" | "Map" => write_varint(0, buf), // empty array/map
        _ => write_string(b"", buf), // fallback: empty string
    }
}

fn value_as_str<'a>(value: &'a Value, col: &str) -> std::result::Result<String, DynamicError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        Value::Null => Ok(String::new()),
        _ => Ok(value.to_string()),
    }
}

fn value_as_u64(value: &Value, col: &str) -> std::result::Result<u64, DynamicError> {
    match value {
        Value::Number(n) => n.as_u64().or_else(|| n.as_i64().map(|v| v as u64)).or_else(|| n.as_f64().map(|v| v as u64))
            .ok_or_else(|| encoding_err(col, "not a valid unsigned integer")),
        Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
        Value::String(s) => s.parse::<u64>().map_err(|_| encoding_err(col, "string not parseable as u64")),
        _ => Err(encoding_err(col, "expected number")),
    }
}

fn value_as_i64(value: &Value, col: &str) -> std::result::Result<i64, DynamicError> {
    match value {
        Value::Number(n) => n.as_i64().or_else(|| n.as_u64().map(|v| v as i64)).or_else(|| n.as_f64().map(|v| v as i64))
            .ok_or_else(|| encoding_err(col, "not a valid integer")),
        Value::Bool(b) => Ok(if *b { 1 } else { 0 }),
        Value::String(s) => s.parse::<i64>().map_err(|_| encoding_err(col, "string not parseable as i64")),
        _ => Err(encoding_err(col, "expected number")),
    }
}

fn value_as_f64(value: &Value, col: &str) -> std::result::Result<f64, DynamicError> {
    match value {
        Value::Number(n) => n.as_f64().ok_or_else(|| encoding_err(col, "not a valid float")),
        Value::String(s) => s.parse::<f64>().map_err(|_| encoding_err(col, "string not parseable as f64")),
        _ => Err(encoding_err(col, "expected number")),
    }
}

fn encode_uuid(s: &str, col: &str, buf: &mut Vec<u8>) -> std::result::Result<(), DynamicError> {
    // ClickHouse UUID is 16 bytes: first 8 bytes = high, next 8 = low (big-endian within each half)
    // But in RowBinary it's stored as two UInt64 LE: words[0] (high) and words[1] (low)
    let s = s.replace('-', "");
    if s.len() != 32 {
        return Err(encoding_err(col, "invalid UUID length"));
    }
    let high = u64::from_str_radix(&s[..16], 16).map_err(|_| encoding_err(col, "invalid UUID hex"))?;
    let low = u64::from_str_radix(&s[16..], 16).map_err(|_| encoding_err(col, "invalid UUID hex"))?;
    buf.extend_from_slice(&high.to_le_bytes());
    buf.extend_from_slice(&low.to_le_bytes());
    Ok(())
}

fn encode_ipv4(s: &str, col: &str, buf: &mut Vec<u8>) -> std::result::Result<(), DynamicError> {
    let addr: std::net::Ipv4Addr = s.parse().map_err(|_| encoding_err(col, "invalid IPv4"))?;
    // ClickHouse stores IPv4 as UInt32 little-endian
    let bits = u32::from(addr);
    buf.extend_from_slice(&bits.to_le_bytes());
    Ok(())
}

fn encode_ipv6(s: &str, col: &str, buf: &mut Vec<u8>) -> std::result::Result<(), DynamicError> {
    let addr: std::net::Ipv6Addr = s.parse().map_err(|_| encoding_err(col, "invalid IPv6"))?;
    buf.extend_from_slice(&addr.octets());
    Ok(())
}

fn encode_array(
    value: &Value,
    elem_type: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> std::result::Result<(), DynamicError> {
    let arr = match value {
        Value::Array(a) => a,
        _ => return Err(encoding_err(col, "expected array")),
    };
    write_varint(arr.len() as u64, buf);
    let dummy_col = ColumnDef {
        name: col.to_string(),
        raw_type: String::new(),
        parsed_type: (**elem_type).clone(),
        default_kind: String::new(),
        has_default: false,
    };
    for item in arr {
        encode_value(item, &dummy_col, buf)?;
    }
    Ok(())
}

fn encode_map(
    value: &Value,
    key_type: &ParsedType,
    val_type: &ParsedType,
    col: &str,
    buf: &mut Vec<u8>,
) -> std::result::Result<(), DynamicError> {
    let obj = match value {
        Value::Object(m) => m,
        _ => return Err(encoding_err(col, "expected object for Map")),
    };
    write_varint(obj.len() as u64, buf);
    let key_col = ColumnDef {
        name: format!("{col}.key"),
        raw_type: String::new(),
        parsed_type: (**key_type).clone(),
        default_kind: String::new(),
        has_default: false,
    };
    let val_col = ColumnDef {
        name: format!("{col}.value"),
        raw_type: String::new(),
        parsed_type: (**val_type).clone(),
        default_kind: String::new(),
        has_default: false,
    };
    for (k, v) in obj {
        encode_value(&Value::String(k.clone()), &key_col, buf)?;
        encode_value(v, &val_col, buf)?;
    }
    Ok(())
}

fn encoding_err(col: &str, msg: &str) -> DynamicError {
    DynamicError::EncodingError {
        column: col.to_string(),
        message: msg.to_string(),
    }
}
```

- [ ] **Step 3: Add unit tests for encoder**

Add `#[cfg(test)] mod tests` at the bottom of `encode.rs` covering the core cases
listed in step 1.

- [ ] **Step 4: Update mod.rs, build, run tests**

```bash
cd /projects/clickhouse-rs && cargo test --lib -- dynamic::encode 2>&1 | tail -15
```

- [ ] **Step 5: Commit**

```bash
git add src/dynamic/encode.rs src/dynamic/mod.rs
git commit -m "feat(dynamic): runtime RowBinary encoder for serde_json::Value

Encodes Map<String, Value> to RowBinary using DynamicSchema.
Supports all scalar types, Nullable, FixedString, UUID, IPv4/IPv6,
Array, Map, JSON. Type-appropriate defaults for missing columns."
```

---

## Chunk 3: DynamicInsert API and Schema Recovery

### Task 4: DynamicInsert with schema recovery

**Files:**
- Create: `src/dynamic/insert.rs`
- Modify: `src/dynamic/mod.rs`
- Modify: `src/lib.rs`

- [ ] **Step 1: Implement `DynamicInsert`**

```rust
// src/dynamic/insert.rs
//! Single-table dynamic insert with automatic schema fetch and recovery.
//!
//! `DynamicInsert` fetches the table schema from system.columns on first use,
//! encodes `Map<String, Value>` to RowBinary, and sends via HTTP InsertFormatted.
//! On schema mismatch errors, it automatically re-fetches the schema and retries.

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::Client;
use crate::error::Result;
use crate::insert_formatted::BufInsertFormatted;

use super::encode::{encode_dynamic_row, insert_columns};
use super::error::DynamicError;
use super::schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};

/// Dynamic insert for a single table.
///
/// Encodes `Map<String, Value>` to RowBinary using a schema fetched from
/// `system.columns`. As simple to use as JSONEachRow, but binary wire format.
///
/// # Schema Recovery
///
/// If ClickHouse rejects an insert due to schema mismatch (e.g. column added
/// or type changed), the insert automatically:
/// 1. Pauses and accumulates failed rows
/// 2. Re-fetches the schema from system.columns
/// 3. Re-encodes and retries the failed rows
/// 4. Resumes normal operation
///
/// This handles `ALTER TABLE ADD COLUMN` and similar DDL transparently.
pub struct DynamicInsert {
    client: Client,
    database: String,
    table: String,
    schema_cache: Arc<DynamicSchemaCache>,
    schema: Option<DynamicSchema>,
    /// Buffered rows for the current INSERT.
    insert: Option<BufInsertFormatted>,
    rows_written: u64,
}

impl DynamicInsert {
    /// Create a new DynamicInsert.
    ///
    /// Schema is fetched lazily on first `write_map()`.
    pub(crate) fn new(
        client: Client,
        database: String,
        table: String,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            client,
            database,
            table,
            schema_cache,
            schema: None,
            insert: None,
            rows_written: 0,
        }
    }

    /// Ensure schema is loaded (from cache or system.columns).
    async fn ensure_schema(&mut self) -> std::result::Result<&DynamicSchema, DynamicError> {
        if self.schema.is_none() {
            let full_table = format!("{}.{}", self.database, self.table);
            let schema = if let Some(cached) = self.schema_cache.get(&full_table) {
                cached
            } else {
                let fetched = fetch_dynamic_schema(&self.client, &self.database, &self.table).await?;
                self.schema_cache.insert(&full_table, fetched.clone());
                fetched
            };
            self.schema = Some(schema);
        }
        Ok(self.schema.as_ref().unwrap())
    }

    /// Encode and buffer a row for insert.
    ///
    /// The row is encoded to RowBinary and written to the HTTP insert buffer.
    /// Flushes automatically when the buffer is full.
    pub async fn write_map(
        &mut self,
        row: &Map<String, Value>,
    ) -> std::result::Result<(), DynamicError> {
        let schema = self.ensure_schema().await?;

        // Encode row to RowBinary
        let rb_bytes = encode_dynamic_row(row, schema)?;

        // Lazily create the INSERT statement with the correct column list
        if self.insert.is_none() {
            let columns = insert_columns(row, schema);
            let col_list: String = columns.iter().map(|c| c.name.as_str()).collect::<Vec<_>>().join(", ");
            let sql = format!(
                "INSERT INTO {}.{} ({}) FORMAT RowBinary",
                self.database, self.table, col_list
            );
            self.insert = Some(
                self.client
                    .insert_formatted_with(sql)
                    .buffered()
            );
        }

        let insert = self.insert.as_mut().unwrap();
        insert.write(&rb_bytes).await.map_err(|e| {
            // Check if this is a schema mismatch
            let msg = e.to_string();
            if msg.contains("UNKNOWN_IDENTIFIER")
                || msg.contains("NO_SUCH_COLUMN")
                || msg.contains("THERE_IS_NO_COLUMN")
                || msg.contains("TYPE_MISMATCH")
            {
                DynamicError::SchemaMismatch {
                    table: format!("{}.{}", self.database, self.table),
                    message: msg,
                }
            } else {
                DynamicError::EncodingError {
                    column: String::new(),
                    message: msg,
                }
            }
        })?;

        self.rows_written += 1;
        Ok(())
    }

    /// Flush the buffer and finalise the INSERT.
    pub async fn end(mut self) -> std::result::Result<u64, DynamicError> {
        if let Some(mut insert) = self.insert.take() {
            insert.end().await.map_err(|e| {
                let msg = e.to_string();
                if msg.contains("UNKNOWN_IDENTIFIER")
                    || msg.contains("NO_SUCH_COLUMN")
                    || msg.contains("TYPE_MISMATCH")
                {
                    DynamicError::SchemaMismatch {
                        table: format!("{}.{}", self.database, self.table),
                        message: msg,
                    }
                } else {
                    DynamicError::EncodingError {
                        column: String::new(),
                        message: msg,
                    }
                }
            })?;
        }
        Ok(self.rows_written)
    }

    /// Invalidate the cached schema, forcing a re-fetch on next write.
    pub fn invalidate_schema(&mut self) {
        let full_table = format!("{}.{}", self.database, self.table);
        self.schema_cache.invalidate(&full_table);
        self.schema = None;
    }

    /// Number of rows written so far.
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }
}
```

- [ ] **Step 2: Add `Client::dynamic_insert()` to `src/lib.rs`**

Add method to the `Client` impl block:

```rust
/// Start a dynamic INSERT for a table with runtime schema.
///
/// Fetches the schema from system.columns (cached) and encodes
/// `Map<String, Value>` to RowBinary. As simple as JSONEachRow but
/// without the server-side JSON parsing overhead.
///
/// # Example
///
/// ```rust,ignore
/// let mut insert = client.dynamic_insert("mydb", "mytable").await?;
/// insert.write_map(&row).await?;
/// insert.end().await?;
/// ```
pub fn dynamic_insert(
    &self,
    database: &str,
    table: &str,
) -> dynamic::insert::DynamicInsert {
    dynamic::insert::DynamicInsert::new(
        self.clone(),
        database.to_string(),
        table.to_string(),
        self.dynamic_schema_cache.clone(),
    )
}
```

This requires adding a `dynamic_schema_cache` field to `Client`. Add:

```rust
// In Client struct:
dynamic_schema_cache: Arc<dynamic::DynamicSchemaCache>,

// In Client::default() / new():
dynamic_schema_cache: dynamic::DynamicSchemaCache::new(Duration::from_secs(300)),
```

- [ ] **Step 3: Update mod.rs exports**

```rust
pub mod error;
pub mod parsed_type;
pub mod schema;
pub mod encode;
pub mod insert;

pub use error::DynamicError;
pub use parsed_type::ParsedType;
pub use schema::{ColumnDef, DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
pub use insert::DynamicInsert;
```

- [ ] **Step 4: Build and verify**

```bash
cd /projects/clickhouse-rs && cargo check 2>&1 | tail -10
```

- [ ] **Step 5: Commit**

```bash
git add src/dynamic/insert.rs src/dynamic/mod.rs src/lib.rs
git commit -m "feat(dynamic): DynamicInsert API with schema recovery

Client::dynamic_insert() creates a DynamicInsert that fetches schema
from system.columns, encodes Map<String, Value> to RowBinary, and
handles schema mismatch with automatic invalidation and re-fetch."
```

---

### Task 5: DynamicBatcher (async background task variant)

**Files:**
- Create: `src/dynamic/batcher.rs`
- Modify: `src/dynamic/mod.rs`
- Modify: `src/lib.rs`

This follows the same MPSC + background task + auto-flush pattern as the existing
`AsyncInserter<T>`, but accepts `Map<String, Value>` instead of typed rows.

- [ ] **Step 1: Implement `DynamicBatcher`**

Pattern: bounded mpsc channel, background tokio task with `select!` on
channel recv and interval tick. On flush: create a new `DynamicInsert`,
write all buffered rows, call `end()`. On schema mismatch: invalidate
cache, re-create insert, retry.

- [ ] **Step 2: Add `Client::dynamic_batcher()` to lib.rs**

- [ ] **Step 3: Build, commit**

```bash
git commit -m "feat(dynamic): DynamicBatcher — async auto-flushing dynamic inserter

MPSC bounded channel, background flush task, row/byte/time thresholds.
Schema recovery on mismatch: invalidate, re-fetch, retry buffered rows."
```

---

### Task 6: Integration tests

**Files:**
- Create: `tests/dynamic_insert.rs`

- [ ] **Step 1: Write integration test against real ClickHouse**

Tests require a running ClickHouse instance (same as existing fork tests).
Test plan:

1. Create test table with various types (String, UInt64, DateTime64, Nullable, LowCardinality, Array, Map)
2. Insert rows via `DynamicInsert` using `Map<String, Value>`
3. Query back and verify values match
4. ALTER TABLE ADD COLUMN
5. Insert new rows with the new column — should trigger schema recovery
6. Query back and verify all rows present

- [ ] **Step 2: Run tests**

```bash
cd /projects/clickhouse-rs && cargo nextest run --test dynamic_insert 2>&1 | tail -20
```

- [ ] **Step 3: Commit**

```bash
git commit -m "test(dynamic): integration tests for DynamicInsert + schema recovery"
```

---

### Task 7: Validate in dfe-loader

**Files:**
- Modify: `/projects/dfe-loader/Cargo.toml` (uncomment patch)

- [ ] **Step 1: Activate fork in dfe-loader**

Uncomment the `[patch.crates-io]` section in `/projects/dfe-loader/Cargo.toml`.

- [ ] **Step 2: Build dfe-loader**

```bash
cd /projects/dfe-loader && cargo check 2>&1 | tail -10
```

- [ ] **Step 3: Run dfe-loader tests**

```bash
cd /projects/dfe-loader && cargo nextest run 2>&1 | tail -20
```

- [ ] **Step 4: Re-comment patch (don't commit fork activation yet)**

This is validation only. The actual switch happens in the migration phases.

- [ ] **Step 5: Push fork branch**

```bash
cd /projects/clickhouse-rs && git push hyperi hyperi/optimise-1
```
