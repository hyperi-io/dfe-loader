// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/insert.rs
// Purpose:   DynamicInsert adapter over the clickhouse-rs RowBinary/Native sinks
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Single-table dynamic insert with lazy schema fetch and mismatch recovery.
//!
//! `DynamicInsert` fetches the table schema from `system.columns` on first
//! write, encodes each `Map<String, Value>` against it, and ships the rows
//! through a clickhouse-rs sink. On a schema-mismatch error it invalidates the
//! cached schema so the next insert re-fetches.
//!
//! # Sink abstraction
//!
//! The active sink is `Client::insert_formatted_with(... FORMAT RowBinary)`
//! (HTTP), fed row-wise `encode()` bytes -- the path proven against the live
//! cluster. FORMAT Native over HTTP (`InsertNative::with_columns`) mis-frames
//! the block on this server and is tracked as clickhouse-rs#15.
//!
//! The native/TCP sink (`insert_native_with_columns` -> `with_columns_tcp`,
//! clickhouse-rs#14) is selected by transport once #15 lands; only the
//! `ensure_sink` internals here change -- the encoder and the public
//! `write_map` API are unaffected.

use std::sync::Arc;

use serde_json::{Map, Value};

use clickhouse::Client;

use super::encode::{ColumnDef, DynamicRow};
use super::error::DynamicError;
use super::schema::{fetch_dynamic_schema, DynamicSchema, DynamicSchemaCache};

/// The clickhouse-rs setting that makes the server read a length-prefixed
/// string as a JSON value, which is how the dynamic encoder writes JSON
/// columns on the wire.
const JSON_AS_STRING_SETTING: &str = "input_format_binary_read_json_as_string";

/// Backtick-quote a SQL identifier, doubling any internal backticks.
fn escape_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Dynamic insert for a single table.
///
/// Encodes runtime-shaped `Map<String, Value>` rows to ClickHouse using a
/// schema fetched from `system.columns`. As simple to drive as JSONEachRow,
/// but binary on the wire so the server skips JSON parsing.
#[must_use = "a DynamicInsert must be finished with `.end().await` to commit the rows"]
pub struct DynamicInsert {
    client: Client,
    database: String,
    table: String,
    schema_cache: Arc<DynamicSchemaCache>,
    schema: Option<DynamicSchema>,
    /// Resolved column subset for the active INSERT (fixed from the first row).
    insert_columns: Option<Vec<ColumnDef>>,
    /// The active sink (HTTP `FORMAT RowBinary`), created lazily on first write.
    sink: Option<clickhouse::insert_formatted::BufInsertFormatted>,
    rows_written: u64,
}

impl DynamicInsert {
    /// Create a `DynamicInsert`. The schema is fetched lazily on first write.
    pub fn new(
        client: Client,
        database: impl Into<String>,
        table: impl Into<String>,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            client,
            database: database.into(),
            table: table.into(),
            schema_cache,
            schema: None,
            insert_columns: None,
            sink: None,
            rows_written: 0,
        }
    }

    fn full_table(&self) -> String {
        format!("{}.{}", self.database, self.table)
    }

    /// Load the schema (from cache, else `system.columns`) if not already held.
    async fn ensure_schema(&mut self) -> Result<(), DynamicError> {
        if self.schema.is_some() {
            return Ok(());
        }
        let full = self.full_table();
        let schema = if let Some(cached) = self.schema_cache.get(&full) {
            cached
        } else {
            let fetched = fetch_dynamic_schema(&self.client, &self.database, &self.table).await?;
            self.schema_cache.insert(&full, fetched.clone());
            fetched
        };
        self.schema = Some(schema);
        Ok(())
    }

    /// On the first row, fix the column subset and open the sink.
    ///
    /// `raw_names` are column names supplied via raw passthrough (e.g. `_json`)
    /// that must be in the INSERT even if absent from the row map.
    async fn ensure_sink(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
    ) -> Result<(), DynamicError> {
        if self.sink.is_some() {
            return Ok(());
        }
        let schema = self.schema.as_ref().ok_or_else(|| DynamicError::EncodingError {
            column: String::new(),
            message: "schema not available after fetch".to_string(),
        })?;

        let columns = select_columns(row, raw_names, schema);
        if columns.is_empty() {
            return Err(DynamicError::EncodingError {
                column: String::new(),
                message: "no columns to insert for this row".to_string(),
            });
        }
        // Stream encode()'d rows into `INSERT ... FORMAT RowBinary`. The server
        // parses RowBinary row-wise directly -- the proven pre-migration path.
        // (FORMAT Native, via InsertNative's columnar transpose, is rejected by
        // the server here; tracked as a fork issue. RowBinary is HTTP-side.)
        let cols_sql = columns
            .iter()
            .map(|c| escape_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "INSERT INTO {}.{} ({cols_sql}) FORMAT RowBinary",
            escape_ident(&self.database),
            escape_ident(&self.table),
        );
        let mut client = self.client.clone();
        if schema.has_json_columns() {
            client = client.with_setting(JSON_AS_STRING_SETTING, "1");
        }
        let sink = client.insert_formatted_with(sql).buffered();

        self.sink = Some(sink);
        self.insert_columns = Some(columns);
        Ok(())
    }

    /// Encode and buffer a row for insert. Fetches the schema and opens the
    /// sink on the first call.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] on schema fetch failure, an unsupported column
    /// type, an encoding failure, or a transport error.
    pub async fn write_map(&mut self, row: &Map<String, Value>) -> Result<(), DynamicError> {
        self.ensure_schema().await?;
        self.ensure_sink(row, &[]).await?;
        self.encode_and_write(row, None).await
    }

    /// Like [`write_map`][Self::write_map], but the named columns are written
    /// from pre-encoded raw bytes (e.g. the original payload for `_json`),
    /// avoiding a re-serialise. Only the first raw column is threaded through
    /// the encoder's zero-copy passthrough; any others fall back to the row map.
    ///
    /// # Errors
    ///
    /// As [`write_map`][Self::write_map].
    pub async fn write_map_with_raw(
        &mut self,
        row: &Map<String, Value>,
        raw_columns: &[(&str, &[u8])],
    ) -> Result<(), DynamicError> {
        let raw_names: Vec<&str> = raw_columns.iter().map(|(n, _)| *n).collect();
        self.ensure_schema().await?;
        self.ensure_sink(row, &raw_names).await?;
        self.encode_and_write(row, raw_columns.first().copied()).await
    }

    /// Build the dynamic row (with optional raw passthrough) and write it.
    async fn encode_and_write(
        &mut self,
        row: &Map<String, Value>,
        raw: Option<(&str, &[u8])>,
    ) -> Result<(), DynamicError> {
        // Encode to RowBinary in a scope so the immutable borrow of
        // insert_columns ends before borrowing self.sink mutably.
        let bytes = {
            let columns =
                self.insert_columns.as_ref().ok_or_else(|| DynamicError::EncodingError {
                    column: String::new(),
                    message: "insert columns not initialised".to_string(),
                })?;
            let dyn_row = match raw {
                Some((json_col, raw_bytes)) => DynamicRow::with_raw(row, columns, raw_bytes, json_col),
                None => DynamicRow::new(row, columns),
            };
            dyn_row.encode()?
        };
        let sink = self.sink.as_mut().ok_or_else(|| DynamicError::EncodingError {
            column: String::new(),
            message: "sink not initialised".to_string(),
        })?;
        sink.write_buffered(&bytes);
        self.rows_written += 1;
        Ok(())
    }

    /// Flush and finalise the INSERT, returning the number of rows written.
    ///
    /// On a schema-mismatch error the cached schema is invalidated so the next
    /// insert re-fetches from `system.columns`.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] if the server rejects the insert.
    pub async fn end(mut self) -> Result<u64, DynamicError> {
        if let Some(mut sink) = self.sink.take()
            && let Err(e) = sink.end().await
        {
            let err = classify_error(&self.full_table(), &e);
            if matches!(err, DynamicError::SchemaMismatch { .. }) {
                self.schema_cache.invalidate(&self.full_table());
            }
            return Err(err);
        }
        Ok(self.rows_written)
    }

    /// Invalidate the cached schema, forcing a re-fetch on the next insert.
    pub fn invalidate_schema(&mut self) {
        self.schema_cache.invalidate(&self.full_table());
        self.schema = None;
    }

    /// Number of rows written so far.
    #[must_use]
    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    /// The resolved schema, if it has been loaded.
    #[must_use]
    pub fn schema(&self) -> Option<&DynamicSchema> {
        self.schema.as_ref()
    }
}

/// Choose the columns to include in the INSERT: every column present in the
/// row, every raw-passthrough column, and every required column (no
/// server-side default). Columns that have a default and are not supplied are
/// omitted so the server fills them (e.g. `_uuid`, `_timestamp_load`).
fn select_columns(
    row: &Map<String, Value>,
    raw_names: &[&str],
    schema: &DynamicSchema,
) -> Vec<ColumnDef> {
    schema
        .columns
        .iter()
        .filter(|c| row.contains_key(&c.name) || raw_names.contains(&c.name.as_str()) || !c.has_default)
        .cloned()
        .collect()
}

/// Classify a clickhouse-rs error as a schema mismatch (cached schema is stale)
/// or a generic encoding/transport error. Schema mismatch covers explicit
/// column/type errors and the data-format errors that indicate schema drift.
fn classify_error(full_table: &str, e: &clickhouse::error::Error) -> DynamicError {
    let msg = e.to_string();
    let mismatch = msg.contains("UNKNOWN_IDENTIFIER")
        || msg.contains("NO_SUCH_COLUMN")
        || msg.contains("THERE_IS_NO_COLUMN")
        || msg.contains("TYPE_MISMATCH")
        || msg.contains("ILLEGAL_COLUMN")
        || msg.contains("CANNOT_PARSE")
        || msg.contains("cannot parse")
        || msg.contains("INCORRECT_DATA")
        || msg.contains("incorrect data")
        || msg.contains("Code: 117");
    if mismatch {
        DynamicError::SchemaMismatch {
            table: full_table.to_string(),
            message: msg,
        }
    } else {
        DynamicError::EncodingError {
            column: String::new(),
            message: msg,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> DynamicSchema {
        DynamicSchema::from_columns(
            "db.t",
            vec![
                ColumnDef::with_default_kind("id", "UInt64", ""),
                ColumnDef::with_default_kind("name", "String", ""),
                ColumnDef::with_default_kind("_uuid", "UUID", "DEFAULT"),
                ColumnDef::with_default_kind("_json", "JSON", ""),
            ],
        )
    }

    #[test]
    fn select_columns_includes_present_and_required_omits_absent_default() {
        let s = schema();
        let row = json!({ "id": 1, "name": "a" });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &[], &s);
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        // id, name present; _json required (no default); _uuid omitted (default, absent)
        assert_eq!(names, vec!["id", "name", "_json"]);
    }

    #[test]
    fn select_columns_includes_raw_passthrough_column() {
        let s = schema();
        let row = json!({ "id": 1 });
        let obj = row.as_object().unwrap().clone();
        let cols = select_columns(&obj, &["_json"], &s);
        assert!(cols.iter().any(|c| c.name == "_json"));
    }

    #[test]
    fn classify_recognises_schema_drift() {
        let e = clickhouse::error::Error::Custom("Code: 117. DB::Exception: incorrect data".into());
        assert!(matches!(
            classify_error("db.t", &e),
            DynamicError::SchemaMismatch { .. }
        ));

        let e = clickhouse::error::Error::Custom("network reset".into());
        assert!(matches!(
            classify_error("db.t", &e),
            DynamicError::EncodingError { .. }
        ));
    }
}
