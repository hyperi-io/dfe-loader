// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse_ext/insert.rs
// Purpose:   DynamicInsert adapter over the ClickHouse HTTP RowBinary sink
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Single-table dynamic insert with lazy schema fetch and mismatch recovery.
//!
//! `DynamicInsert` fetches the table schema from `system.columns` on first
//! write, encodes each `Map<String, Value>` against it, and ships the rows
//! through the HTTP client. On a schema-mismatch error it invalidates the
//! cached schema so the next insert re-fetches.
//!
//! # Sink
//!
//! The only sink is `Client::insert_formatted_with(... FORMAT
//! RowBinaryWithNamesAndTypes)` over HTTP: a header naming each column and the
//! type it was encoded for, then row-wise `encode()` bytes.
//! `clickhouse.protocol: native` is refused at startup.

use std::sync::Arc;

use serde_json::{Map, Value};

use dfe_clickhouse::TcpClient;
use dfe_clickhouse::native::{ColumnSchema, encode_columns};

use super::encode::{ColumnDef, DynamicRow};
use super::error::DynamicError;
use super::schema::{DynamicSchema, DynamicSchemaCache, fetch_dynamic_schema};
use crate::clickhouse::Timeouts;
use crate::clickhouse::client_http::{ChClient, ReadBound, insert_sink};
use crate::clickhouse::error::{error_text, native_error_text};

/// Bytes of encoded rows per native block, so one wide batch is not one
/// unbounded block.
const NATIVE_BLOCK_BYTES: usize = 1 << 20;

/// Where the encoded rows go: an HTTP request body, or rows held for the
/// native-protocol INSERT that [`DynamicInsert::end`] sends.
enum Sink {
    Http(clickhouse::insert_formatted::BufInsertFormatted),
    Native { sql: String, rows: Vec<Vec<u8>> },
}

/// The server setting that makes it read a length-prefixed
/// string as a JSON value, which is how the dynamic encoder writes JSON
/// columns on the wire.
const JSON_AS_STRING_SETTING: &str = "input_format_binary_read_json_as_string";

/// Settings that make the server check the insert header's column names and
/// types against the table and refuse a mismatch (code 117), pinned on so a
/// user profile cannot turn the check off.
const HEADER_CHECK_SETTINGS: [&str; 2] = [
    "input_format_with_names_use_header",
    "input_format_with_types_use_header",
];

/// Append `value` as the unsigned LEB128 varint `RowBinary` lengths use.
fn write_varint(out: &mut Vec<u8>, mut value: usize) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// The `RowBinaryWithNamesAndTypes` header for `columns`: the column count,
/// then each name, then each type as `system.columns` reported it.
fn names_and_types_header(columns: &[ColumnDef]) -> Vec<u8> {
    let mut header = Vec::new();
    write_varint(&mut header, columns.len());
    for text in columns
        .iter()
        .map(|c| c.name.as_str())
        .chain(columns.iter().map(|c| c.type_string.as_str()))
    {
        write_varint(&mut header, text.len());
        header.extend_from_slice(text.as_bytes());
    }
    header
}

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
    client: ChClient,
    database: String,
    table: String,
    schema_cache: Arc<DynamicSchemaCache>,
    schema: Option<DynamicSchema>,
    /// Resolved column subset for the active INSERT (fixed from the first row).
    insert_columns: Option<Vec<ColumnDef>>,
    /// The active sink, created lazily on first write.
    sink: Option<Sink>,
    rows_written: u64,
    /// Whether a schema-mismatch error at `end` drops the cached schema.
    refresh_on_error: bool,
    /// Bounds on the insert.
    timeouts: Timeouts,
    /// Bound on the schema fetch.
    read_bound: Arc<ReadBound>,
}

impl DynamicInsert {
    /// Create a `DynamicInsert`. The schema is fetched lazily on first write.
    pub fn new(
        client: impl Into<ChClient>,
        database: impl Into<String>,
        table: impl Into<String>,
        schema_cache: Arc<DynamicSchemaCache>,
    ) -> Self {
        Self {
            client: client.into(),
            database: database.into(),
            table: table.into(),
            schema_cache,
            schema: None,
            insert_columns: None,
            sink: None,
            rows_written: 0,
            refresh_on_error: true,
            timeouts: Timeouts::default(),
            read_bound: Arc::new(ReadBound::new(Timeouts::default().request)),
        }
    }

    /// Set the bounds on the insert (default [`Timeouts::default`]).
    pub fn with_timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Share the bound the schema fetch runs under, so a timeout here widens
    /// the next read anywhere it is shared.
    pub fn with_read_bound(mut self, read_bound: Arc<ReadBound>) -> Self {
        self.read_bound = read_bound;
        self
    }

    /// Set whether a schema-mismatch error at [`end`][Self::end] drops the
    /// cached schema (default `true`). With `false` the stale schema stays
    /// until the cache's TTL expires.
    pub fn refresh_on_error(mut self, enabled: bool) -> Self {
        self.refresh_on_error = enabled;
        self
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
            let fetched =
                fetch_dynamic_schema(&self.client, &self.database, &self.table, &self.read_bound)
                    .await?;
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
    ///
    /// Synchronous: opening the sink is a pure builder step with no suspension
    /// point. It sits between the async `ensure_schema` and `encode_and_write`
    /// in the write path.
    fn ensure_sink(
        &mut self,
        row: &Map<String, Value>,
        raw_names: &[&str],
    ) -> Result<(), DynamicError> {
        if self.sink.is_some() {
            return Ok(());
        }
        let schema = self
            .schema
            .as_ref()
            .ok_or_else(|| DynamicError::EncodingError {
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
        // Bytes encoded against a stale schema can parse as other rows, so the
        // header names the types they were encoded for and the server refuses
        // a mismatch.
        let cols_sql = columns
            .iter()
            .map(|c| escape_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let target = format!(
            "{}.{} ({cols_sql})",
            escape_ident(&self.database),
            escape_ident(&self.table),
        );
        let sink = match &self.client {
            ChClient::Http(client) => {
                let sql = format!("INSERT INTO {target} FORMAT RowBinaryWithNamesAndTypes");
                let mut client = client.clone();
                for setting in HEADER_CHECK_SETTINGS {
                    client = client.with_setting(setting, "1");
                }
                if schema.has_json_columns() {
                    client = client.with_setting(JSON_AS_STRING_SETTING, "1");
                }
                let mut sink = insert_sink(&client, sql, self.timeouts).buffered();
                sink.write_buffered(&names_and_types_header(&columns));
                Sink::Http(sink)
            }
            // A native block declares each column's type, and a JSON column is
            // declared String, which the server casts.
            ChClient::Native(_) => Sink::Native {
                sql: format!("INSERT INTO {target} FORMAT Native"),
                rows: Vec::new(),
            },
        };

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
        self.ensure_sink(row, &[])?;
        self.encode_and_write(row, None)
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
        self.ensure_sink(row, &raw_names)?;
        self.encode_and_write(row, raw_columns.first().copied())
    }

    /// Build the dynamic row (with optional raw passthrough) and write it.
    ///
    /// Synchronous: RowBinary encode and the buffered write are in-memory with
    /// no suspension point. The actual network flush happens in `end`.
    fn encode_and_write(
        &mut self,
        row: &Map<String, Value>,
        raw: Option<(&str, &[u8])>,
    ) -> Result<(), DynamicError> {
        // Encode to RowBinary in a scope so the immutable borrow of
        // insert_columns ends before borrowing self.sink mutably.
        let bytes = {
            let columns =
                self.insert_columns
                    .as_ref()
                    .ok_or_else(|| DynamicError::EncodingError {
                        column: String::new(),
                        message: "insert columns not initialised".to_string(),
                    })?;
            let dyn_row = match raw {
                Some((json_col, raw_bytes)) => {
                    DynamicRow::with_raw(row, columns, raw_bytes, json_col)
                }
                None => DynamicRow::new(row, columns),
            };
            dyn_row.encode()?
        };
        let sink = self
            .sink
            .as_mut()
            .ok_or_else(|| DynamicError::EncodingError {
                column: String::new(),
                message: "sink not initialised".to_string(),
            })?;
        match sink {
            Sink::Http(insert) => insert.write_buffered(&bytes),
            Sink::Native { rows, .. } => rows.push(bytes),
        }
        self.rows_written += 1;
        Ok(())
    }

    /// Flush and finalise the INSERT, returning the number of rows written.
    ///
    /// On a schema-mismatch error the cached schema is invalidated so the next
    /// insert re-fetches from `system.columns`, unless
    /// [`refresh_on_error`][Self::refresh_on_error] turned that off.
    ///
    /// # Errors
    ///
    /// Returns [`DynamicError`] if the server rejects the insert, or
    /// [`DynamicError::TimedOut`] if its answer does not arrive within the bound.
    pub async fn end(mut self) -> Result<u64, DynamicError> {
        let outcome = match self.sink.take() {
            None => Ok(()),
            Some(Sink::Http(mut insert)) => insert.end().await.map_err(|e| {
                if matches!(e, clickhouse::error::Error::TimedOut) {
                    None
                } else {
                    Some(error_text(&e))
                }
            }),
            Some(Sink::Native { sql, rows }) => {
                let columns = self.insert_columns.take().unwrap_or_default();
                let sent = match &self.client {
                    ChClient::Native(client) => {
                        tokio::time::timeout(
                            self.timeouts.insert,
                            send_native(client, &sql, &columns, &rows),
                        )
                        .await
                    }
                    ChClient::Http(_) => unreachable!("a native sink is opened on a native client"),
                };
                sent.unwrap_or(Err(dfe_clickhouse::Error::TimedOut))
                    .map_err(|e| {
                        if matches!(e, dfe_clickhouse::Error::TimedOut) {
                            None
                        } else {
                            Some(native_error_text(&e))
                        }
                    })
            }
        };
        match outcome {
            Ok(()) => Ok(self.rows_written),
            Err(None) => Err(DynamicError::TimedOut {
                table: self.full_table(),
            }),
            Err(Some(message)) => {
                let err = classify_error(&self.full_table(), message);
                if self.refresh_on_error && matches!(err, DynamicError::SchemaMismatch { .. }) {
                    self.schema_cache.invalidate(&self.full_table());
                }
                Err(err)
            }
        }
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

    /// Take the resolved schema the rows were encoded against. [`end`][Self::end]
    /// still finishes the insert: it needs only the rows already written.
    pub fn take_schema(&mut self) -> Option<DynamicSchema> {
        self.schema.take()
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
        .filter(|c| {
            row.contains_key(&c.name) || raw_names.contains(&c.name.as_str()) || !c.has_default
        })
        .cloned()
        .collect()
}

/// Send `rows` as native blocks of at most [`NATIVE_BLOCK_BYTES`] and commit.
async fn send_native(
    client: &TcpClient,
    sql: &str,
    columns: &[ColumnDef],
    rows: &[Vec<u8>],
) -> dfe_clickhouse::Result<()> {
    let headers: Vec<(String, String)> = columns
        .iter()
        .map(|c| (c.name.clone(), c.type_string.clone()))
        .collect();
    let schema = ColumnSchema::from_headers(&headers)?;
    // Empty query id: the server assigns one.
    let session = client.insert_native("", sql).await?;
    let mut start = 0;
    while start < rows.len() {
        let mut end = start;
        let mut bytes = 0;
        while end < rows.len() && (end == start || bytes + rows[end].len() <= NATIVE_BLOCK_BYTES) {
            bytes += rows[end].len();
            end += 1;
        }
        let sent = match encode_columns(&rows[start..end], &schema, session.server_revision) {
            Ok(block) => {
                session
                    .send_block(block, schema.len() as u64, (end - start) as u64)
                    .await
            }
            Err(e) => Err(e),
        };
        if let Err(e) = sent {
            session.abort();
            return Err(e);
        }
        start = end;
    }
    session.finish().await
}

/// Classify a client error's text as a schema mismatch (cached schema is
/// stale) or a generic encoding/transport error. Schema mismatch covers
/// explicit column/type errors and the data-format errors that indicate
/// schema drift.
fn classify_error(full_table: &str, msg: String) -> DynamicError {
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
    fn the_header_carries_each_name_then_each_type_as_varint_strings() {
        let columns = vec![
            ColumnDef::with_default_kind("id", "UInt64", ""),
            ColumnDef::with_default_kind("_json", "JSON(max_dynamic_paths = 2048)", ""),
        ];
        let mut expected = vec![2, 2];
        expected.extend_from_slice(b"id");
        expected.push(5);
        expected.extend_from_slice(b"_json");
        expected.push(6);
        expected.extend_from_slice(b"UInt64");
        expected.push(30);
        expected.extend_from_slice(b"JSON(max_dynamic_paths = 2048)");
        assert_eq!(names_and_types_header(&columns), expected);

        // A length past 127 takes a second varint byte.
        let mut long = Vec::new();
        write_varint(&mut long, 300);
        assert_eq!(long, vec![0xac, 0x02]);
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
        let e = clickhouse::error::Error::BadResponse(
            "Code: 117. DB::Exception: incorrect data (version 26.3.42.3 (official build))".into(),
        );
        assert!(matches!(
            classify_error("db.t", error_text(&e)),
            DynamicError::SchemaMismatch { .. }
        ));

        let e = dfe_clickhouse::Error::ServerException {
            code: 117,
            name: Some("INCORRECT_DATA".into()),
            message: "incorrect data".into(),
            stack_trace: None,
        };
        assert!(matches!(
            classify_error("db.t", native_error_text(&e)),
            DynamicError::SchemaMismatch { .. }
        ));

        let e = clickhouse::error::Error::Custom("network reset".into());
        assert!(matches!(
            classify_error("db.t", error_text(&e)),
            DynamicError::EncodingError { .. }
        ));
    }
}
