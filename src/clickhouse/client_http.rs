// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/client_http.rs
// Purpose:   ClickHouse HTTP client for DDL, schema queries, and health checks
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` client for DDL, schema queries, and health checks.
//!
//! Uses a single `clickhouse::Client` (from the `HyperI` fork) -- HTTP or
//! native TCP based on config. Data inserts go through `DynamicInsert`
//! (`RowBinary`, via `clickhouse_ext`) or `InsertFormatted` (`JSONEachRow`)
//! -- see `Inserter` for insert dispatch.
//!
//! This client handles:
//! - DDL execution (CREATE, ALTER, DROP)
//! - Schema fetching from `system.columns`
//! - Table existence checks
//! - Health checks (ping)

use std::sync::Arc;

use rustc_hash::FxHashMap;
use serde::Deserialize;

use super::config::ClickHouseConfig;
use super::error::ClickHouseError;
use super::types::{ColumnInfo, ParsedType, TableSchema};

/// Result type for `ClickHouse` operations.
pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// `ClickHouse` client for DDL, schema queries, and health checks.
///
/// Wraps a single `clickhouse::Client` (HTTP or TCP per config). Data inserts
/// are handled by `Inserter` via `DynamicInsert` (RowBinary) or
/// `InsertFormatted` (JSONEachRow).
pub struct ClickHouseQueryClient {
    /// The fork client -- HTTP or native TCP depending on config transport.
    ch_client: clickhouse::Client,
    /// Database name.
    database: String,
}

/// Connection-pool statistics for the native TCP transport.
///
/// The hyperi-port chain's deadpool TCP pool does not yet expose its status
/// publicly (the pool handle is crate-private), so `Inserter::pool_stats`
/// returns `None` and these gauges stay flat. Follow-up: surface deadpool
/// `Status` on the fork `Client`, then map it here.
#[derive(Debug, Clone, Copy, Default)]
pub struct PoolStats {
    /// Configured maximum pool size.
    pub max_size: usize,
    /// Current number of connections managed by the pool.
    pub size: usize,
    /// Connections currently idle and available.
    pub available: usize,
    /// Callers waiting for a connection.
    pub waiting: usize,
}

/// Build a `clickhouse::Client` from config, selecting HTTP or native TCP
/// transport. Shared by the query client and the inserter so both use an
/// identically-configured client.
///
/// # Errors
///
/// Returns an error if the config has no hosts.
pub(crate) fn build_client(config: &ClickHouseConfig) -> Result<clickhouse::Client> {
    use super::config::Transport;

    let endpoint = config
        .primary_endpoint()
        .ok_or_else(|| ClickHouseError::Connection("No ClickHouse hosts configured".into()))?;

    let client = match config.transport {
        Transport::Http => {
            let scheme = if config.tls { "https" } else { "http" };
            let mut c = clickhouse::Client::default()
                .with_url(format!("{scheme}://{endpoint}"))
                .with_user(&config.username)
                .with_database(&config.database);
            if !config.password.is_empty() {
                c = c.with_password(&config.password);
            }
            c
        }
        Transport::Native => {
            // Strip the port for the TLS SNI server name.
            let host = endpoint.split(':').next().unwrap_or(&endpoint).to_string();
            let mut c = if config.tls {
                clickhouse::Client::tcp_tls(endpoint.clone(), host)
            } else {
                clickhouse::Client::tcp(endpoint.clone())
            };
            c = c
                .with_user(&config.username)
                .with_database(&config.database)
                .with_compression(clickhouse::Compression::Lz4);
            if !config.password.is_empty() {
                c = c.with_password(&config.password);
            }
            // Multi-host failover: the pool round-robins the endpoint list and
            // skips a refusing endpoint within one acquire pass.
            if config.hosts.len() > 1 {
                c = c.with_tcp_addrs(config.hosts.iter().cloned());
            }
            c
        }
    };

    Ok(client)
}

/// Row type for system.columns queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct SystemColumn {
    name: String,
    r#type: String,
    position: u64,
    default_kind: String,
    default_expression: String,
    comment: String,
    is_in_primary_key: u8,
    is_in_sorting_key: u8,
}

/// Row type for single-string result queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct SingleString {
    value: String,
}

/// Row type for table name queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct TableName {
    name: String,
}

/// Row type for scalar count queries.
#[derive(Debug, Deserialize, clickhouse::Row)]
struct CountRow {
    count: u64,
}

impl ClickHouseQueryClient {
    /// Create a new client from config, dispatching to HTTP or native TCP.
    ///
    /// # Errors
    ///
    /// Returns an error if the config has no hosts.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        let ch_client = build_client(config)?;
        Ok(Self {
            ch_client,
            database: config.database.clone(),
        })
    }

    /// Execute a DDL or DML statement (CREATE, DROP, ALTER, TRUNCATE, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        self.ch_client
            .query(sql)
            .execute()
            .await
            .map_err(|e| ClickHouseError::Query(format!("{e}")))?;
        Ok(())
    }

    /// Fetch table schema from system.columns.
    ///
    /// Queries `ClickHouse` directly for column metadata, which gives us
    /// actual `ClickHouse` type strings (better than reverse-engineering from Arrow).
    ///
    /// # Errors
    ///
    /// Returns an error if the table doesn't exist or the query fails.
    pub async fn fetch_table_schema(&self, table: &str) -> Result<TableSchema> {
        let (db, tbl) = parse_db_table(table, &self.database);

        let sql = format!(
            "SELECT name, type, position, default_kind, default_expression, comment, \
             is_in_primary_key, is_in_sorting_key \
             FROM system.columns \
             WHERE database = {} AND table = {} \
             ORDER BY position",
            escape_string(&db),
            escape_string(&tbl)
        );

        let rows: Vec<SystemColumn> =
            self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
                ClickHouseError::Schema(format!("Failed to fetch schema for {table}: {e}"))
            })?;

        if rows.is_empty() {
            return Err(ClickHouseError::Schema(format!(
                "Table {db}.{tbl} not found or has no columns"
            )));
        }

        let columns: Vec<ColumnInfo> = rows
            .into_iter()
            .map(|row| ColumnInfo {
                parsed_type: ParsedType::parse(&row.r#type),
                name: row.name,
                type_name: row.r#type,
                position: row.position,
                default_kind: row.default_kind,
                default_expression: row.default_expression,
                comment: row.comment,
                is_in_primary_key: row.is_in_primary_key != 0,
                is_in_sorting_key: row.is_in_sorting_key != 0,
            })
            .collect();

        let comment = self.fetch_table_comment(table).await.unwrap_or_default();

        Ok(TableSchema {
            database: db,
            table: tbl,
            columns,
            comment,
        })
    }

    /// Fetch a table's COMMENT string from system.tables.
    ///
    /// Returns empty string if no comment is set or fetch fails.
    pub async fn fetch_table_comment(&self, table: &str) -> Result<String> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT comment AS value FROM system.tables WHERE database = {} AND name = {}",
            escape_string(&db),
            escape_string(&tbl)
        );

        let rows: Vec<SingleString> =
            self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
                ClickHouseError::Schema(format!("Failed to fetch table comment: {e}"))
            })?;

        Ok(rows.into_iter().next().map(|r| r.value).unwrap_or_default())
    }

    /// Fetch column comments for a table.
    ///
    /// Returns a map of `column_name` -> comment for columns with non-empty comments.
    pub async fn fetch_column_comments(&self, table: &str) -> Result<FxHashMap<String, String>> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let sql = format!(
            "SELECT name, comment FROM system.columns \
             WHERE database = {} AND table = {} AND comment != '' \
             ORDER BY position",
            escape_string(&db),
            escape_string(&tbl)
        );

        // The clickhouse crate requires a Row type for fetch_all.
        // For two-column results, use a simple struct.
        #[derive(Deserialize, clickhouse::Row)]
        struct NameComment {
            name: String,
            comment: String,
        }

        let rows: Vec<NameComment> = self.ch_client.query(&sql).fetch_all().await.map_err(|e| {
            ClickHouseError::Schema(format!("Failed to fetch column comments: {e}"))
        })?;

        let mut comments = FxHashMap::default();
        for row in rows {
            comments.insert(row.name, row.comment);
        }
        Ok(comments)
    }

    /// Check if a table exists.
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.fetch_table_schema(table).await {
            Ok(_) => Ok(true),
            Err(ClickHouseError::Schema(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List all tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name AS name FROM system.tables WHERE database = {}",
            escape_string(&self.database)
        );

        let rows: Vec<TableName> = self
            .ch_client
            .query(&sql)
            .fetch_all()
            .await
            .map_err(|e| ClickHouseError::Schema(format!("Failed to list tables: {e}")))?;

        Ok(rows.into_iter().map(|r| r.name).collect())
    }

    /// Execute a `SELECT COUNT(*)` query and return the count.
    ///
    /// # Arguments
    ///
    /// * `table` - Table name (may include "db.table" format)
    /// * `where_clause` - Optional WHERE condition (without "WHERE" keyword).
    ///   Restricted to a conservative character set
    ///   `[A-Za-z0-9_.,= <>!'%-]` and a length cap so SQL fragments cannot
    ///   smuggle subqueries, semicolons, or comment markers. Anything
    ///   richer must be expressed as parameter binding (this method is
    ///   for internal/test usage only).
    ///
    /// # Errors
    ///
    /// Returns `ClickHouseError::Query` if the WHERE clause contains
    /// characters outside the allow-list or exceeds 256 bytes.
    pub async fn query_count(&self, table: &str, where_clause: Option<&str>) -> Result<usize> {
        let (db, tbl) = parse_db_table(table, &self.database);
        let fq_table = format!("{}.{}", escape_identifier(&db), escape_identifier(&tbl));
        let sql = match where_clause {
            Some(w) => {
                validate_where_clause(w)?;
                format!("SELECT COUNT(*) AS count FROM {fq_table} WHERE {w}")
            }
            None => format!("SELECT COUNT(*) AS count FROM {fq_table}"),
        };

        let row: CountRow = self
            .ch_client
            .query(&sql)
            .fetch_one()
            .await
            .map_err(|e| ClickHouseError::Query(format!("{e}")))?;

        Ok(row.count as usize)
    }

    /// Health check — executes `SELECT 1`.
    pub async fn health_check(&self) -> Result<()> {
        self.ch_client
            .query("SELECT 1")
            .execute()
            .await
            .map_err(|e| ClickHouseError::Connection(format!("Health check failed: {e}")))?;
        Ok(())
    }

    /// Get the database name.
    #[must_use]
    pub fn database(&self) -> &str {
        &self.database
    }

    /// Insert rows as `JSONEachRow` via HTTP.
    ///
    /// Convenience method for tests and ad-hoc data injection. Production inserts
    /// go through `Inserter` which uses `DynamicInsert` (`RowBinary`) by default.
    ///
    /// The `_raw_payloads` parameter is ignored — kept for backward compatibility
    /// with test call sites.
    ///
    /// # Errors
    ///
    /// Returns an error if the insert fails (HTTP-only — errors on native transport).
    pub async fn insert_json_rows(
        &self,
        table: &str,
        rows: &[serde_json::Map<String, serde_json::Value>],
        _raw_payloads: &[std::sync::Arc<[u8]>],
    ) -> Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let (db, tbl) = parse_db_table(table, &self.database);

        // Build NDJSON body
        let mut body = Vec::with_capacity(rows.len() * 256);
        for row in rows {
            serde_json::to_writer(&mut body, row)
                .map_err(|e| ClickHouseError::Query(format!("JSON serialise: {e}")))?;
            body.push(b'\n');
        }

        // Use InsertFormatted for JSONEachRow via HTTP
        let sql = format!(
            "INSERT INTO {}.{} FORMAT JSONEachRow",
            escape_identifier(&db),
            escape_identifier(&tbl)
        );
        let mut insert = self.ch_client.insert_formatted_with(sql).buffered();

        insert.write_buffered(&body);

        insert
            .end()
            .await
            .map_err(|e| ClickHouseError::Insert(format!("{e}")))?;

        Ok(rows.len())
    }
}

/// Parse "db.table" format, falling back to default database.
fn parse_db_table(table: &str, default_db: &str) -> (String, String) {
    if let Some((db, tbl)) = table.split_once('.') {
        (db.to_string(), tbl.to_string())
    } else {
        (default_db.to_string(), table.to_string())
    }
}

/// Validate a `WHERE` fragment against a conservative allow-list before
/// interpolating it into SQL. Used by `query_count()` (test/internal helper).
///
/// Permits identifier chars + comparison operators + quoted-string contents:
/// `[A-Za-z0-9_.,= <>!'%-]`. Rejects: `;`, `(`, `)`, `--`, `/*`, `*/`, `\\`,
/// backticks, newlines, tabs, and anything > 256 bytes.
///
/// This is defence-in-depth; the method is not currently called from
/// production paths. Real query parameterisation should be used for any
/// caller-controlled WHERE content.
fn validate_where_clause(s: &str) -> Result<()> {
    if s.len() > 256 {
        return Err(ClickHouseError::Query(format!(
            "WHERE clause too long ({} bytes, max 256)",
            s.len()
        )));
    }
    if s.contains(';')
        || s.contains("--")
        || s.contains("/*")
        || s.contains("*/")
        || s.contains('\\')
        || s.contains('`')
        || s.contains('\n')
        || s.contains('\t')
        || s.contains('\r')
    {
        return Err(ClickHouseError::Query(
            "WHERE clause contains forbidden characters \
             (`;`, `--`, `/*`, `*/`, `\\`, backtick, newline, tab)"
                .into(),
        ));
    }
    let allowed = |c: char| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '_' | '.' | ',' | '=' | ' ' | '<' | '>' | '!' | '\'' | '%' | '-'
            )
    };
    if !s.chars().all(allowed) {
        return Err(ClickHouseError::Query(
            "WHERE clause contains characters outside the allow-list \
             [A-Za-z0-9_.,= <>!'%-]"
                .into(),
        ));
    }
    Ok(())
}

/// Escape a ClickHouse identifier (database, table, column name) with backticks.
///
/// Escapes backslashes, single quotes, backticks, tabs, and newlines inside
/// the identifier. Mirrors the fork's `sql::escape::identifier()`.
pub(crate) fn escape_identifier(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 2);
    out.push('`');
    for ch in name.chars() {
        match ch {
            '\\' | '\'' | '`' | '\t' | '\n' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('`');
    out
}

/// Escape a ClickHouse string value with single quotes.
///
/// Escapes backslashes, single quotes, backticks, tabs, and newlines inside
/// the value. Mirrors the fork's `sql::escape::string()`.
fn escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('\'');
    for ch in value.chars() {
        match ch {
            '\\' | '\'' | '`' | '\t' | '\n' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out.push('\'');
    out
}

/// Thread-safe reference to HTTP `ClickHouse` client.
pub type SharedQueryClient = Arc<ClickHouseQueryClient>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_db_table() {
        let (db, tbl) = parse_db_table("mydb.events", "default");
        assert_eq!(db, "mydb");
        assert_eq!(tbl, "events");

        let (db, tbl) = parse_db_table("events", "default");
        assert_eq!(db, "default");
        assert_eq!(tbl, "events");
    }

    #[test]
    fn test_parse_db_table_with_dots() {
        let (db, tbl) = parse_db_table("my.db.events", "default");
        assert_eq!(db, "my");
        assert_eq!(tbl, "db.events");
    }

    #[test]
    fn test_escape_identifier() {
        assert_eq!(escape_identifier("events"), "`events`");
        assert_eq!(escape_identifier("my`table"), "`my\\`table`");
        assert_eq!(escape_identifier("back\\slash"), "`back\\\\slash`");
        // Tab character should be escaped
        assert_eq!(escape_identifier("tab\there"), "`tab\\\there`");
    }

    #[test]
    fn test_validate_where_clause_accepts_safe_fragments() {
        assert!(validate_where_clause("id = 42").is_ok());
        assert!(validate_where_clause("name = 'alice'").is_ok());
        assert!(validate_where_clause("ts >= 1700000000").is_ok());
        assert!(validate_where_clause("status != 'deleted' AND age < 100").is_ok());
        // Keyword-shaped fragments pass — the validator is character-class
        // based, not keyword-aware. The point is to block punctuation and
        // sequences that enable injection (`;`, `--`, `/*`).
        assert!(validate_where_clause("name LIKE 'foo%'").is_ok());
    }

    #[test]
    fn test_validate_where_clause_rejects_injection_attempts() {
        // Stacked statements
        assert!(validate_where_clause("1=1; DROP TABLE users").is_err());
        // SQL line comments
        assert!(validate_where_clause("1=1 -- and now whatever").is_err());
        // SQL block comments
        assert!(validate_where_clause("1=1 /* comment */").is_err());
        // Backslash escapes
        assert!(validate_where_clause("name = '\\' OR 1=1").is_err());
        // Backticks (identifier abuse)
        assert!(validate_where_clause("`users`.id = 42").is_err());
        // Newlines
        assert!(validate_where_clause("id = 1\nOR 1=1").is_err());
    }

    #[test]
    fn test_validate_where_clause_length_cap() {
        let big = "x".repeat(257);
        assert!(validate_where_clause(&big).is_err());
        let ok = "x".repeat(256);
        // 256 'x' characters are all in the allow-list, so this passes the
        // length check (and the chars-allowed check too).
        assert!(validate_where_clause(&ok).is_ok());
    }

    #[test]
    fn test_escape_string() {
        assert_eq!(escape_string("hello"), "'hello'");
        assert_eq!(escape_string("it's"), "'it\\'s'");
        assert_eq!(escape_string("back\\slash"), "'back\\\\slash'");
        // Newline character should be escaped
        assert_eq!(escape_string("new\nline"), "'new\\\nline'");
    }

    #[test]
    fn test_client_new_no_hosts() {
        let config = ClickHouseConfig {
            hosts: vec![],
            ..Default::default()
        };
        let result = ClickHouseQueryClient::new(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_new_with_defaults() {
        let config = ClickHouseConfig::default();
        let client = ClickHouseQueryClient::new(&config);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.database(), "default");
    }

    // ============================================================
    // parse_db_table — additional edge cases
    // ============================================================

    #[test]
    fn test_parse_db_table_empty_string_fallback() {
        let (db, tbl) = parse_db_table("", "my_default");
        assert_eq!(db, "my_default");
        assert_eq!(tbl, "");
    }

    #[test]
    fn test_parse_db_table_leading_dot() {
        // Leading dot — empty db string, non-empty table
        let (db, tbl) = parse_db_table(".events", "defaultdb");
        assert_eq!(db, "");
        assert_eq!(tbl, "events");
    }

    #[test]
    fn test_parse_db_table_unicode() {
        let (db, tbl) = parse_db_table("données.évènements", "default");
        assert_eq!(db, "données");
        assert_eq!(tbl, "évènements");
    }

    #[test]
    fn test_parse_db_table_returns_owned_strings() {
        // Regression: parse_db_table returns (String, String) - verify independence
        // from the input lifetime.
        let result;
        {
            let input = String::from("foo.bar");
            result = parse_db_table(&input, "def");
        }
        assert_eq!(result.0, "foo");
        assert_eq!(result.1, "bar");
    }

    #[test]
    fn test_parse_db_table_first_dot_wins() {
        // split_once uses the FIRST delimiter — dotted table names go to the "table" half
        let (db, tbl) = parse_db_table("db.schema.table", "default");
        assert_eq!(db, "db");
        assert_eq!(tbl, "schema.table");
    }

    // ============================================================
    // escape_identifier — edge cases
    // ============================================================

    #[test]
    fn test_escape_identifier_empty() {
        assert_eq!(escape_identifier(""), "``");
    }

    #[test]
    fn test_escape_identifier_unicode() {
        // Non-ASCII characters pass through unescaped — only the 5 special chars get escaped.
        assert_eq!(escape_identifier("événements"), "`événements`");
        assert_eq!(escape_identifier("テーブル"), "`テーブル`");
    }

    #[test]
    fn test_escape_identifier_all_special_chars_at_once() {
        let input = "a\\b'c`d\te\nf";
        let expected = "`a\\\\b\\'c\\`d\\\te\\\nf`";
        assert_eq!(escape_identifier(input), expected);
    }

    #[test]
    fn test_escape_identifier_quote_wrap_always_added() {
        // Even pathological input is wrapped in backticks
        assert_eq!(escape_identifier("x"), "`x`");
        assert_eq!(escape_identifier("`"), "`\\``");
    }

    #[test]
    fn test_escape_identifier_safe_chars_passthrough() {
        // Numbers, underscores, hyphens, spaces — no escaping needed
        assert_eq!(escape_identifier("table_1-2 name"), "`table_1-2 name`");
    }

    // ============================================================
    // escape_string — edge cases
    // ============================================================

    #[test]
    fn test_escape_string_empty() {
        assert_eq!(escape_string(""), "''");
    }

    #[test]
    fn test_escape_string_unicode() {
        assert_eq!(escape_string("日本語"), "'日本語'");
        assert_eq!(escape_string("emoji 🔥"), "'emoji 🔥'");
    }

    #[test]
    fn test_escape_string_sql_injection_attempt() {
        // Common SQL injection pattern — must be safely escaped.
        let payload = "'; DROP TABLE users; --";
        let escaped = escape_string(payload);
        // The outer single-quote in the payload is escaped with backslash.
        assert_eq!(escaped, "'\\'; DROP TABLE users; --'");
    }

    #[test]
    fn test_escape_string_all_special_chars() {
        let input = "\\'`\t\n";
        let expected = "'\\\\\\'\\`\\\t\\\n'";
        assert_eq!(escape_string(input), expected);
    }

    #[test]
    fn test_escape_string_repeated_quotes() {
        assert_eq!(escape_string("'''"), "'\\'\\'\\''");
    }

    #[test]
    fn test_escape_string_capacity_preallocation() {
        // Whitebox: result length should be at least input + 2 (quotes),
        // and at most 2 * input + 2 (every char escaped).
        let input = "test";
        let out = escape_string(input);
        assert!(out.len() >= input.len() + 2);
        assert!(out.len() <= 2 * input.len() + 2);
    }

    // ============================================================
    // Client construction — HTTP and Native transports
    // ============================================================

    #[test]
    fn test_client_new_http_transport() {
        let config = ClickHouseConfig {
            hosts: vec!["localhost:8123".to_string()],
            transport: super::super::config::Transport::Http,
            database: "events".to_string(),
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "events");
    }

    #[test]
    fn test_client_new_native_transport() {
        let config = ClickHouseConfig {
            hosts: vec!["localhost:9000".to_string()],
            transport: super::super::config::Transport::Native,
            database: "logs".to_string(),
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "logs");
    }

    #[test]
    fn test_client_new_http_without_tls() {
        // HTTP constructor without TLS — builds cleanly without rustls CryptoProvider.
        let config = ClickHouseConfig {
            hosts: vec!["secure.example.com:8443".to_string()],
            transport: super::super::config::Transport::Http,
            database: "secure_db".to_string(),
            tls: false,
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "secure_db");
    }

    #[test]
    fn test_client_new_http_with_password() {
        // Ensures the password branch of the HTTP builder is exercised.
        let config = ClickHouseConfig {
            hosts: vec!["localhost:8123".to_string()],
            transport: super::super::config::Transport::Http,
            database: "db".to_string(),
            username: "user".to_string(),
            password: "s3cret!@#$".to_string(),
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "db");
    }

    #[test]
    fn test_client_new_native_with_password_no_tls() {
        // Native + password, no TLS — exercises the password branch without
        // needing a rustls CryptoProvider.
        let config = ClickHouseConfig {
            hosts: vec!["clickhouse.example.com:9000".to_string()],
            transport: super::super::config::Transport::Native,
            database: "prod".to_string(),
            username: "app".to_string(),
            password: "password".to_string(),
            tls: false,
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "prod");
    }

    #[test]
    fn test_client_new_native_multi_host_failover() {
        // Multi-host config should produce a client with multi-host failover on native.
        // We cannot reach the server — we only verify construction succeeds.
        let config = ClickHouseConfig {
            hosts: vec![
                "127.0.0.1:9000".to_string(),
                "127.0.0.1:9001".to_string(),
                "127.0.0.1:9002".to_string(),
            ],
            transport: super::super::config::Transport::Native,
            database: "default".to_string(),
            ..Default::default()
        };
        let client = ClickHouseQueryClient::new(&config).unwrap();
        assert_eq!(client.database(), "default");
    }

    #[test]
    fn test_client_new_native_multi_host_unresolvable() {
        // Hostnames that cannot resolve are filtered out by filter_map.
        // Construction still succeeds (may fall back to single host).
        let config = ClickHouseConfig {
            hosts: vec![
                "this-host-will-never-resolve.invalid.example:9000".to_string(),
                "127.0.0.1:9000".to_string(),
            ],
            transport: super::super::config::Transport::Native,
            database: "default".to_string(),
            ..Default::default()
        };
        // Either succeeds (at least one resolves) or fails — both are acceptable,
        // the test asserts the call doesn't panic.
        let _ = ClickHouseQueryClient::new(&config);
    }

    #[test]
    fn test_client_new_empty_hosts_returns_connection_error() {
        let config = ClickHouseConfig {
            hosts: vec![],
            ..Default::default()
        };
        // ClickHouseQueryClient doesn't impl Debug, so can't use unwrap_err().
        match ClickHouseQueryClient::new(&config) {
            Ok(_) => panic!("expected error for empty hosts"),
            Err(ClickHouseError::Connection(msg)) => {
                assert!(
                    msg.contains("No ClickHouse hosts"),
                    "unexpected error message: {msg}"
                );
            }
            Err(other) => panic!("expected Connection error, got {other:?}"),
        }
    }

    #[test]
    fn test_client_database_accessor_various_names() {
        // Exercise the database() accessor across a few realistic names.
        for db in ["default", "events", "prod_logs", "db-with-dash", "数据库"] {
            let config = ClickHouseConfig {
                hosts: vec!["localhost:9000".to_string()],
                database: db.to_string(),
                ..Default::default()
            };
            let client = ClickHouseQueryClient::new(&config).unwrap();
            assert_eq!(client.database(), db, "database() should return {db}");
        }
    }

    #[test]
    fn test_client_new_empty_password_skips_builder_branch() {
        // Empty password (explicit or default) bypasses the with_password() call.
        // Both HTTP and Native branches must still build successfully.
        let mut http_config = ClickHouseConfig::default();
        http_config.transport = super::super::config::Transport::Http;
        http_config.password = String::new();
        assert!(ClickHouseQueryClient::new(&http_config).is_ok());

        let mut native_config = ClickHouseConfig::default();
        native_config.transport = super::super::config::Transport::Native;
        native_config.password = String::new();
        assert!(ClickHouseQueryClient::new(&native_config).is_ok());
    }

    #[test]
    fn test_client_new_host_without_tls_builds() {
        // Native + host:port, no TLS — builder does not touch rustls.
        let config = ClickHouseConfig {
            hosts: vec!["ch.example.com:9000".to_string()],
            transport: super::super::config::Transport::Native,
            tls: false,
            database: "default".to_string(),
            ..Default::default()
        };
        assert!(ClickHouseQueryClient::new(&config).is_ok());
    }
}
