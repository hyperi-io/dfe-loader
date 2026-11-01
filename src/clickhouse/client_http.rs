// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      src/clickhouse/client_http.rs
// Purpose:   ClickHouse HTTP client for DDL, schema queries, and health checks
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! `ClickHouse` client for DDL, schema queries, and health checks.
//!
//! Uses a single `clickhouse::Client` over HTTP or HTTPS. Data inserts go
//! through `DynamicInsert`
//! (`RowBinary`, via `clickhouse_ext`) or `InsertFormatted` (`JSONEachRow`)
//! -- see `Inserter` for insert dispatch.
//!
//! This client handles:
//! - DDL execution (CREATE, ALTER, DROP)
//! - Schema fetching from `system.columns`
//! - Table existence checks
//! - Health checks (ping)

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use dfe_clickhouse::TcpClient;
use dfe_clickhouse::native::DecodedBlock;
use dfe_clickhouse::tls::TlsTrust;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use rustc_hash::FxHashMap;
use serde::Deserialize;

use super::config::{ClickHouseConfig, Timeouts};
use super::error::ClickHouseError;
use super::types::{ColumnInfo, ParsedType, TableSchema};

/// Result type for `ClickHouse` operations.
pub type Result<T> = std::result::Result<T, ClickHouseError>;

/// TCP keepalive interval on `ClickHouse` connections.
const TCP_KEEPALIVE: Duration = Duration::from_secs(60);

/// Shorter than `ClickHouse`'s keep-alive timeout, so the client closes an idle
/// connection before the server times it out and logs an error for it.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(2);

/// Widened reads stop at the 300 s `ClickHouse` gives its own socket reads and
/// writes by default.
const READ_BOUND_CEILING: Duration = Duration::from_secs(300);

/// `ClickHouse` client for DDL, schema queries, and health checks.
///
/// Wraps a single [`ChClient`]. Data inserts are handled by `Inserter` via
/// `DynamicInsert` (RowBinary) or `InsertFormatted` (JSONEachRow).
pub struct ClickHouseQueryClient {
    ch_client: ChClient,
    /// Database name.
    database: String,
    /// Bounds on each insert.
    timeouts: Timeouts,
    /// Bound on each query, shared with the inserter's schema reads.
    read_bound: Arc<ReadBound>,
}

/// A ClickHouse client over the protocol `clickhouse.protocol` selects.
#[derive(Clone)]
pub enum ChClient {
    /// HTTP or HTTPS, port 8123 or 8443.
    Http(clickhouse::Client),
    /// The native protocol, port 9000 or 9440.
    Native(TcpClient),
}

impl From<clickhouse::Client> for ChClient {
    fn from(client: clickhouse::Client) -> Self {
        Self::Http(client)
    }
}

impl From<TcpClient> for ChClient {
    fn from(client: TcpClient) -> Self {
        Self::Native(client)
    }
}

impl ChClient {
    /// Add a per-query setting on whichever protocol this client speaks.
    #[must_use]
    pub fn with_setting(self, name: &str, value: &str) -> Self {
        match self {
            Self::Http(client) => Self::Http(client.with_setting(name, value)),
            Self::Native(client) => Self::Native(client.with_setting(name, value)),
        }
    }
}

/// A client error that can say it timed out, so one bound serves both protocols.
pub trait TimesOut {
    /// The error a request that ran past its bound fails with.
    fn timed_out() -> Self;
    /// Whether this is that error.
    fn is_timed_out(&self) -> bool;
}

impl TimesOut for clickhouse::error::Error {
    fn timed_out() -> Self {
        Self::TimedOut
    }

    fn is_timed_out(&self) -> bool {
        matches!(self, Self::TimedOut)
    }
}

impl TimesOut for dfe_clickhouse::Error {
    fn timed_out() -> Self {
        Self::TimedOut
    }

    fn is_timed_out(&self) -> bool {
        matches!(self, Self::TimedOut)
    }
}

/// Run `request`, failing it with `TimedOut` once `bound` passes.
pub(crate) async fn bounded<T, E: TimesOut>(
    bound: Duration,
    request: impl Future<Output = std::result::Result<T, E>>,
) -> std::result::Result<T, E> {
    tokio::time::timeout(bound, request)
        .await
        .unwrap_or_else(|_| Err(E::timed_out()))
}

/// The bound on `ClickHouse` reads, which widens while reads keep timing out.
///
/// A fixed bound below how long a slow but working server takes would fail
/// every attempt at a schema read, and nothing waiting on that schema would
/// ever land. So a read that times out doubles the bound for the next one, up
/// to 300 s, and a read answered in under half the next narrower bound steps
/// it back down.
#[derive(Debug)]
pub struct ReadBound {
    base: Duration,
    ceiling: Duration,
    level: AtomicU32,
}

impl ReadBound {
    /// A bound that starts at `base` and widens no further than the ceiling, or
    /// than `base` itself if that is larger.
    #[must_use]
    pub fn new(base: Duration) -> Self {
        Self {
            base,
            ceiling: READ_BOUND_CEILING.max(base),
            level: AtomicU32::new(0),
        }
    }

    /// The bound the next read runs under.
    #[must_use]
    pub fn current(&self) -> Duration {
        self.at(self.level.load(Ordering::Relaxed))
    }

    fn at(&self, level: u32) -> Duration {
        self.base
            .checked_mul(1_u32.checked_shl(level).unwrap_or(u32::MAX))
            .unwrap_or(self.ceiling)
            .min(self.ceiling)
    }

    /// Run `read` under the current bound, widening it after a timeout and
    /// narrowing it after a quick answer.
    pub async fn run<T, E: TimesOut>(
        &self,
        read: impl Future<Output = std::result::Result<T, E>>,
    ) -> std::result::Result<T, E> {
        let level = self.level.load(Ordering::Relaxed);
        let started = tokio::time::Instant::now();
        let result = bounded(self.at(level), read).await;
        if result.as_ref().is_err_and(TimesOut::is_timed_out) {
            if self.at(level) < self.ceiling {
                self.level.fetch_max(level + 1, Ordering::Relaxed);
            }
        } else if level > 0 && started.elapsed() <= self.at(level - 1) / 2 {
            let _ =
                self.level
                    .compare_exchange(level, level - 1, Ordering::Relaxed, Ordering::Relaxed);
        }
        result
    }
}

/// Open an insert sink for `sql` on `client`, bounded by `timeouts`.
pub(crate) fn insert_sink(
    client: &clickhouse::Client,
    sql: String,
    timeouts: Timeouts,
) -> clickhouse::insert_formatted::InsertFormatted {
    client
        .insert_formatted_with(sql)
        .with_timeouts(Some(timeouts.request), Some(timeouts.insert))
}

/// The HTTP client a `clickhouse::Client` sends through.
///
/// HTTPS verifies the server's certificate and host name against the system
/// trust store, which honours `SSL_CERT_FILE` and `SSL_CERT_DIR`.
fn http_transport(config: &ClickHouseConfig) -> Result<clickhouse::Client> {
    let mut connector = HttpConnector::new();
    connector.set_keepalive(Some(TCP_KEEPALIVE));
    connector.set_connect_timeout(Some(config.connect_timeout()));
    let mut pool = HyperClient::builder(TokioExecutor::new());
    // hyper-util evicts idle connections only when the pool has a timer.
    pool.pool_timer(TokioTimer::new())
        .pool_idle_timeout(POOL_IDLE_TIMEOUT);
    if !config.tls {
        return Ok(clickhouse::Client::with_http_client(pool.build(connector)));
    }
    connector.enforce_http(false);
    let https = HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(rustls::crypto::aws_lc_rs::default_provider())
        .map_err(|e| {
            ClickHouseError::Connection(format!(
                "no CA certificate could be read from the system trust store: {e}"
            ))
        })?
        .https_only()
        .enable_http1()
        .wrap_connector(connector);
    Ok(clickhouse::Client::with_http_client(pool.build(https)))
}

/// Connection-pool statistics for the `clickhouse_pool_*` gauges.
///
/// The HTTP client exposes no pool status, so `Inserter::pool_stats` returns
/// `None` and these gauges stay flat.
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

/// Build the HTTP `clickhouse::Client` from config. Shared by the query client
/// and the inserter so both use an identically-configured client.
///
/// # Errors
///
/// Returns an error if the config has no hosts, names the native transport,
/// or has TLS on and no CA certificate can be read from the system trust store.
pub fn build_client(config: &ClickHouseConfig) -> Result<ChClient> {
    use super::config::Transport;

    let endpoint = config
        .primary_endpoint()
        .ok_or_else(|| ClickHouseError::Connection("No ClickHouse hosts configured".into()))?;

    let client = match config.transport {
        Transport::Http => {
            let scheme = if config.tls { "https" } else { "http" };
            let mut c = http_transport(config)?
                .with_url(format!("{scheme}://{endpoint}"))
                .with_user(&config.username)
                .with_database(&config.database);
            if !config.password.is_empty() {
                c = c.with_password(&config.password);
            }
            ChClient::Http(c)
        }
        Transport::Native => ChClient::Native(native_transport(config, &endpoint)),
    };

    Ok(client)
}

/// The native-protocol client: the same credentials, database and bounds as
/// HTTP, every configured host in its failover list, and with TLS on, each
/// host verified against its own name and the system trust store only, as
/// HTTPS is.
fn native_transport(config: &ClickHouseConfig, endpoint: &str) -> TcpClient {
    let mut client = if config.tls {
        let mut trust = TlsTrust::default();
        trust.webpki_roots = false;
        // Its own host as the name makes the client verify each host against its own.
        TcpClient::new_tls(endpoint, endpoint_host(endpoint)).with_tls_trust(trust)
    } else {
        TcpClient::new(endpoint)
    };
    if config.hosts.len() > 1 {
        let port = config.default_port();
        client = client.with_addrs(config.hosts.iter().map(|host| {
            if host.contains(':') {
                host.clone()
            } else {
                format!("{host}:{port}")
            }
        }));
    }
    client = client
        .with_user(&config.username)
        .with_database(&config.database)
        .with_pool_create_timeout(Some(config.connect_timeout()))
        .with_read_timeout(Some(config.timeouts().request));
    if !config.password.is_empty() {
        client = client.with_password(&config.password);
    }
    client
}

/// The host of a `host:port` endpoint, an IPv6 literal without its brackets.
fn endpoint_host(endpoint: &str) -> &str {
    let host = endpoint.rsplit_once(':').map_or(endpoint, |(host, _)| host);
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
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

/// Every value of column `name` across `blocks`, in server order.
fn column<T: dfe_clickhouse::native::FromColumn>(
    blocks: &[DecodedBlock],
    name: &str,
) -> dfe_clickhouse::Result<Vec<T>> {
    let mut values = Vec::new();
    for block in blocks {
        values.extend(block.column_as::<T>(name)?);
    }
    Ok(values)
}

/// The `system.columns` rows a native-protocol schema query returned.
fn system_columns(blocks: &[DecodedBlock]) -> dfe_clickhouse::Result<Vec<SystemColumn>> {
    let names: Vec<String> = column(blocks, "name")?;
    let types: Vec<String> = column(blocks, "type")?;
    let positions: Vec<u64> = column(blocks, "position")?;
    let default_kinds: Vec<String> = column(blocks, "default_kind")?;
    let default_expressions: Vec<String> = column(blocks, "default_expression")?;
    let comments: Vec<String> = column(blocks, "comment")?;
    let primary: Vec<u8> = column(blocks, "is_in_primary_key")?;
    let sorting: Vec<u8> = column(blocks, "is_in_sorting_key")?;
    Ok(names
        .into_iter()
        .zip(types)
        .zip(positions)
        .zip(default_kinds)
        .zip(default_expressions)
        .zip(comments)
        .zip(primary)
        .zip(sorting)
        .map(
            |(
                ((((((name, r#type), position), default_kind), default_expression), comment), pk),
                sk,
            )| {
                SystemColumn {
                    name,
                    r#type,
                    position,
                    default_kind,
                    default_expression,
                    comment,
                    is_in_primary_key: pk,
                    is_in_sorting_key: sk,
                }
            },
        )
        .collect())
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
    // Aliased `_dfe_count` (not `count`) so a user table column named `count`
    // referenced in the WHERE does not collide with the aggregate alias --
    // ClickHouse otherwise rejects it as "aggregate function in WHERE" (code 184).
    #[serde(rename = "_dfe_count")]
    count: u64,
}

impl ClickHouseQueryClient {
    /// Create a new client from config.
    ///
    /// # Errors
    ///
    /// Returns an error if the config has no hosts or names the native transport.
    pub fn new(config: &ClickHouseConfig) -> Result<Self> {
        let ch_client = build_client(config)?;
        let timeouts = config.timeouts();
        Ok(Self {
            ch_client,
            database: config.database.clone(),
            timeouts,
            read_bound: Arc::new(ReadBound::new(timeouts.request)),
        })
    }

    /// The bound on queries, for the inserter's schema reads to share.
    #[must_use]
    pub fn read_bound(&self) -> Arc<ReadBound> {
        Arc::clone(&self.read_bound)
    }

    /// Execute a DDL or DML statement (CREATE, DROP, ALTER, TRUNCATE, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn execute(&self, sql: &str) -> Result<()> {
        match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(sql).execute())
                .await
                .map_err(|e| ClickHouseError::Query(super::error::error_text(&e))),
            ChClient::Native(client) => self
                .read_bound
                .run(client.query(sql).execute())
                .await
                .map_err(|e| ClickHouseError::Query(super::error::native_error_text(&e))),
        }
    }

    /// Every block a native-protocol `sql` returns, under the read bound.
    async fn native_blocks(
        &self,
        client: &TcpClient,
        sql: &str,
    ) -> std::result::Result<Vec<DecodedBlock>, String> {
        self.read_bound
            .run(client.query(sql).fetch_blocks())
            .await
            .map_err(|e| super::error::native_error_text(&e))
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

        let rows: Vec<SystemColumn> = match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(&sql).fetch_all())
                .await
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => self
                .native_blocks(client, &sql)
                .await
                .and_then(|blocks| system_columns(&blocks).map_err(|e| e.to_string())),
        }
        .map_err(|e| ClickHouseError::Schema(format!("Failed to fetch schema for {table}: {e}")))?;

        // The query succeeded and system.columns has nothing: the table really is
        // absent. Distinct from Schema above, which a transient outage produces.
        if rows.is_empty() {
            return Err(ClickHouseError::TableNotFound(format!("{db}.{tbl}")));
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

        let values: Vec<String> = match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(&sql).fetch_all::<SingleString>())
                .await
                .map(|rows| rows.into_iter().map(|r| r.value).collect())
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => self
                .native_blocks(client, &sql)
                .await
                .and_then(|blocks| column(&blocks, "value").map_err(|e| e.to_string())),
        }
        .map_err(|e| ClickHouseError::Schema(format!("Failed to fetch table comment: {e}")))?;

        Ok(values.into_iter().next().unwrap_or_default())
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

        let rows: Vec<(String, String)> = match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(&sql).fetch_all::<NameComment>())
                .await
                .map(|rows| rows.into_iter().map(|r| (r.name, r.comment)).collect())
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => self.native_blocks(client, &sql).await.and_then(|blocks| {
                let names: Vec<String> = column(&blocks, "name").map_err(|e| e.to_string())?;
                let comments: Vec<String> =
                    column(&blocks, "comment").map_err(|e| e.to_string())?;
                Ok(names.into_iter().zip(comments).collect())
            }),
        }
        .map_err(|e| ClickHouseError::Schema(format!("Failed to fetch column comments: {e}")))?;

        Ok(rows.into_iter().collect())
    }

    /// Check if a table exists.
    ///
    /// Only a `TableNotFound` answers "no". A failed query means the answer is
    /// unknown and propagates as an error — reporting it as absent would let a
    /// transient outage look like a deleted table.
    pub async fn table_exists(&self, table: &str) -> Result<bool> {
        match self.fetch_table_schema(table).await {
            Ok(_) => Ok(true),
            Err(ClickHouseError::TableNotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// List all tables in the database.
    pub async fn list_tables(&self) -> Result<Vec<String>> {
        let sql = format!(
            "SELECT name AS name FROM system.tables WHERE database = {}",
            escape_string(&self.database)
        );

        match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(&sql).fetch_all::<TableName>())
                .await
                .map(|rows| rows.into_iter().map(|r| r.name).collect())
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => self
                .native_blocks(client, &sql)
                .await
                .and_then(|blocks| column(&blocks, "name").map_err(|e| e.to_string())),
        }
        .map_err(|e| ClickHouseError::Schema(format!("Failed to list tables: {e}")))
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
                format!("SELECT COUNT(*) AS _dfe_count FROM {fq_table} WHERE {w}")
            }
            None => format!("SELECT COUNT(*) AS _dfe_count FROM {fq_table}"),
        };

        let count = self
            .fetch_count(&sql)
            .await
            .map_err(ClickHouseError::Query)?;

        Ok(count as usize)
    }

    /// The `_dfe_count` a one-row count query returns.
    async fn fetch_count(&self, sql: &str) -> std::result::Result<u64, String> {
        match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query(sql).fetch_one::<CountRow>())
                .await
                .map(|row| row.count)
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => {
                let blocks = self.native_blocks(client, sql).await?;
                let counts: Vec<u64> = column(&blocks, "_dfe_count").map_err(|e| e.to_string())?;
                counts
                    .first()
                    .copied()
                    .ok_or_else(|| "a count query returned no row".to_string())
            }
        }
    }

    /// Health check — executes `SELECT 1`.
    pub async fn health_check(&self) -> Result<()> {
        match &self.ch_client {
            ChClient::Http(client) => self
                .read_bound
                .run(client.query("SELECT 1").execute())
                .await
                .map_err(|e| super::error::error_text(&e)),
            ChClient::Native(client) => self
                .read_bound
                .run(client.query("SELECT 1").execute())
                .await
                .map_err(|e| super::error::native_error_text(&e)),
        }
        .map_err(|e| ClickHouseError::Connection(format!("Health check failed: {e}")))
    }

    /// Log in as the configured user and read `system.tables` and
    /// `system.columns` for the configured database, the reads the schema
    /// lookups make.
    ///
    /// # Errors
    ///
    /// Returns [`ClickHouseError::Query`] naming the first read `ClickHouse`
    /// refused, with the server's error code and message.
    pub async fn probe_access(&self) -> Result<()> {
        for table in ["system.tables", "system.columns"] {
            let sql = format!(
                "SELECT count() AS _dfe_count FROM {table} WHERE database = {}",
                escape_string(&self.database)
            );
            self.fetch_count(&sql)
                .await
                .map_err(|e| ClickHouseError::Query(format!("reading {table}: {e}")))?;
        }
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
    /// Returns an error if the insert fails.
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
        let ChClient::Http(client) = &self.ch_client else {
            return Err(ClickHouseError::Insert(
                "JSONEachRow is sent over HTTP: this client speaks the native protocol".into(),
            ));
        };
        let mut insert = insert_sink(client, sql, self.timeouts).buffered();

        insert.write_buffered(&body);

        insert
            .end()
            .await
            .map_err(|e| ClickHouseError::Insert(super::error::error_text(&e)))?;

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
/// the identifier. Mirrors the client's `sql::escape::identifier()`.
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
/// the value. Mirrors the client's `sql::escape::string()`.
pub(crate) fn escape_string(value: &str) -> String {
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
        assert_eq!(client.database(), "dfe");
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
    // Client construction
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

    #[test_case::test_case(&["localhost:9000"], "", false ; "single_host")]
    #[test_case::test_case(&["clickhouse.example.com:9000"], "password", false ; "with_password")]
    #[test_case::test_case(&["ch.example.com:9440"], "", true ; "with_tls")]
    #[test_case::test_case(&["127.0.0.1:9000", "127.0.0.1:9001"], "", false ; "multi_host")]
    fn the_native_transport_builds_a_native_client(hosts: &[&str], password: &str, tls: bool) {
        let config = ClickHouseConfig {
            hosts: hosts.iter().map(ToString::to_string).collect(),
            transport: super::super::config::Transport::Native,
            password: password.to_string(),
            tls,
            ..Default::default()
        };
        // Building connects nothing: the pool dials on first use.
        assert!(matches!(
            build_client(&config).expect("a native config builds"),
            ChClient::Native(_)
        ));
    }

    #[test_case::test_case("ch-1.example:9440", "ch-1.example" ; "name")]
    #[test_case::test_case("10.0.0.5:9440", "10.0.0.5" ; "ipv4")]
    #[test_case::test_case("[::1]:9440", "::1" ; "ipv6")]
    #[test_case::test_case("localhost", "localhost" ; "no_port")]
    fn the_endpoint_host_is_the_name_its_certificate_carries(endpoint: &str, host: &str) {
        assert_eq!(endpoint_host(endpoint), host);
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
                hosts: vec!["localhost:8123".to_string()],
                transport: super::super::config::Transport::Http,
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
        let mut http_config = ClickHouseConfig::default();
        http_config.transport = super::super::config::Transport::Http;
        http_config.password = String::new();
        assert!(ClickHouseQueryClient::new(&http_config).is_ok());
    }

    // ============================================================
    // Connection bounds, against loopback servers that misbehave
    // ============================================================

    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpSocket, TcpStream};

    /// An HTTP config pointing at `addr` with the given connect and request bounds.
    fn http_config(addr: SocketAddr, connect_ms: u64, request_ms: u64) -> ClickHouseConfig {
        ClickHouseConfig {
            hosts: vec![addr.to_string()],
            transport: super::super::config::Transport::Http,
            database: "default".to_string(),
            connect_timeout_ms: connect_ms,
            request_timeout_ms: request_ms,
            ..Default::default()
        }
    }

    /// Reads one request with a `Content-Length` body, so nothing is left unread.
    async fn read_request(socket: &mut TcpStream) {
        let mut request = Vec::new();
        let mut chunk = [0u8; 1024];
        let head_end = loop {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0, "client closed before sending a request");
            request.extend_from_slice(&chunk[..n]);
            if let Some(pos) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let head = String::from_utf8_lossy(&request[..head_end]).to_ascii_lowercase();
        let body_len: usize = head
            .lines()
            .find_map(|line| line.strip_prefix("content-length:"))
            .map_or(0, |v| v.trim().parse().unwrap());
        while request.len() < head_end + body_len {
            let n = socket.read(&mut chunk).await.unwrap();
            assert!(n > 0, "client closed mid-request");
            request.extend_from_slice(&chunk[..n]);
        }
    }

    /// A listener whose accept queue is full, so the kernel drops every further
    /// SYN and a connect to it never completes. The returned streams keep it full.
    async fn listener_that_never_completes_a_connect() -> (TcpListener, Vec<TcpStream>, SocketAddr)
    {
        let socket = TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = socket.listen(1).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut queued = Vec::new();
        for _ in 0..4 {
            if let Ok(Ok(stream)) =
                tokio::time::timeout(Duration::from_millis(300), TcpStream::connect(addr)).await
            {
                queued.push(stream);
            }
        }
        (listener, queued, addr)
    }

    /// A read the server answers after `delay`.
    async fn read_answered_after(delay: Duration) -> clickhouse::error::Result<()> {
        tokio::time::sleep(delay).await;
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_slower_than_the_bound_lands_on_the_next_attempt() {
        let bound = ReadBound::new(Duration::from_secs(2));

        let first = bound.run(read_answered_after(Duration::from_secs(3))).await;
        let second = bound.run(read_answered_after(Duration::from_secs(3))).await;

        assert!(
            matches!(first, Err(clickhouse::error::Error::TimedOut)),
            "a 3 s read under a 2 s bound answered: {first:?}"
        );
        assert!(second.is_ok(), "the widened bound timed the read out again");
        assert_eq!(
            bound.current(),
            Duration::from_secs(4),
            "a read that still needs 3 s narrowed the bound it only just fits"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_read_that_never_answers_widens_the_bound_no_further_than_the_ceiling() {
        let bound = ReadBound::new(Duration::from_secs(60));
        let mut seen = Vec::new();
        for _ in 0..5 {
            let _ = bound
                .run(std::future::pending::<clickhouse::error::Result<()>>())
                .await;
            seen.push(bound.current().as_secs());
        }
        assert_eq!(seen, vec![120, 240, 300, 300, 300]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_quick_answer_narrows_a_widened_bound() {
        let bound = ReadBound::new(Duration::from_secs(2));
        let _ = bound.run(read_answered_after(Duration::from_secs(3))).await;
        assert_eq!(bound.current(), Duration::from_secs(4));

        let quick = bound
            .run(read_answered_after(Duration::from_millis(100)))
            .await;

        assert!(quick.is_ok());
        assert_eq!(bound.current(), Duration::from_secs(2));
    }

    #[test]
    fn a_base_above_the_ceiling_is_its_own_ceiling() {
        let bound = ReadBound::new(Duration::from_secs(400));
        assert_eq!(bound.current(), Duration::from_secs(400));
        assert_eq!(bound.at(5), Duration::from_secs(400));
    }

    #[tokio::test]
    async fn an_idle_connection_closes_about_two_seconds_after_its_last_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                .await
                .unwrap();
            let answered = Instant::now();
            // A zero-byte read is the client closing its end.
            let mut byte = [0u8; 1];
            let read = socket.read(&mut byte).await.unwrap();
            (read, answered.elapsed())
        });

        let ChClient::Http(client) = build_client(&http_config(addr, 5_000, 30_000)).unwrap()
        else {
            panic!("an http config builds an HTTP client")
        };
        client.query("SELECT 1").execute().await.unwrap();

        let (read, idle) = tokio::time::timeout(Duration::from_secs(8), server)
            .await
            .expect("the pool kept an idle connection open for 8 s")
            .unwrap();
        assert_eq!(read, 0, "the client sent more bytes instead of closing");
        assert!(
            idle >= Duration::from_millis(1_500) && idle <= Duration::from_millis(5_000),
            "an idle connection closed after {idle:?}, not about 2 s"
        );
        // Dropping the client also closes the pool, so it must outlive the wait above.
        drop(client);
    }

    #[tokio::test]
    async fn connect_timeout_bounds_a_connect_that_never_completes() {
        let (_listener, _queued, addr) = listener_that_never_completes_a_connect().await;
        let ChClient::Http(client) = build_client(&http_config(addr, 300, 30_000)).unwrap() else {
            panic!("an http config builds an HTTP client")
        };

        let started = Instant::now();
        let result =
            tokio::time::timeout(Duration::from_secs(10), client.query("SELECT 1").execute())
                .await
                .expect(
                    "a connect to a full accept queue ran past 10 s with a 300 ms connect timeout",
                );
        let elapsed = started.elapsed();

        assert!(
            result.is_err(),
            "a connect that never completed reported success"
        );
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(3),
            "the connect gave up after {elapsed:?}, not at its 300 ms bound"
        );
    }

    #[tokio::test]
    async fn request_timeout_bounds_a_query_the_server_never_answers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            // Holds the connection open and never answers.
            let mut byte = [0u8; 1];
            let _ = socket.read(&mut byte).await;
        });
        let client = ClickHouseQueryClient::new(&http_config(addr, 5_000, 300)).unwrap();

        let started = Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(10), client.health_check())
            .await
            .expect("a query the server never answered ran past 10 s with a 300 ms bound");
        let elapsed = started.elapsed();

        assert!(result.is_err(), "an unanswered query reported success");
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(3),
            "the query gave up after {elapsed:?}, not at its 300 ms bound"
        );
    }
}
