// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

// Project:   dfe-loader
// File:      mod.rs
// Purpose:   Embedded schema definitions (compiled-in)
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HyperI

//! Embedded schema definitions
//!
//! Schema files are embedded at compile time from the `schemas/` directory.
//! This ensures schema definitions are always available without external files.
//!
//! ## Available Schemas
//!
//! - `COMMON_TABLE_DDL` - ClickHouse DDL for the common events table
//! - `COMMON_HEADER_CSV` - CSV definition of common header fields
//!
//! ## Engine Detection
//!
//! The module auto-detects the best table engine:
//! 1. SharedMergeTree (ClickHouse Cloud / 24.1+)
//! 2. ReplicatedMergeTree (clustered deployments)
//! 3. MergeTree (single-node fallback)
//!
//! ## Template Variables
//!
//! The DDL template uses placeholders:
//! - `{db}` - Database name
//! - `{table}` - Table name
//! - `{engine}` - Table engine (auto-detected or specified)
//! - `{table_comment}` - Table-level tags (see Table Tags below)
//!
//! ## Table Tags
//!
//! Tables support metadata tags stored in the COMMENT field, using the same
//! `@tag: key=value` syntax as the column expression language:
//!
//! ```sql
//! COMMENT '@schema_source: core | @schema_version: 2 | @created_by: dfe-loader'
//! ```
//!
//! **Standard tags:**
//! - `@schema_source` - `core` (pre-supplied) or absent/`user` (user-created)
//! - `@schema_version` - Schema version number for migrations
//! - `@created_by` - Tool that created the table
//!
//! Query tables by tag:
//! ```sql
//! SELECT database, name, comment
//! FROM system.tables
//! WHERE comment LIKE '%@schema_source: core%'
//! ```
//!
//! ```ignore
//! use dfe_loader::schema::{render_ddl_with_engine, TableEngine, TableTags};
//!
//! let tags = TableTags::core();
//! let ddl = render_ddl_with_tags("common", "events", TableEngine::SharedMergeTree, &tags);
//! ```

use std::collections::BTreeMap;

/// Common table DDL template (ClickHouse)
///
/// Template variables:
/// - `{db}` - Database name
/// - `{table}` - Table name
/// - `{engine}` - Table engine clause
/// - `{table_comment}` - Table-level tags
pub const COMMON_TABLE_DDL: &str = include_str!("../../schemas/common_table.sql");

/// Common header field definitions (CSV format)
///
/// Columns: column, type, default, nullable, codec, comment
pub const COMMON_HEADER_CSV: &str = include_str!("../../schemas/common_header.csv");

/// ClickHouse table engine types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableEngine {
    /// SharedMergeTree - ClickHouse Cloud and 24.1+ with shared storage
    /// Best option: automatic replication, no ZooKeeper needed
    SharedMergeTree,

    /// ReplicatedMergeTree - Traditional clustered deployments
    /// Requires ZooKeeper/ClickHouse Keeper path
    ReplicatedMergeTree,

    /// MergeTree - Single-node deployments
    /// No replication, simplest setup
    MergeTree,
}

impl TableEngine {
    /// Generate the ENGINE clause for this engine type
    pub fn to_engine_clause(&self, db: &str, table: &str) -> String {
        match self {
            TableEngine::SharedMergeTree => "SharedMergeTree()".to_string(),
            TableEngine::ReplicatedMergeTree => {
                // Use database and table name for ZK path to ensure uniqueness
                format!(
                    "ReplicatedMergeTree('/clickhouse/tables/{{shard}}/{db}/{table}', '{{replica}}')",
                    db = db,
                    table = table
                )
            }
            TableEngine::MergeTree => "MergeTree()".to_string(),
        }
    }
}

impl std::fmt::Display for TableEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableEngine::SharedMergeTree => write!(f, "SharedMergeTree"),
            TableEngine::ReplicatedMergeTree => write!(f, "ReplicatedMergeTree"),
            TableEngine::MergeTree => write!(f, "MergeTree"),
        }
    }
}

/// Table-level tags stored in ClickHouse COMMENT field
///
/// Uses `@tag: key=value` syntax similar to column expression language.
/// Tags are separated by ` | ` (pipe with spaces) for readability.
///
/// # Example
///
/// ```
/// use dfe_loader::schema::TableTags;
///
/// let tags = TableTags::core()
///     .with("custom_field", "custom_value");
///
/// assert_eq!(tags.to_comment(), "@custom_field: custom_value | @schema_source: core | @schema_version: 2");
/// ```
#[derive(Debug, Clone, Default)]
pub struct TableTags {
    /// Key-value pairs (ordered for deterministic output)
    tags: BTreeMap<String, String>,
}

impl TableTags {
    /// Create empty tags
    pub fn new() -> Self {
        Self::default()
    }

    /// Create tags for core (pre-supplied) schemas
    ///
    /// Sets:
    /// - `@schema_source: core`
    /// - `@schema_version: 2`
    pub fn core() -> Self {
        Self::new()
            .with("schema_source", "core")
            .with("schema_version", "2")
    }

    /// Create tags for user-created schemas
    ///
    /// Sets:
    /// - `@schema_source: user`
    /// - `@schema_version: 1`
    pub fn user() -> Self {
        Self::new()
            .with("schema_source", "user")
            .with("schema_version", "1")
    }

    /// Add a tag (builder pattern)
    pub fn with(mut self, key: &str, value: &str) -> Self {
        self.tags.insert(key.to_string(), value.to_string());
        self
    }

    /// Add a tag in-place
    pub fn set(&mut self, key: &str, value: &str) {
        self.tags.insert(key.to_string(), value.to_string());
    }

    /// Get a tag value
    pub fn get(&self, key: &str) -> Option<&str> {
        self.tags.get(key).map(|s| s.as_str())
    }

    /// Check if a tag exists
    pub fn has(&self, key: &str) -> bool {
        self.tags.contains_key(key)
    }

    /// Remove a tag
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.tags.remove(key)
    }

    /// Check if tags are empty
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Iterate over tags
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.tags.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// Format as ClickHouse COMMENT string
    ///
    /// Uses `@key: value` syntax, separated by ` | `
    pub fn to_comment(&self) -> String {
        self.tags
            .iter()
            .map(|(k, v)| format!("@{}: {}", k, v))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    /// Parse tags from a ClickHouse COMMENT string
    ///
    /// Expects `@key: value` pairs separated by ` | `
    pub fn from_comment(comment: &str) -> Self {
        let mut tags = BTreeMap::new();

        for part in comment.split(" | ") {
            let part = part.trim();
            if let Some(stripped) = part.strip_prefix('@') {
                if let Some((key, value)) = stripped.split_once(':') {
                    tags.insert(key.trim().to_string(), value.trim().to_string());
                }
            }
        }

        Self { tags }
    }
}

impl std::fmt::Display for TableTags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_comment())
    }
}

/// ClickHouse cluster capabilities detected at runtime
#[derive(Debug, Clone)]
pub struct ClusterCapabilities {
    /// ClickHouse version (e.g., "24.8.1.123")
    pub version: String,
    /// Major version number
    pub major_version: u32,
    /// Minor version number
    pub minor_version: u32,
    /// Whether SharedMergeTree is available
    pub shared_merge_tree: bool,
    /// Whether full_text index is available (GA, not experimental)
    pub full_text_index: bool,
    /// Whether JSON type is available (stable)
    pub json_type: bool,
    /// Whether this is a clustered deployment
    pub is_clustered: bool,
}

impl ClusterCapabilities {
    /// Detect capabilities from version string and cluster info
    pub fn from_version(version: &str, is_clustered: bool) -> Self {
        let parts: Vec<u32> = version
            .split('.')
            .filter_map(|s| s.split('-').next()?.parse().ok())
            .collect();

        let major = parts.first().copied().unwrap_or(0);
        let minor = parts.get(1).copied().unwrap_or(0);

        Self {
            version: version.to_string(),
            major_version: major,
            minor_version: minor,
            // SharedMergeTree available in 24.1+ (Cloud) or with shared storage
            shared_merge_tree: major >= 24,
            // full_text index GA in 25.1+ (experimental in 24.x)
            full_text_index: major >= 25 || (major == 24 && minor >= 8),
            // JSON type stable in 25.x
            json_type: major >= 25 || (major == 24 && minor >= 1),
            is_clustered,
        }
    }

    /// Select the best table engine for this cluster
    pub fn best_engine(&self) -> TableEngine {
        if self.shared_merge_tree {
            TableEngine::SharedMergeTree
        } else if self.is_clustered {
            TableEngine::ReplicatedMergeTree
        } else {
            TableEngine::MergeTree
        }
    }

    /// Get the appropriate text index DDL
    pub fn text_index_ddl(&self, column: &str) -> String {
        if self.full_text_index {
            format!("INDEX idx_{column} {column} TYPE full_text(0) GRANULARITY 1")
        } else {
            // Fallback to n-gram bloom filter
            format!("INDEX idx_{column}_ngram {column} TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4")
        }
    }
}

impl Default for ClusterCapabilities {
    fn default() -> Self {
        // Conservative defaults for unknown cluster
        Self {
            version: "unknown".to_string(),
            major_version: 0,
            minor_version: 0,
            shared_merge_tree: false,
            full_text_index: false,
            json_type: false,
            is_clustered: false,
        }
    }
}

/// Render the common table DDL with database, table, and auto-detected engine
pub fn render_ddl(db: &str, table: &str) -> String {
    // Default to MergeTree for backwards compatibility
    // Use render_ddl_with_engine for auto-detection
    render_ddl_with_engine(db, table, TableEngine::MergeTree)
}

/// Render the common table DDL with specified engine
pub fn render_ddl_with_engine(db: &str, table: &str, engine: TableEngine) -> String {
    render_ddl_with_tags(db, table, engine, &TableTags::core())
}

/// Render the common table DDL with specified engine and tags
pub fn render_ddl_with_tags(
    db: &str,
    table: &str,
    engine: TableEngine,
    tags: &TableTags,
) -> String {
    COMMON_TABLE_DDL
        .replace("{db}", db)
        .replace("{table}", table)
        .replace("{engine}", &engine.to_engine_clause(db, table))
        .replace("{table_comment}", &tags.to_comment())
}

/// Render DDL with full capability detection
pub fn render_ddl_with_capabilities(
    db: &str,
    table: &str,
    capabilities: &ClusterCapabilities,
) -> String {
    let engine = capabilities.best_engine();
    let ddl = render_ddl_with_engine(db, table, engine);

    // Add text index if available
    if capabilities.full_text_index {
        // The DDL template should have a placeholder for optional indexes
        // For now, we just return the base DDL
        // TODO: Add index injection point in template
    }

    ddl
}

/// SQL to detect cluster capabilities
pub const DETECT_VERSION_SQL: &str = "SELECT version()";

/// SQL to check if SharedMergeTree is available
pub const DETECT_SHARED_MERGE_TREE_SQL: &str =
    "SELECT count() > 0 FROM system.table_engines WHERE name = 'SharedMergeTree'";

/// SQL to check if cluster is configured (multi-node, not single-node defaults)
///
/// ClickHouse always has entries in system.clusters even on single-node deployments.
/// We check for clusters with more than one host (shard/replica) to detect actual
/// multi-node setups that would benefit from ReplicatedMergeTree.
pub const DETECT_CLUSTER_SQL: &str =
    "SELECT count() > 0 FROM (SELECT cluster FROM system.clusters GROUP BY cluster HAVING count() > 1)";

/// SQL to check if full_text index is available (non-experimental)
pub const DETECT_FULL_TEXT_SQL: &str =
    "SELECT count() > 0 FROM system.data_skipping_indices WHERE type = 'full_text'";

/// DDL to add text search index (ngram bloom filter - fallback)
pub fn add_text_index_ngram_ddl(db: &str, table: &str, column: &str) -> String {
    format!(
        "ALTER TABLE {db}.{table} ADD INDEX idx_{column}_ngram {column} TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4",
        db = db,
        table = table,
        column = column
    )
}

/// DDL to add text search index (full_text - when GA)
pub fn add_text_index_fulltext_ddl(db: &str, table: &str, column: &str) -> String {
    format!(
        "ALTER TABLE {db}.{table} ADD INDEX idx_{column} {column} TYPE full_text(0) GRANULARITY 1",
        db = db,
        table = table,
        column = column
    )
}

/// DDL to add the appropriate text index based on capabilities
pub fn add_text_index_ddl(
    db: &str,
    table: &str,
    column: &str,
    capabilities: &ClusterCapabilities,
) -> String {
    if capabilities.full_text_index {
        add_text_index_fulltext_ddl(db, table, column)
    } else {
        add_text_index_ngram_ddl(db, table, column)
    }
}

/// Common header field definition
#[derive(Debug, Clone)]
pub struct HeaderField {
    pub column: String,
    pub data_type: String,
    pub default: Option<String>,
    pub nullable: bool,
    pub codec: Option<String>,
    pub source: String,
    pub comment: String,
}

/// Parse the common header CSV into field definitions
///
/// CSV format: column,type,default,nullable,codec,source,comment
///
/// Source expression language:
/// - `@source: field_name` - Copy from source field
/// - `@source: field_name | now()` - Copy from source, fallback to now()
/// - `@source: first(a/b/c)` - First non-null from list (/ separator)
/// - `@generated: expression` - Generated by ClickHouse DEFAULT
/// - `@captured: description` - Captured from raw payload
pub fn parse_common_header() -> Vec<HeaderField> {
    let mut fields = Vec::new();

    for line in COMMON_HEADER_CSV.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 7 {
            fields.push(HeaderField {
                column: parts[0].to_string(),
                data_type: parts[1].to_string(),
                default: if parts[2].is_empty() {
                    None
                } else {
                    Some(parts[2].to_string())
                },
                nullable: parts[3] == "true",
                codec: if parts[4].is_empty() {
                    None
                } else {
                    Some(parts[4].to_string())
                },
                source: parts[5].to_string(),
                comment: parts[6].to_string(),
            });
        }
    }

    fields
}

/// Required (non-nullable) columns that must be present in every insert
pub fn required_columns() -> Vec<&'static str> {
    vec!["_timestamp", "_org_id"]
}

/// Columns with ClickHouse DEFAULT values (can be omitted from inserts)
pub fn default_columns() -> Vec<&'static str> {
    vec!["_timestamp_load", "_uuid"]
}

/// Nullable columns (can be null or omitted)
pub fn nullable_columns() -> Vec<&'static str> {
    vec!["_timestamp_received", "_raw", "_json", "_tags"]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_table_ddl_embedded() {
        assert!(COMMON_TABLE_DDL.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(COMMON_TABLE_DDL.contains("{db}.{table}"));
        assert!(COMMON_TABLE_DDL.contains("_org_id"));
        assert!(COMMON_TABLE_DDL.contains("generateUUIDv7()"));
    }

    #[test]
    fn test_common_header_csv_embedded() {
        assert!(COMMON_HEADER_CSV.contains("column,type,default"));
        assert!(COMMON_HEADER_CSV.contains("timestamp"));
        assert!(COMMON_HEADER_CSV.contains("_org_id"));
    }

    #[test]
    fn test_render_ddl() {
        let ddl = render_ddl("mydb", "mytable");
        assert!(ddl.contains("mydb.mytable"));
        assert!(!ddl.contains("{db}"));
        assert!(!ddl.contains("{table}"));
    }

    #[test]
    fn test_render_ddl_with_engine() {
        let ddl = render_ddl_with_engine("common", "events", TableEngine::SharedMergeTree);
        assert!(ddl.contains("SharedMergeTree()"));

        let ddl = render_ddl_with_engine("common", "events", TableEngine::ReplicatedMergeTree);
        assert!(ddl.contains("ReplicatedMergeTree("));
        assert!(ddl.contains("/common/events"));

        let ddl = render_ddl_with_engine("common", "events", TableEngine::MergeTree);
        assert!(ddl.contains("MergeTree()"));
    }

    #[test]
    fn test_engine_clause_generation() {
        assert_eq!(
            TableEngine::SharedMergeTree.to_engine_clause("db", "tbl"),
            "SharedMergeTree()"
        );
        assert_eq!(
            TableEngine::MergeTree.to_engine_clause("db", "tbl"),
            "MergeTree()"
        );

        let replicated = TableEngine::ReplicatedMergeTree.to_engine_clause("mydb", "mytable");
        assert!(replicated.contains("ReplicatedMergeTree"));
        assert!(replicated.contains("mydb/mytable"));
        assert!(replicated.contains("{shard}"));
        assert!(replicated.contains("{replica}"));
    }

    #[test]
    fn test_cluster_capabilities_from_version() {
        // ClickHouse 25.1 - all features
        let caps = ClusterCapabilities::from_version("25.1.2.123", false);
        assert!(caps.shared_merge_tree);
        assert!(caps.full_text_index);
        assert!(caps.json_type);
        assert_eq!(caps.best_engine(), TableEngine::SharedMergeTree);

        // ClickHouse 24.8 - SharedMergeTree but not full_text GA
        let caps = ClusterCapabilities::from_version("24.8.1.0", true);
        assert!(caps.shared_merge_tree);
        assert!(caps.full_text_index); // 24.8+ has it
        assert!(caps.json_type);
        assert_eq!(caps.best_engine(), TableEngine::SharedMergeTree);

        // ClickHouse 23.x - fallback to Replicated/MergeTree
        let caps = ClusterCapabilities::from_version("23.8.1.0", true);
        assert!(!caps.shared_merge_tree);
        assert!(!caps.full_text_index);
        assert_eq!(caps.best_engine(), TableEngine::ReplicatedMergeTree);

        let caps = ClusterCapabilities::from_version("23.8.1.0", false);
        assert_eq!(caps.best_engine(), TableEngine::MergeTree);
    }

    #[test]
    fn test_text_index_ddl() {
        let caps_new = ClusterCapabilities::from_version("25.1.0.0", false);
        let idx = caps_new.text_index_ddl("logoriginal");
        assert!(idx.contains("full_text(0)"));

        let caps_old = ClusterCapabilities::from_version("23.8.0.0", false);
        let idx = caps_old.text_index_ddl("logoriginal");
        assert!(idx.contains("ngrambf_v1"));
    }

    #[test]
    fn test_parse_common_header() {
        let fields = parse_common_header();
        assert!(!fields.is_empty());

        let timestamp = fields.iter().find(|f| f.column == "_timestamp").unwrap();
        assert_eq!(timestamp.data_type, "DateTime64(3)");
        assert!(!timestamp.nullable);
        assert_eq!(timestamp.source, "@source: timestamp | now()");

        let org_id = fields.iter().find(|f| f.column == "_org_id").unwrap();
        assert_eq!(org_id.data_type, "String");
        assert!(!org_id.nullable);
        assert_eq!(org_id.source, "@source: org_id");

        let json_field = fields.iter().find(|f| f.column == "_json").unwrap();
        assert!(json_field.nullable);
        assert_eq!(json_field.source, "@captured: raw_payload as JSON");

        let tags_field = fields.iter().find(|f| f.column == "_tags").unwrap();
        assert!(tags_field.nullable);
        assert_eq!(
            tags_field.source,
            "@source: first(tags/_tags/meta/metadata.tags)"
        );
    }

    #[test]
    fn test_required_columns() {
        let required = required_columns();
        assert!(required.contains(&"_timestamp"));
        assert!(required.contains(&"_org_id"));
    }

    #[test]
    fn test_default_columns() {
        let defaults = default_columns();
        assert!(defaults.contains(&"_timestamp_load"));
        assert!(defaults.contains(&"_uuid"));
    }

    #[test]
    fn test_nullable_columns() {
        let nullable = nullable_columns();
        assert!(nullable.contains(&"_timestamp_received"));
        assert!(nullable.contains(&"_raw"));
        assert!(nullable.contains(&"_json"));
        assert!(nullable.contains(&"_tags"));
    }

    #[test]
    fn test_table_tags_core() {
        let tags = TableTags::core();
        assert_eq!(tags.get("schema_source"), Some("core"));
        assert_eq!(tags.get("schema_version"), Some("2"));
        assert!(tags.has("schema_source"));
        assert!(!tags.has("nonexistent"));
    }

    #[test]
    fn test_table_tags_user() {
        let tags = TableTags::user();
        assert_eq!(tags.get("schema_source"), Some("user"));
        assert_eq!(tags.get("schema_version"), Some("1"));
    }

    #[test]
    fn test_table_tags_custom() {
        let tags = TableTags::new()
            .with("schema_source", "core")
            .with("custom_tag", "custom_value");

        assert_eq!(tags.len(), 2);
        assert_eq!(tags.get("custom_tag"), Some("custom_value"));
    }

    #[test]
    fn test_table_tags_to_comment() {
        let tags = TableTags::core();
        let comment = tags.to_comment();

        // BTreeMap maintains sorted order
        assert!(comment.contains("@schema_source: core"));
        assert!(comment.contains("@schema_version: 2"));
        assert!(comment.contains(" | "));
    }

    #[test]
    fn test_table_tags_from_comment() {
        let comment = "@schema_source: core | @schema_version: 2 | @custom: value";
        let tags = TableTags::from_comment(comment);

        assert_eq!(tags.get("schema_source"), Some("core"));
        assert_eq!(tags.get("schema_version"), Some("2"));
        assert_eq!(tags.get("custom"), Some("value"));
        assert_eq!(tags.len(), 3);
    }

    #[test]
    fn test_table_tags_roundtrip() {
        let original = TableTags::core().with("extra", "data");
        let comment = original.to_comment();
        let parsed = TableTags::from_comment(&comment);

        assert_eq!(original.get("schema_source"), parsed.get("schema_source"));
        assert_eq!(original.get("schema_version"), parsed.get("schema_version"));
        assert_eq!(original.get("extra"), parsed.get("extra"));
    }

    #[test]
    fn test_render_ddl_with_tags() {
        let tags = TableTags::core();
        let ddl = render_ddl_with_tags("common", "events", TableEngine::MergeTree, &tags);

        assert!(ddl.contains("common.events"));
        assert!(ddl.contains("MergeTree()"));
        assert!(ddl.contains("@schema_source: core"));
        assert!(ddl.contains("@schema_version: 2"));
        assert!(ddl.contains("COMMENT"));
    }

    #[test]
    fn test_render_ddl_includes_core_tags() {
        // render_ddl_with_engine should now include core tags by default
        let ddl = render_ddl_with_engine("common", "events", TableEngine::MergeTree);
        assert!(ddl.contains("@schema_source: core"));
    }
}
