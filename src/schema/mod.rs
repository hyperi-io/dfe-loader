// Project:   dfe-loader
// File:      mod.rs
// Purpose:   Embedded schema definitions (compiled-in)
// Language:  Rust
//
// License:   LicenseRef-HyperSec-EULA
// Copyright: (c) 2026 HyperSec

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
//!
//! ```ignore
//! use dfe_loader::schema::{render_ddl_with_engine, TableEngine};
//!
//! let ddl = render_ddl_with_engine("common", "events", TableEngine::SharedMergeTree);
//! ```

/// Common table DDL template (ClickHouse)
///
/// Template variables:
/// - `{db}` - Database name
/// - `{table}` - Table name
/// - `{engine}` - Table engine clause
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
    COMMON_TABLE_DDL
        .replace("{db}", db)
        .replace("{table}", table)
        .replace("{engine}", &engine.to_engine_clause(db, table))
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

/// SQL to check if cluster is configured
pub const DETECT_CLUSTER_SQL: &str = "SELECT count() > 0 FROM system.clusters WHERE cluster != ''";

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
pub fn add_text_index_ddl(db: &str, table: &str, column: &str, capabilities: &ClusterCapabilities) -> String {
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
    pub comment: String,
}

/// Parse the common header CSV into field definitions
pub fn parse_common_header() -> Vec<HeaderField> {
    let mut fields = Vec::new();

    for line in COMMON_HEADER_CSV.lines().skip(1) {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() >= 6 {
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
                comment: parts[5].to_string(),
            });
        }
    }

    fields
}

/// Required (non-nullable) columns that must be present in every insert
pub fn required_columns() -> Vec<&'static str> {
    vec!["timestamp", "_org_id"]
}

/// Columns with ClickHouse DEFAULT values (can be omitted from inserts)
pub fn default_columns() -> Vec<&'static str> {
    vec!["timestamp_load", "_uuid"]
}

/// Nullable columns (can be null or omitted)
pub fn nullable_columns() -> Vec<&'static str> {
    vec!["logoriginal", "logjson", "_tags"]
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

        let timestamp = fields.iter().find(|f| f.column == "timestamp").unwrap();
        assert_eq!(timestamp.data_type, "DateTime64(3)");
        assert!(!timestamp.nullable);

        let org_id = fields.iter().find(|f| f.column == "_org_id").unwrap();
        assert_eq!(org_id.data_type, "String");
        assert!(!org_id.nullable);

        let logjson = fields.iter().find(|f| f.column == "logjson").unwrap();
        assert!(logjson.nullable);
    }

    #[test]
    fn test_required_columns() {
        let required = required_columns();
        assert!(required.contains(&"timestamp"));
        assert!(required.contains(&"_org_id"));
    }

    #[test]
    fn test_default_columns() {
        let defaults = default_columns();
        assert!(defaults.contains(&"timestamp_load"));
        assert!(defaults.contains(&"_uuid"));
    }

    #[test]
    fn test_nullable_columns() {
        let nullable = nullable_columns();
        assert!(nullable.contains(&"logoriginal"));
        assert!(nullable.contains(&"logjson"));
        assert!(nullable.contains(&"_tags"));
    }
}
