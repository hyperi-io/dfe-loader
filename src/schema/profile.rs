// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Common Header Profile definitions
//!
//! A profile defines the **common header** portion of a ClickHouse table schema —
//! the system fields (`_timestamp`, `_org_id`, `_uuid`, etc.) that the loader
//! injects into every event. A profile is NOT a complete table schema.
//!
//! ## Common Header vs Complete Schema
//!
//! The common header is the **base** that every table gets. Real destination
//! tables will typically have additional columns on top — either from the source
//! data (via ClickHouse's JSON type or explicit schema) or added by DBAs.
//!
//! The **default table** (`dfe.default`) is the one exception: its schema IS
//! just the common header, because it's the catch-all for unrouted events.
//!
//! ```text
//! ┌─────────────────────────────────┐
//! │      default table (dfe.default)│  ← schema = common header only
//! │  ┌───────────────────────────┐  │
//! │  │     common header         │  │
//! │  │  (_timestamp, _org_id, …) │  │
//! │  └───────────────────────────┘  │
//! └─────────────────────────────────┘
//!
//! ┌─────────────────────────────────┐
//! │   non-default table (dfe.auth)  │  ← schema = common header + data columns
//! │  ┌───────────────────────────┐  │
//! │  │     common header         │  │
//! │  │  (_timestamp, _org_id, …) │  │
//! │  ├───────────────────────────┤  │
//! │  │   data columns            │  │
//! │  │  (user_id, action, ip, …) │  │
//! │  └───────────────────────────┘  │
//! └─────────────────────────────────┘
//! ```
//!
//! ## What a Profile Controls
//!
//! - **Field set**: which common header columns to inject (e.g., `_raw`, `_tags`)
//! - **Field behaviour**: how each field is populated (source expression language)
//! - **DDL structure**: ORDER BY, PARTITION BY, indexes, settings for the
//!   common header portion (used when auto-creating the default table)
//!
//! ## Built-in Profiles
//!
//! - `timeseries` — Full common header for time-series event ingestion (default)
//! - `minimal` — Timestamped org-scoped JSON storage (no _raw, _tags, _source)
//! - `passthrough` — Pure transport, raw JSON capture only (no field injection)
//!
//! ## User Profiles
//!
//! Custom profiles can be loaded from YAML files in a directory specified by
//! `profiles.custom_dir` in the config. User profiles follow the same YAML
//! format as built-in profiles.
//!
//! ## Source Expression Language
//!
//! Profile fields use source expressions to describe injection behaviour:
//! - `@generated: expr` — ClickHouse DEFAULT, loader omits field
//! - `@source: field | fallback` — Extract from data, fallback if missing
//! - `@source: first(a/b/c)` — First non-null from multiple fields
//! - `@renamed: field` — Zero-copy rename from source field
//! - `@captured: description` — Special handling (raw payload sidecar)
//! - `@derived: description` — Derived from routing/topic metadata

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use super::{ClusterCapabilities, TableEngine, TableTags};

/// Built-in profiles (embedded at compile time)
const TIMESERIES_YAML: &str = include_str!("../../schemas/profiles/timeseries.yaml");
const MINIMAL_YAML: &str = include_str!("../../schemas/profiles/minimal.yaml");
const PASSTHROUGH_YAML: &str = include_str!("../../schemas/profiles/passthrough.yaml");

/// Common header profile definition
///
/// Defines the common header (system fields) that the loader injects into
/// every event. This is the BASE of a table schema, not the complete schema.
/// Non-default tables will have additional data-specific columns beyond what
/// the profile defines.
///
/// Tables are associated with profiles via config (`profiles.table_profiles`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    /// Profile name (e.g., "timeseries")
    pub name: String,
    /// Profile version (incremented on breaking changes)
    pub version: u32,
    /// Human-readable description
    pub description: String,
    /// Field definitions (columns in the table)
    pub fields: Vec<ProfileField>,
    /// DDL structure (ORDER BY, PARTITION BY, indexes)
    pub ddl: DdlStructure,
}

/// Field definition within a profile
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileField {
    /// Column name (e.g., "_timestamp_load")
    pub name: String,
    /// ClickHouse data type (e.g., "DateTime64(3)", "LowCardinality(String)")
    #[serde(rename = "type")]
    pub data_type: String,
    /// ClickHouse DEFAULT expression (e.g., "now64(3)")
    #[serde(default)]
    pub default: Option<String>,
    /// Whether the column is nullable
    #[serde(default)]
    pub nullable: bool,
    /// Compression codec (e.g., "Delta, ZSTD(1)")
    #[serde(default)]
    pub codec: Option<String>,
    /// Source expression describing injection behaviour
    pub source: String,
    /// Human-readable comment
    #[serde(default)]
    pub comment: String,
}

/// DDL table structure definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlStructure {
    /// ORDER BY columns
    pub order_by: Vec<String>,
    /// PARTITION BY expression
    pub partition_by: String,
    /// Table settings (e.g., index_granularity)
    #[serde(default)]
    pub settings: BTreeMap<String, String>,
    /// Secondary indexes
    #[serde(default)]
    pub indexes: Vec<DdlIndex>,
    /// Text search index configuration
    #[serde(default)]
    pub text_index: Option<TextIndexConfig>,
}

/// Secondary index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DdlIndex {
    /// Index name (e.g., "idx_timestamp")
    pub name: String,
    /// Column to index
    pub column: String,
    /// Index type (e.g., "minmax", "bloom_filter")
    #[serde(rename = "type")]
    pub index_type: String,
    /// Granularity setting
    #[serde(default = "default_granularity")]
    pub granularity: u32,
}

fn default_granularity() -> u32 {
    1
}

/// Text search index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TextIndexConfig {
    /// Column to index (e.g., "_raw")
    pub column: String,
    /// Auto-detect index type based on ClickHouse version
    /// true: full_text on 25.1+, ngrambf_v1 fallback
    #[serde(default = "default_auto_detect")]
    pub auto_detect: bool,
}

fn default_auto_detect() -> bool {
    true
}

/// Source expression type parsed from the `source` field
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceBehaviour {
    /// `@generated: expr` — ClickHouse DEFAULT handles it, loader omits field
    Generated(String),
    /// `@source: field | fallback` — Extract from source data
    Source {
        fields: Vec<String>,
        fallback: Option<String>,
    },
    /// `@renamed: field` — Zero-copy rename from source field
    Renamed(String),
    /// `@captured: description` — Special sidecar handling (e.g., raw payload)
    Captured(String),
    /// `@derived: description` — Derived from routing/topic metadata
    Derived(String),
    /// Unknown or unparseable source expression
    Unknown(String),
}

impl ProfileField {
    /// Parse the source expression into a `SourceBehaviour`
    pub fn behaviour(&self) -> SourceBehaviour {
        let source = self.source.trim();

        if let Some(expr) = source.strip_prefix("@generated:") {
            return SourceBehaviour::Generated(expr.trim().to_string());
        }

        if let Some(expr) = source.strip_prefix("@renamed:") {
            return SourceBehaviour::Renamed(expr.trim().to_string());
        }

        if let Some(expr) = source.strip_prefix("@captured:") {
            return SourceBehaviour::Captured(expr.trim().to_string());
        }

        if let Some(expr) = source.strip_prefix("@derived:") {
            return SourceBehaviour::Derived(expr.trim().to_string());
        }

        if let Some(expr) = source.strip_prefix("@source:") {
            let expr = expr.trim();

            // Handle first(a/b/c) syntax
            if let Some(inner) = expr
                .strip_prefix("first(")
                .and_then(|s| s.strip_suffix(')'))
            {
                let fields: Vec<String> = inner.split('/').map(|s| s.trim().to_string()).collect();
                return SourceBehaviour::Source {
                    fields,
                    fallback: None,
                };
            }

            // Handle field | fallback syntax
            if let Some((field, fallback)) = expr.split_once('|') {
                let field = field.trim().to_string();
                let fallback = fallback.trim().to_string();
                return SourceBehaviour::Source {
                    fields: vec![field],
                    fallback: Some(fallback),
                };
            }

            // Simple field reference
            return SourceBehaviour::Source {
                fields: vec![expr.to_string()],
                fallback: None,
            };
        }

        SourceBehaviour::Unknown(source.to_string())
    }
}

impl Profile {
    /// Parse a profile from YAML string
    pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml_ng::Error> {
        serde_yaml_ng::from_str(yaml)
    }

    /// Load a profile from a YAML file
    pub fn from_file(path: &Path) -> Result<Self, ProfileError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ProfileError::Io(path.display().to_string(), e))?;
        Self::from_yaml(&content).map_err(|e| ProfileError::Parse(path.display().to_string(), e))
    }

    /// Get field names in this profile
    pub fn field_names(&self) -> Vec<&str> {
        self.fields.iter().map(|f| f.name.as_str()).collect()
    }

    /// Check if a field exists in this profile
    pub fn has_field(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }

    /// Get a field by name
    pub fn field(&self, name: &str) -> Option<&ProfileField> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// Get fields with their parsed source behaviour
    pub fn field_behaviours(&self) -> Vec<(&ProfileField, SourceBehaviour)> {
        self.fields.iter().map(|f| (f, f.behaviour())).collect()
    }

    /// Generate CREATE TABLE DDL from this profile's common header fields
    ///
    /// The generated DDL contains ONLY the common header columns defined in
    /// the profile. This is appropriate for the default table (where the schema
    /// IS the common header). Non-default tables should be created externally
    /// with their own data columns plus the common header.
    pub fn render_ddl(
        &self,
        db: &str,
        table: &str,
        engine: TableEngine,
        tags: &TableTags,
    ) -> String {
        let mut sql = format!("CREATE TABLE IF NOT EXISTS {db}.{table}\n(\n");

        // Column definitions
        let mut col_defs: Vec<String> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let mut def = format!("    `{}`", field.name);

            // Type (with Nullable wrapper if needed)
            if field.nullable {
                def.push_str(&format!(" Nullable({})", field.data_type));
            } else {
                def.push_str(&format!(" {}", field.data_type));
            }

            // DEFAULT expression
            if let Some(ref default) = field.default {
                def.push_str(&format!(" DEFAULT {default}"));
            }

            // CODEC
            if let Some(ref codec) = field.codec {
                def.push_str(&format!(" CODEC({codec})"));
            }

            col_defs.push(def);
        }

        // Inline indexes
        for index in &self.ddl.indexes {
            col_defs.push(format!(
                "    INDEX {} {} TYPE {} GRANULARITY {}",
                index.name, index.column, index.index_type, index.granularity
            ));
        }

        sql.push_str(&col_defs.join(",\n"));
        sql.push_str("\n)\n");

        // ENGINE
        sql.push_str(&format!(
            "ENGINE = {}\n",
            engine.to_engine_clause(db, table)
        ));

        // ORDER BY
        sql.push_str(&format!("ORDER BY ({})\n", self.ddl.order_by.join(", ")));

        // PARTITION BY
        sql.push_str(&format!("PARTITION BY ({})\n", self.ddl.partition_by));

        // SETTINGS
        if !self.ddl.settings.is_empty() {
            let settings: Vec<String> = self
                .ddl
                .settings
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect();
            sql.push_str(&format!("SETTINGS {}\n", settings.join(", ")));
        }

        // COMMENT with table tags
        let comment = tags.to_comment();
        if !comment.is_empty() {
            sql.push_str(&format!("COMMENT '{comment}'\n"));
        }

        sql
    }

    /// Generate ALTER TABLE DDL for the text search index
    ///
    /// Returns `None` if the profile has no text index configured.
    pub fn text_index_ddl(
        &self,
        db: &str,
        table: &str,
        capabilities: &ClusterCapabilities,
    ) -> Option<String> {
        let text_idx = self.ddl.text_index.as_ref()?;

        let column = &text_idx.column;
        if capabilities.full_text_index && text_idx.auto_detect {
            Some(format!(
                "ALTER TABLE {db}.{table} ADD INDEX idx_{column} {column} TYPE full_text(0) GRANULARITY 1"
            ))
        } else {
            Some(format!(
                "ALTER TABLE {db}.{table} ADD INDEX idx_{column}_ngram {column} TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4"
            ))
        }
    }
}

impl fmt::Display for Profile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@v{}", self.name, self.version)
    }
}

// --- Profile migration/versioning ---

/// Describes the type of column change detected between a profile and existing table
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnChange {
    /// Column exists in profile but not in table — needs ADD COLUMN
    Added {
        name: String,
        data_type: String,
        nullable: bool,
        default: Option<String>,
        codec: Option<String>,
    },
    /// Column type changed — needs MODIFY COLUMN (or manual intervention)
    TypeChanged {
        name: String,
        profile_type: String,
        existing_type: String,
    },
    /// Column exists in table but not in profile — informational only
    Removed { name: String },
}

/// Result of comparing a profile against an existing table's columns
#[derive(Debug)]
pub struct ProfileDiff {
    /// Profile name
    pub profile_name: String,
    /// Profile version
    pub profile_version: u32,
    /// Profile version stored in table tags (if any)
    pub table_version: Option<u32>,
    /// Database.table being compared
    pub table: String,
    /// Detected column changes
    pub changes: Vec<ColumnChange>,
}

impl ProfileDiff {
    /// Whether any migration is needed
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Columns to add
    pub fn additions(&self) -> Vec<&ColumnChange> {
        self.changes
            .iter()
            .filter(|c| matches!(c, ColumnChange::Added { .. }))
            .collect()
    }

    /// Columns with type changes
    pub fn type_changes(&self) -> Vec<&ColumnChange> {
        self.changes
            .iter()
            .filter(|c| matches!(c, ColumnChange::TypeChanged { .. }))
            .collect()
    }

    /// Columns present in table but not in profile
    pub fn removals(&self) -> Vec<&ColumnChange> {
        self.changes
            .iter()
            .filter(|c| matches!(c, ColumnChange::Removed { .. }))
            .collect()
    }

    /// Generate safe ALTER TABLE DDL for additive changes only
    ///
    /// Only generates ADD COLUMN statements (safe, non-destructive).
    /// Type changes and removals require manual intervention and are
    /// returned as warnings.
    pub fn migration_ddl(&self, db: &str, table: &str) -> MigrationPlan {
        let mut ddl_statements = Vec::new();
        let mut warnings = Vec::new();

        for change in &self.changes {
            match change {
                ColumnChange::Added {
                    name,
                    data_type,
                    nullable,
                    default,
                    codec,
                } => {
                    let col_type = if *nullable {
                        format!("Nullable({data_type})")
                    } else {
                        data_type.clone()
                    };

                    let mut stmt = format!("ALTER TABLE {db}.{table} ADD COLUMN `{name}` {col_type}");

                    if let Some(ref def) = default {
                        stmt.push_str(&format!(" DEFAULT {def}"));
                    }

                    if let Some(ref c) = codec {
                        stmt.push_str(&format!(" CODEC({c})"));
                    }

                    ddl_statements.push(stmt);
                }
                ColumnChange::TypeChanged {
                    name,
                    profile_type,
                    existing_type,
                } => {
                    warnings.push(format!(
                        "Column `{name}` type mismatch: profile has {profile_type}, table has {existing_type}. Manual MODIFY COLUMN required."
                    ));
                }
                ColumnChange::Removed { name } => {
                    warnings.push(format!(
                        "Column `{name}` exists in table but not in profile. No action taken (columns are not dropped automatically)."
                    ));
                }
            }
        }

        MigrationPlan {
            profile: self.profile_name.clone(),
            from_version: self.table_version,
            to_version: self.profile_version,
            table: format!("{db}.{table}"),
            ddl_statements,
            warnings,
        }
    }
}

impl fmt::Display for ProfileDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ProfileDiff({} → {}, {} changes)",
            self.profile_name,
            self.table,
            self.changes.len()
        )
    }
}

/// Migration plan generated from a profile diff
#[derive(Debug, Clone)]
pub struct MigrationPlan {
    /// Profile name
    pub profile: String,
    /// Version currently in the table (from tags), if known
    pub from_version: Option<u32>,
    /// Target profile version
    pub to_version: u32,
    /// Fully qualified table name
    pub table: String,
    /// ALTER TABLE DDL statements to execute (safe, additive only)
    pub ddl_statements: Vec<String>,
    /// Warnings for changes that require manual intervention
    pub warnings: Vec<String>,
}

impl MigrationPlan {
    /// Whether the migration has any DDL to execute
    pub fn has_ddl(&self) -> bool {
        !self.ddl_statements.is_empty()
    }

    /// Whether the migration has warnings (type changes or removals)
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Log the migration plan
    pub fn log_plan(&self) {
        if self.has_ddl() {
            info!(
                profile = %self.profile,
                table = %self.table,
                from_version = ?self.from_version,
                to_version = self.to_version,
                statements = self.ddl_statements.len(),
                "Migration plan generated"
            );
            for stmt in &self.ddl_statements {
                info!(ddl = %stmt, "Migration DDL");
            }
        }

        for warning in &self.warnings {
            warn!(table = %self.table, "{}", warning);
        }
    }
}

impl Profile {
    /// Compare this profile against existing table columns to detect schema drift
    ///
    /// `existing_columns` is a slice of (column_name, column_type) pairs from
    /// ClickHouse `system.columns` or a DESCRIBE query.
    pub fn diff_columns(
        &self,
        table: &str,
        existing_columns: &[(&str, &str)],
        table_version: Option<u32>,
    ) -> ProfileDiff {
        let mut changes = Vec::new();

        // Build lookup of existing columns
        let existing: FxHashMap<&str, &str> = existing_columns.iter().copied().collect();

        // Check profile fields against existing columns
        for field in &self.fields {
            match existing.get(field.name.as_str()) {
                None => {
                    // Column missing from table
                    changes.push(ColumnChange::Added {
                        name: field.name.clone(),
                        data_type: field.data_type.clone(),
                        nullable: field.nullable,
                        default: field.default.clone(),
                        codec: field.codec.clone(),
                    });
                }
                Some(&existing_type) => {
                    // Column exists — check type compatibility
                    let profile_type = if field.nullable {
                        format!("Nullable({})", field.data_type)
                    } else {
                        field.data_type.clone()
                    };

                    // Normalise for comparison (ClickHouse may report types differently)
                    if !types_compatible(&profile_type, existing_type) {
                        changes.push(ColumnChange::TypeChanged {
                            name: field.name.clone(),
                            profile_type,
                            existing_type: existing_type.to_string(),
                        });
                    }
                }
            }
        }

        // Check for columns in table that aren't in profile
        let profile_names: FxHashMap<&str, ()> =
            self.fields.iter().map(|f| (f.name.as_str(), ())).collect();
        for &(col_name, _) in existing_columns {
            if !profile_names.contains_key(col_name) {
                changes.push(ColumnChange::Removed {
                    name: col_name.to_string(),
                });
            }
        }

        ProfileDiff {
            profile_name: self.name.clone(),
            profile_version: self.version,
            table_version,
            table: table.to_string(),
            changes,
        }
    }
}

/// Check if two ClickHouse type strings are compatible
///
/// Handles common variations in type reporting (e.g., "String" vs "String",
/// "LowCardinality(String)" normalisation).
fn types_compatible(profile_type: &str, existing_type: &str) -> bool {
    // Exact match (most common case)
    if profile_type == existing_type {
        return true;
    }

    // Normalise whitespace and compare
    let normalise = |s: &str| s.replace(' ', "");
    normalise(profile_type) == normalise(existing_type)
}

/// Profile loading errors
#[derive(Debug)]
pub enum ProfileError {
    /// File read error
    Io(String, std::io::Error),
    /// YAML parse error
    Parse(String, serde_yaml_ng::Error),
    /// Profile not found
    NotFound(String),
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfileError::Io(path, e) => write!(f, "Failed to read profile {path}: {e}"),
            ProfileError::Parse(path, e) => write!(f, "Failed to parse profile {path}: {e}"),
            ProfileError::NotFound(name) => write!(f, "Profile not found: {name}"),
        }
    }
}

impl std::error::Error for ProfileError {}

/// Registry of available profiles
///
/// Holds built-in profiles and any user-defined profiles loaded from YAML.
/// Resolves profile names to `Profile` structs for DDL generation and
/// transformer configuration.
pub struct ProfileRegistry {
    profiles: FxHashMap<String, Profile>,
}

impl ProfileRegistry {
    /// Create a new registry with built-in profiles
    pub fn new() -> Self {
        let mut registry = Self {
            profiles: FxHashMap::default(),
        };
        registry.register_builtins();
        registry
    }

    /// Register built-in profiles
    fn register_builtins(&mut self) {
        let builtins: &[(&str, &str)] = &[
            ("timeseries", TIMESERIES_YAML),
            ("minimal", MINIMAL_YAML),
            ("passthrough", PASSTHROUGH_YAML),
        ];

        for (label, yaml) in builtins {
            match Profile::from_yaml(yaml) {
                Ok(profile) => {
                    debug!(
                        profile = %profile.name,
                        version = profile.version,
                        fields = profile.fields.len(),
                        "Registered built-in profile"
                    );
                    self.profiles.insert(profile.name.clone(), profile);
                }
                Err(e) => {
                    // Built-in profiles are compile-time embedded and should always parse
                    panic!("Failed to parse built-in {label} profile: {e}");
                }
            }
        }
    }

    /// Load user-defined profiles from a directory
    ///
    /// Reads all `.yaml` and `.yml` files in the directory. User profiles
    /// with the same name as built-in profiles will override them.
    pub fn load_custom_dir(&mut self, dir: &Path) -> Result<usize, ProfileError> {
        if !dir.exists() {
            debug!(dir = %dir.display(), "Custom profiles directory does not exist, skipping");
            return Ok(0);
        }

        let mut loaded = 0;
        let entries = std::fs::read_dir(dir)
            .map_err(|e| ProfileError::Io(dir.display().to_string(), e))?;

        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|e| e.to_str());

            if !matches!(ext, Some("yaml" | "yml")) {
                continue;
            }

            match Profile::from_file(&path) {
                Ok(profile) => {
                    let is_override = self.profiles.contains_key(&profile.name);
                    info!(
                        profile = %profile.name,
                        version = profile.version,
                        path = %path.display(),
                        override_builtin = is_override,
                        "Loaded custom profile"
                    );
                    self.profiles.insert(profile.name.clone(), profile);
                    loaded += 1;
                }
                Err(e) => {
                    warn!(
                        path = %path.display(),
                        error = %e,
                        "Failed to load custom profile, skipping"
                    );
                }
            }
        }

        Ok(loaded)
    }

    /// Resolve a profile by name
    pub fn get(&self, name: &str) -> Option<&Profile> {
        self.profiles.get(name)
    }

    /// Resolve the profile for a specific table
    ///
    /// Checks `table_profiles` mapping first, falls back to `default_profile`.
    pub fn resolve_for_table(
        &self,
        table: &str,
        default_profile: &str,
        table_profiles: &FxHashMap<String, String>,
    ) -> Result<&Profile, ProfileError> {
        let profile_name = table_profiles
            .get(table)
            .map(|s| s.as_str())
            .unwrap_or(default_profile);

        self.profiles
            .get(profile_name)
            .ok_or_else(|| ProfileError::NotFound(profile_name.to_string()))
    }

    /// List all registered profile names
    pub fn names(&self) -> Vec<&str> {
        self.profiles.keys().map(|k| k.as_str()).collect()
    }

    /// Number of registered profiles
    pub fn len(&self) -> usize {
        self.profiles.len()
    }

    /// Whether the registry is empty
    pub fn is_empty(&self) -> bool {
        self.profiles.is_empty()
    }
}

impl Default for ProfileRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_builtin_timeseries() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).expect("Failed to parse timeseries");
        assert_eq!(profile.name, "timeseries");
        assert_eq!(profile.version, 1);
        assert_eq!(profile.fields.len(), 9);
    }

    #[test]
    fn test_timeseries_fields() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();

        let ts_load = profile.field("_timestamp_load").unwrap();
        assert_eq!(ts_load.data_type, "DateTime64(3)");
        assert_eq!(ts_load.default.as_deref(), Some("now64(3)"));
        assert!(!ts_load.nullable);
        assert_eq!(ts_load.codec.as_deref(), Some("Delta, ZSTD(1)"));

        let org_id = profile.field("_org_id").unwrap();
        assert_eq!(org_id.data_type, "LowCardinality(String)");
        assert!(!org_id.nullable);

        let raw = profile.field("_raw").unwrap();
        assert!(raw.nullable);
        assert_eq!(raw.codec.as_deref(), Some("ZSTD(3)"));

        let json = profile.field("_json").unwrap();
        assert!(json.nullable);
        assert_eq!(json.data_type, "JSON");
    }

    #[test]
    fn test_timeseries_ddl_structure() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();

        assert_eq!(
            profile.ddl.order_by,
            vec!["_org_id", "_timestamp_load", "_uuid"]
        );
        assert_eq!(
            profile.ddl.partition_by,
            "toYYYYMM(_timestamp_load), _org_id"
        );
        assert_eq!(profile.ddl.settings.get("index_granularity").unwrap(), "8192");
        assert_eq!(profile.ddl.indexes.len(), 1);
        assert_eq!(profile.ddl.indexes[0].name, "idx_timestamp");
        assert_eq!(profile.ddl.indexes[0].column, "_timestamp");
        assert_eq!(profile.ddl.indexes[0].index_type, "minmax");

        let text_idx = profile.ddl.text_index.as_ref().unwrap();
        assert_eq!(text_idx.column, "_raw");
        assert!(text_idx.auto_detect);
    }

    #[test]
    fn test_field_names() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let names = profile.field_names();
        assert!(names.contains(&"_timestamp_load"));
        assert!(names.contains(&"_timestamp"));
        assert!(names.contains(&"_uuid"));
        assert!(names.contains(&"_org_id"));
        assert!(names.contains(&"_raw"));
        assert!(names.contains(&"_json"));
        assert!(names.contains(&"_tags"));
        assert!(names.contains(&"_source"));
    }

    #[test]
    fn test_has_field() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        assert!(profile.has_field("_timestamp"));
        assert!(profile.has_field("_org_id"));
        assert!(!profile.has_field("nonexistent"));
    }

    #[test]
    fn test_source_behaviour_generated() {
        let field = ProfileField {
            name: "_uuid".into(),
            data_type: "UUID".into(),
            default: Some("generateUUIDv7()".into()),
            nullable: false,
            codec: None,
            source: "@generated: generateUUIDv7()".into(),
            comment: String::new(),
        };
        assert_eq!(
            field.behaviour(),
            SourceBehaviour::Generated("generateUUIDv7()".into())
        );
    }

    #[test]
    fn test_source_behaviour_source_with_fallback() {
        let field = ProfileField {
            name: "_timestamp".into(),
            data_type: "DateTime64(3)".into(),
            default: None,
            nullable: false,
            codec: None,
            source: "@source: timestamp | now()".into(),
            comment: String::new(),
        };
        assert_eq!(
            field.behaviour(),
            SourceBehaviour::Source {
                fields: vec!["timestamp".into()],
                fallback: Some("now()".into()),
            }
        );
    }

    #[test]
    fn test_source_behaviour_source_first() {
        let field = ProfileField {
            name: "_tags".into(),
            data_type: "JSON".into(),
            default: None,
            nullable: true,
            codec: None,
            source: "@source: first(tags/_tags/meta/metadata.tags)".into(),
            comment: String::new(),
        };
        match field.behaviour() {
            SourceBehaviour::Source { fields, fallback } => {
                assert_eq!(
                    fields,
                    vec!["tags", "_tags", "meta", "metadata.tags"]
                );
                assert!(fallback.is_none());
            }
            other => panic!("Expected Source, got {other:?}"),
        }
    }

    #[test]
    fn test_source_behaviour_renamed() {
        let field = ProfileField {
            name: "_raw".into(),
            data_type: "String".into(),
            default: None,
            nullable: true,
            codec: None,
            source: "@renamed: logoriginal".into(),
            comment: String::new(),
        };
        assert_eq!(
            field.behaviour(),
            SourceBehaviour::Renamed("logoriginal".into())
        );
    }

    #[test]
    fn test_source_behaviour_captured() {
        let field = ProfileField {
            name: "_json".into(),
            data_type: "JSON".into(),
            default: None,
            nullable: true,
            codec: None,
            source: "@captured: raw_payload as JSON".into(),
            comment: String::new(),
        };
        assert_eq!(
            field.behaviour(),
            SourceBehaviour::Captured("raw_payload as JSON".into())
        );
    }

    #[test]
    fn test_source_behaviour_derived() {
        let field = ProfileField {
            name: "_source".into(),
            data_type: "LowCardinality(String)".into(),
            default: None,
            nullable: false,
            codec: None,
            source: "@derived: first(_source) | topic_name".into(),
            comment: String::new(),
        };
        assert_eq!(
            field.behaviour(),
            SourceBehaviour::Derived("first(_source) | topic_name".into())
        );
    }

    #[test]
    fn test_render_ddl_basic() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let tags = TableTags::core().with("profile", "timeseries");
        let ddl = profile.render_ddl("dfe", "default", TableEngine::MergeTree, &tags);

        // Table name
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dfe.default"));

        // All columns present
        assert!(ddl.contains("`_timestamp_load`"));
        assert!(ddl.contains("`_timestamp`"));
        assert!(ddl.contains("`_timestamp_received`"));
        assert!(ddl.contains("`_uuid`"));
        assert!(ddl.contains("`_org_id`"));
        assert!(ddl.contains("`_source`"));
        assert!(ddl.contains("`_raw`"));
        assert!(ddl.contains("`_json`"));
        assert!(ddl.contains("`_tags`"));

        // Types
        assert!(ddl.contains("DateTime64(3) DEFAULT now64(3)"));
        assert!(ddl.contains("UUID DEFAULT generateUUIDv7()"));
        assert!(ddl.contains("LowCardinality(String)"));
        assert!(ddl.contains("Nullable(String)"));
        assert!(ddl.contains("Nullable(JSON)"));

        // DDL structure
        assert!(ddl.contains("ENGINE = MergeTree()"));
        assert!(ddl.contains("ORDER BY (_org_id, _timestamp_load, _uuid)"));
        assert!(ddl.contains("PARTITION BY (toYYYYMM(_timestamp_load), _org_id)"));
        assert!(ddl.contains("index_granularity = 8192"));

        // Index
        assert!(ddl.contains("INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1"));

        // Tags
        assert!(ddl.contains("@profile: timeseries"));
        assert!(ddl.contains("@schema_source: core"));
    }

    #[test]
    fn test_render_ddl_replicated() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let tags = TableTags::core();
        let ddl = profile.render_ddl("dfe", "default", TableEngine::ReplicatedMergeTree, &tags);
        assert!(ddl.contains("ReplicatedMergeTree("));
    }

    #[test]
    fn test_text_index_ddl_fulltext() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let caps = ClusterCapabilities::from_version("25.1.0.0", false);
        let ddl = profile.text_index_ddl("dfe", "default", &caps).unwrap();
        assert!(ddl.contains("full_text(0)"));
        assert!(ddl.contains("dfe.default"));
    }

    #[test]
    fn test_text_index_ddl_ngrambf() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let caps = ClusterCapabilities::from_version("23.8.0.0", false);
        let ddl = profile.text_index_ddl("dfe", "default", &caps).unwrap();
        assert!(ddl.contains("ngrambf_v1"));
    }

    #[test]
    fn test_registry_has_builtins() {
        let registry = ProfileRegistry::new();
        assert_eq!(registry.len(), 3);
        assert!(registry.get("timeseries").is_some());
        assert!(registry.get("minimal").is_some());
        assert!(registry.get("passthrough").is_some());
    }

    #[test]
    fn test_registry_resolve_default() {
        let registry = ProfileRegistry::new();
        let table_profiles = FxHashMap::default();
        let profile = registry
            .resolve_for_table("dfe.default", "timeseries", &table_profiles)
            .unwrap();
        assert_eq!(profile.name, "timeseries");
    }

    #[test]
    fn test_registry_resolve_table_override() {
        let registry = ProfileRegistry::new();
        let mut table_profiles = FxHashMap::default();
        table_profiles.insert("dfe.special".to_string(), "timeseries".to_string());

        let profile = registry
            .resolve_for_table("dfe.special", "timeseries", &table_profiles)
            .unwrap();
        assert_eq!(profile.name, "timeseries");
    }

    #[test]
    fn test_registry_resolve_not_found() {
        let registry = ProfileRegistry::new();
        let table_profiles = FxHashMap::default();
        let result = registry.resolve_for_table("dfe.x", "nonexistent", &table_profiles);
        assert!(result.is_err());
    }

    #[test]
    fn test_registry_names() {
        let registry = ProfileRegistry::new();
        let names = registry.names();
        assert!(names.contains(&"timeseries"));
        assert!(names.contains(&"minimal"));
        assert!(names.contains(&"passthrough"));
    }

    #[test]
    fn test_profile_display() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        assert_eq!(format!("{profile}"), "timeseries@v1");
    }

    #[test]
    fn test_field_behaviours() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let behaviours = profile.field_behaviours();

        // All fields should have a behaviour
        assert_eq!(behaviours.len(), 9);

        // Check specific behaviours
        let ts_load = behaviours
            .iter()
            .find(|(f, _)| f.name == "_timestamp_load")
            .unwrap();
        assert!(matches!(ts_load.1, SourceBehaviour::Generated(_)));

        let org_id = behaviours
            .iter()
            .find(|(f, _)| f.name == "_org_id")
            .unwrap();
        assert!(matches!(org_id.1, SourceBehaviour::Source { .. }));

        let raw = behaviours.iter().find(|(f, _)| f.name == "_raw").unwrap();
        assert!(matches!(raw.1, SourceBehaviour::Renamed(_)));

        let json = behaviours.iter().find(|(f, _)| f.name == "_json").unwrap();
        assert!(matches!(json.1, SourceBehaviour::Captured(_)));
    }

    // --- Minimal profile tests ---

    #[test]
    fn test_minimal_profile_parse() {
        let profile = Profile::from_yaml(MINIMAL_YAML).expect("Failed to parse minimal profile");
        assert_eq!(profile.name, "minimal");
        assert_eq!(profile.version, 1);
        assert_eq!(profile.fields.len(), 5);
    }

    #[test]
    fn test_minimal_profile_fields() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();

        // Has these fields
        assert!(profile.has_field("_timestamp_load"));
        assert!(profile.has_field("_timestamp"));
        assert!(profile.has_field("_uuid"));
        assert!(profile.has_field("_org_id"));
        assert!(profile.has_field("_json"));

        // Does NOT have these fields
        assert!(!profile.has_field("_raw"));
        assert!(!profile.has_field("_tags"));
        assert!(!profile.has_field("_source"));
        assert!(!profile.has_field("_timestamp_received"));
    }

    #[test]
    fn test_minimal_profile_ddl() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        let tags = TableTags::core_with_profile("minimal");
        let ddl = profile.render_ddl("dfe", "events", TableEngine::MergeTree, &tags);

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dfe.events"));
        assert!(ddl.contains("_timestamp_load"));
        assert!(ddl.contains("_uuid"));
        assert!(ddl.contains("_org_id"));
        assert!(ddl.contains("_json"));
        // No _raw or _tags columns
        assert!(!ddl.contains("`_raw`"));
        assert!(!ddl.contains("`_tags`"));
        assert!(!ddl.contains("`_source`"));
    }

    // --- Passthrough profile tests ---

    #[test]
    fn test_passthrough_profile_parse() {
        let profile =
            Profile::from_yaml(PASSTHROUGH_YAML).expect("Failed to parse passthrough profile");
        assert_eq!(profile.name, "passthrough");
        assert_eq!(profile.version, 1);
        assert_eq!(profile.fields.len(), 4);
    }

    #[test]
    fn test_passthrough_profile_fields() {
        let profile = Profile::from_yaml(PASSTHROUGH_YAML).unwrap();

        // Has these fields
        assert!(profile.has_field("_timestamp_load"));
        assert!(profile.has_field("_uuid"));
        assert!(profile.has_field("_org_id"));
        assert!(profile.has_field("_json"));

        // Does NOT have these fields — no common header injection
        assert!(!profile.has_field("_timestamp"));
        assert!(!profile.has_field("_timestamp_received"));
        assert!(!profile.has_field("_raw"));
        assert!(!profile.has_field("_tags"));
        assert!(!profile.has_field("_source"));
    }

    #[test]
    fn test_passthrough_profile_ddl() {
        let profile = Profile::from_yaml(PASSTHROUGH_YAML).unwrap();
        let tags = TableTags::core_with_profile("passthrough");
        let ddl = profile.render_ddl("dfe", "raw", TableEngine::MergeTree, &tags);

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS dfe.raw"));
        assert!(ddl.contains("_timestamp_load"));
        assert!(ddl.contains("_uuid"));
        assert!(ddl.contains("_org_id"));
        assert!(ddl.contains("_json"));
        // No _timestamp, _raw, _tags, _source columns
        assert!(!ddl.contains("`_timestamp`"));
        assert!(!ddl.contains("`_raw`"));
        assert!(!ddl.contains("`_tags`"));
        assert!(!ddl.contains("`_source`"));
    }

    #[test]
    fn test_passthrough_no_common_header() {
        let profile = Profile::from_yaml(PASSTHROUGH_YAML).unwrap();
        // Passthrough doesn't have _timestamp, so common header injection should be disabled
        assert!(!profile.has_field("_timestamp"));
        // But it does have _json for raw capture
        assert!(profile.has_field("_json"));
    }

    #[test]
    fn test_registry_resolve_minimal() {
        let registry = ProfileRegistry::new();
        let table_profiles = FxHashMap::default();
        let profile = registry
            .resolve_for_table("dfe.metrics", "minimal", &table_profiles)
            .unwrap();
        assert_eq!(profile.name, "minimal");
        assert_eq!(profile.fields.len(), 5);
    }

    #[test]
    fn test_registry_table_override_to_minimal() {
        let registry = ProfileRegistry::new();
        let mut table_profiles = FxHashMap::default();
        table_profiles.insert("dfe.metrics".to_string(), "minimal".to_string());

        // Table override takes precedence over default
        let profile = registry
            .resolve_for_table("dfe.metrics", "timeseries", &table_profiles)
            .unwrap();
        assert_eq!(profile.name, "minimal");
    }

    // --- Migration/versioning tests ---

    #[test]
    fn test_diff_no_changes() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        // Simulate existing table with exact same columns
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "LowCardinality(String)"),
            ("_json", "Nullable(JSON)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        assert!(!diff.has_changes());
        assert!(diff.additions().is_empty());
        assert!(diff.type_changes().is_empty());
        assert!(diff.removals().is_empty());
    }

    #[test]
    fn test_diff_missing_columns() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        // Simulate table that's missing _json column
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "LowCardinality(String)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        assert!(diff.has_changes());
        assert_eq!(diff.additions().len(), 1);

        let added = &diff.additions()[0];
        if let ColumnChange::Added { name, .. } = added {
            assert_eq!(name, "_json");
        } else {
            panic!("Expected Added change");
        }
    }

    #[test]
    fn test_diff_extra_columns_in_table() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        // Simulate table with an extra column not in profile
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "LowCardinality(String)"),
            ("_json", "Nullable(JSON)"),
            ("_custom_field", "String"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        assert!(diff.has_changes());
        assert_eq!(diff.removals().len(), 1);

        let removed = &diff.removals()[0];
        if let ColumnChange::Removed { name } = removed {
            assert_eq!(name, "_custom_field");
        } else {
            panic!("Expected Removed change");
        }
    }

    #[test]
    fn test_diff_type_change() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        // _org_id has wrong type in existing table
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "String"), // Profile says LowCardinality(String)
            ("_json", "Nullable(JSON)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        assert!(diff.has_changes());
        assert_eq!(diff.type_changes().len(), 1);

        let changed = &diff.type_changes()[0];
        if let ColumnChange::TypeChanged {
            name,
            profile_type,
            existing_type,
        } = changed
        {
            assert_eq!(name, "_org_id");
            assert_eq!(profile_type, "LowCardinality(String)");
            assert_eq!(existing_type, "String");
        } else {
            panic!("Expected TypeChanged");
        }
    }

    #[test]
    fn test_migration_ddl_add_column() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "LowCardinality(String)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        let plan = diff.migration_ddl("dfe", "events");

        assert!(plan.has_ddl());
        assert_eq!(plan.ddl_statements.len(), 1);
        assert!(plan.ddl_statements[0].contains("ADD COLUMN `_json`"));
        assert!(plan.ddl_statements[0].contains("Nullable(JSON)"));
    }

    #[test]
    fn test_migration_ddl_with_codec() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        // Table missing _timestamp field
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "LowCardinality(String)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, None);
        let plan = diff.migration_ddl("dfe", "events");

        // Should have ADD COLUMN for _timestamp, _timestamp_received, _source, _raw, _json, _tags
        assert!(plan.has_ddl());

        // Find the _timestamp ADD statement
        let ts_stmt = plan
            .ddl_statements
            .iter()
            .find(|s| s.contains("`_timestamp`"))
            .expect("Should have _timestamp ADD statement");
        assert!(ts_stmt.contains("CODEC(Delta, ZSTD(1))"));
    }

    #[test]
    fn test_migration_ddl_type_change_is_warning() {
        let profile = Profile::from_yaml(MINIMAL_YAML).unwrap();
        let existing: Vec<(&str, &str)> = vec![
            ("_timestamp_load", "DateTime64(3)"),
            ("_timestamp", "DateTime64(3)"),
            ("_uuid", "UUID"),
            ("_org_id", "String"), // Wrong type
            ("_json", "Nullable(JSON)"),
        ];
        let diff = profile.diff_columns("dfe.events", &existing, Some(1));
        let plan = diff.migration_ddl("dfe", "events");

        // Type changes are warnings, not DDL
        assert!(!plan.has_ddl());
        assert!(plan.has_warnings());
        assert!(plan.warnings[0].contains("_org_id"));
        assert!(plan.warnings[0].contains("Manual MODIFY COLUMN"));
    }

    #[test]
    fn test_migration_plan_version_tracking() {
        let profile = Profile::from_yaml(TIMESERIES_YAML).unwrap();
        let existing: Vec<(&str, &str)> = vec![("_uuid", "UUID")];
        let diff = profile.diff_columns("dfe.events", &existing, Some(0));
        let plan = diff.migration_ddl("dfe", "events");

        assert_eq!(plan.from_version, Some(0));
        assert_eq!(plan.to_version, 1);
        assert_eq!(plan.profile, "timeseries");
        assert_eq!(plan.table, "dfe.events");
    }

    #[test]
    fn test_types_compatible_exact() {
        assert!(types_compatible("String", "String"));
        assert!(types_compatible("DateTime64(3)", "DateTime64(3)"));
    }

    #[test]
    fn test_types_compatible_whitespace() {
        assert!(types_compatible("Nullable(JSON)", "Nullable( JSON)"));
        assert!(types_compatible("LowCardinality(String)", "LowCardinality( String )"));
    }

    #[test]
    fn test_types_not_compatible() {
        assert!(!types_compatible("String", "Int32"));
        assert!(!types_compatible("LowCardinality(String)", "String"));
    }
}
