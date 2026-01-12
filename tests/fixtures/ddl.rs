//! ClickHouse DDL template builders

/// Builder for ClickHouse table DDL statements
#[derive(Debug, Clone)]
pub struct DdlBuilder {
    database: String,
    table: String,
    engine: String,
    columns: Vec<(String, String, String)>, // (name, type, default/comment)
    order_by: Vec<String>,
    partition_by: Option<String>,
}

impl DdlBuilder {
    /// Create a new DDL builder
    pub fn new(database: &str, table: &str) -> Self {
        Self {
            database: database.to_string(),
            table: table.to_string(),
            engine: "MergeTree()".to_string(),
            columns: Vec::new(),
            order_by: vec!["timestamp".to_string()],
            partition_by: None,
        }
    }

    /// Set the table engine
    pub fn engine<S: Into<String>>(mut self, engine: S) -> Self {
        self.engine = engine.into();
        self
    }

    /// Add a column
    pub fn column<S: Into<String>>(mut self, name: S, col_type: S, extra: S) -> Self {
        self.columns.push((name.into(), col_type.into(), extra.into()));
        self
    }

    /// Add timestamp column (DateTime64(3))
    pub fn with_timestamp(self, name: &str, nullable: bool, default: Option<&str>) -> Self {
        let col_type = if nullable {
            "Nullable(DateTime64(3))".to_string()
        } else {
            "DateTime64(3)".to_string()
        };

        let extra = match default {
            Some(d) => format!("DEFAULT {}", d),
            None => String::new(),
        };

        self.column(name, col_type, extra)
    }

    /// Add string column
    pub fn with_string(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(String)".to_string()
        } else {
            "String".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add UUID column
    pub fn with_uuid(self, name: &str, nullable: bool, default: Option<&str>) -> Self {
        let col_type = if nullable {
            "Nullable(UUID)".to_string()
        } else {
            "UUID".to_string()
        };

        let extra = match default {
            Some(d) => format!("DEFAULT {}", d),
            None => String::new(),
        };

        self.column(name, col_type, extra)
    }

    /// Add UInt32 column
    pub fn with_uint32(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(UInt32)".to_string()
        } else {
            "UInt32".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add UInt64 column
    pub fn with_uint64(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(UInt64)".to_string()
        } else {
            "UInt64".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add Int32 column
    pub fn with_int32(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(Int32)".to_string()
        } else {
            "Int32".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add Int64 column
    pub fn with_int64(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(Int64)".to_string()
        } else {
            "Int64".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add Float64 column
    pub fn with_float64(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(Float64)".to_string()
        } else {
            "Float64".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add Boolean column
    pub fn with_bool(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(Bool)".to_string()
        } else {
            "Bool".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Add JSON column
    pub fn with_json(self, name: &str, nullable: bool) -> Self {
        let col_type = if nullable {
            "Nullable(JSON)".to_string()
        } else {
            "JSON".to_string()
        };
        self.column(name, col_type, "")
    }

    /// Set ORDER BY clause
    pub fn order_by(mut self, fields: Vec<&str>) -> Self {
        self.order_by = fields.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set PARTITION BY clause
    pub fn partition_by<S: Into<String>>(mut self, expr: S) -> Self {
        self.partition_by = Some(expr.into());
        self
    }

    /// Build the CREATE TABLE statement
    pub fn build(self) -> String {
        let mut ddl = format!("CREATE TABLE IF NOT EXISTS {}.{} (\n", self.database, self.table);

        // Add columns
        for (i, (name, col_type, extra)) in self.columns.iter().enumerate() {
            ddl.push_str("  ");
            ddl.push_str(name);
            ddl.push(' ');
            ddl.push_str(col_type);
            if !extra.is_empty() {
                ddl.push(' ');
                ddl.push_str(extra);
            }
            if i < self.columns.len() - 1 {
                ddl.push(',');
            }
            ddl.push('\n');
        }

        ddl.push_str(")\n");
        ddl.push_str(&format!("ENGINE = {}\n", self.engine));

        // Add ORDER BY
        if !self.order_by.is_empty() {
            ddl.push_str(&format!("ORDER BY ({})\n", self.order_by.join(", ")));
        }

        // Add PARTITION BY
        if let Some(partition) = &self.partition_by {
            ddl.push_str(&format!("PARTITION BY {}\n", partition));
        }

        ddl
    }

    /// Build the DROP TABLE statement
    pub fn build_drop(&self) -> String {
        format!("DROP TABLE IF EXISTS {}.{}", self.database, self.table)
    }
}

/// Standard event table DDL (Common Header v2 schema)
pub fn event_table_ddl(database: &str, table: &str) -> String {
    DdlBuilder::new(database, table)
        .with_timestamp("timestamp", false, None)
        .with_timestamp("timestamp_load", false, Some("now64(3)"))
        .with_uuid("_uuid", false, Some("generateUUIDv7()"))
        .with_string("_org_id", false)
        .with_string("logoriginal", true)
        .with_json("logjson", true)
        .with_json("_tags", true)
        .engine("MergeTree()")
        .order_by(vec!["timestamp", "_org_id"])
        .partition_by("toYYYYMM(timestamp)")
        .build()
}

/// RLS test table DDL
pub fn rls_table_ddl(database: &str, table: &str) -> String {
    DdlBuilder::new(database, table)
        .with_timestamp("timestamp", false, None)
        .with_string("_org_id", false)
        .with_string("action", false)
        .with_uint32("user_id", false)
        .engine("MergeTree()")
        .order_by(vec!["timestamp"])
        .build()
}

/// Auth events table DDL
pub fn auth_table_ddl(database: &str, table: &str) -> String {
    DdlBuilder::new(database, table)
        .with_timestamp("timestamp", false, None)
        .with_string("_org_id", false)
        .with_string("event_category", false)
        .with_string("action", false)
        .with_uint64("user_id", false)
        .with_string("ip_address", true)
        .with_string("user_agent", true)
        .with_bool("success", false)
        .engine("MergeTree()")
        .order_by(vec!["timestamp", "_org_id", "user_id"])
        .partition_by("toYYYYMM(timestamp)")
        .build()
}

/// API events table DDL
pub fn api_table_ddl(database: &str, table: &str) -> String {
    DdlBuilder::new(database, table)
        .with_timestamp("timestamp", false, None)
        .with_string("_org_id", false)
        .with_string("event_category", false)
        .with_string("endpoint", false)
        .with_string("method", false)
        .with_uint32("status_code", false)
        .with_float64("duration_ms", false)
        .with_uint64("bytes_sent", false)
        .with_uint64("bytes_received", false)
        .engine("MergeTree()")
        .order_by(vec!["timestamp", "_org_id"])
        .partition_by("toYYYYMM(timestamp)")
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ddl_builder() {
        let ddl = DdlBuilder::new("test", "events")
            .with_timestamp("timestamp", false, None)
            .with_string("org_id", false)
            .with_uint32("count", true)
            .order_by(vec!["timestamp"])
            .build();

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS test.events"));
        assert!(ddl.contains("timestamp DateTime64(3)"));
        assert!(ddl.contains("org_id String"));
        assert!(ddl.contains("count Nullable(UInt32)"));
        assert!(ddl.contains("ORDER BY (timestamp)"));
    }

    #[test]
    fn test_event_table_ddl() {
        let ddl = event_table_ddl("common", "events");

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS common.events"));
        assert!(ddl.contains("timestamp DateTime64(3)"));
        assert!(ddl.contains("timestamp_load DateTime64(3) DEFAULT now64(3)"));
        assert!(ddl.contains("_uuid UUID DEFAULT generateUUIDv7()"));
        assert!(ddl.contains("_org_id String"));
        assert!(ddl.contains("logoriginal Nullable(String)"));
        assert!(ddl.contains("logjson Nullable(JSON)"));
        assert!(ddl.contains("_tags Nullable(JSON)"));
        assert!(ddl.contains("ORDER BY (timestamp, _org_id)"));
        assert!(ddl.contains("PARTITION BY toYYYYMM(timestamp)"));
    }

    #[test]
    fn test_rls_table_ddl() {
        let ddl = rls_table_ddl("test", "rls_events");

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS test.rls_events"));
        assert!(ddl.contains("timestamp DateTime64(3)"));
        assert!(ddl.contains("_org_id String"));
        assert!(ddl.contains("action String"));
        assert!(ddl.contains("user_id UInt32"));
        assert!(ddl.contains("ORDER BY (timestamp)"));
    }

    #[test]
    fn test_auth_table_ddl() {
        let ddl = auth_table_ddl("common", "auth_events");

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS common.auth_events"));
        assert!(ddl.contains("user_id UInt64"));
        assert!(ddl.contains("ip_address Nullable(String)"));
        assert!(ddl.contains("success Bool"));
        assert!(ddl.contains("ORDER BY (timestamp, _org_id, user_id)"));
    }

    #[test]
    fn test_api_table_ddl() {
        let ddl = api_table_ddl("common", "api_events");

        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS common.api_events"));
        assert!(ddl.contains("endpoint String"));
        assert!(ddl.contains("method String"));
        assert!(ddl.contains("status_code UInt32"));
        assert!(ddl.contains("duration_ms Float64"));
        assert!(ddl.contains("bytes_sent UInt64"));
    }

    #[test]
    fn test_drop_table() {
        let builder = DdlBuilder::new("test", "events");
        let drop = builder.build_drop();

        assert_eq!(drop, "DROP TABLE IF EXISTS test.events");
    }

    #[test]
    fn test_partition_by() {
        let ddl = DdlBuilder::new("test", "events")
            .with_timestamp("timestamp", false, None)
            .partition_by("toYYYYMMDD(timestamp)")
            .build();

        assert!(ddl.contains("PARTITION BY toYYYYMMDD(timestamp)"));
    }
}
