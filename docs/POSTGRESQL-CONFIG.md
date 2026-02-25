# PostgreSQL Configuration Source

Implementation specification for adding PostgreSQL as a configuration source in the hyperi-rustlib config cascade.

## Overview

Add PostgreSQL as a configuration source that sits between hard-coded defaults and file-based config in the cascade priority.

**Updated Cascade (highest to lowest priority):**

1. CLI args
2. ENV vars (`MYAPP_*`)
3. `.env` file
4. Config file (YAML/TOML)
5. **PostgreSQL** ← new layer
6. Hard-coded defaults

Files override PostgreSQL, ENV overrides files, CLI overrides everything.

---

## Design: Option B - Separate Async Layer

PostgreSQL config is loaded asynchronously *before* feeding into config-rs. This avoids sync/async bridging issues since `config::Source::collect()` is synchronous.

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Config Loading                           │
├─────────────────────────────────────────────────────────────────┤
│  1. Load PostgreSQL config (async)                              │
│     ↓                                                           │
│  2. Convert to config::Config or Map<String, Value>             │
│     ↓                                                           │
│  3. Feed into ConfigBuilder as a source                         │
│     ↓                                                           │
│  4. Add file, env, CLI sources (higher priority)                │
│     ↓                                                           │
│  5. Build final Config                                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Dependencies

Add to `Cargo.toml`:

```toml
[dependencies]
# PostgreSQL (async, compile-time checked queries optional)
sqlx = { version = "0.8", features = ["runtime-tokio", "tls-rustls", "postgres", "json"] }

[features]
# Optional: only include PostgreSQL config if needed
config-postgres = ["dep:sqlx"]
```

**Feature flag rationale:** Not all consumers need PostgreSQL config. Make it opt-in to avoid pulling in sqlx for simple deployments.

---

## Database Schema

### Option A: Key-Value Table (Simple)

```sql
CREATE TABLE IF NOT EXISTS config (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    description TEXT,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT
);

-- Index for prefix queries (e.g., all kafka.* keys)
CREATE INDEX idx_config_key_prefix ON config USING btree (key text_pattern_ops);

-- Example data
INSERT INTO config (key, value, description) VALUES
    ('kafka.brokers', '["broker1:9092", "broker2:9092"]', 'Kafka bootstrap servers'),
    ('kafka.group', '"my-consumer-group"', 'Consumer group ID'),
    ('clickhouse.hosts', '["ch1:9000", "ch2:9000"]', 'ClickHouse native protocol hosts'),
    ('buffer.flush_rows', '50000', 'Rows per batch'),
    ('buffer.flush_bytes', '10485760', 'Bytes per batch (10MB)');
```

**Pros:** Simple, flat, easy to query and update.
**Cons:** Nested config requires dot-notation keys, reconstruction on load.

### Option B: Hierarchical JSON Document (Flexible)

```sql
CREATE TABLE IF NOT EXISTS config_documents (
    id          TEXT PRIMARY KEY DEFAULT 'default',
    config      JSONB NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT
);

-- Example data
INSERT INTO config_documents (id, config) VALUES ('default', '{
    "kafka": {
        "brokers": ["broker1:9092", "broker2:9092"],
        "group": "my-consumer-group"
    },
    "clickhouse": {
        "hosts": ["ch1:9000", "ch2:9000"]
    },
    "buffer": {
        "flush_rows": 50000,
        "flush_bytes": 10485760
    }
}');
```

**Pros:** Natural JSON structure, single query, easy to diff/version.
**Cons:** Harder to update individual keys, larger payloads.

### Option C: Hybrid (Recommended)

Use key-value for granular overrides, with support for nested keys via dot notation:

```sql
CREATE TABLE IF NOT EXISTS app_config (
    app_id      TEXT NOT NULL DEFAULT 'default',
    key         TEXT NOT NULL,
    value       JSONB NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT,
    PRIMARY KEY (app_id, key)
);

-- Audit trail (optional)
CREATE TABLE IF NOT EXISTS app_config_history (
    id          BIGSERIAL PRIMARY KEY,
    app_id      TEXT NOT NULL,
    key         TEXT NOT NULL,
    old_value   JSONB,
    new_value   JSONB,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    changed_by  TEXT
);

-- Trigger for audit trail
CREATE OR REPLACE FUNCTION config_audit_trigger()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        INSERT INTO app_config_history (app_id, key, old_value, new_value, changed_by)
        VALUES (OLD.app_id, OLD.key, OLD.value, NEW.value, NEW.updated_by);
    ELSIF TG_OP = 'DELETE' THEN
        INSERT INTO app_config_history (app_id, key, old_value, new_value, changed_by)
        VALUES (OLD.app_id, OLD.key, OLD.value, NULL, current_user);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER config_audit
    AFTER UPDATE OR DELETE ON app_config
    FOR EACH ROW EXECUTE FUNCTION config_audit_trigger();
```

---

## Rust Implementation

### Configuration for PostgreSQL Connection

```rust
// src/config/postgres.rs

use serde::{Deserialize, Serialize};

/// Configuration for PostgreSQL config source
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresConfigSource {
    /// Enable PostgreSQL config source
    pub enabled: bool,

    /// PostgreSQL connection URL
    /// Format: postgres://user:password@host:port/database
    /// Can also use environment variable: MYAPP_CONFIG_POSTGRES_URL
    pub url: Option<String>,

    /// Application ID for multi-tenant config
    /// Default: "default"
    pub app_id: String,

    /// Connection timeout in seconds
    pub connect_timeout_secs: u64,

    /// Query timeout in seconds
    pub query_timeout_secs: u64,

    /// Retry attempts on connection failure
    pub retry_attempts: u32,

    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,

    /// Continue startup if PostgreSQL is unavailable
    /// If true: log warning, use file/env config only
    /// If false: fail startup
    pub optional: bool,
}

impl Default for PostgresConfigSource {
    fn default() -> Self {
        Self {
            enabled: false,
            url: None,
            app_id: "default".to_string(),
            connect_timeout_secs: 5,
            query_timeout_secs: 10,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            optional: true,
        }
    }
}
```

### Config Loader

```rust
// src/config/postgres.rs (continued)

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, warn, info};

/// Loaded configuration from PostgreSQL
#[derive(Debug, Clone)]
pub struct PostgresConfig {
    /// Flat map of dot-notation keys to JSON values
    /// e.g., "kafka.brokers" -> ["broker1:9092"]
    pub values: HashMap<String, serde_json::Value>,
}

impl PostgresConfig {
    /// Load configuration from PostgreSQL
    pub async fn load(source: &PostgresConfigSource) -> Result<Option<Self>, PostgresConfigError> {
        if !source.enabled {
            debug!("PostgreSQL config source disabled");
            return Ok(None);
        }

        let url = match &source.url {
            Some(url) => url.clone(),
            None => {
                if source.optional {
                    debug!("PostgreSQL config URL not configured, skipping");
                    return Ok(None);
                } else {
                    return Err(PostgresConfigError::NotConfigured);
                }
            }
        };

        let pool = Self::connect_with_retry(&url, source).await?;
        let values = Self::query_config(&pool, &source.app_id, source.query_timeout_secs).await?;

        info!(
            keys = values.len(),
            app_id = %source.app_id,
            "Loaded configuration from PostgreSQL"
        );

        Ok(Some(Self { values }))
    }

    async fn connect_with_retry(
        url: &str,
        source: &PostgresConfigSource,
    ) -> Result<PgPool, PostgresConfigError> {
        let mut last_error = None;

        for attempt in 1..=source.retry_attempts {
            match PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(Duration::from_secs(source.connect_timeout_secs))
                .connect(url)
                .await
            {
                Ok(pool) => {
                    debug!(attempt, "Connected to PostgreSQL config database");
                    return Ok(pool);
                }
                Err(e) => {
                    warn!(
                        attempt,
                        max_attempts = source.retry_attempts,
                        error = %e,
                        "Failed to connect to PostgreSQL config database"
                    );
                    last_error = Some(e);

                    if attempt < source.retry_attempts {
                        tokio::time::sleep(Duration::from_millis(source.retry_delay_ms)).await;
                    }
                }
            }
        }

        if source.optional {
            warn!("PostgreSQL config unavailable, continuing with file/env config only");
            Err(PostgresConfigError::Unavailable)
        } else {
            Err(PostgresConfigError::Connection(
                last_error.map(|e| e.to_string()).unwrap_or_default(),
            ))
        }
    }

    async fn query_config(
        pool: &PgPool,
        app_id: &str,
        timeout_secs: u64,
    ) -> Result<HashMap<String, serde_json::Value>, PostgresConfigError> {
        let rows = sqlx::query(
            r#"
            SELECT key, value
            FROM app_config
            WHERE app_id = $1
            ORDER BY key
            "#,
        )
        .bind(app_id)
        .fetch_all(pool)
        .await
        .map_err(|e| PostgresConfigError::Query(e.to_string()))?;

        let mut values = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row.try_get("key")
                .map_err(|e| PostgresConfigError::Query(e.to_string()))?;
            let value: serde_json::Value = row.try_get("value")
                .map_err(|e| PostgresConfigError::Query(e.to_string()))?;
            values.insert(key, value);
        }

        Ok(values)
    }

    /// Convert to a nested HashMap suitable for config-rs
    /// "kafka.brokers" -> {"kafka": {"brokers": [...]}}
    pub fn to_nested(&self) -> HashMap<String, config::Value> {
        let mut root: HashMap<String, serde_json::Value> = HashMap::new();

        for (key, value) in &self.values {
            insert_nested(&mut root, key, value.clone());
        }

        // Convert serde_json::Value to config::Value
        json_to_config_map(&serde_json::Value::Object(
            root.into_iter().collect()
        ))
    }
}

/// Insert a dot-notation key into a nested map
fn insert_nested(
    map: &mut HashMap<String, serde_json::Value>,
    key: &str,
    value: serde_json::Value,
) {
    let parts: Vec<&str> = key.split('.').collect();

    if parts.len() == 1 {
        map.insert(key.to_string(), value);
        return;
    }

    let first = parts[0];
    let rest = parts[1..].join(".");

    let entry = map
        .entry(first.to_string())
        .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));

    if let serde_json::Value::Object(ref mut obj) = entry {
        let mut inner: HashMap<String, serde_json::Value> = obj
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        insert_nested(&mut inner, &rest, value);
        *obj = inner.into_iter().collect();
    }
}

/// Convert serde_json::Value to config::ValueKind
fn json_to_config_value(json: &serde_json::Value) -> config::Value {
    use config::ValueKind;

    let kind = match json {
        serde_json::Value::Null => ValueKind::Nil,
        serde_json::Value::Bool(b) => ValueKind::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ValueKind::I64(i)
            } else if let Some(f) = n.as_f64() {
                ValueKind::Float(f)
            } else {
                ValueKind::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => ValueKind::String(s.clone()),
        serde_json::Value::Array(arr) => {
            ValueKind::Array(arr.iter().map(json_to_config_value).collect())
        }
        serde_json::Value::Object(obj) => {
            ValueKind::Table(
                obj.iter()
                    .map(|(k, v)| (k.clone(), json_to_config_value(v)))
                    .collect(),
            )
        }
    };

    config::Value::new(None, kind)
}

fn json_to_config_map(json: &serde_json::Value) -> HashMap<String, config::Value> {
    match json {
        serde_json::Value::Object(obj) => {
            obj.iter()
                .map(|(k, v)| (k.clone(), json_to_config_value(v)))
                .collect()
        }
        _ => HashMap::new(),
    }
}

/// Errors from PostgreSQL config loading
#[derive(Debug, thiserror::Error)]
pub enum PostgresConfigError {
    #[error("PostgreSQL config source not configured")]
    NotConfigured,

    #[error("PostgreSQL config unavailable (optional, continuing)")]
    Unavailable,

    #[error("PostgreSQL connection error: {0}")]
    Connection(String),

    #[error("PostgreSQL query error: {0}")]
    Query(String),
}
```

### Integration with Config Cascade

```rust
// src/config/loader.rs (modified)

use crate::config::postgres::{PostgresConfig, PostgresConfigSource, PostgresConfigError};

impl Config {
    /// Load configuration with cascade (async version for PostgreSQL support)
    pub async fn load_async(config_path: Option<&str>) -> Result<Self> {
        // Load .env file if present
        let _ = dotenvy::dotenv();

        // Determine PostgreSQL config from env (bootstrap problem)
        let pg_source = Self::load_postgres_source_from_env();

        // Load PostgreSQL config first (async)
        let pg_config = match PostgresConfig::load(&pg_source).await {
            Ok(Some(cfg)) => Some(cfg),
            Ok(None) => None,
            Err(PostgresConfigError::Unavailable) => None,
            Err(e) if pg_source.optional => {
                tracing::warn!(error = %e, "PostgreSQL config failed, continuing without");
                None
            }
            Err(e) => return Err(Error::Config(format!("PostgreSQL config: {}", e))),
        };

        // Build config with cascade
        let mut builder = ConfigBuilder::builder();

        // 6. Hard-coded defaults (lowest priority)
        builder = builder.add_source(config::Config::try_from(&Config::default())?);

        // 5. PostgreSQL config
        if let Some(ref pg) = pg_config {
            builder = builder.add_source(config::Config::try_from(&pg.to_nested())?);
        }

        // 4. Config file
        if let Some(path) = config_path {
            if Path::new(path).exists() {
                builder = builder.add_source(File::new(path, FileFormat::Yaml));
            }
        } else {
            for path in &["config.yaml", "config.yml"] {
                if Path::new(path).exists() {
                    builder = builder.add_source(File::new(path, FileFormat::Yaml));
                    break;
                }
            }
        }

        // 3. .env already loaded via dotenvy

        // 2. Environment variables (highest after CLI)
        builder = builder.add_source(
            Environment::with_prefix("LOADER")
                .separator("_")
                .list_separator(",")
                .with_list_parse_key("kafka.brokers")
                .with_list_parse_key("kafka.topics")
                .with_list_parse_key("clickhouse.hosts")
                .with_list_parse_key("clickhouse.tables")
                .try_parsing(true),
        );

        let config = builder.build()?;
        let result: Config = config.try_deserialize()?;

        Ok(result)
    }

    /// Load PostgreSQL source config from environment
    /// This solves the bootstrap problem: we need to know where PostgreSQL is
    /// before we can load config from it.
    fn load_postgres_source_from_env() -> PostgresConfigSource {
        PostgresConfigSource {
            enabled: std::env::var("LOADER_CONFIG_POSTGRES_ENABLED")
                .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                .unwrap_or(false),
            url: std::env::var("LOADER_CONFIG_POSTGRES_URL").ok(),
            app_id: std::env::var("LOADER_CONFIG_POSTGRES_APP_ID")
                .unwrap_or_else(|_| "default".to_string()),
            connect_timeout_secs: std::env::var("LOADER_CONFIG_POSTGRES_CONNECT_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            query_timeout_secs: std::env::var("LOADER_CONFIG_POSTGRES_QUERY_TIMEOUT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            retry_attempts: std::env::var("LOADER_CONFIG_POSTGRES_RETRY_ATTEMPTS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            retry_delay_ms: std::env::var("LOADER_CONFIG_POSTGRES_RETRY_DELAY_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
            optional: std::env::var("LOADER_CONFIG_POSTGRES_OPTIONAL")
                .map(|v| !v.eq_ignore_ascii_case("false") && v != "0")
                .unwrap_or(true),
        }
    }

    /// Sync load (existing API, no PostgreSQL)
    pub fn load(config_path: Option<&str>) -> Result<Self> {
        // ... existing implementation unchanged ...
    }
}
```

---

## Environment Variables

Bootstrap PostgreSQL connection via environment (solves chicken-and-egg problem):

| Variable | Description | Default |
|----------|-------------|---------|
| `LOADER_CONFIG_POSTGRES_ENABLED` | Enable PostgreSQL config source | `false` |
| `LOADER_CONFIG_POSTGRES_URL` | Connection URL | None (required if enabled) |
| `LOADER_CONFIG_POSTGRES_APP_ID` | Application ID for multi-tenant | `default` |
| `LOADER_CONFIG_POSTGRES_CONNECT_TIMEOUT` | Connection timeout (seconds) | `5` |
| `LOADER_CONFIG_POSTGRES_QUERY_TIMEOUT` | Query timeout (seconds) | `10` |
| `LOADER_CONFIG_POSTGRES_RETRY_ATTEMPTS` | Retry attempts | `3` |
| `LOADER_CONFIG_POSTGRES_RETRY_DELAY_MS` | Delay between retries (ms) | `1000` |
| `LOADER_CONFIG_POSTGRES_OPTIONAL` | Continue if unavailable | `true` |

**Example:**

```bash
export LOADER_CONFIG_POSTGRES_ENABLED=true
export LOADER_CONFIG_POSTGRES_URL="postgres://config_user:secret@config-db:5432/config"
export LOADER_CONFIG_POSTGRES_APP_ID="dfe-loader-prod"
```

---

## Usage Examples

### Basic Setup

```sql
-- Create table
CREATE TABLE app_config (
    app_id      TEXT NOT NULL DEFAULT 'default',
    key         TEXT NOT NULL,
    value       JSONB NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_by  TEXT,
    PRIMARY KEY (app_id, key)
);

-- Insert config for dfe-loader
INSERT INTO app_config (app_id, key, value, description) VALUES
    ('dfe-loader-prod', 'kafka.brokers', '["kafka1:9092", "kafka2:9092", "kafka3:9092"]', 'Production Kafka cluster'),
    ('dfe-loader-prod', 'kafka.group', '"dfe-loader-prod"', 'Consumer group'),
    ('dfe-loader-prod', 'clickhouse.hosts', '["ch1:9000", "ch2:9000"]', 'ClickHouse cluster'),
    ('dfe-loader-prod', 'clickhouse.database', '"events"', 'Target database'),
    ('dfe-loader-prod', 'buffer.flush_rows', '50000', 'Batch size'),
    ('dfe-loader-prod', 'buffer.flush_bytes', '10485760', '10MB batches');
```

### Multi-Tenant Config

```sql
-- Shared base config
INSERT INTO app_config (app_id, key, value) VALUES
    ('base', 'buffer.flush_rows', '10000'),
    ('base', 'buffer.flush_age_secs', '5');

-- Tenant-specific overrides
INSERT INTO app_config (app_id, key, value) VALUES
    ('tenant-acme', 'kafka.group', '"acme-loader"'),
    ('tenant-acme', 'clickhouse.database', '"acme_events"');

-- Query with inheritance (application layer)
-- Load 'base' first, then 'tenant-acme' to override
```

### Override with File

PostgreSQL has `buffer.flush_rows = 50000`, but local `config.yaml`:

```yaml
buffer:
  flush_rows: 100000  # Override PostgreSQL value
```

Result: `flush_rows = 100000` (file wins)

### Override with ENV

```bash
export LOADER_BUFFER_FLUSH_ROWS=200000
```

Result: `flush_rows = 200000` (ENV wins over file and PostgreSQL)

---

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_nested_single_level() {
        let mut map = HashMap::new();
        insert_nested(&mut map, "key", serde_json::json!("value"));
        assert_eq!(map.get("key"), Some(&serde_json::json!("value")));
    }

    #[test]
    fn test_insert_nested_multi_level() {
        let mut map = HashMap::new();
        insert_nested(&mut map, "kafka.brokers", serde_json::json!(["a:9092"]));
        insert_nested(&mut map, "kafka.group", serde_json::json!("my-group"));

        let kafka = map.get("kafka").unwrap().as_object().unwrap();
        assert_eq!(kafka.get("brokers"), Some(&serde_json::json!(["a:9092"])));
        assert_eq!(kafka.get("group"), Some(&serde_json::json!("my-group")));
    }

    #[test]
    fn test_insert_nested_deep() {
        let mut map = HashMap::new();
        insert_nested(&mut map, "a.b.c.d", serde_json::json!(42));

        let a = map.get("a").unwrap().as_object().unwrap();
        let b = a.get("b").unwrap().as_object().unwrap();
        let c = b.get("c").unwrap().as_object().unwrap();
        assert_eq!(c.get("d"), Some(&serde_json::json!(42)));
    }

    #[test]
    fn test_postgres_source_default() {
        let source = PostgresConfigSource::default();
        assert!(!source.enabled);
        assert!(source.optional);
        assert_eq!(source.app_id, "default");
    }

    #[test]
    fn test_json_to_config_value_primitives() {
        assert!(matches!(
            json_to_config_value(&serde_json::json!(true)).kind,
            config::ValueKind::Boolean(true)
        ));
        assert!(matches!(
            json_to_config_value(&serde_json::json!(42)).kind,
            config::ValueKind::I64(42)
        ));
        assert!(matches!(
            json_to_config_value(&serde_json::json!("hello")).kind,
            config::ValueKind::String(s) if s == "hello"
        ));
    }
}
```

### Integration Tests (requires PostgreSQL)

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use sqlx::PgPool;

    async fn setup_test_db(pool: &PgPool) {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS app_config (
                app_id TEXT NOT NULL DEFAULT 'default',
                key TEXT NOT NULL,
                value JSONB NOT NULL,
                PRIMARY KEY (app_id, key)
            )
            "#,
        )
        .execute(pool)
        .await
        .unwrap();

        sqlx::query("DELETE FROM app_config WHERE app_id = 'test'")
            .execute(pool)
            .await
            .unwrap();

        sqlx::query(
            r#"
            INSERT INTO app_config (app_id, key, value) VALUES
                ('test', 'kafka.brokers', '["localhost:9092"]'),
                ('test', 'buffer.flush_rows', '5000')
            "#,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    #[ignore] // Requires PostgreSQL
    async fn test_load_from_postgres() {
        let url = std::env::var("TEST_POSTGRES_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());

        let pool = PgPool::connect(&url).await.unwrap();
        setup_test_db(&pool).await;

        let source = PostgresConfigSource {
            enabled: true,
            url: Some(url),
            app_id: "test".to_string(),
            ..Default::default()
        };

        let config = PostgresConfig::load(&source).await.unwrap().unwrap();

        assert_eq!(
            config.values.get("kafka.brokers"),
            Some(&serde_json::json!(["localhost:9092"]))
        );
        assert_eq!(
            config.values.get("buffer.flush_rows"),
            Some(&serde_json::json!(5000))
        );
    }

    #[tokio::test]
    async fn test_disabled_returns_none() {
        let source = PostgresConfigSource {
            enabled: false,
            ..Default::default()
        };

        let result = PostgresConfig::load(&source).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_optional_unavailable_returns_none() {
        let source = PostgresConfigSource {
            enabled: true,
            url: Some("postgres://invalid:invalid@localhost:59999/nonexistent".to_string()),
            optional: true,
            retry_attempts: 1,
            connect_timeout_secs: 1,
            ..Default::default()
        };

        let result = PostgresConfig::load(&source).await;
        // Should return Unavailable error which is handled gracefully
        assert!(matches!(result, Err(PostgresConfigError::Unavailable) | Ok(None)));
    }
}
```

---

## Future Enhancements

### Hot Reload (Watch for Changes)

```rust
impl PostgresConfig {
    /// Watch for config changes using PostgreSQL LISTEN/NOTIFY
    pub async fn watch(
        pool: &PgPool,
        app_id: &str,
        tx: tokio::sync::watch::Sender<Self>,
    ) -> Result<(), PostgresConfigError> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        listener.listen(&format!("config_changed_{}", app_id)).await?;

        loop {
            let notification = listener.recv().await?;
            tracing::info!(payload = %notification.payload(), "Config change notification");

            // Reload config
            let values = Self::query_config(pool, app_id, 10).await?;
            let _ = tx.send(Self { values });
        }
    }
}

-- PostgreSQL trigger to notify on change
CREATE OR REPLACE FUNCTION notify_config_change()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('config_changed_' || NEW.app_id, NEW.key);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER config_change_notify
    AFTER INSERT OR UPDATE ON app_config
    FOR EACH ROW EXECUTE FUNCTION notify_config_change();
```

### Config Versioning

```sql
ALTER TABLE app_config ADD COLUMN version INTEGER NOT NULL DEFAULT 1;

-- Load specific version
SELECT key, value FROM app_config
WHERE app_id = $1 AND version <= $2
ORDER BY key, version DESC;
```

### Encryption for Sensitive Values

```rust
// Decrypt sensitive values after loading
impl PostgresConfig {
    pub fn decrypt_sensitive(&mut self, key: &[u8]) -> Result<(), CryptoError> {
        for (k, v) in &mut self.values {
            if k.contains("password") || k.contains("secret") || k.contains("key") {
                if let Some(encrypted) = v.as_str() {
                    *v = serde_json::Value::String(decrypt(encrypted, key)?);
                }
            }
        }
        Ok(())
    }
}
```

---

## Migration Checklist

1. [ ] Add `sqlx` dependency to hyperi-rustlib `Cargo.toml` (feature-gated)
2. [ ] Create `src/config/postgres.rs` module
3. [ ] Add `PostgresConfigError` to error types
4. [ ] Implement `PostgresConfigSource` struct
5. [ ] Implement `PostgresConfig::load()` async function
6. [ ] Implement `to_nested()` conversion for config-rs
7. [ ] Add `Config::load_async()` to loader
8. [ ] Add env var parsing for bootstrap config
9. [ ] Write unit tests for nested key insertion
10. [ ] Write integration tests (optional, requires PostgreSQL)
11. [ ] Document env vars in README
12. [ ] Create SQL migration scripts

---

## References

- [config-rs documentation](https://docs.rs/config/)
- [sqlx documentation](https://docs.rs/sqlx/)
- [PostgreSQL LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html)
