# DFE-Loader Development & Testing Guide

Developer guide for testing and running dfe-loader locally or against remote infrastructure.

## Prerequisites

- Rust toolchain (1.75+)
- Docker (for local ClickHouse/Kafka)
- MaxMind GeoIP databases (optional, for enrichment)
- Access to k8s.tyrell.com.au (optional, for remote testing)

## Building

```bash
cd /projects/dfe-loader

# Development build (faster compilation)
cargo build

# Release build (optimized, ~2min)
cargo build --release

# Binary locations (after build):
# - Debug: target/debug/dfe-loader
# - Release: target/release/dfe-loader
```

## Quick Start

### 1. Start Local Infrastructure (Docker)

```bash
# Start ClickHouse
docker run -d --name clickhouse \
  -p 9000:9000 -p 8123:8123 \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD= \
  clickhouse/clickhouse-server:25.12

# Start Kafka (Redpanda - lighter weight)
docker run -d --name redpanda \
  -p 9092:9092 \
  docker.redpanda.com/redpandadata/redpanda:latest \
  redpanda start --smp 1 --memory 512M --overprovisioned \
  --kafka-addr PLAINTEXT://0.0.0.0:9092 \
  --advertise-kafka-addr PLAINTEXT://localhost:9092
```

### 2. Create Config File

```bash
cp config.dev.yaml config.yaml
# Edit config.yaml as needed
```

### 3. Run the Loader

```bash
# Validate config
./target/release/dfe-loader --config config.yaml --validate

# Run with text logging (development)
./target/release/dfe-loader --config config.yaml --log-level debug --log-format text

# Run with JSON logging (production)
./target/release/dfe-loader --config config.yaml --log-level info --log-format json
```

## Configuration

### Config File Locations

The loader searches for config in this order:

1. Path specified by `--config` or `DFE_LOADER_CONFIG` env var
2. `config.yaml` in current directory
3. `config.yml` in current directory

### Environment Variables

All config values can be overridden via environment variables with `DFE_LOADER` prefix.
Nested config uses `__` (double underscore) as separator:

```bash
# Kafka
export DFE_LOADER__KAFKA__BROKERS=localhost:9092
export DFE_LOADER__KAFKA__GROUP_ID=my-consumer-group
export DFE_LOADER__KAFKA__TOPICS=events,logs

# ClickHouse
export DFE_LOADER__CLICKHOUSE__URL=http://localhost:8123
export DFE_LOADER__CLICKHOUSE__DATABASE=default
export DFE_LOADER__CLICKHOUSE__USERNAME=default
export DFE_LOADER__CLICKHOUSE__PASSWORD=secret

# Logging (CLI-level env vars)
export DFE_LOADER_LOG_LEVEL=debug
export DFE_LOADER_LOG_FORMAT=text
```

### Config File Reference

```yaml
# Kafka Configuration
kafka:
  brokers:
    - localhost:9092
  group: dfe-loader
  topics:
    - events
  client_id: dfe-loader

  # SASL Authentication
  sasl:
    enabled: true
    # Mechanism: none, plain, scram_sha_256, scram_sha_512, oauthbearer, aws_msk_iam
    mechanism: scram_sha_512
    username: loader
    password: secret

  # TLS Configuration
  tls:
    enabled: false
    ca_cert_file: /certs/ca.pem
    cert_file: /certs/client.pem      # For mTLS
    key_file: /certs/client-key.pem   # For mTLS
    skip_verify: false

# ClickHouse Configuration
clickhouse:
  hosts:
    - localhost:9000
  database: default
  username: default
  password: ""
  protocol: http  # HTTP with JSONEachRow (default)

  tls:
    enabled: false
    ca_cert_file: /certs/ca.pem
    skip_verify: false

# Message Routing
routing:
  # Fields to extract database name (first match wins)
  db_fields:
    - org_id
  # Fields to extract table name
  table_fields:
    - event_category
    - tags.event_category
  default_db: dfe
  default_table: default

  # Source value to table name mapping
  source_to_table:
    auth: auth_events
    api: api_events

  # Dead Letter Queue
  dlq:
    enabled: true
    topic_suffix: ".dlq"

# Buffer Settings
buffer:
  flush_bytes: 1048576    # 1MB
  flush_rows: 10000
  flush_age_secs: 5

# Memory Management
memory:
  limit_bytes: 0          # 0 = auto (67% available)
  pressure_threshold: 0.8

# Metrics
metrics:
  enabled: true
  address: "0.0.0.0:9090"

# Logging
logging:
  level: info    # trace, debug, info, warn, error
  format: json   # json, text

# Timestamp Validation
timestamp_dq:
  enabled: true
  max_future_seconds: 600
  invalid_action: replace_with_now
  correct_known_bad: true

# Field Sanitization
field_sanitization:
  strip_at_prefix: true
  handle_numeric_prefix: true
  numeric_prefix: "col_"
  collapse_underscores: true
  trim_underscores: true
  collision_strategy: last_wins

# Metadata Injection
metadata:
  inject_timestamp_load: true
  extract_timestamp_collector: true
  collector_timestamp_path: "tags.collector.timestamp"

# Schema Caching
schema:
  cache_ttl_secs: 300
  refresh_on_error: true

# Auto-Initialization (creates topics/tables on startup)
auto_init:
  enabled: true
  create_topics: true
  create_database: true
  create_table: true
  create_text_index: true
  topic_partitions: 3
  topic_replication_factor: 1
```

## Kafka Authentication

### Design Decision: SASL-SCRAM as Default

SASL-SCRAM-SHA-512 is the standard mechanism for all production Kafka deployments.
It works identically across every major Kafka platform with no code changes:

| Platform | SASL-SCRAM support | Notes |
|----------|--------------------|-------|
| Apache Kafka (self-managed) | Native | Standard since Kafka 0.10.2 |
| AutoMQ | Native | Drop-in Kafka replacement |
| AWS MSK | Native | SCRAM via IAM-managed secrets |
| Confluent Cloud | Native | Standard credential type |
| Redpanda | Native | Full compatibility |
| Strimzi (K8s) | Native | KafkaUser + ExternalSecret |

Certificate-based auth (mTLS) and AWS IAM have high variance between platforms
and require platform-specific implementations — avoid them for cross-platform workloads.

### Transport Security

| Scenario | Protocol | When to use |
|----------|----------|-------------|
| External / internet-facing | `SASL_SSL` | Production, external NodePorts, MSK, Confluent Cloud |
| Internal K8s (pod-to-pod) | `SASL_PLAINTEXT` | Within a trusted K8s namespace |
| Local dev only | `PLAINTEXT` | No auth, no TLS — never in production |

TLS for the external Kafka listener uses the HyperSec PKI (DevEx) or your cloud
provider's certificate. The `tls.enabled: true` config uses the system trust store
by default — no `ca_cert_file` required when the CA is installed system-wide.

### Config Reference

```yaml
kafka:
  brokers:
    - kafka.example.com:9094  # External SASL_SSL bootstrap
  sasl:
    enabled: true
    mechanism: scram_sha_512  # Works for Apache Kafka, AutoMQ, MSK, Confluent Cloud
    username: dfe-loader
    password: "${KAFKA_PASSWORD}"
  tls:
    enabled: true             # SASL_SSL — required for external listeners
    # ca_cert_file: /certs/ca.pem  # Only if CA not in system trust store
```

For internal K8s deployment, omit `tls` or set `enabled: false` and use port 9095
(`SASL_PLAINTEXT`).

---

## DevEx Test Environment (devex.hyperi.io)

Pre-configured test environment. Full connection reference:
`/projects/hyperi-infra/docs/DEVEX-KAFKA-CH-CONNECTIONS.md`

### Connection Details

| Service | Host | Port | Protocol |
|---------|------|------|----------|
| ClickHouse HTTP | clickhouse.devex.hyperi.io | 8123 | HTTP |
| ClickHouse Native | clickhouse.devex.hyperi.io | 9000 | TCP |
| Kafka (external) | kafka.devex.hyperi.io | 32089 | SASL_SSL |
| Kafka (internal, K8s only) | kafka-kafka-bootstrap.kafka.svc | 9095 | SASL_PLAINTEXT |

The Kafka external listener uses per-broker NodePorts for correct partition-leader routing:
bootstrap `32089`, brokers `k8s-2:32090`, `k8s-1:32091`, `k8s-3:32092`.

### Credentials

```bash
# ClickHouse
CLICKHOUSE_HOST=clickhouse.devex.hyperi.io
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=<see .env>

# Kafka — dfe-loader user (topic CRUD, produce, consume)
KAFKA_BROKERS=kafka.devex.hyperi.io:32089
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-512
KAFKA_SASL_USER=dfe-loader
KAFKA_SASL_PASSWORD=<see .env or OpenBao kv/infrastructure/kafka>
```

The HyperSec Root CA is installed system-wide on devex VMs — no `ca_cert_file` needed.

### Testing Kafka Connection

```bash
# List topics (SASL_SSL — external listener)
kcat -b kafka.devex.hyperi.io:32089 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=dfe-loader \
  -X sasl.password=<password> \
  -L

# Produce test message
echo '{"_source":"test","message":"hello"}' | \
kcat -b kafka.devex.hyperi.io:32089 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=dfe-loader \
  -X sasl.password=<password> \
  -P -t events_land

# Consume from beginning
kcat -b kafka.devex.hyperi.io:32089 \
  -X security.protocol=SASL_SSL \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=dfe-loader \
  -X sasl.password=<password> \
  -C -t events_land -o beginning -e
```

### Testing ClickHouse Connection

```bash
# Via HTTP
curl "http://clickhouse.devex.hyperi.io:8123/?user=default&password=<password>" \
  --data "SELECT version()"

# Via clickhouse-client
clickhouse-client \
  --host clickhouse.devex.hyperi.io \
  --port 9000 \
  --user default \
  --password <password> \
  --query "SELECT version()"
```

## CLI Reference

```
dfe-loader [OPTIONS]

Options:
  -c, --config <CONFIG>              Path to configuration file [env: DFE_LOADER_CONFIG=]
      --log-level <LOG_LEVEL>        Log level (trace, debug, info, warn, error) [default: info]
      --log-format <LOG_FORMAT>      Log format (json, text) [default: json]
      --validate                     Validate config and exit
      --print-config                 Print effective config and exit
      --metrics-addr <METRICS_ADDR>  Metrics server bind address [default: 0.0.0.0:9090]
  -h, --help                         Print help
  -V, --version                      Print version
```

### Common Commands

```bash
# Validate configuration
./target/release/dfe-loader --config config.yaml --validate

# Print merged config (all sources combined)
./target/release/dfe-loader --config config.yaml --print-config

# Run with verbose logging
./target/release/dfe-loader --config config.yaml --log-level debug --log-format text

# Run with custom metrics port
./target/release/dfe-loader --config config.yaml --metrics-addr 0.0.0.0:8080
```

## ClickHouse Schema

The loader auto-creates tables using the common schema. See [schemas/common_table.sql](../schemas/common_table.sql).

### Common Header Fields

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `_timestamp_load` | DateTime64(3) | Generated | Load time (ClickHouse DEFAULT) |
| `_timestamp` | DateTime64(3) | `@source: timestamp \| now()` | Event time |
| `_timestamp_received` | DateTime64(3) | `@source: timestamp_received` | Receive time (nullable) |
| `_uuid` | UUID | Generated | UUIDv7 (ClickHouse DEFAULT) |
| `_org_id` | LowCardinality(String) | `@source: org_id` | Organization ID (RLS) |
| `_raw` | String | `@captured: raw_payload` | Original raw data as received (per-table configurable, nullable) |
| `_json` | JSON | `@captured: raw_payload as JSON` | Complete message as native JSON (structured path-based queries) |
| `_tags` | JSON | `@source: first(tags/_tags/meta)` | Metadata/tags |

### Manual Table Creation

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS benchmark;

-- Create table (MergeTree for single-node)
CREATE TABLE IF NOT EXISTS benchmark.events
(
    `_timestamp_load` DateTime64(3) DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    `_timestamp` DateTime64(3) CODEC(Delta, ZSTD(1)),
    `_timestamp_received` Nullable(DateTime64(3)) CODEC(Delta, ZSTD(1)),
    `_uuid` UUID DEFAULT generateUUIDv7(),
    `_org_id` LowCardinality(String) CODEC(ZSTD(1)),
    `_source` LowCardinality(String) CODEC(ZSTD(1)),
    `_raw` Nullable(String) CODEC(ZSTD(3)),
    `_json` Nullable(JSON) CODEC(ZSTD(3)),
    `_tags` Nullable(JSON) CODEC(ZSTD(3)),
    INDEX idx_timestamp _timestamp TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY (_org_id, _timestamp_load, _uuid)
PARTITION BY (toYYYYMM(_timestamp_load), _org_id)
SETTINGS index_granularity = 8192;

-- Add full-text search index (ClickHouse 25.1+, tested with 25.12)
ALTER TABLE benchmark.events ADD INDEX idx_raw _raw TYPE full_text(0) GRANULARITY 1;
```

## Message Routing

Messages are routed to `{database}.{table}` based on field values.

### Routing Configuration

```yaml
routing:
  # Extract database from these fields (first match wins)
  db_fields:
    - org_id
    - tenant_id

  # Extract table from these fields
  table_fields:
    - event_category
    - tags.event.category

  # Defaults if no match
  default_db: dfe
  default_table: default

  # Optional: map values to table names
  source_to_table:
    auth: auth_events
    api: api_events
    error: error_events
```

### Example Message Routing

```json
// Input message
{
  "org_id": "acme",
  "event_category": "auth",
  "user": "alice",
  "action": "login"
}

// Routed to: acme.auth_events
// (org_id -> database, event_category -> auth -> auth_events via mapping)
```

### Shared Schema Mode (Default)

By default, all messages go to `{default_db}.{table}` regardless of org_id:

```yaml
routing:
  db_fields: []            # Empty = shared schema
  default_db: dfe          # All data goes here
  table_fields:
    - event_category
```

This enables multi-tenant RLS using the `_org_id` column.

## Enrichment Modules

The loader supports IP-based enrichment during transformation.

### GeoIP Enrichment

Uses MaxMind MMDB databases for geographic lookup.

**Setup:**

```bash
# Download MaxMind databases (requires license)
# Set in .env or environment:
export MAXMIND_ACCOUNT_ID=123456
export MAXMIND_LICENSE_KEY=your-license-key

# Or specify paths directly:
export GEOIP_CITY_DB=/path/to/GeoLite2-City.mmdb
export GEOIP_ASN_DB=/path/to/GeoLite2-ASN.mmdb
```

**Available Fields:**

```sql
-- @computed: geoip(client_ip).country_code
`geo_country` LowCardinality(String),

-- @computed: geoip(client_ip).city
`geo_city` String,

-- @computed: geoip(client_ip).latitude
`geo_lat` Float64,

-- @computed: geoip(client_ip).longitude
`geo_lon` Float64,

-- @computed: geoip(client_ip).asn
`geo_asn` UInt32,

-- @computed: geoip(client_ip).asn_org
`geo_asn_org` String,
```

### Reputation Enrichment

IP reputation checking against threat intelligence feeds.

**Available Fields:**

```sql
-- @computed: reputation(client_ip).is_vpn
`is_vpn` Bool,

-- @computed: reputation(client_ip).is_tor
`is_tor` Bool,

-- @computed: reputation(client_ip).is_proxy
`is_proxy` Bool,

-- @computed: reputation(client_ip).threat_type
`threat_type` LowCardinality(String),

-- @computed: reputation(client_ip).abuse_score
`abuse_score` UInt8,
```

### Risk Scoring

Composite risk score from multiple enrichment sources.

**Available Fields:**

```sql
-- @computed: risk(client_ip).score
`risk_score` UInt8,

-- @computed: risk(client_ip).level
`risk_level` LowCardinality(String),  -- minimal/low/medium/high/critical

-- @computed: risk(client_ip).factors
`risk_factors` Array(String),
```

See [docs/DDL-EXPRESSION.md](DDL-EXPRESSION.md) for the complete expression language reference.

## Metrics

Prometheus metrics are exposed at the configured address (default: `:9090`).

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `loader_messages_received_total` | Counter | Total messages consumed from Kafka |
| `loader_messages_processed_total` | Counter | Messages successfully processed |
| `loader_messages_dlq_total` | Counter | Messages sent to DLQ |
| `loader_rows_inserted_total` | Counter | Rows inserted into ClickHouse |
| `loader_batches_flushed_total` | Counter | Batch flushes to ClickHouse |
| `loader_buffer_bytes` | Gauge | Current buffer memory usage |
| `loader_kafka_lag` | GaugeVec | Consumer lag by topic/partition |
| `loader_insert_latency_seconds` | Histogram | ClickHouse insert latency |
| `loader_kafka_offsets_committed_total` | Counter | Kafka offsets committed |

### Viewing Metrics

```bash
# While loader is running
curl http://localhost:9090/metrics

# Example output
loader_messages_received_total 12345
loader_rows_inserted_total 12340
loader_buffer_bytes 524288
```

## Testing

### Run Tests

```bash
# All library tests
cargo test --lib

# Integration tests (requires ClickHouse/Kafka)
cargo test --test '*'

# Specific test
cargo test test_transformer_basic

# With output
cargo test -- --nocapture
```

### Test Message Producer

```bash
# Produce test events to Kafka
for i in {1..100}; do
  echo "{\"org_id\":\"test\",\"event_category\":\"auth\",\"action\":\"login\",\"user\":\"user$i\",\"timestamp\":\"$(date -Iseconds)\"}"
done | kcat -b localhost:9092 -P -t events
```

### Verify Data in ClickHouse

```bash
# Count rows
clickhouse-client --query "SELECT count() FROM benchmark.events"

# Recent events
clickhouse-client --query "
  SELECT _org_id, _timestamp, _json
  FROM benchmark.events
  ORDER BY _timestamp_load DESC
  LIMIT 10
"

# Events by org
clickhouse-client --query "
  SELECT _org_id, count()
  FROM benchmark.events
  GROUP BY _org_id
"
```

## Troubleshooting

### Common Issues

**Config validation fails:**

```bash
# Check config syntax
./target/release/dfe-loader --config config.yaml --validate

# Print effective config
./target/release/dfe-loader --config config.yaml --print-config
```

**Kafka connection issues:**

```bash
# Test connectivity
kcat -b localhost:9092 -L

# Check SASL
kcat -b localhost:9092 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=user \
  -X sasl.password=pass \
  -L
```

**ClickHouse connection issues:**

```bash
# Test connectivity
clickhouse-client --host localhost --port 9000 --query "SELECT 1"

# Check TLS
clickhouse-client --host localhost --port 9440 --secure --query "SELECT 1"
```

**High memory usage:**

```yaml
# Reduce buffer sizes
buffer:
  flush_bytes: 262144     # 256KB
  flush_rows: 1000

memory:
  limit_bytes: 67108864   # 64MB fixed limit
```

**Slow inserts:**

```yaml
# Increase batch sizes
buffer:
  flush_bytes: 4194304    # 4MB
  flush_rows: 50000
  flush_age_secs: 10
```

### Debug Logging

```bash
# Full trace logging (very verbose)
./target/release/dfe-loader --config config.yaml --log-level trace --log-format text

# Debug specific modules
RUST_LOG=dfe_loader::transform=debug ./target/release/dfe-loader --config config.yaml
```

## Version History

| Version | Date | Notes |
|---------|------|-------|
| 1.13.6 | 2026-03 | Arrow/Mison dropped; JSONEachRow via reqwest (simpler, schema-flexible) |
| 1.13.5 | 2026-03 | OCSF remap preset, default topic changed to `default_land` |
| 1.9.3 | 2026-03 | Config cascade (figment), config hot-reload, DFE_LOADER__ env prefix |
| 1.3.0 | 2026-01 | Auto-initialisation, text search indexes |
| 1.2.0 | 2025-12 | Enrichment modules (GeoIP, reputation, risk) |
| 1.0.0 | 2025-12 | Initial release |
