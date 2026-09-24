# Testing Guide

## Structure

```text
src/                              # Unit tests inline (#[cfg(test)])
tests/
├── common/                       # Shared helpers — NOT compiled as test crate
│   ├── mod.rs                    # TestMode, ClickHouseTestConfig, KafkaTestConfig, skip macros
│   ├── containers.rs             # Testcontainers infrastructure
│   └── metrics.rs                # Performance metrics snapshots
├── integration/                  # Single binary — all integration tests as submodules
│   ├── mod.rs                    # mod config; mod errors; mod resilience; ...
│   ├── clickhouse.rs             # ClickHouse HTTP client
│   ├── config.rs                 # Config loading, validation, hot-reload
│   ├── datatypes.rs              # Type coercion (DateTime64, UUID, Bool, etc.)
│   ├── deployment.rs             # Helm chart + Dockerfile generation from contract
│   ├── dlq.rs                    # Dead letter queue
│   ├── errors.rs                 # Error handling, edge cases, negative paths
│   ├── field_mapping.rs          # Field remap presets
│   ├── helm_contract.rs          # Deployment contract validation
│   ├── inserter.rs               # Batch insert + salvage
│   ├── kafka.rs                  # Kafka transport creation
│   ├── offset.rs                 # Kafka offset tracking
│   ├── property.rs               # Property-based tests (proptest)
│   ├── resilience.rs             # MemoryGuard backpressure
│   ├── rls.rs                    # Row-level security
│   └── schema.rs                 # Schema introspection from system.columns
├── e2e/                          # Real infra tests — #[ignore] by default
│   ├── mod.rs
│   ├── full_pipeline.rs          # Full orchestrator with ClickHouse
│   ├── kafka_to_clickhouse.rs    # Kafka produce → loader → ClickHouse verify
│   ├── pipeline.rs               # Pipeline construction
│   └── stress.rs                 # Stress/throughput tests
├── smoke.rs                      # Startup smoke test (MANDATORY, runs on every push)
├── unit/                         # Transport unit tests
│   └── transport.rs
├── performance_example.rs        # Metrics snapshot workflow example
└── integration_tests.rs          # Entry point (mod common; mod e2e; mod integration;)

benches/
└── insert_bakeoff.rs             # Criterion benchmarks
```

## Running Tests

```bash
# Smoke test (fastest, no infra needed)
cargo nextest run --test smoke

# Unit tests (inline in src/)
cargo nextest run --lib

# Integration tests (some need ClickHouse)
cargo nextest run --test integration_tests

# E2E tests (need Kafka + ClickHouse, ignored by default)
cargo nextest run -- --ignored

# All non-ignored tests
cargo nextest run

# Full suite (recommended before push)
cargo nextest run --lib
cargo nextest run --test smoke
cargo nextest run --test integration_tests
```

## Test Modes

Controlled by `TEST_MODE` in `.env` (or env var):

### Remote (default)

Uses devex cluster from `.env`. Tests skip if endpoints unreachable.

```bash
TEST_MODE=remote cargo nextest run
```

- ClickHouse: `clickhouse.devex.hyperi.io` (HTTPS/native TLS, 3-node cluster)
- Kafka: `kafka.devex.hyperi.io` (SASL_SSL, SCRAM-SHA-512)
- DDL uses `ON CLUSTER 'default'`

### Docker-local

Uses `dfe-docker` infra profile (single-node, no auth, no TLS).

```bash
# Start infrastructure
cd /projects/dfe-docker && docker compose --profile infra up -d

# Run tests
TEST_MODE=docker cargo nextest run

# Tear down
docker compose --profile infra down
```

- ClickHouse: `localhost:8123` / `localhost:9000`
- Kafka: `localhost:19092` (PLAINTEXT)
- DDL omits `ON CLUSTER`

### Config Helpers

```rust
use crate::common::{ClickHouseTestConfig, KafkaTestConfig};

let ch = ClickHouseTestConfig::from_env();
let kf = KafkaTestConfig::from_env();

// Skip if infrastructure unavailable
skip_if_no_clickhouse!();
skip_if_no_kafka!();
```

## Test Categories

| Category | Count | Location | Needs Infra | Trigger |
|----------|-------|----------|-------------|---------|
| Unit | ~384 | `src/` inline | No | Every push |
| Smoke | 9 | `tests/smoke.rs` | No | Every push |
| Integration | ~150 | `tests/integration/` | Some need CH | Every push |
| Property | 11 | `tests/integration/property.rs` | No | Every push |
| E2E | ~18 | `tests/e2e/` | Kafka + CH | `--ignored` only |

## Key Patterns

### E2E tests use `#[ignore]`

```rust
#[tokio::test]
#[ignore = "requires infrastructure"]
async fn test_kafka_to_clickhouse_roundtrip() { ... }
```

Run with `cargo nextest run -- --ignored`.

### Clustered ClickHouse DDL

```rust
// Use on_cluster_clause() from common/ — returns "ON CLUSTER 'default'" or "" per mode
let ddl = format!(
    "CREATE TABLE {} {} (...) ENGINE = MergeTree() ORDER BY tuple()",
    table_name, on_cluster_clause()
);
```

### Shared MetricsManager for tests

The Prometheus recorder is global (one per process). Tests sharing a binary use `OnceLock`:

```rust
fn shared_manager() -> &'static MetricsManager {
    static MANAGER: OnceLock<MetricsManager> = OnceLock::new();
    MANAGER.get_or_init(|| MetricsManager::new("test"))
}
```

`tests/smoke.rs` is its own binary so it gets its own recorder automatically.
