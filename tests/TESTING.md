# Testing Guide

This document describes the testing infrastructure, patterns, and best practices for the dfe-loader project.

---

## Test Structure

```
tests/
├── common/              # Shared test utilities
│   ├── mod.rs          # Common helpers (query_count, query_one, etc.)
│   ├── containers.rs   # Testcontainers infrastructure
│   └── metrics.rs      # Performance metrics snapshots
├── fixtures/            # Test data builders
│   ├── events.rs       # EventBuilder, BatchEventBuilder
│   ├── config.rs       # Config builders
│   └── ddl.rs          # ClickHouse DDL builders
├── integration/         # Integration tests
│   ├── clickhouse.rs   # ClickHouse Arrow client tests
│   ├── inserter.rs     # Inserter tests
│   ├── datatypes.rs    # Type handling tests
│   ├── rls.rs          # Row-level security tests
│   ├── property.rs     # Property-based tests
│   └── ...
├── unit/                # Unit tests (transport, etc.)
└── e2e/                 # End-to-end pipeline tests
```

---

## Test Categories

### 1. Unit Tests (294 tests)

Located in `src/` using `#[cfg(test)]` modules. Test individual functions and modules in isolation.

**Run:**

```bash
cargo test --lib
```

**Examples:**

- Buffer management
- JSON flattening
- Routing logic
- Timestamp validation

### 2. Integration Tests (124 tests)

Located in `tests/integration/`. Test components interacting with external systems (ClickHouse, Kafka).

**Run:**

```bash
cargo test --test integration_tests
```

**Key Tests:**

- [clickhouse.rs](tests/integration/clickhouse.rs) - ClickHouse HTTP client tests
- [inserter.rs](tests/integration/inserter.rs) - Batch insert tests
- [datatypes.rs](tests/integration/datatypes.rs) - Type handling tests
- [rls.rs](tests/integration/rls.rs) - Row-level security tests
- [property.rs](tests/integration/property.rs) - Property-based tests (11 tests)

### 3. Property-Based Tests (11 tests)

Use `proptest` to generate random inputs and test invariants.

**Location:** [tests/integration/property.rs](tests/integration/property.rs)

**Test Categories:**

- **Routing:** Arbitrary org_ids, nested fields, extraction
- **Transformation:** Underscore fields, nested flattening, type handling
- **Buffer:** Accumulation, multiple tables
- **Timestamp:** Edge cases, RFC3339 strings

**Example:**

```rust
proptest! {
    #[test]
    fn prop_routing_handles_arbitrary_org_ids(
        org_id in "[a-zA-Z0-9_-]{1,100}",
        category in "[a-zA-Z0-9_-]{1,50}"
    ) {
        let router = Router::default();
        let event = json!({
            "org_id": org_id,
            "event_category": category,
        });
        let result = router.route(&serde_json::to_vec(&event).unwrap());
        prop_assert!(matches!(result, RouteResult::Table(_)));
    }
}
```

### 4. Performance Tests

Use metrics snapshots to validate that code changes improve performance.

**Location:** [tests/performance_example.rs](tests/performance_example.rs)

**Workflow:**

```rust
// Capture baseline
let baseline = MetricsSnapshot::capture(&registry, "baseline_v1");
baseline.save("target/metrics_baseline.json")?;

// ... make changes ...

// Capture current
let current = MetricsSnapshot::capture(&registry2, "optimized_v2");
current.save("target/metrics_current.json")?;

// Compare
let comparison = current.compare(&baseline);
comparison.print_report();
comparison.save_markdown("target/metrics_comparison.md")?;

// Auto-detects improvements vs regressions
assert!(comparison.regressions.is_empty());
```

See [PERFORMANCE_TESTING.md](tests/PERFORMANCE_TESTING.md) for details.

---

## Testing Patterns

### Query-Back Verification

**Pattern:** Always query back after INSERT to verify data was written correctly.

**Why:** INSERT row count can succeed even if data is malformed. SELECT queries confirm data integrity.

**Example:**

```rust
// Insert data
let result = client.insert(&table_name, batch).await;
assert!(result.is_ok());
assert_eq!(result.unwrap(), 1000);

// Query back to verify row count
let count = query_count(&client, &table_name, None).await?;
assert_eq!(count, 1000, "Should have 1000 rows");

// Query back to verify specific data
let count = query_count(&client, &table_name, Some("org_id = 'acme'")).await?;
assert_eq!(count, 500, "ACME should have 500 rows");
```

**Helpers Available:**

- `client.query_count(table, where_clause)` - Get row count with optional WHERE clause (method on `HttpClickHouseClient`)
- `create_http_test_client()` - Creates a test HTTP client from `.env` config
- `drop_http_test_table(client, table)` - Drops a table with `ON CLUSTER 'default'`

### Test DDL Convention

All test CREATE TABLE statements MUST use `ON CLUSTER 'default'` and `MergeTree()`.
This is required because the test ClickHouse is a 3-node load-balanced cluster — without
`ON CLUSTER`, CREATE goes to one node and INSERT may go to another.

**Cluster database types:**
- `benchmark` — `Atomic` engine (standard). Needs `ON CLUSTER` to create on all 3 nodes.
  `MergeTree()` stays as MergeTree (no auto-conversion). Inserts and queries are NOT
  consistent across nodes — avoid query-back verification tests in this database.
- `default` — `Replicated` engine. DDL propagates automatically. Use `ReplicatedMergeTree()`
  (empty args, ZK paths auto-filled) for tables that need data replication and query-back.
  Do NOT specify explicit ZK paths — forbidden in Replicated databases.

**Standard pattern (benchmark database — most tests):**

```rust
// ✅ CORRECT — creates on all 3 nodes via ON CLUSTER
let ddl = format!(
    "CREATE TABLE {} ON CLUSTER 'default' (
        id UInt64,
        name String
    ) ENGINE = MergeTree()
    ORDER BY id",
    table_name
);
client.execute(&ddl).await.expect("Failed to create table");
```

**Pattern for query-back verification (default database):**

```rust
// ✅ CORRECT — ReplicatedMergeTree in Replicated DB, no ON CLUSTER needed
// Auto-fills ZK paths; use SYSTEM SYNC REPLICA before query-back
let ddl = format!(
    "CREATE TABLE default.{} (
        id UInt64,
        name String
    ) ENGINE = ReplicatedMergeTree()
    ORDER BY id",
    table_name
);
client.execute(&ddl).await.expect("Failed to create table");
// Sync all replicas after INSERT before querying back:
client.execute(&format!("SYSTEM SYNC REPLICA ON CLUSTER 'default' default.{}", table_name))
    .await.expect("sync failed");
```

**Wrong patterns:**

```rust
// ❌ WRONG — only creates on one node (no ON CLUSTER in benchmark Atomic DB)
let ddl = format!("CREATE TABLE {} (id UInt64) ENGINE = MergeTree() ORDER BY id", table_name);

// ❌ WRONG — explicit ZK paths forbidden in Replicated database
let ddl = format!(
    "CREATE TABLE default.{} ON CLUSTER 'default' (id UInt64)
    ENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/{{database}}/{{table}}', '{{replica}}')
    ORDER BY id",
    table_name
);
```

Drop tables also use `ON CLUSTER`:

```rust
drop_http_test_table(&client, &full_name).await;  // Calls ON CLUSTER 'default' internally
```

### Fixture Builders

**Location:** [tests/fixtures/](tests/fixtures/)

**Use fixture builders for reusable test data:**

**Event Data:**

```rust
use crate::fixtures::EventBuilder;

let event = EventBuilder::new()
    .org_id("acme")
    .category("auth")
    .action("login")
    .with_int("user_id", 1001)
    .with_timestamp_now()
    .build();

// Batch generation
let events = BatchEventBuilder::new()
    .count(1000)
    .with_orgs(vec!["acme", "bigcorp"])
    .with_categories(vec!["auth", "api"])
    .build();
```

**Configuration:**

```rust
use crate::fixtures::BufferConfigBuilder;

let config = BufferConfigBuilder::new()
    .flush_rows(5000)
    .flush_bytes(2_000_000)
    .flush_age_secs(120)
    .build();
```

**DDL Statements:**

```rust
use crate::fixtures::{event_table_ddl, DdlBuilder};

let ddl = event_table_ddl("common", "events");
client.query(&ddl).await?;

let custom_ddl = DdlBuilder::new("test", "my_table")
    .with_timestamp("timestamp", false, None)
    .with_string("org_id", false)
    .with_uint64("count", true)
    .order_by(vec!["timestamp"])
    .partition_by("toYYYYMM(timestamp)")
    .build();
```

### Testcontainers

**Feature Flag:** `--features testcontainers`

**Why:** Isolated Docker environments for CI/CD without external dependencies.

**Status:** Infrastructure created but not yet integrated into tests (future work).

**Example (future):**

```rust
#[tokio::test]
async fn test_with_isolated_clickhouse() {
    let infra = TestInfrastructure::new(true, false).await; // ClickHouse only
    let client = ArrowClickHouseClient::new(&infra.clickhouse_config()).await?;

    // Test runs in isolation
    // Container auto-stops on drop
}
```

---

## Test Modes

Tests support two backends, controlled by `TEST_MODE` in `.env`:

### Remote (default)

Uses the devex cluster endpoints from `.env`. Tests skip if endpoints are unreachable.

```bash
# Ensure .env has CLICKHOUSE_HOST, KAFKA_BROKERS, etc.
TEST_MODE=remote cargo nextest run
```

- ClickHouse: `clickhouse.devex.hyperi.io:8543` (HTTPS) / `:9440` (native TLS)
- Kafka: `kafka.devex.hyperi.io:32089` (SASL_SSL, SCRAM-SHA-512)
- 3-node cluster: DDL uses `ON CLUSTER 'default'`

### Docker-local

Uses `dfe-docker` infra profile (ClickHouse + Kafka on localhost, no auth, no TLS).

```bash
# Start infrastructure (once, stays running)
cd /projects/dfe-docker
docker compose --profile infra up -d

# Run tests
TEST_MODE=docker cargo nextest run

# Tear down (when done)
docker compose --profile infra down
```

- ClickHouse: `localhost:8123` (HTTP) / `localhost:9000` (native)
- Kafka: `localhost:19092` (PLAINTEXT, no SASL)
- Single node: DDL omits `ON CLUSTER`

### Config Helpers

Tests use `ClickHouseTestConfig::from_env()` and `KafkaTestConfig::from_env()` which
return the correct connection details for the active mode. Use `skip_if_no_clickhouse!()`
and `skip_if_no_kafka!()` macros to gracefully skip when endpoints are unreachable.

---

## Running Tests

### All Tests

```bash
cargo nextest run
```

### Unit Tests Only

```bash
cargo nextest run --lib
```

### Integration Tests Only

```bash
cargo nextest run --test integration_tests
```

### Property Tests Only

```bash
cargo nextest run --test integration_tests -E 'test(property)'
```

### Performance Example

```bash
cargo nextest run --test performance_example --no-capture
```

---

## Test Metrics

| Category | Count | Status |
|----------|-------|--------|
| Unit tests | 294 | ✅ Passing |
| Integration tests | 113 | ✅ Passing |
| Property tests | 11 | ✅ Passing |
| **Total** | **421** | **✅ All Passing** |

---

## Best Practices

### 1. Always Query Back After Insert

```rust
// ❌ BAD - Only check insert row count
let count = client.insert(&table, batch).await?;
assert_eq!(count, 1000);

// ✅ GOOD - Verify data was actually written
let count = client.insert(&table, batch).await?;
assert_eq!(count, 1000);

let actual_count = query_count(&client, &table, None).await?;
assert_eq!(actual_count, 1000, "Data should be in table");
```

### 2. Use Explicit Arrow Schemas

```rust
// ❌ BAD - JSON inference
let batch = json_batch_to_arrow(&rows)?;

// ✅ GOOD - Explicit schema matching ClickHouse DDL
let schema = Arc::new(Schema::new(vec![
    Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
    // ...
]));
let batch = RecordBatch::try_new(schema, columns)?;
```

### 3. Use Fixture Builders for Reusable Data

```rust
// ❌ BAD - Manual JSON construction
let event = json!({
    "org_id": "acme",
    "event_category": "auth",
    "action": "login",
    "timestamp": 1705315200000_i64,
});

// ✅ GOOD - Builder pattern
let event = EventBuilder::new()
    .org_id("acme")
    .category("auth")
    .action("login")
    .with_timestamp_now()
    .build();
```

### 4. Test Edge Cases with Property Tests

```rust
// ✅ GOOD - Test with random inputs
proptest! {
    #[test]
    fn prop_routing_handles_arbitrary_org_ids(
        org_id in "[a-zA-Z0-9_-]{1,100}"
    ) {
        let router = Router::default();
        let event = json!({"org_id": org_id});
        let result = router.route_value(&event);
        prop_assert!(matches!(result, RouteResult::Table(_)));
    }
}
```

### 5. Use Metrics Snapshots for Performance Testing

```rust
// ✅ GOOD - Capture before/after metrics
let baseline = MetricsSnapshot::capture(&registry, "before_optimization");
// ... run workload ...

// After optimization
let current = MetricsSnapshot::capture(&registry2, "after_optimization");
let comparison = current.compare(&baseline);
assert!(comparison.regressions.is_empty());
```

### 6. Handle ClickHouse String Types

```rust
// ClickHouse returns String as Binary via Arrow protocol
use arrow::array::BinaryArray;

if let Some(col) = batch.column(0).as_any().downcast_ref::<BinaryArray>() {
    let value = std::str::from_utf8(col.value(0))?;
    assert_eq!(value, "expected_value");
}
```

---

## Common Issues

### Issue: Timestamp Conversion Errors

**Symptom:** `ClickHouse could not parse timestamp`

**Cause:** JSON schema inference creates `String` type instead of `Timestamp` type.

**Solution:** Use explicit Arrow schema with `TimestampMillisecondArray`.

See: [tests/integration/rls.rs:161-271](tests/integration/rls.rs)

### Issue: Test Passes But Data Is Wrong

**Symptom:** INSERT returns success, but data is malformed in ClickHouse.

**Cause:** Only checking INSERT row count, not querying back.

**Solution:** Always add query-back verification after INSERT.

See: Query-Back Verification pattern above.

### Issue: Property Test Failures

**Symptom:** Property test fails with random input.

**Cause:** Edge case not handled in implementation.

**Solution:** Fix the implementation or add constraint to property test.

**Example:**

```rust
// If empty strings cause issues, constrain the generator:
org_id in "[a-zA-Z0-9_-]{1,100}" // Minimum length 1
```

### Issue: Floating Point Comparison Failures

**Symptom:** `assertion failed: left == right` with very small difference.

**Cause:** Floating-point arithmetic precision.

**Solution:** Use approximate comparison:

```rust
// ❌ BAD
assert_eq!(delta, -0.1);

// ✅ GOOD
assert!((delta + 0.1).abs() < 0.01);
```

---

## Future Enhancements (Tier 3)

### 1. Row Policy Enforcement Testing

Test ClickHouse row policies with multiple users to verify data isolation.

**Approach:**

- Use testcontainers with preconfigured users/policies
- Connect as different users
- Verify each user only sees their own data

### 2. Mutation Testing

Use `cargo-mutants` to validate test quality by mutating code and checking if tests catch it.

```bash
cargo install cargo-mutants
cargo mutants --test-tool=cargo test --lib
```

### 3. Fuzzing

Use `cargo-fuzz` to find edge cases in parsers and transformers.

```bash
cargo install cargo-fuzz
cargo fuzz run fuzz_json_parser
```

### 4. Chaos Testing

Use testcontainers + toxiproxy to simulate failures:

- Network delays
- Connection drops
- Resource exhaustion

---

## Summary

- **421 tests** across unit, integration, and property test categories
- **Query-back verification** ensures data integrity in all integration tests
- **Explicit Arrow schemas** prevent type conversion errors
- **Fixture builders** provide reusable test data patterns
- **Property tests** catch edge cases with random inputs
- **Metrics snapshots** validate performance improvements
- **Testcontainers infrastructure** ready for CI/CD (not yet integrated)

All tests passing ✅

---

**Last Updated:** 2026-01-12
**Test Count:** 421 passing
