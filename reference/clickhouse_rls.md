# ClickHouse Row-Level Security (RLS) for Multi-Tenancy

**Version:** 1.0
**Date:** 2026-01-12
**Status:** Required Dependency

---

## Overview

The DFE loader uses a shared-schema multi-tenancy model by default, where all organisations' data is stored in common tables (e.g., `common.auth`, `common.api`). To enforce data isolation between organisations, ClickHouse Row Policies must be configured to restrict access based on the `_org_id` field.

**Key Principle:** Each ClickHouse role sees only rows where `_org_id` matches their organisation, with no way to bypass this filter.

---

## Architecture

### Default Routing Behaviour

```
Event → Extract event_category → Route to common.{event_category}
      → Extract org_id → Store in _org_id field
      → No per-org database routing by default

Examples:
  {"org_id": "acme", "event_category": "auth"} → common.auth (_org_id='acme')
  {"org_id": "bigcorp", "event_category": "auth"} → common.auth (_org_id='bigcorp')
  {"event_category": "api"} → common.api (_org_id=NULL or default)
```

### Per-Org Database Routing (Optional)

When specific organisations require database-level isolation (compliance, volume, etc.):

```yaml
# config.yaml
routing:
  default_db: "common"

  # Option 1: Allowlist specific orgs
  routed_orgs:
    - "acme"        # → acme.auth, acme.api, etc.
    - "bigcorp"     # → bigcorp.auth, bigcorp.api, etc.

  # Option 2: Route all orgs to own databases
  route_all_by_org: false  # Default: shared schema
```

**When routed to own database:** RLS is optional (database-level grants provide isolation).

---

## Row Policies (Required for Shared Schema)

### Prerequisites

- ClickHouse 20.5+ (row policies introduced)
- `_org_id String` column in all tables
- `_org_id` included in ORDER BY for index efficiency

### Basic Policy

```sql
-- Create role for organisation
CREATE ROLE IF NOT EXISTS acme_reader;

-- Grant read access to common database
GRANT SELECT ON common.* TO acme_reader;

-- Enforce row-level filtering by _org_id
CREATE ROW POLICY acme_filter
ON common.auth
FOR SELECT
TO acme_reader
USING _org_id = 'acme'
AS RESTRICTIVE;

-- Repeat for all tables in common database
CREATE ROW POLICY acme_filter_api
ON common.api
FOR SELECT
TO acme_reader
USING _org_id = 'acme'
AS RESTRICTIVE;

-- Create user with role
CREATE USER acme_user
IDENTIFIED WITH sha256_password BY 'secure_password'
DEFAULT ROLE acme_reader;
```

### How It Works

```sql
-- User query (as acme_user)
SELECT * FROM common.auth WHERE timestamp > now() - INTERVAL 7 DAY;

-- Automatically becomes (policy enforced)
SELECT * FROM common.auth
WHERE _org_id = 'acme'  -- Added by row policy
  AND timestamp > now() - INTERVAL 7 DAY;

-- Attempting to bypass fails
SELECT * FROM common.auth WHERE _org_id = 'bigcorp';
-- Returns 0 rows - policy filter is ANDed with user's WHERE clause
```

### Policy Types

```sql
-- RESTRICTIVE (mandatory filter, ANDed with query)
CREATE ROW POLICY org_filter
ON common.auth
USING _org_id = 'acme'
AS RESTRICTIVE;  -- Default behaviour

-- PERMISSIVE (optional filter, ORed with other PERMISSIVE policies)
CREATE ROW POLICY allow_own_org
ON common.auth
USING _org_id = currentUser()
AS PERMISSIVE;

CREATE ROW POLICY allow_public
ON common.auth
USING is_public = 1
AS PERMISSIVE;
-- Result: WHERE (_org_id = currentUser()) OR (is_public = 1)
```

---

## Admin Access (Bypass Row Policies)

Admins need unrestricted access across all organisations.

### Option 1: Exclude Admin Role

```sql
-- Create admin role
CREATE ROLE IF NOT EXISTS admin_reader;
GRANT SELECT ON common.* TO admin_reader;

-- Apply row policies to all EXCEPT admin
CREATE ROW POLICY enforce_org_isolation
ON common.auth
FOR SELECT
TO ALL EXCEPT admin_reader  -- Admin bypasses policy
USING _org_id = 'acme'
AS RESTRICTIVE;
```

### Option 2: Permissive Policy for Admin

```sql
-- Admin role with unrestricted access
CREATE ROLE admin_reader;

-- Permissive policy allows all
CREATE ROW POLICY admin_sees_all
ON common.auth
FOR SELECT
TO admin_reader
USING 1 = 1  -- Always true
AS PERMISSIVE;
```

**Recommendation:** Use Option 1 (exclude from policy) for clarity.

---

## Applying Policies to All Tables

### Manual Approach

```sql
-- Get list of tables
SELECT name FROM system.tables
WHERE database = 'common'
  AND engine LIKE '%MergeTree%';

-- Create policy for each table
CREATE ROW POLICY acme_filter_auth ON common.auth FOR SELECT TO acme_reader USING _org_id = 'acme' AS RESTRICTIVE;
CREATE ROW POLICY acme_filter_api ON common.api FOR SELECT TO acme_reader USING _org_id = 'acme' AS RESTRICTIVE;
CREATE ROW POLICY acme_filter_network ON common.network FOR SELECT TO acme_reader USING _org_id = 'acme' AS RESTRICTIVE;
-- ... repeat for all tables
```

### Automated Script (Bash)

```bash
#!/bin/bash
ORG_ID="acme"
ROLE_NAME="${ORG_ID}_reader"
DATABASE="common"

# Get list of tables
TABLES=$(clickhouse-client --query "
    SELECT name FROM system.tables
    WHERE database = '${DATABASE}'
      AND engine LIKE '%MergeTree%'
    FORMAT TSV
")

# Create role
clickhouse-client --query "CREATE ROLE IF NOT EXISTS ${ROLE_NAME};"
clickhouse-client --query "GRANT SELECT ON ${DATABASE}.* TO ${ROLE_NAME};"

# Create row policy for each table
for TABLE in $TABLES; do
    POLICY_NAME="${ORG_ID}_filter_${TABLE}"
    clickhouse-client --query "
        CREATE ROW POLICY IF NOT EXISTS ${POLICY_NAME}
        ON ${DATABASE}.${TABLE}
        FOR SELECT
        TO ${ROLE_NAME}
        USING _org_id = '${ORG_ID}'
        AS RESTRICTIVE;
    "
    echo "Created policy: ${POLICY_NAME} on ${DATABASE}.${TABLE}"
done

echo "Created role: ${ROLE_NAME} with row policies for all tables in ${DATABASE}"
```

### Automated Script (SQL Function)

```sql
-- Not directly supported - ClickHouse doesn't have CREATE FUNCTION for DDL
-- Use external script or manual creation
```

---

## Performance Optimization

### Schema Design

Include `_org_id` in ORDER BY for efficient filtering:

```sql
CREATE TABLE common.auth (
    timestamp DateTime64(3),
    _org_id String,
    _uuid UUID DEFAULT generateUUIDv7(),
    -- ... other fields
) ENGINE = MergeTree()
ORDER BY (timestamp, _org_id, _uuid)  -- _org_id in primary key
PARTITION BY (toYYYYMM(timestamp), _org_id)  -- Partition by month + org
SETTINGS index_granularity = 8192;
```

**Benefits:**

- **Index usage:** Primary key includes `_org_id` for fast filtering
- **Partition pruning:** Queries only read relevant org's partitions
- **Granule skipping:** ClickHouse skips granules (8192 rows) not matching org

### Query Performance

```sql
-- User query
SELECT COUNT(*) FROM common.auth
WHERE timestamp > now() - INTERVAL 7 DAY;

-- With row policy (added automatically)
SELECT COUNT(*) FROM common.auth
WHERE _org_id = 'acme'  -- Policy filter
  AND timestamp > now() - INTERVAL 7 DAY;

-- ClickHouse execution:
--   1. Partition pruning: only (202601, 'acme'), (202602, 'acme')
--   2. Primary key index: timestamp range
--   3. Granule-level _org_id filtering (marks file)
--   4. Final WHERE filter
```

**Expected Overhead:** <5% for simple policies (ClickHouse documentation).

---

## Security Considerations

### Cannot Bypass

Row policies are enforced **before** query execution at the ClickHouse server level:

- ✅ Cannot bypass with `SETTINGS` changes
- ✅ Cannot bypass with query syntax tricks
- ✅ Cannot bypass with JOINs to other tables (policy applies recursively)
- ✅ Survives server restart (stored in system tables)
- ✅ Applies to views, subqueries, CTEs

### Audit

```sql
-- View active row policies
SELECT * FROM system.row_policies
WHERE database = 'common';

-- View policy details
SHOW CREATE ROW POLICY acme_filter ON common.auth;

-- Test as user
-- (Requires logging in as that user)
```

---

## Migration Path

### Phase 1: Add _org_id Column

```sql
-- Add column to existing tables
ALTER TABLE common.auth ADD COLUMN IF NOT EXISTS _org_id String DEFAULT '';

-- Backfill from routing logic (application-specific)
-- NOTE: Loader will populate _org_id for new inserts automatically
```

### Phase 2: Modify Schema (Optional but Recommended)

```sql
-- Rebuild table with _org_id in ORDER BY for better performance
-- WARNING: This is a heavy operation on large tables

CREATE TABLE common.auth_new (
    timestamp DateTime64(3),
    _org_id String,
    _uuid UUID DEFAULT generateUUIDv7(),
    -- ... copy all columns
) ENGINE = MergeTree()
ORDER BY (timestamp, _org_id, _uuid)  -- Include _org_id
PARTITION BY (toYYYYMM(timestamp), _org_id);  -- Partition by org

-- Copy data
INSERT INTO common.auth_new SELECT * FROM common.auth;

-- Atomic swap
RENAME TABLE common.auth TO common.auth_old,
             common.auth_new TO common.auth;

-- Verify, then drop old table
DROP TABLE common.auth_old;
```

### Phase 3: Create Row Policies

```sql
-- Per organisation (see scripts above)
-- Apply to all tables in common database
```

### Phase 4: Create Users

```sql
-- Per organisation
CREATE USER acme_user IDENTIFIED BY 'secure_password' DEFAULT ROLE acme_reader;
CREATE USER bigcorp_user IDENTIFIED BY 'secure_password' DEFAULT ROLE bigcorp_reader;
```

---

## Verification

### Test Isolation

```sql
-- As acme_user
SELECT DISTINCT _org_id FROM common.auth;
-- Should return only: acme

SELECT COUNT(*) FROM common.auth WHERE _org_id = 'bigcorp';
-- Should return: 0

-- As admin_reader
SELECT _org_id, COUNT(*) FROM common.auth GROUP BY _org_id;
-- Should return all orgs:
--   acme      | 150000
--   bigcorp   | 250000
--   customer3 | 50000
```

### Test Query Performance

```sql
-- Enable query profiling
SET send_logs_level = 'trace';

-- Run query as org user
SELECT COUNT(*) FROM common.auth WHERE timestamp > now() - INTERVAL 1 DAY;

-- Check execution plan
EXPLAIN SELECT COUNT(*) FROM common.auth WHERE timestamp > now() - INTERVAL 1 DAY;

-- Look for:
--   - "Condition: _org_id = 'acme'" (policy applied)
--   - "Parts: X/Y" (partition pruning working)
--   - "Granules: X/Y" (granule skipping working)
```

---

## Troubleshooting

### Policy Not Applied

```sql
-- Check policy exists
SELECT * FROM system.row_policies WHERE name LIKE '%acme%';

-- Check user's roles
SELECT * FROM system.role_grants WHERE user_name = 'acme_user';

-- Verify policy targets correct role
SHOW CREATE ROW POLICY acme_filter ON common.auth;
```

### User Sees No Data

```sql
-- Check _org_id values in table
SELECT DISTINCT _org_id FROM common.auth LIMIT 10;

-- Check if _org_id matches policy
-- (May be case sensitivity, whitespace, etc.)

-- As admin, check data exists
SELECT COUNT(*) FROM common.auth WHERE _org_id = 'acme';
```

### Performance Degradation

```sql
-- Check if _org_id in ORDER BY
SHOW CREATE TABLE common.auth;

-- Check partition strategy
SELECT partition, count() FROM system.parts
WHERE database = 'common' AND table = 'auth'
GROUP BY partition
ORDER BY partition;

-- Consider adding _org_id to ORDER BY or PARTITION BY
```

---

## References

- [ClickHouse Row Policies Documentation](https://clickhouse.com/docs/en/operations/access-rights#row-policy-management)
- [ClickHouse Security Best Practices](https://clickhouse.com/docs/en/operations/security)
- DFE Loader: `reference/common_header.sql` - Common header schema
- DFE Loader: `CLAUDE.md` - Routing architecture

---

**Last Updated:** 2026-01-12
**Maintainer:** HyperI Platform Team
