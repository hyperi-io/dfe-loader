-- ClickHouse test schema initialization
-- Creates tables matching the example configuration

-- Auth events table
CREATE TABLE IF NOT EXISTS auth_events (
    timestamp DateTime64(3) DEFAULT now64(3),
    timestamp_load DateTime64(3) DEFAULT now64(3),
    event_category LowCardinality(String) DEFAULT 'auth',
    event_type LowCardinality(String),
    user_id String,
    username String,
    ip_address IPv4,
    user_agent String,
    success UInt8,
    failure_reason Nullable(String),
    metadata String DEFAULT '{}'
) ENGINE = MergeTree()
ORDER BY (timestamp, user_id)
PARTITION BY toYYYYMM(timestamp);

-- API events table
CREATE TABLE IF NOT EXISTS api_events (
    timestamp DateTime64(3) DEFAULT now64(3),
    timestamp_load DateTime64(3) DEFAULT now64(3),
    event_category LowCardinality(String) DEFAULT 'api',
    endpoint String,
    method LowCardinality(String),
    status_code UInt16,
    response_time_ms UInt32,
    request_id UUID,
    user_id Nullable(String),
    ip_address IPv4,
    metadata String DEFAULT '{}'
) ENGINE = MergeTree()
ORDER BY (timestamp, endpoint)
PARTITION BY toYYYYMM(timestamp);

-- Admin events table
CREATE TABLE IF NOT EXISTS admin_events (
    timestamp DateTime64(3) DEFAULT now64(3),
    timestamp_load DateTime64(3) DEFAULT now64(3),
    event_category LowCardinality(String) DEFAULT 'admin',
    action LowCardinality(String),
    admin_user_id String,
    target_user_id Nullable(String),
    target_resource Nullable(String),
    details String DEFAULT '{}',
    ip_address IPv4
) ENGINE = MergeTree()
ORDER BY (timestamp, admin_user_id)
PARTITION BY toYYYYMM(timestamp);

-- Error events table
CREATE TABLE IF NOT EXISTS error_events (
    timestamp DateTime64(3) DEFAULT now64(3),
    timestamp_load DateTime64(3) DEFAULT now64(3),
    event_category LowCardinality(String) DEFAULT 'errors',
    error_type LowCardinality(String),
    error_message String,
    stack_trace Nullable(String),
    service LowCardinality(String),
    request_id Nullable(UUID),
    user_id Nullable(String),
    metadata String DEFAULT '{}'
) ENGINE = MergeTree()
ORDER BY (timestamp, error_type)
PARTITION BY toYYYYMM(timestamp);

-- DLQ table for failed messages
CREATE TABLE IF NOT EXISTS dlq_events (
    timestamp DateTime64(3) DEFAULT now64(3),
    original_topic String,
    original_partition UInt32,
    original_offset UInt64,
    error_reason String,
    error_stage LowCardinality(String),
    raw_message String
) ENGINE = MergeTree()
ORDER BY (timestamp, original_topic)
PARTITION BY toYYYYMM(timestamp)
TTL timestamp + INTERVAL 30 DAY;
