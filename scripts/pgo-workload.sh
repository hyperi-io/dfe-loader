#!/usr/bin/env bash
# Project:   dfe-loader
# File:      scripts/pgo-workload.sh
# Purpose:   PGO workload orchestrator — Kafka + ClickHouse + loader + producer
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/pgo-workload.sh <path-to-dfe-loader-binary>
#
# Drives the loader's hot path (Kafka consume → SIMD JSON parse → route →
# transform → ClickHouse insert) under representative load so a PGO-
# instrumented binary accumulates useful profile data.
#
# Environment variables (all optional):
#   PGO_WORKLOAD_DURATION_SECS   Duration of load (default 300, floor 60)
#   PGO_WORKLOAD_KAFKA_IMAGE     Override Kafka image
#   PGO_WORKLOAD_CH_IMAGE        Override ClickHouse image
#   PGO_WORKLOAD_KEEP            Set to 1 to skip cleanup (debug)
#   PGO_DRIVER_PATH              Override pgo-driver binary path
#
# Preconditions:
#   - Docker daemon running, user has access
#   - $1 is the loader binary built with --features jemalloc
#   - pgo-driver binary built with --features pgo-driver (auto-built if missing)
#
# Behaviour:
#   - Starts single-node Kafka (KRaft) and single-node ClickHouse
#   - Writes ephemeral loader config pointing at both
#   - Starts the passed-in loader binary in background
#   - Waits for loader readiness probe
#   - Runs pgo-driver to produce messages for the configured duration
#   - Cleans up (traps EXIT): kills loader, removes containers

set -euo pipefail

# ----------------------------------------------------------------------------
# Args + env
# ----------------------------------------------------------------------------

if [[ $# -lt 1 ]]; then
    echo "usage: $0 <path-to-dfe-loader-binary>" >&2
    exit 1
fi

LOADER_BIN="$1"
if [[ ! -x "$LOADER_BIN" ]]; then
    echo "error: $LOADER_BIN is not executable" >&2
    exit 1
fi

DURATION="${PGO_WORKLOAD_DURATION_SECS:-300}"
# Match what we deploy. A PGO profile is only as good as the traffic that
# produced it: collect it against a two-year-old broker or a datastore two LTS
# lines back and the optimiser tunes for code paths production no longer takes.
# renovate: datasource=docker depName=apache/kafka
KAFKA_IMAGE="${PGO_WORKLOAD_KAFKA_IMAGE:-apache/kafka:4.1.1}"
# renovate: datasource=docker depName=clickhouse/clickhouse-server
CH_IMAGE="${PGO_WORKLOAD_CH_IMAGE:-clickhouse/clickhouse-server:26.3}"
KEEP="${PGO_WORKLOAD_KEEP:-0}"

# Floor of 60s — shorter workloads produce bad PGO profiles
if [[ "$DURATION" -lt 60 ]]; then
    echo "error: PGO_WORKLOAD_DURATION_SECS must be >= 60 (got $DURATION)" >&2
    echo "  short workloads produce NEGATIVE PGO gains by biasing the" >&2
    echo "  compiler toward startup paths instead of hot paths" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Locate pgo-driver binary; build on demand if missing.
PGO_DRIVER_PATH="${PGO_DRIVER_PATH:-}"
if [[ -z "$PGO_DRIVER_PATH" ]]; then
    for candidate in \
        "$PROJECT_ROOT/target/release/pgo-driver" \
        "$PROJECT_ROOT/target/debug/pgo-driver"; do
        if [[ -x "$candidate" ]]; then
            PGO_DRIVER_PATH="$candidate"
            break
        fi
    done
fi
if [[ -z "$PGO_DRIVER_PATH" || ! -x "$PGO_DRIVER_PATH" ]]; then
    echo "pgo-workload: pgo-driver not found, building..." >&2
    (cd "$PROJECT_ROOT" && cargo build --release --features pgo-driver --bin pgo-driver) \
        || { echo "error: failed to build pgo-driver" >&2; exit 1; }
    PGO_DRIVER_PATH="$PROJECT_ROOT/target/release/pgo-driver"
    if [[ ! -x "$PGO_DRIVER_PATH" ]]; then
        echo "error: pgo-driver still missing after build at $PGO_DRIVER_PATH" >&2
        exit 1
    fi
fi

# ----------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------

LOADER_PID=""
KAFKA_CID=""
CH_CID=""
CONFIG_DIR=""

cleanup() {
    local rc=$?
    if [[ "$KEEP" == "1" ]]; then
        echo "PGO_WORKLOAD_KEEP=1 — skipping cleanup" >&2
        echo "  loader PID: $LOADER_PID" >&2
        echo "  kafka CID:  $KAFKA_CID" >&2
        echo "  ch CID:     $CH_CID" >&2
        echo "  config dir: $CONFIG_DIR" >&2
        return $rc
    fi
    echo "pgo-workload: cleanup" >&2
    if [[ -n "$LOADER_PID" ]] && kill -0 "$LOADER_PID" 2>/dev/null; then
        kill -TERM "$LOADER_PID" 2>/dev/null || true
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            if ! kill -0 "$LOADER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$LOADER_PID" 2>/dev/null || true
    fi
    if [[ -n "$KAFKA_CID" ]]; then
        docker rm -f "$KAFKA_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CH_CID" ]]; then
        docker rm -f "$CH_CID" >/dev/null 2>&1 || true
    fi
    if [[ -n "$CONFIG_DIR" && -d "$CONFIG_DIR" ]]; then
        rm -rf "$CONFIG_DIR"
    fi
    exit $rc
}
trap cleanup EXIT INT TERM

# ----------------------------------------------------------------------------
# Start ClickHouse (single-node, default user, no auth)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting ClickHouse ($CH_IMAGE)"
CH_CID=$(docker run -d --rm \
    -p 18123:8123 \
    -p 19000:9000 \
    -e CLICKHOUSE_SKIP_USER_SETUP=1 \
    --ulimit nofile=262144:262144 \
    "$CH_IMAGE")
echo "pgo-workload: ClickHouse CID: $CH_CID"

for attempt in $(seq 1 60); do
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:18123/ping"; then
        echo "pgo-workload: ClickHouse ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: ClickHouse did not become ready in 60s" >&2
        docker logs --tail 50 "$CH_CID" >&2
        exit 1
    fi
    sleep 1
done

# Pre-create the destination database the loader will write to. The loader
# also auto-creates tables from its embedded DDL on first insert.
curl -sf -X POST "http://127.0.0.1:18123/" \
    --data "CREATE DATABASE IF NOT EXISTS dfe" \
    >/dev/null

# ----------------------------------------------------------------------------
# Start Kafka (KRaft mode, single-node, auto-create topics)
# ----------------------------------------------------------------------------

echo "pgo-workload: starting Kafka ($KAFKA_IMAGE)"
KAFKA_CID=$(docker run -d --rm \
    -p 19092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS='PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093' \
    -e KAFKA_ADVERTISED_LISTENERS='PLAINTEXT://localhost:19092' \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP='CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT' \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS='1@localhost:9093' \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
    -e KAFKA_NUM_PARTITIONS=3 \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR=1 \
    -e CLUSTER_ID="$(printf '%s' "pgo$(date +%s)$$" | base64 | head -c 22)" \
    "$KAFKA_IMAGE")
echo "pgo-workload: Kafka CID: $KAFKA_CID"

for attempt in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/19092) 2>/dev/null; then
        sleep 2  # let RAFT bootstrap finish
        echo "pgo-workload: Kafka ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 30 ]]; then
        echo "error: Kafka did not become ready in 60s" >&2
        docker logs --tail 50 "$KAFKA_CID" >&2
        exit 1
    fi
    sleep 2
done

# ----------------------------------------------------------------------------
# Write ephemeral loader config
# ----------------------------------------------------------------------------

CONFIG_DIR=$(mktemp -d -t pgo-workload-XXXXXX)
CONFIG_FILE="$CONFIG_DIR/config.yaml"

cat > "$CONFIG_FILE" <<'YAML'
kafka:
  brokers:
    - "localhost:19092"
  group: "pgo-workload"
  topics:
    - "default_land"
  client_id: "pgo-workload"
  librdkafka_overrides:
    statistics.interval.ms: "0"
    auto.offset.reset: "earliest"

clickhouse:
  hosts:
    - "localhost:19000"
  database: "dfe"
  username: "default"
  password: ""
  protocol: "native"
  tables: []

routing:
  db_fields: []
  table_fields:
    - "_source"
  default_db: "dfe"
  default_table: "default"
  org_routes: []
  source_to_table:
    auth: "auth_events"
    api: "api_events"
    admin: "admin_events"
    errors: "error_events"

buffer:
  flush_rows: 5000
  flush_bytes: 4194304
  flush_age_secs: 2

metrics:
  enabled: true
  bind_address: "127.0.0.1:9090"

log:
  format: "json"
  level: "warn"

hot_reload:
  enabled: false
YAML

# ----------------------------------------------------------------------------
# Start loader
# ----------------------------------------------------------------------------

echo "pgo-workload: starting loader: $LOADER_BIN"
echo "pgo-workload: config: $CONFIG_FILE"

# PGO profiles go here by default with cargo-pgo
export LLVM_PROFILE_FILE="${LLVM_PROFILE_FILE:-$PROJECT_ROOT/target/pgo-profiles/pgo-%p_%m.profraw}"
mkdir -p "$(dirname "$LLVM_PROFILE_FILE")"

"$LOADER_BIN" --config "$CONFIG_FILE" \
    >"$CONFIG_DIR/loader.log" 2>&1 &
LOADER_PID=$!
echo "pgo-workload: loader PID: $LOADER_PID"

for attempt in $(seq 1 60); do
    if ! kill -0 "$LOADER_PID" 2>/dev/null; then
        echo "error: loader died during startup" >&2
        tail -100 "$CONFIG_DIR/loader.log" >&2
        exit 1
    fi
    if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/readyz" \
        || curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:9090/healthz"; then
        echo "pgo-workload: loader ready (attempt $attempt)"
        break
    fi
    if [[ $attempt -eq 60 ]]; then
        echo "error: loader did not become ready in 60s" >&2
        tail -100 "$CONFIG_DIR/loader.log" >&2
        exit 1
    fi
    sleep 1
done

# Extra settle so the consumer group is fully joined before we start producing
sleep 2

# ----------------------------------------------------------------------------
# Run load driver
# ----------------------------------------------------------------------------

echo "pgo-workload: driving load for ${DURATION}s via $PGO_DRIVER_PATH"

PGO_DRIVER_DURATION_SECS="$DURATION" \
PGO_DRIVER_BROKERS="127.0.0.1:19092" \
PGO_DRIVER_TOPIC="default_land" \
PGO_DRIVER_RPS="${PGO_DRIVER_RPS:-5000}" \
    "$PGO_DRIVER_PATH"

echo "pgo-workload: driver complete"

# Give the loader a moment to drain buffers + flush profile data
sleep 5

echo "pgo-workload: done (loader logs: $CONFIG_DIR/loader.log)"
