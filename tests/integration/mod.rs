// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Integration tests

#[cfg(feature = "testcontainers")]
mod broker_startup;
mod clickhouse;
#[cfg(feature = "testcontainers")]
mod clickhouse_inserter_e2e;
#[cfg(feature = "testcontainers")]
mod clickhouse_readiness;
#[cfg(feature = "testcontainers")]
mod clickhouse_tls;
mod config;
mod config_reachability;
// A leak check has to fail the test when the container is still there, and the
// poll loop it sits after cannot express that as an assert.
#[allow(clippy::panic)]
mod container_hygiene;
#[cfg(feature = "testcontainers")]
mod datatypes;
mod deployment;
mod dlq;
mod errors;
mod field_mapping;
mod geoip_provisioning;
#[cfg(feature = "testcontainers")]
mod grpc_outage;
#[cfg(feature = "testcontainers")]
mod grpc_startup;
#[cfg(feature = "testcontainers")]
mod header_keys;
mod helm_contract;
#[cfg(feature = "testcontainers")]
mod hot_reload;
mod inserter;
#[cfg(feature = "testcontainers")]
mod json_depth;
mod kafka;
#[cfg(feature = "testcontainers")]
mod kafka_dlq_metrics;
#[cfg(feature = "testcontainers")]
mod kafka_offset_floor;
#[cfg(feature = "testcontainers")]
mod kafka_rebalance;
#[cfg(feature = "testcontainers")]
mod kafka_tls_startup;
#[cfg(feature = "testcontainers")]
mod kafka_transport_e2e;
#[cfg(feature = "testcontainers")]
mod memory_balance;
mod offset;
mod property;
mod resilience;
mod rls;
mod schema;
mod schema_pending_e2e;
#[cfg(feature = "testcontainers")]
mod tenancy;
mod transport;
