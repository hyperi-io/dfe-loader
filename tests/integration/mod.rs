// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Integration tests

mod clickhouse;
#[cfg(feature = "testcontainers")]
mod clickhouse_inserter_e2e;
mod coerce_integration;
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
mod geoip_download_e2e;
#[cfg(feature = "testcontainers")]
mod grpc_outage;
mod helm_contract;
mod inserter;
mod kafka;
#[cfg(feature = "testcontainers")]
mod kafka_offset_floor;
#[cfg(feature = "testcontainers")]
mod kafka_transport_e2e;
mod offset;
mod property;
mod resilience;
mod rls;
mod schema;
mod schema_pending_e2e;
mod transport;
