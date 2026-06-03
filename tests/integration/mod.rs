// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Integration tests

mod clickhouse;
#[cfg(feature = "testcontainers")]
mod clickhouse_inserter_e2e;
mod coerce_integration;
mod config;
mod datatypes;
mod deployment;
mod dlq;
mod errors;
mod field_mapping;
mod geoip_download_e2e;
mod helm_contract;
mod inserter;
mod kafka;
#[cfg(feature = "testcontainers")]
mod kafka_transport_e2e;
mod offset;
mod property;
mod resilience;
mod rls;
mod schema;
mod schema_pending_e2e;
mod transport;
