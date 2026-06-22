// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Unit tests using MemoryTransport
//!
//! These tests don't require external infrastructure (Kafka, ClickHouse).
//! They use the in-memory transport to test pipeline components in isolation.

#[cfg(feature = "transport-memory")]
mod transport;

mod parallel_test;
