// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Test fixture builders for creating test data
//!
//! Provides builder patterns for events, configs, Arrow schemas, and DDL

pub mod config;
pub mod ddl;
pub mod events;

// Re-export commonly used builders
pub use config::{
    BufferConfigBuilder, ClickHouseConfigBuilder, MetadataConfigBuilder, RoutingConfigBuilder,
    TimestampConfigBuilder,
};
pub use ddl::{api_table_ddl, auth_table_ddl, event_table_ddl, rls_table_ddl, DdlBuilder};
pub use events::{BatchEventBuilder, EventBuilder};
