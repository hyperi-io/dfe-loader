// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Test fixture builders for creating test data
//!
//! Provides builder patterns for events, configs, Arrow schemas, and DDL

pub mod arrow_schema;
pub mod config;
pub mod ddl;
pub mod events;

// Re-export commonly used builders
pub use events::{EventBuilder, BatchEventBuilder};
pub use config::{
    BufferConfigBuilder, ClickHouseConfigBuilder, RoutingConfigBuilder,
    TimestampConfigBuilder, MetadataConfigBuilder,
};
pub use arrow_schema::{
    ArrowSchemaBuilder, event_schema, rls_schema, auth_schema, api_schema, minimal_schema,
};
pub use ddl::{DdlBuilder, event_table_ddl, rls_table_ddl, auth_table_ddl, api_table_ddl};
