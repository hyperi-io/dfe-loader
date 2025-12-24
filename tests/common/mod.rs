//! Shared test utilities and fixtures

use std::env;

/// Check if external test environment is configured
pub fn has_external_env() -> bool {
    env::var("CLICKHOUSE_HOST").is_ok() && env::var("KAFKA_BROKERS").is_ok()
}

/// Check if Docker is available for testcontainers
pub fn has_docker() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Skip test if no test environment is available
#[macro_export]
macro_rules! skip_if_no_env {
    () => {
        if !$crate::common::has_external_env() && !$crate::common::has_docker() {
            eprintln!("Skipping test: no test environment available");
            return;
        }
    };
}
