// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Helm chart + Dockerfile contract sync tests
//!
//! Uses hyperi-rustlib's `DeploymentContract` to validate that
//! `chart/values.yaml` and `Dockerfile` stay in sync with app defaults.
//!
//! If you change a default port, health path, or KEDA threshold in
//! the Rust config, these tests fail until the chart/Dockerfile are
//! updated (or vice versa).

use std::path::Path;

use dfe_loader::config::Config;
use hyperi_rustlib::deployment::DeploymentContract;

fn app_contract() -> DeploymentContract {
    Config::deployment_contract()
}

// ============================================================================
// Helm Chart Validation
// ============================================================================

#[test]
fn test_helm_chart_matches_contract() {
    let contract = app_contract();
    let chart_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("chart");

    let mismatches =
        hyperi_rustlib::deployment::validate_helm_values(&contract, &chart_dir).unwrap();

    assert!(
        mismatches.is_empty(),
        "Helm chart mismatches with app contract:\n{}",
        mismatches
            .iter()
            .map(|m| format!("  - {m}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ============================================================================
// Dockerfile Validation
// ============================================================================

#[test]
fn test_dockerfile_matches_contract() {
    let contract = app_contract();
    let dockerfile = Path::new(env!("CARGO_MANIFEST_DIR")).join("Dockerfile");

    let mismatches =
        hyperi_rustlib::deployment::validate_dockerfile(&contract, &dockerfile).unwrap();

    assert!(
        mismatches.is_empty(),
        "Dockerfile mismatches with app contract:\n{}",
        mismatches
            .iter()
            .map(|m| format!("  - {m}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

// ============================================================================
// Chart Metadata
// ============================================================================

#[test]
fn test_chart_yaml_valid() {
    let chart_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("chart/Chart.yaml");
    assert!(chart_path.exists(), "chart/Chart.yaml not found");

    let content = std::fs::read_to_string(&chart_path).expect("Failed to read Chart.yaml");
    let chart: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&content).expect("Failed to parse Chart.yaml");

    assert_eq!(
        chart["apiVersion"].as_str().unwrap(),
        "v2",
        "Chart apiVersion should be v2"
    );
}
