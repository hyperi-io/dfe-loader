// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Helm chart + Dockerfile contract sync tests
//!
//! Uses scalo's `DeploymentContract` to validate that
//! `chart/values.yaml` and `Dockerfile` stay in sync with app defaults.
//!
//! If you change a default port, health path, or KEDA threshold in
//! the Rust config, these tests fail until the chart/Dockerfile are
//! updated (or vice versa).

use std::path::Path;

use dfe_loader::config::Config;
use scalo::deployment::DeploymentContract;

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

    let mismatches = scalo::deployment::validate_helm_values(&contract, &chart_dir).unwrap();

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

    let mismatches = scalo::deployment::validate_dockerfile(&contract, &dockerfile).unwrap();

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

// ============================================================================
// CI feature coverage
// ============================================================================

/// Every cargo feature that gates a TEST must appear in a `.hyperi-ci.yaml`
/// feature set, or those tests are compiled out of every CI run and the run
/// still reports green.
///
/// `default = []`, so both named features here are off unless a feature set
/// asks for them. `transport-memory` gates `MemoryTransportAdapter`, the
/// `#[cfg(all(test, feature = "transport-memory"))]` module in
/// `src/kafka/transport.rs`, and `tests/unit/transport.rs`; `testcontainers`
/// gates the container-backed integration modules.
///
/// hyperi-ci's test stage reads `test.rust.features` and falls back to
/// `quality.rust.features`, which is why both keys are checked.
///
/// Asserts on the named features rather than parsing every `#[cfg(feature)]`
/// in the tree -- these specific gates are the contract, not a generic lint.
#[test]
fn test_ci_config_covers_test_gating_features() {
    let ci_path = Path::new(env!("CARGO_MANIFEST_DIR")).join(".hyperi-ci.yaml");
    let content = std::fs::read_to_string(&ci_path).expect("Failed to read .hyperi-ci.yaml");
    let ci: serde_yaml_ng::Value =
        serde_yaml_ng::from_str(&content).expect("Failed to parse .hyperi-ci.yaml");

    // hyperi-ci cascade: test.rust.features wins, else quality.rust.features.
    let sets = ci["test"]["rust"]["features"]
        .as_sequence()
        .or_else(|| ci["quality"]["rust"]["features"].as_sequence())
        .expect("no rust feature-set list in .hyperi-ci.yaml");

    let covered = |feature: &str| {
        sets.iter()
            .filter_map(serde_yaml_ng::Value::as_str)
            .any(|s| s.split(',').any(|f| f.trim() == feature))
    };

    for feature in ["transport-memory", "testcontainers"] {
        assert!(
            covered(feature),
            "cargo feature '{feature}' gates test code but no .hyperi-ci.yaml \
             feature set enables it, so those tests are compiled out of every \
             CI run. Add \"default,{feature}\" to quality.rust.features."
        );
    }
}
