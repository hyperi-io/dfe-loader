// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Deployment artifact generation tests
//!
//! Verifies that the generated Dockerfile and compose fragment match the
//! contract values.

use std::path::Path;

use dfe_loader::config::Config;
use scalo::deployment::{generate_compose_fragment, generate_dockerfile, validate_dockerfile};

fn contract() -> scalo::deployment::DeploymentContract {
    Config::deployment_contract()
}

#[test]
fn test_generated_dockerfile_matches_existing() {
    let contract = contract();
    let generated = generate_dockerfile(&contract, None);

    // Verify key contract points are present. base_image is cascade-resolved
    // (deployment.base_image config/env wins, else scalo's DEFAULT_BASE_IMAGE),
    // so assert the FROM uses the contract's resolved value rather than a pinned
    // distro.
    assert!(
        generated.contains(&format!("FROM {}", contract.base_image)),
        "Should use the contract base_image: {}",
        contract.base_image
    );
    assert!(
        generated.contains("COPY dfe-loader /usr/local/bin/dfe-loader"),
        "Should COPY binary"
    );
    assert!(
        generated.contains("EXPOSE 9090"),
        "Should expose metrics port"
    );
    assert!(
        generated.contains("localhost:9090/livez"),
        "Should healthcheck against liveness endpoint"
    );
    assert!(
        generated.contains("ENTRYPOINT [\"dfe-loader\"]"),
        "Should set entrypoint to binary"
    );
    assert!(
        generated.contains("CMD [\"--config\", \"/etc/dfe/loader.yaml\"]"),
        "Should set CMD from entrypoint_args"
    );
}

/// The library chart injects each secret env var the contract declares, and
/// `Config::load` reads exactly these names.
#[test]
fn test_contract_declares_secret_env_vars() {
    let contract = contract();
    let env_vars: Vec<&str> = contract
        .secrets
        .iter()
        .flat_map(|group| group.env_vars.iter().map(|env| env.env_var.as_str()))
        .collect();
    for name in [
        "DFE_LOADER__KAFKA__SASL__USERNAME",
        "DFE_LOADER__KAFKA__SASL__PASSWORD",
        "DFE_LOADER__CLICKHOUSE__PASSWORD",
    ] {
        assert!(
            env_vars.contains(&name),
            "contract should declare {name}: {env_vars:?}"
        );
    }
}

#[test]
fn test_generated_compose_fragment() {
    let contract = contract();
    let compose = generate_compose_fragment(&contract);

    assert!(compose.contains("dfe-loader:"));
    assert!(compose.contains("ghcr.io/hyperi-io/dfe-loader"));
    assert!(compose.contains("9090:9090"));
    assert!(compose.contains("kafka:"));
    assert!(compose.contains("clickhouse:"));
    assert!(compose.contains("localhost:9090/livez"));
}

#[test]
fn test_validate_generated_dockerfile() {
    let contract = contract();
    let dockerfile = generate_dockerfile(&contract, None);

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("Dockerfile");
    std::fs::write(&path, &dockerfile).unwrap();

    // Generated Dockerfile should pass validation against its own contract
    let result = validate_dockerfile(&contract, path.to_str().unwrap());
    assert!(
        result.is_ok(),
        "Generated Dockerfile should validate: {result:?}"
    );
}

/// Write generated artifacts to .tmp/ for manual comparison.
/// Run with: cargo test --test deployment -- `write_artifacts` --ignored --nocapture
#[test]
#[ignore = "writes to /tmp — manual verification only"]
fn write_artifacts_to_tmp() {
    let contract = contract();
    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp/generated");
    std::fs::create_dir_all(&base).unwrap();

    // Dockerfile
    let dockerfile = generate_dockerfile(&contract, None);
    std::fs::write(base.join("Dockerfile"), &dockerfile).unwrap();

    // Compose fragment
    let compose = generate_compose_fragment(&contract);
    std::fs::write(base.join("compose.service.yaml"), &compose).unwrap();

    eprintln!("Generated artifacts written to: {}", base.display());
}
