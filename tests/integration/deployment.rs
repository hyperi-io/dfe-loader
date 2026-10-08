// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Deployment artifact generation tests
//!
//! Verifies that generated Dockerfile and Helm chart match the contract values.

use std::path::Path;

use dfe_loader::config::Config;
use scalo::deployment::{
    generate_chart, generate_compose_fragment, generate_dockerfile, validate_dockerfile,
    validate_helm_values,
};

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

#[test]
fn test_generated_chart_structure() {
    let contract = contract();
    let dir = tempfile::tempdir().unwrap();
    generate_chart(&contract, dir.path(), None).unwrap();

    // Chart.yaml
    let chart = std::fs::read_to_string(dir.path().join("Chart.yaml")).unwrap();
    assert!(chart.contains("name: dfe-loader"));
    assert!(chart.contains("description: High-performance Kafka to ClickHouse data loader"));

    // values.yaml
    let values = std::fs::read_to_string(dir.path().join("values.yaml")).unwrap();
    assert!(values.contains("repository: ghcr.io/hyperi-io/dfe-loader"));
    assert!(values.contains("port: 9090"));

    // Secret sections
    assert!(
        values.contains("kafka:"),
        "Should have kafka secret section"
    );
    assert!(
        values.contains("clickhouse:"),
        "Should have clickhouse secret section"
    );
    assert!(
        values.contains("existingSecret:"),
        "Should support existing secrets"
    );

    // KEDA section
    assert!(values.contains("keda:"), "Should have KEDA section");

    // templates/
    let templates = dir.path().join("templates");
    assert!(templates.join("_helpers.tpl").exists());
    assert!(templates.join("deployment.yaml").exists());
    assert!(templates.join("service.yaml").exists());
    assert!(templates.join("serviceaccount.yaml").exists());
    assert!(templates.join("configmap.yaml").exists());
    assert!(templates.join("secret.yaml").exists());
    assert!(templates.join("hpa.yaml").exists());
    assert!(templates.join("keda-scaledobject.yaml").exists());
    assert!(templates.join("keda-triggerauth.yaml").exists());
    assert!(templates.join("NOTES.txt").exists());
}

#[test]
fn test_generated_chart_helpers() {
    let contract = contract();
    let dir = tempfile::tempdir().unwrap();
    generate_chart(&contract, dir.path(), None).unwrap();

    let helpers = std::fs::read_to_string(dir.path().join("templates/_helpers.tpl")).unwrap();
    assert!(
        helpers.contains("kafkaSecretName"),
        "Should have kafka secret helper"
    );
    assert!(
        helpers.contains("clickhouseSecretName"),
        "Should have clickhouse secret helper"
    );
}

#[test]
fn test_generated_deployment_has_env_vars() {
    let contract = contract();
    let dir = tempfile::tempdir().unwrap();
    generate_chart(&contract, dir.path(), None).unwrap();

    let deploy = std::fs::read_to_string(dir.path().join("templates/deployment.yaml")).unwrap();
    assert!(
        deploy.contains("DFE_LOADER__KAFKA__SASL__USERNAME"),
        "Should inject kafka username env"
    );
    assert!(
        deploy.contains("DFE_LOADER__KAFKA__SASL__PASSWORD"),
        "Should inject kafka password env"
    );
    assert!(
        deploy.contains("DFE_LOADER__CLICKHOUSE__PASSWORD"),
        "Should inject clickhouse password env"
    );
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

#[test]
fn test_validate_generated_chart() {
    let contract = contract();
    let dir = tempfile::tempdir().unwrap();
    generate_chart(&contract, dir.path(), None).unwrap();

    // Generated chart should pass validation against its own contract
    let result = validate_helm_values(&contract, dir.path().to_str().unwrap());
    assert!(
        result.is_ok(),
        "Generated chart should validate: {result:?}"
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

    // Helm chart
    let chart_dir = base.join("chart");
    let _ = std::fs::remove_dir_all(&chart_dir);
    generate_chart(&contract, &chart_dir, None).unwrap();

    eprintln!("Generated artifacts written to: {}", base.display());
}
