// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Config reachability: a setting a deployment can write must reach something.
//!
//! Every test here guards a knob that once parsed cleanly and then did nothing —
//! no error, no warning. The class is worth its own file because the failures
//! look identical from outside: the process starts, logs a healthy line, and
//! runs on a value the operator did not set.
//!
//! The generic one ([`every_declared_secret_env_var_reaches_the_config`]) walks
//! the deployment contract rather than a hand-written list, so a secret added to
//! the contract later is checked without touching this file.

use dfe_loader::config::{Config, KedaConfig, SaslConfig, TlsConfig};
use scalo::config::sensitive::expose_during;
use serde_json::Value;

/// Walk a dotted path through a JSON object.
fn at<'a>(root: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(root, |node, key| node.get(key))
}

/// The config key an env var names, by the cascade's own rules: drop the
/// `DFE_LOADER` prefix and either separator that follows it, then `__` nests.
fn config_key_for(env_var: &str, env_prefix: &str) -> String {
    env_var
        .strip_prefix(env_prefix)
        .unwrap_or(env_var)
        .trim_start_matches('_')
        .to_lowercase()
        .replace("__", ".")
}

fn write_config(dir: &tempfile::TempDir, body: &str) -> String {
    let path = dir.path().join("loader.yaml");
    std::fs::write(&path, body).expect("write config");
    path.to_str().expect("utf-8 path").to_string()
}

// ============================================================================
// Deployment contract <-> config reader
// ============================================================================

/// Every secret the deployment contract declares must land on the config key
/// its name spells out.
///
/// The chart injects the Kafka SASL credentials and the `ClickHouse` password as
/// `DFE_LOADER__SECTION__FIELD`. figment strips exactly `DFE_LOADER_`, so that
/// form used to arrive as the key `_kafka.sasl.username`, which matches no field
/// and serde drops without a word: the pod ran with no SASL and an empty
/// `ClickHouse` password while the chart said otherwise.
///
/// Env vars are process-global, so the whole sweep is one test with set/remove
/// around each case.
#[test]
#[allow(unsafe_code)]
fn every_declared_secret_env_var_reaches_the_config() {
    let contract = Config::deployment_contract();
    let prefix = contract.env_prefix.clone();

    let mut checked = 0;
    for group in &contract.secrets {
        for secret in &group.env_vars {
            let key = config_key_for(&secret.env_var, &prefix);
            let sentinel = format!("reachability-{}", key.replace('.', "-"));

            // SAFETY: test-only; set and removed inside this one sequential test.
            unsafe { std::env::set_var(&secret.env_var, &sentinel) };
            let loaded = Config::load(None).expect("config loads");
            unsafe { std::env::remove_var(&secret.env_var) };

            let json = expose_during(|| serde_json::to_value(&loaded)).expect("config serialises");
            let reached = at(&json, &key).and_then(Value::as_str) == Some(sentinel.as_str());

            // The message carries the env var and group names only, never a value.
            assert!(
                reached,
                "{} ({}) was set and the config key its name spells did not read it",
                secret.env_var, group.group_name
            );
            checked += 1;
        }
    }

    assert!(
        checked >= 3,
        "expected the contract to declare the kafka + clickhouse secrets, checked {checked}"
    );
}

/// The KEDA half of the contract must come from this crate's own `KedaConfig`.
///
/// It was `KedaContract::default()` — scalo's own numbers — while `KedaConfig`
/// documented itself as the source. The two agreed, which is exactly why
/// nothing caught it.
#[test]
fn keda_contract_tracks_this_crate_s_keda_defaults() {
    let keda = KedaConfig::default();
    let contract = Config::deployment_contract()
        .keda
        .expect("contract carries a KEDA section");

    assert_eq!(contract.enabled, keda.enabled);
    assert_eq!(contract.min_replicas, keda.min_replicas);
    assert_eq!(contract.max_replicas, keda.max_replicas);
    assert_eq!(contract.polling_interval, keda.polling_interval);
    assert_eq!(contract.cooldown_period, keda.cooldown_period);
    assert_eq!(contract.kafka_lag_threshold, keda.kafka_lag_threshold);
    assert_eq!(
        contract.activation_lag_threshold,
        keda.activation_lag_threshold
    );
    assert_eq!(contract.cpu_enabled, keda.cpu_enabled);
    assert_eq!(contract.cpu_threshold, keda.cpu_threshold);
}

/// Raw consumer-group lag rises when a downstream stage breaks, so the contract
/// must never scale the loader on it.
#[test]
fn keda_scales_on_cpu_and_never_on_kafka_lag() {
    let contract = Config::deployment_contract()
        .keda
        .expect("contract carries a KEDA section");
    assert!(
        !contract.kafka_trigger.enabled,
        "the deployment contract turned the Kafka lag trigger back on"
    );
    assert!(
        contract.cpu_enabled,
        "the deployment contract lost its CPU trigger"
    );
}

// ============================================================================
// Kafka SASL
// ============================================================================

/// `enabled: false` must survive `normalize()`.
///
/// The auto-enable test included `!mechanism.is_empty()`, and `mechanism` carries
/// a non-empty serde default, so the condition was constant-true: every config
/// with a `sasl:` block at all came out enabled.
#[test]
fn sasl_enabled_false_stays_false() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\n  sasl:\n    enabled: false\n",
    );
    let config = Config::load(Some(&path)).expect("config loads");
    assert_eq!(
        config.kafka.sasl.map(|s| s.enabled),
        Some(false),
        "an explicit kafka.sasl.enabled: false must not be flipped on"
    );
}

/// Credentials with no explicit `enabled` still turn SASL on — that side-effect
/// is what makes the chart's two secret env vars sufficient on their own.
#[test]
fn sasl_credentials_alone_enable_it() {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let path = write_config(
        &dir,
        "kafka:\n  brokers: [broker:9092]\n  sasl:\n    username: u\n    password: pw\n",
    );
    let config = Config::load(Some(&path)).expect("config loads");
    assert_eq!(config.kafka.sasl.map(|s| s.enabled), Some(true));
}

/// `Config::validate()` must run the SASL checks. They existed with no caller
/// outside their own unit tests, so a SCRAM config with no username started
/// cleanly and failed at the broker.
#[test]
fn scram_without_credentials_fails_startup_validation() {
    let mut config = Config::default();
    config.kafka.sasl = Some(SaslConfig {
        enabled: true,
        mechanism: "scram_sha_512".into(),
        ..SaslConfig::default()
    });
    let err = config.validate().expect_err("must fail").to_string();
    assert!(
        err.contains("requires username"),
        "expected the SASL username check to reach startup, got: {err}"
    );
}

/// An unrecognised mechanism string must be an error, not a quiet fall back.
/// `mechanism: gssapi` used to connect as SCRAM-SHA-512.
#[test]
fn unknown_sasl_mechanism_is_rejected_not_defaulted() {
    let mut config = Config::default();
    config.kafka.sasl = Some(SaslConfig {
        enabled: true,
        mechanism: "gssapi".into(),
        username: "u".into(),
        password: "pw".into(),
        ..SaslConfig::default()
    });
    let err = config.validate().expect_err("must fail").to_string();
    assert!(
        err.contains("unknown kafka.sasl.mechanism 'gssapi'"),
        "expected a named rejection, got: {err}"
    );
}

/// The `oauth_*` and `aws_*` blocks reach no client, so the mechanisms that
/// need them must be refused rather than connecting as OAUTHBEARER with an
/// empty username.
#[test]
fn mechanisms_with_no_wiring_are_rejected() {
    for (mechanism, needle) in [
        ("oauthbearer", "oauth_token_endpoint"),
        ("aws_msk_iam", "aws_region"),
    ] {
        let mut config = Config::default();
        config.kafka.sasl = Some(SaslConfig {
            enabled: true,
            mechanism: mechanism.into(),
            oauth_token_endpoint: Some("https://auth.example.com/token".into()),
            oauth_client_id: Some("client".into()),
            aws_region: Some("ap-southeast-2".into()),
            ..SaslConfig::default()
        });
        let err = config.validate().expect_err("must fail").to_string();
        assert!(
            err.contains("not supported by this build") && err.contains(needle),
            "expected '{mechanism}' to be refused by name, got: {err}"
        );
    }
}

// ============================================================================
// ClickHouse TLS
// ============================================================================

/// Only `clickhouse.tls.enabled` reaches the client. The other four fields were
/// accepted and dropped, so a private-CA bundle or skip_verify read as applied.
#[test]
fn clickhouse_tls_fields_with_no_reader_are_rejected() {
    for (label, tls) in [
        (
            "ca_cert_file",
            TlsConfig {
                enabled: true,
                ca_cert_file: Some("/certs/ca.pem".into()),
                ..TlsConfig::default()
            },
        ),
        (
            "skip_verify",
            TlsConfig {
                enabled: true,
                skip_verify: true,
                ..TlsConfig::default()
            },
        ),
    ] {
        let mut config = Config::default();
        config.clickhouse.tls = Some(tls);
        let err = config.validate().expect_err("must fail").to_string();
        assert!(
            err.contains(&format!("clickhouse.tls.{label}")),
            "expected clickhouse.tls.{label} to be named as unread, got: {err}"
        );
    }

    // enabled on its own is honoured and must stay accepted.
    let mut config = Config::default();
    config.clickhouse.tls = Some(TlsConfig {
        enabled: true,
        ..TlsConfig::default()
    });
    config
        .validate()
        .expect("clickhouse.tls.enabled alone is honoured");
}

// ============================================================================
// Env-var cascade
// ============================================================================

/// Both separator forms after the prefix must reach the same key, and
/// `DFE_LOADER_CONFIG` must name the config file the cascade docs say it does.
///
/// One test because env vars are process-global.
#[test]
#[allow(unsafe_code)]
fn env_cascade_forms_reach_the_config() {
    // Chart / deployment-contract form: DFE_LOADER__SECTION__FIELD.
    {
        // SAFETY: test-only; removed at the end of the block.
        unsafe { std::env::set_var("DFE_LOADER__ROUTING__DEFAULT_TABLE", "double_sep") };
        let config = Config::load(None).expect("config loads");
        unsafe { std::env::remove_var("DFE_LOADER__ROUTING__DEFAULT_TABLE") };
        assert_eq!(config.routing.default_table, "double_sep");
    }

    // Documented single-separator form: DFE_LOADER_SECTION__FIELD.
    {
        unsafe { std::env::set_var("DFE_LOADER_ROUTING__DEFAULT_TABLE", "single_sep") };
        let config = Config::load(None).expect("config loads");
        unsafe { std::env::remove_var("DFE_LOADER_ROUTING__DEFAULT_TABLE") };
        assert_eq!(config.routing.default_table, "single_sep");
    }

    // The scaling weights the generated config-schema docs advertise as
    // DFE_LOADER__SCALING__* -- these do feed a runtime signal.
    {
        unsafe { std::env::set_var("DFE_LOADER__SCALING__WEIGHT_KAFKA_LAG", "0.45") };
        let config = Config::load(None).expect("config loads");
        unsafe { std::env::remove_var("DFE_LOADER__SCALING__WEIGHT_KAFKA_LAG") };
        assert!((config.scaling.weight_kafka_lag - 0.45).abs() < f64::EPSILON);
    }

    // DFE_LOADER_CONFIG names the config file when --config is absent.
    {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = write_config(&dir, "routing:\n  default_table: from_env_named_file\n");
        unsafe { std::env::set_var("DFE_LOADER_CONFIG", &path) };
        let config = Config::load(None).expect("config loads");
        unsafe { std::env::remove_var("DFE_LOADER_CONFIG") };
        assert_eq!(config.routing.default_table, "from_env_named_file");
    }

    // --config still wins over DFE_LOADER_CONFIG.
    {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let env_path = write_config(&dir, "routing:\n  default_table: from_env\n");
        let flag_dir = tempfile::TempDir::new().expect("tempdir");
        let flag_path = write_config(&flag_dir, "routing:\n  default_table: from_flag\n");
        unsafe { std::env::set_var("DFE_LOADER_CONFIG", &env_path) };
        let config = Config::load(Some(&flag_path)).expect("config loads");
        unsafe { std::env::remove_var("DFE_LOADER_CONFIG") };
        assert_eq!(config.routing.default_table, "from_flag");
    }
}
