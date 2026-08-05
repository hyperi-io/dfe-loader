// Project:   dfe-loader
// File:      src/config/credentials.rs
// Purpose:   Resolve env:/vault:/literal credential specs in ClickHouseConfig
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! Post-load credential resolution for the loader's `ClickHouseConfig`.
//!
//! Run once at startup, after `Config::load()` and before
//! `From<&loader::ClickHouseConfig>` is invoked to build the internal
//! `clickhouse::ClickHouseConfig`.

use scalo::config::sensitive::SensitiveString;
use scalo::secrets::{CredentialError, resolve};

use crate::config::loader::ClickHouseConfig;

/// Resolve `username` and `password` spec strings into their plaintext values.
///
/// `env:VAR_NAME` reads `$VAR_NAME` (hard error if unset). `vault:path:key`
/// fetches from OpenBao (returns `VaultUnsupported` because the loader enables
/// scalo's `secrets` feature but NOT `secrets-vault`). Any other string is used
/// literally.
pub async fn resolve_clickhouse_credentials(
    cfg: &mut ClickHouseConfig,
) -> Result<(), CredentialError> {
    cfg.username = resolve(&cfg.username).await?;
    let pwd = resolve(cfg.password.expose()).await?;
    cfg.password = SensitiveString::from(pwd);
    Ok(())
}

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn base_cfg() -> ClickHouseConfig {
        let mut c = ClickHouseConfig::default();
        c.username = "default".to_string();
        c.password = SensitiveString::default();
        c
    }

    #[tokio::test]
    async fn literal_values_pass_through_unchanged() {
        let mut c = base_cfg();
        c.username = "alice".to_string();
        c.password = SensitiveString::from("plain-text".to_string());
        resolve_clickhouse_credentials(&mut c).await.unwrap();
        assert_eq!(c.username, "alice");
        assert_eq!(c.password.expose(), "plain-text");
    }

    #[tokio::test]
    async fn env_specs_get_resolved() {
        // SAFETY: test-only; uses unique var names to avoid interference with parallel tests
        unsafe { std::env::set_var("DFE_LOADER_TEST_CH_USER", "carol") };
        unsafe { std::env::set_var("DFE_LOADER_TEST_CH_PASS", "s3cret!") };

        let mut c = base_cfg();
        c.username = "env:DFE_LOADER_TEST_CH_USER".to_string();
        c.password = SensitiveString::from("env:DFE_LOADER_TEST_CH_PASS".to_string());

        resolve_clickhouse_credentials(&mut c).await.unwrap();
        assert_eq!(c.username, "carol");
        assert_eq!(c.password.expose(), "s3cret!");

        unsafe { std::env::remove_var("DFE_LOADER_TEST_CH_USER") };
        unsafe { std::env::remove_var("DFE_LOADER_TEST_CH_PASS") };
    }

    #[tokio::test]
    async fn missing_env_returns_clear_error() {
        let mut c = base_cfg();
        c.password = SensitiveString::from("env:DFE_LOADER_NONEXISTENT_VAR_XYZ".to_string());
        let err = resolve_clickhouse_credentials(&mut c).await.unwrap_err();
        match err {
            CredentialError::MissingEnvVar { name } => {
                assert_eq!(name, "DFE_LOADER_NONEXISTENT_VAR_XYZ");
            }
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn vault_spec_without_secrets_feature_errors_cleanly() {
        let mut c = base_cfg();
        c.password = SensitiveString::from("vault:secret/ch:password".to_string());
        let err = resolve_clickhouse_credentials(&mut c).await.unwrap_err();
        assert!(matches!(err, CredentialError::VaultUnsupported));
    }
}
