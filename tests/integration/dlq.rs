// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! DLQ (Dead Letter Queue) integration tests
//!
//! Tests unified DLQ from rustlib with file and Kafka backends.

use dfe_loader::config::DlqConfig;

// ============================================================================
// DLQ Config Tests
// ============================================================================

#[test]
fn test_dlq_config_default() {
    let config = DlqConfig::default();
    assert!(config.enabled);
    assert_eq!(config.topic_suffix, ".dlq");
    assert_eq!(config.mode, "cascade");
    assert!(config.file_enabled);
    assert!(config.kafka_enabled);
}

#[test]
fn test_dlq_config_custom_suffix() {
    let config = DlqConfig {
        enabled: true,
        topic_suffix: "_errors".to_string(),
        ..DlqConfig::default()
    };
    assert_eq!(config.topic_suffix, "_errors");
}

#[test]
fn test_dlq_config_to_rustlib() {
    let config = DlqConfig::default();
    let rustlib_config = config.to_rustlib_config();
    assert!(rustlib_config.enabled);
    assert_eq!(rustlib_config.mode, hyperi_rustlib::dlq::DlqMode::Cascade);
    assert!(rustlib_config.file.enabled);
}

#[test]
fn test_dlq_config_file_only_mode() {
    let config = DlqConfig {
        mode: "file_only".to_string(),
        kafka_enabled: false,
        ..DlqConfig::default()
    };
    let rustlib_config = config.to_rustlib_config();
    assert_eq!(rustlib_config.mode, hyperi_rustlib::dlq::DlqMode::FileOnly);
}

#[test]
fn test_dlq_file_backend_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = DlqConfig {
        mode: "file_only".to_string(),
        file_path: dir.path().to_str().unwrap().to_string(),
        kafka_enabled: false,
        ..DlqConfig::default()
    };

    let rustlib_config = config.to_rustlib_config();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let shutdown = tokio_util::sync::CancellationToken::new();
        let dlq =
            hyperi_rustlib::dlq::Dlq::spawn(&rustlib_config, "loader", None, shutdown.clone())
                .expect("create file-only DLQ");

        let entry =
            hyperi_rustlib::dlq::DlqEntry::new("loader", "parse_error", b"bad data".to_vec())
                .with_destination("acme.auth")
                .with_source(hyperi_rustlib::dlq::DlqSource::kafka("events", 1, 42));

        dlq.send(entry).await.expect("DLQ send");
        dlq.flush().await.expect("DLQ flush");
    });

    // Verify NDJSON file was created
    let content =
        std::fs::read_to_string(dir.path().join("loader/dlq.ndjson")).expect("read DLQ file");
    assert!(!content.is_empty());
    let parsed: serde_json::Value = serde_json::from_str(content.trim()).expect("parse JSON");
    assert_eq!(parsed["service"], "loader");
    assert_eq!(parsed["reason"], "parse_error");
}
