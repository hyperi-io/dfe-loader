// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! HTTPS to `ClickHouse` is verified against the system trust store.
//!
//! A private CA reaches the loader's client the only way the config allows,
//! through the trust store (`SSL_CERT_FILE` here), so the same server is
//! accepted when its CA is there and refused when it is not, when another CA
//! is, and when the host name is not the one the certificate names.
//!
//! Gated behind `#[cfg(feature = "testcontainers")]`. Needs process-per-test
//! (nextest): it sets `SSL_CERT_FILE` for the whole process.

#![cfg(feature = "testcontainers")]

use std::path::Path;

use dfe_loader::clickhouse::client_http::build_client;
use dfe_loader::clickhouse::{ClickHouseConfig, ClickHouseQueryClient, Transport};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, IsCa, KeyPair, KeyUsagePurpose,
};

use super::grpc_outage::free_port;
use crate::common::containers::start_clickhouse_https;
use crate::test_name;

/// A self-signed CA named `name`.
fn private_ca(name: &str) -> CertifiedIssuer<'static, KeyPair> {
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.distinguished_name.push(DnType::CommonName, name);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    CertifiedIssuer::self_signed(params, KeyPair::generate().expect("CA key")).expect("CA cert")
}

/// A server certificate for `localhost` only, signed by `ca`, as (cert, key) PEM.
fn localhost_cert(ca: &CertifiedIssuer<'static, KeyPair>) -> (String, String) {
    let key = KeyPair::generate().expect("server key");
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
    params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let cert = params.signed_by(&key, ca).expect("server cert");
    (cert.pem(), key.serialize_pem())
}

/// Two distinct free host ports.
fn two_free_ports() -> (u16, u16) {
    let first = free_port();
    let _held = std::net::TcpListener::bind(("127.0.0.1", first)).expect("hold the first port");
    (first, free_port())
}

fn config(host: &str, port: u16, tls: bool) -> ClickHouseConfig {
    ClickHouseConfig {
        hosts: vec![format!("{host}:{port}")],
        transport: Transport::Http,
        database: "default".to_string(),
        tls,
        ..Default::default()
    }
}

/// Point the system trust store at `bundle`, or back at the platform's own.
fn trust(bundle: Option<&Path>) {
    // SAFETY: test-only; nextest runs this test in a process of its own.
    unsafe {
        std::env::remove_var("SSL_CERT_DIR");
        match bundle {
            Some(path) => std::env::set_var("SSL_CERT_FILE", path),
            None => std::env::remove_var("SSL_CERT_FILE"),
        }
    }
}

/// Every message in `err`'s source chain, so a TLS refusal is visible past
/// the transport's own wording.
fn chain(err: &(dyn std::error::Error + 'static)) -> String {
    let mut out = err.to_string();
    let mut next = err.source();
    while let Some(e) = next {
        out.push_str(" <- ");
        out.push_str(&e.to_string());
        next = e.source();
    }
    out
}

/// `SELECT 1` through the loader's client, with the trust store as it now stands.
async fn select_one(config: &ClickHouseConfig) -> Result<(), String> {
    let client = build_client(config).map_err(|e| e.to_string())?;
    client
        .query("SELECT 1")
        .execute()
        .await
        .map_err(|e| chain(&e))
}

#[tokio::test(flavor = "multi_thread")]
async fn https_is_verified_against_the_system_trust_store() {
    let ca = private_ca("dfe-loader test CA");
    let other_ca = private_ca("unrelated CA");
    let (cert, key) = localhost_cert(&ca);
    let (http_port, https_port) = two_free_ports();
    let _clickhouse = start_clickhouse_https(test_name!(), cert, key, http_port, https_port).await;

    let dir = tempfile::tempdir().expect("CA bundle directory");
    let ca_bundle = dir.path().join("private-ca.pem");
    let other_bundle = dir.path().join("other-ca.pem");
    std::fs::write(&ca_bundle, ca.pem()).expect("write CA bundle");
    std::fs::write(&other_bundle, other_ca.pem()).expect("write other CA bundle");

    trust(Some(&ca_bundle));
    let with_its_ca = select_one(&config("localhost", https_port, true)).await;
    let query_client = ClickHouseQueryClient::new(&config("localhost", https_port, true))
        .expect("query client")
        .health_check()
        .await;
    let wrong_name = select_one(&config("127.0.0.1", https_port, true)).await;
    let plain_http = select_one(&config("localhost", http_port, false)).await;

    trust(Some(&other_bundle));
    let wrong_ca = select_one(&config("localhost", https_port, true)).await;

    trust(None);
    let no_ca = select_one(&config("localhost", https_port, true)).await;

    assert!(
        with_its_ca.is_ok(),
        "HTTPS was refused with the server's CA in the trust store: {with_its_ca:?}"
    );
    assert!(
        query_client.is_ok(),
        "the query client was refused with the server's CA in the trust store: {query_client:?}"
    );
    assert!(
        plain_http.is_ok(),
        "plain HTTP with TLS off failed: {plain_http:?}"
    );
    for (case, result) in [
        ("a host name the certificate does not name", &wrong_name),
        ("only an unrelated CA in the trust store", &wrong_ca),
        ("the platform trust store alone", &no_ca),
    ] {
        let err = result
            .as_ref()
            .expect_err(&format!("HTTPS was accepted with {case}"));
        // A platform store with no CA in it fails at the client build instead.
        assert!(
            err.contains("invalid peer certificate")
                || err.contains("no CA certificate could be read"),
            "with {case} the request failed for a reason other than certificate \
             verification: {err}"
        );
    }
}
