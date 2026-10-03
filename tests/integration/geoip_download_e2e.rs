// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! HTTP-emulated integration tests for `GeoIP` database download.
//!
//! Exercises the download mechanics (HTTP GET, gzip decompression, tar
//! extraction, atomic write) against a local in-process HTTP server and
//! drives the public `ensure_databases()` entry point for every branch
//! that does not require external network access.
//!
//! ## Why a hand-rolled HTTP/1.1 server
//!
//! `axum` and `hyper` are pulled in transitively via `scalo` but are
//! not direct dev-dependencies and the task forbids modifying production
//! `Cargo.toml`. A minimal `tokio::net::TcpListener` fixture covers everything
//! these tests need (fixed-size response bodies, custom status codes, early
//! connection close for partial-download tests) without adding any crates.
//!
//! ## What is covered
//!
//! The private download helpers (`download_gzipped`, `download_tar_gz`,
//! `download_raw`) are not `pub`, and every public provider in `ensure_databases`
//! hardcodes a real-world URL (e.g. `https://download.db-ip.com/...`). Because
//! of this we cannot point `ensure_databases` at the local test server —
//! instead we:
//!
//! 1. Drive `ensure_databases()` through every branch that does not require
//!    external network (Custom provider, auto-download disabled, existing
//!    fresh files, missing credentials, stale files with no network).
//! 2. Reproduce the download protocol (reqwest GET → gzip → atomic write,
//!    reqwest GET → gzip → tar → atomic write, reqwest GET → raw → atomic
//!    write) against the local test server. This proves the same wire
//!    behaviour the production helpers rely on and guards against regressions
//!    in the transitively-pinned `reqwest`/`flate2`/`tar` versions.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use dfe_loader::config::{AutoDownloadConfig, GeoIpConfig, GeoIpProvider};
use dfe_loader::enrich::geoip_download::{DatabasePaths, GeoIpDownloadError, ensure_databases};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

// ============================================================================
// Local HTTP/1.1 test server
// ============================================================================

/// A scripted response the server will return for the next request.
#[derive(Clone)]
enum Response {
    /// Status code with raw body bytes.
    Body { status: u16, body: Vec<u8> },
    /// Status code with no body.
    StatusOnly { status: u16 },
    /// Accept request then close the TCP connection after writing `body` without
    /// sending the declared `Content-Length` (simulates a mid-stream abort).
    Truncated {
        status: u16,
        declared_len: usize,
        body: Vec<u8>,
    },
}

/// Scope-local HTTP server. Dropping `ServerHandle` triggers shutdown.
struct ServerHandle {
    addr: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl ServerHandle {
    fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("http://{}/{}", self.addr, path)
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        // Don't block on join — the spawned tasks are cooperative.
        if let Some(join) = self.join.take() {
            join.abort();
        }
    }
}

/// Spawn a scripted HTTP server that serves the given path→response map.
///
/// Unknown paths return 404. The server shuts down when the returned handle
/// is dropped.
async fn spawn_test_server(routes: Vec<(String, Response)>) -> ServerHandle {
    let routes = Arc::new(routes);
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test server");
    let addr = listener.local_addr().expect("local_addr");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let join = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_rx => break,
                accept = listener.accept() => {
                    let Ok((stream, _)) = accept else { continue };
                    let routes = Arc::clone(&routes);
                    tokio::spawn(async move {
                        let _ = handle_connection(stream, &routes).await;
                    });
                }
            }
        }
    });

    ServerHandle {
        addr,
        shutdown: Some(shutdown_tx),
        join: Some(join),
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    routes: &[(String, Response)],
) -> std::io::Result<()> {
    // Read request bytes until we see the end of the headers. That's enough —
    // we ignore the body.
    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 1024];
    let request_path = loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(end) = find_double_crlf(&buf) {
            let head = &buf[..end];
            let head_str = std::str::from_utf8(head).unwrap_or("");
            let first_line = head_str.lines().next().unwrap_or("");
            // "GET /foo HTTP/1.1"
            let mut parts = first_line.split_whitespace();
            let _method = parts.next();
            let path = parts.next().unwrap_or("/").to_string();
            break path;
        }
        if buf.len() > 64 * 1024 {
            return Ok(());
        }
    };

    // Match path (ignoring leading slash for lookups)
    let normalised = request_path.trim_start_matches('/');
    let matched = routes
        .iter()
        .find(|(p, _)| p.trim_start_matches('/') == normalised);

    match matched {
        Some((_, Response::Body { status, body })) => {
            write_response(&mut stream, *status, body.len(), Some(body)).await?;
        }
        Some((_, Response::StatusOnly { status })) => {
            write_response(&mut stream, *status, 0, None).await?;
        }
        Some((
            _,
            Response::Truncated {
                status,
                declared_len,
                body,
            },
        )) => {
            // Write header advertising `declared_len`, then write only `body`
            // (which is shorter) and close the connection without flushing a
            // trailer. reqwest should surface this as an error.
            write_headers(&mut stream, *status, *declared_len).await?;
            stream.write_all(body).await?;
            stream.flush().await?;
            // Drop the stream to close the connection mid-stream.
        }
        None => {
            write_response(&mut stream, 404, 0, None).await?;
        }
    }

    Ok(())
}

async fn write_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_length: usize,
    body: Option<&[u8]>,
) -> std::io::Result<()> {
    write_headers(stream, status, content_length).await?;
    if let Some(bytes) = body {
        stream.write_all(bytes).await?;
    }
    stream.flush().await?;
    Ok(())
}

async fn write_headers(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_length: usize,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Status",
    };
    let header = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Length: {content_length}\r\n\
         Connection: close\r\n\
         \r\n",
    );
    stream.write_all(header.as_bytes()).await?;
    Ok(())
}

fn find_double_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

// ============================================================================
// Fixture builders
// ============================================================================

/// Gzip-compress the given bytes.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip encode");
    encoder.finish().expect("gzip finish")
}

/// Build a tar.gz archive containing a single file at the given name inside
/// a directory prefix (matching the real MaxMind tar layout).
///
/// Output: gzip(tar containing `GeoLite2-City_20241231/<filename>` entry)
fn build_tar_gz(inner_dir: &str, filename: &str, contents: &[u8]) -> Vec<u8> {
    let mut tar_bytes = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut tar_bytes);
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        let path = format!("{inner_dir}/{filename}");
        builder
            .append_data(&mut header, path, contents)
            .expect("append tar entry");
        builder.finish().expect("finalise tar");
    }
    gzip(&tar_bytes)
}

/// Build an HTTP client matching what `geoip_download` uses (short timeouts for
/// tests).
fn test_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .connect_timeout(Duration::from_secs(2))
        .user_agent("dfe-loader-test")
        .build()
        .expect("build reqwest client")
}

// ----------------------------------------------------------------------------
// Protocol helpers that mirror the private functions in geoip_download.rs.
// Keeping the helpers here (rather than re-using the private fns) lets us
// test every error path, including truncated responses.
// ----------------------------------------------------------------------------

async fn fetch_gzipped_to(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let response = client.get(url).send().await?.error_for_status()?;
    let bytes = response.bytes().await?;
    let mut decoder = GzDecoder::new(&bytes[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;
    let tmp = dest.with_extension("mmdb.tmp");
    fs::write(&tmp, &decompressed)?;
    fs::rename(&tmp, dest)?;
    Ok(())
}

async fn fetch_tar_gz_to(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    target_filename: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let response = client.get(url).send().await?.error_for_status()?;
    let bytes = response.bytes().await?;
    let decoder = GzDecoder::new(&bytes[..]);
    let mut archive = tar::Archive::new(decoder);
    let mut found = false;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        if let Some(filename) = path.file_name()
            && filename == target_filename
        {
            let tmp = dest.with_extension("mmdb.tmp");
            let mut outfile = fs::File::create(&tmp)?;
            std::io::copy(&mut entry, &mut outfile)?;
            fs::rename(&tmp, dest)?;
            found = true;
            break;
        }
    }
    if !found {
        return Err("target not found in tar".into());
    }
    Ok(())
}

async fn fetch_raw_to(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let response = client.get(url).send().await?.error_for_status()?;
    let bytes = response.bytes().await?;
    let tmp = dest.with_extension("mmdb.tmp");
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, dest)?;
    Ok(())
}

// ============================================================================
// Tests: download mechanics against the local server
// ============================================================================

#[tokio::test]
async fn test_download_gzipped_mmdb() {
    let payload = b"fake mmdb bytes for gzip test";
    let compressed = gzip(payload);
    let server = spawn_test_server(vec![(
        "dbip-city-lite.mmdb.gz".into(),
        Response::Body {
            status: 200,
            body: compressed,
        },
    )])
    .await;

    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("city.mmdb");
    let client = test_http_client();

    fetch_gzipped_to(&client, &server.url("dbip-city-lite.mmdb.gz"), &dest)
        .await
        .expect("download should succeed");

    let written = fs::read(&dest).expect("read downloaded file");
    assert_eq!(written, payload, "downloaded content must match original");
    // Atomic-write cleanup: no stray .tmp next to dest
    assert!(!dest.with_extension("mmdb.tmp").exists());
}

#[tokio::test]
async fn test_download_tar_gz_mmdb() {
    let payload = b"tar-embedded fake mmdb bytes";
    let archive = build_tar_gz("GeoLite2-City_20241231", "GeoLite2-City.mmdb", payload);
    let server = spawn_test_server(vec![(
        "geoip/databases/GeoLite2-City.tar.gz".into(),
        Response::Body {
            status: 200,
            body: archive,
        },
    )])
    .await;

    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("GeoLite2-City.mmdb");
    let client = test_http_client();

    fetch_tar_gz_to(
        &client,
        &server.url("geoip/databases/GeoLite2-City.tar.gz"),
        &dest,
        "GeoLite2-City.mmdb",
    )
    .await
    .expect("tar extraction should succeed");

    let written = fs::read(&dest).expect("read extracted file");
    assert_eq!(written, payload, "extracted content must match original");
    assert!(!dest.with_extension("mmdb.tmp").exists());
}

#[tokio::test]
async fn test_download_raw_mmdb() {
    let payload = b"uncompressed mmdb bytes, exactly this";
    let server = spawn_test_server(vec![(
        "data/ipinfo_lite.mmdb".into(),
        Response::Body {
            status: 200,
            body: payload.to_vec(),
        },
    )])
    .await;

    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("ipinfo-lite.mmdb");
    let client = test_http_client();

    fetch_raw_to(&client, &server.url("data/ipinfo_lite.mmdb"), &dest)
        .await
        .expect("raw download should succeed");

    let written = fs::read(&dest).expect("read raw file");
    assert_eq!(written, payload);
    assert!(!dest.with_extension("mmdb.tmp").exists());
}

#[tokio::test]
async fn test_download_404_error() {
    let server = spawn_test_server(vec![]).await; // every path returns 404
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("city.mmdb");
    let client = test_http_client();

    let err = fetch_gzipped_to(&client, &server.url("missing.mmdb.gz"), &dest)
        .await
        .expect_err("404 must produce an error");
    let msg = err.to_string();
    assert!(
        msg.contains("404") || msg.to_ascii_lowercase().contains("not found"),
        "error should mention 404/not found, got: {msg}",
    );
    assert!(
        !dest.exists(),
        "no destination file should be written on 404"
    );
}

#[tokio::test]
async fn test_download_500_error() {
    let server = spawn_test_server(vec![(
        "broken.mmdb.gz".into(),
        Response::StatusOnly { status: 500 },
    )])
    .await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("city.mmdb");
    let client = test_http_client();

    let err = fetch_gzipped_to(&client, &server.url("broken.mmdb.gz"), &dest)
        .await
        .expect_err("500 must produce an error");
    let msg = err.to_string();
    assert!(
        msg.contains("500") || msg.to_ascii_lowercase().contains("server"),
        "error should mention 500/server error, got: {msg}",
    );
    assert!(!dest.exists());
}

#[tokio::test]
async fn test_download_corrupted_gzip() {
    // 200 OK, body is NOT gzipped — decoder must fail.
    let server = spawn_test_server(vec![(
        "bad.mmdb.gz".into(),
        Response::Body {
            status: 200,
            body: b"this is plainly not a gzip stream".to_vec(),
        },
    )])
    .await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("city.mmdb");
    let client = test_http_client();

    let err = fetch_gzipped_to(&client, &server.url("bad.mmdb.gz"), &dest)
        .await
        .expect_err("corrupt gzip must error");
    // flate2 surfaces "invalid gzip header" / "corrupt deflate stream"
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("invalid") || msg.contains("gzip") || msg.contains("corrupt"),
        "error should mention gzip corruption, got: {msg}",
    );
    assert!(!dest.exists());
}

#[tokio::test]
async fn test_download_corrupted_tar() {
    // 200 OK, body IS valid gzip but inside is NOT a tar archive.
    let not_a_tar = b"this is gzipped but not a tar archive at all";
    let server = spawn_test_server(vec![(
        "bad.tar.gz".into(),
        Response::Body {
            status: 200,
            body: gzip(not_a_tar),
        },
    )])
    .await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("city.mmdb");
    let client = test_http_client();

    let err = fetch_tar_gz_to(
        &client,
        &server.url("bad.tar.gz"),
        &dest,
        "GeoLite2-City.mmdb",
    )
    .await
    .expect_err("corrupt tar must error");
    // Either the tar iterator fails parsing, or we fall through to "not found".
    assert!(
        !dest.exists(),
        "dest must not be written when tar is corrupt or missing entry",
    );
    // Message depends on failure mode — just assert it's non-empty.
    assert_ne!(err.to_string(), "");
}

#[tokio::test]
async fn test_download_tar_missing_target_file() {
    // Tar archive is valid but does NOT contain the target filename.
    let archive = build_tar_gz("some_dir", "README.md", b"not the mmdb");
    let server = spawn_test_server(vec![(
        "missing-file.tar.gz".into(),
        Response::Body {
            status: 200,
            body: archive,
        },
    )])
    .await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("GeoLite2-City.mmdb");
    let client = test_http_client();

    let err = fetch_tar_gz_to(
        &client,
        &server.url("missing-file.tar.gz"),
        &dest,
        "GeoLite2-City.mmdb",
    )
    .await
    .expect_err("missing target in tar must error");
    assert!(err.to_string().contains("not found"));
    assert!(!dest.exists());
}

#[tokio::test]
async fn test_atomic_write_no_partial() {
    // Response promises 1000 bytes but server closes after 20 bytes.
    let server = spawn_test_server(vec![(
        "truncated.mmdb".into(),
        Response::Truncated {
            status: 200,
            declared_len: 1000,
            body: b"first twenty bytes..".to_vec(),
        },
    )])
    .await;
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest = tmp.path().join("truncated.mmdb");
    let client = test_http_client();

    let err = fetch_raw_to(&client, &server.url("truncated.mmdb"), &dest)
        .await
        .expect_err("truncated response must error");
    assert_ne!(err.to_string(), "");
    // Critical: no partial file left at final destination.
    assert!(
        !dest.exists(),
        "partial download must not appear at final dest path",
    );
    // Nor at the temp location — fetch failed before rename.
    assert!(
        !dest.with_extension("mmdb.tmp").exists(),
        "on failure there must be no leftover .tmp",
    );
}

#[tokio::test]
async fn test_server_shuts_down_on_drop() {
    // Verify scope-local server semantics: after the handle is dropped the
    // port is released and subsequent connects fail.
    let addr = {
        let server = spawn_test_server(vec![(
            "ping".into(),
            Response::Body {
                status: 200,
                body: b"pong".to_vec(),
            },
        )])
        .await;
        let client = test_http_client();
        let resp = client
            .get(server.url("ping"))
            .send()
            .await
            .expect("first request works while server is alive");
        assert_eq!(resp.status(), 200);
        server.addr
    }; // server dropped here

    // Give the OS a moment to release the port.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = test_http_client();
    let result = client
        .get(format!("http://{addr}/ping"))
        .timeout(Duration::from_millis(500))
        .send()
        .await;
    assert!(
        result.is_err(),
        "after drop the server must not accept connections",
    );
}

// ============================================================================
// Tests: `ensure_databases()` public API branches (no external network)
// ============================================================================

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build rt")
}

#[test]
fn test_ensure_databases_custom_provider_with_server_paths() {
    // The `Custom` provider short-circuits download logic and simply returns
    // whatever paths were configured. Here we prove the paths flow through
    // unchanged — mirroring what a user would do to point the loader at an
    // MMDB served by our test server after manual download.
    let tmp = tempfile::tempdir().expect("tempdir");
    let city = tmp.path().join("custom-city.mmdb");
    let asn = tmp.path().join("custom-asn.mmdb");

    let config = GeoIpConfig {
        provider: GeoIpProvider::Custom,
        city_db_path: Some(city.to_string_lossy().into_owned()),
        asn_db_path: Some(asn.to_string_lossy().into_owned()),
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert_eq!(paths.city, Some(city));
    assert_eq!(paths.asn, Some(asn));
}

#[test]
fn test_ensure_databases_uses_existing_when_fresh() {
    // With auto_download disabled and files already on disk, ensure_databases
    // must return those paths without touching the network.
    let tmp = tempfile::tempdir().expect("tempdir");
    let city_file = tmp.path().join("dbip-city-lite.mmdb");
    let asn_file = tmp.path().join("dbip-asn-lite.mmdb");
    fs::write(&city_file, b"existing city db").unwrap();
    fs::write(&asn_file, b"existing asn db").unwrap();

    let config = GeoIpConfig {
        provider: GeoIpProvider::DbIpLite,
        auto_download: AutoDownloadConfig {
            enabled: false,
            data_dir: tmp.path().to_string_lossy().into_owned(),
            ..Default::default()
        },
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert_eq!(paths.city, Some(city_file));
    assert_eq!(paths.asn, Some(asn_file));
}

#[test]
fn test_ensure_databases_stale_file_no_network_fallback() {
    // Auto-download enabled + stale file on disk + unreachable provider URLs
    // (because we use a port that refuses connections, via PROXY env? No —
    // ensure_databases uses hardcoded URLs. Instead rely on the fact that
    // MaxMind without credentials returns MissingCredential immediately,
    // which triggers the "fall back to existing stale file" branch.)
    let tmp = tempfile::tempdir().expect("tempdir");
    let city_file = tmp.path().join("GeoLite2-City.mmdb");
    let asn_file = tmp.path().join("GeoLite2-ASN.mmdb");
    fs::write(&city_file, b"stale city db").unwrap();
    fs::write(&asn_file, b"stale asn db").unwrap();

    // Force the file mtime to be older than max_age_days.
    let thirty_one_days_ago =
        std::time::SystemTime::now() - std::time::Duration::from_secs(31 * 86400);
    // Use filetime via plain std — best-effort: if we can't touch mtime we
    // still exercise the path, the test just becomes a bit weaker.
    // std::fs doesn't expose set_mtime; rely on max_age_days = 0 instead.
    let _ = thirty_one_days_ago;

    let config = GeoIpConfig {
        enabled: true,
        provider: GeoIpProvider::MaxMindGeoLite2,
        auto_download: AutoDownloadConfig {
            enabled: true,
            data_dir: tmp.path().to_string_lossy().into_owned(),
            max_age_days: 0, // treat everything as stale → triggers download attempt
            maxmind_account_id: None, // missing → download_city_db returns err
            maxmind_license_key: None,
            ..Default::default()
        },
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    // Download fails (missing creds). ensure_databases falls back to the
    // existing stale file, logging a warning.
    assert_eq!(paths.city, Some(city_file));
    assert_eq!(paths.asn, Some(asn_file));
}

#[test]
fn test_ensure_databases_stale_file_no_fallback_when_missing() {
    // max_age_days = 0 → stale trigger. Missing credentials → download fails.
    // No existing file → final result is None.
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = GeoIpConfig {
        enabled: true,
        provider: GeoIpProvider::MaxMindGeoLite2,
        auto_download: AutoDownloadConfig {
            enabled: true,
            data_dir: tmp.path().to_string_lossy().into_owned(),
            max_age_days: 0,
            maxmind_account_id: None,
            maxmind_license_key: None,
            ..Default::default()
        },
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert!(paths.city.is_none());
    assert!(paths.asn.is_none());
}

#[test]
fn test_ensure_databases_fresh_file_skips_download_entirely() {
    // File exists, auto_download enabled, max_age_days very large → file is
    // "fresh" → download code is never reached, even without credentials.
    let tmp = tempfile::tempdir().expect("tempdir");
    let city_file = tmp.path().join("GeoLite2-City.mmdb");
    let asn_file = tmp.path().join("GeoLite2-ASN.mmdb");
    fs::write(&city_file, b"fresh city db").unwrap();
    fs::write(&asn_file, b"fresh asn db").unwrap();

    let config = GeoIpConfig {
        enabled: true,
        provider: GeoIpProvider::MaxMindGeoLite2,
        auto_download: AutoDownloadConfig {
            enabled: true,
            data_dir: tmp.path().to_string_lossy().into_owned(),
            max_age_days: 365,        // very generous
            maxmind_account_id: None, // would fail if download were attempted
            maxmind_license_key: None,
            ..Default::default()
        },
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert_eq!(paths.city, Some(city_file));
    assert_eq!(paths.asn, Some(asn_file));
}

#[test]
fn test_ensure_databases_custom_with_none_paths_returns_none() {
    let config = GeoIpConfig {
        provider: GeoIpProvider::Custom,
        city_db_path: None,
        asn_db_path: None,
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert!(paths.city.is_none());
    assert!(paths.asn.is_none());
}

#[test]
fn test_ensure_databases_explicit_paths_override_provider() {
    // Non-Custom provider + explicit city/asn paths: the explicit paths win.
    let config = GeoIpConfig {
        provider: GeoIpProvider::DbIpLite,
        city_db_path: Some("/explicit/city.mmdb".into()),
        asn_db_path: Some("/explicit/asn.mmdb".into()),
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert_eq!(paths.city, Some(PathBuf::from("/explicit/city.mmdb")));
    assert_eq!(paths.asn, Some(PathBuf::from("/explicit/asn.mmdb")));
}

#[test]
fn test_ensure_databases_disabled_and_missing_returns_none() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config = GeoIpConfig {
        provider: GeoIpProvider::DbIpLite,
        auto_download: AutoDownloadConfig {
            enabled: false,
            data_dir: tmp.path().to_string_lossy().into_owned(),
            ..Default::default()
        },
        ..Default::default()
    };
    let paths = rt().block_on(ensure_databases(&config)).unwrap();
    assert!(paths.city.is_none());
    assert!(paths.asn.is_none());
}

#[test]
fn test_database_paths_debug_and_default() {
    // Round out coverage of the public surface.
    let d: DatabasePaths = Default::default();
    assert!(d.city.is_none());
    assert!(d.asn.is_none());
    let formatted = format!("{d:?}");
    assert!(formatted.contains("DatabasePaths"));
}

#[test]
fn test_download_error_variants_display() {
    let e1 = GeoIpDownloadError::MissingCredential {
        provider: "X",
        field: "Y",
    };
    assert!(e1.to_string().contains('X') && e1.to_string().contains('Y'));

    let e2 = GeoIpDownloadError::NoDatabases("P".into());
    assert!(e2.to_string().contains('P'));

    // IO and Http variants exercised via From; construct one directly.
    let io = GeoIpDownloadError::Io(std::io::Error::other("boom"));
    assert!(io.to_string().contains("boom"));
}
