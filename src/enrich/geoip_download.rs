// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! GeoIP database auto-download
//!
//! Downloads MMDB files from various providers on startup when the local
//! copy is missing or stale. Supports both anonymous and authenticated
//! providers.
//!
//! ## Supported Providers
//!
//! | Provider | Auth | City | ASN |
//! |---|---|---|---|
//! | DB-IP Lite | None | Yes | Yes |
//! | MaxMind GeoLite2 | Basic Auth | Yes | Yes |
//! | IPLocate | None | No | Yes |
//! | IPinfo Lite | Token | Yes | No |
//! | sapics | None | No | Yes |

use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use chrono::Utc;
use flate2::read::GzDecoder;
use tracing::{debug, info, warn};

use crate::config::{AutoDownloadConfig, GeoIpConfig, GeoIpProvider};

/// Errors from GeoIP database download
#[derive(Debug, thiserror::Error)]
pub enum GeoIpDownloadError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Provider {provider} requires {field} but it was not configured")]
    MissingCredential {
        provider: &'static str,
        field: &'static str,
    },

    #[error("No databases available for provider {0}")]
    NoDatabases(String),
}

/// Result of database resolution — paths to city and/or ASN MMDB files
#[derive(Debug, Default)]
pub struct DatabasePaths {
    pub city: Option<PathBuf>,
    pub asn: Option<PathBuf>,
}

/// Ensure GeoIP databases are available, downloading if necessary.
///
/// Returns paths to city and ASN MMDB files. Either or both may be `None`
/// depending on the provider's capabilities and download success.
///
/// This function is non-fatal: download failures are logged as warnings
/// and the enricher proceeds without the missing database.
pub async fn ensure_databases(config: &GeoIpConfig) -> Result<DatabasePaths, GeoIpDownloadError> {
    // Explicit paths override everything
    if config.provider == GeoIpProvider::Custom {
        return Ok(DatabasePaths {
            city: config.city_db_path.as_ref().map(PathBuf::from),
            asn: config.asn_db_path.as_ref().map(PathBuf::from),
        });
    }

    // If explicit paths are set, use them regardless of provider
    if config.city_db_path.is_some() || config.asn_db_path.is_some() {
        return Ok(DatabasePaths {
            city: config.city_db_path.as_ref().map(PathBuf::from),
            asn: config.asn_db_path.as_ref().map(PathBuf::from),
        });
    }

    let auto = &config.auto_download;
    let data_dir = Path::new(&auto.data_dir);

    // Check for existing files first
    let (city_file, asn_file) = provider_filenames(&config.provider);
    let city_path = city_file.map(|f| data_dir.join(f));
    let asn_path = asn_file.map(|f| data_dir.join(f));

    // If auto-download is disabled, just return whatever exists
    if !auto.enabled {
        return Ok(DatabasePaths {
            city: city_path.filter(|p| p.exists()),
            asn: asn_path.filter(|p| p.exists()),
        });
    }

    // Check freshness and download if needed
    let max_age_secs = auto.max_age_days as u64 * 86400;

    let city_result = if let Some(ref path) = city_path {
        if is_fresh(path, max_age_secs) {
            debug!(path = %path.display(), "City database is fresh, skipping download");
            Some(path.clone())
        } else {
            match download_city_db(&config.provider, auto, data_dir).await {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error = %e, provider = ?config.provider, "Failed to download city database");
                    // Fall back to existing stale file if present
                    if path.exists() {
                        warn!(path = %path.display(), "Using stale city database");
                        Some(path.clone())
                    } else {
                        None
                    }
                }
            }
        }
    } else {
        None
    };

    let asn_result = if let Some(ref path) = asn_path {
        if is_fresh(path, max_age_secs) {
            debug!(path = %path.display(), "ASN database is fresh, skipping download");
            Some(path.clone())
        } else {
            match download_asn_db(&config.provider, auto, data_dir).await {
                Ok(p) => Some(p),
                Err(e) => {
                    warn!(error = %e, provider = ?config.provider, "Failed to download ASN database");
                    if path.exists() {
                        warn!(path = %path.display(), "Using stale ASN database");
                        Some(path.clone())
                    } else {
                        None
                    }
                }
            }
        }
    } else {
        None
    };

    Ok(DatabasePaths {
        city: city_result,
        asn: asn_result,
    })
}

/// Return (city_filename, asn_filename) for a provider. `None` means the
/// provider doesn't offer that database type.
fn provider_filenames(provider: &GeoIpProvider) -> (Option<&'static str>, Option<&'static str>) {
    match provider {
        GeoIpProvider::DbIpLite => (Some("dbip-city-lite.mmdb"), Some("dbip-asn-lite.mmdb")),
        GeoIpProvider::MaxMindGeoLite2 => (Some("GeoLite2-City.mmdb"), Some("GeoLite2-ASN.mmdb")),
        GeoIpProvider::IpLocate => (None, Some("iplocate-asn.mmdb")),
        GeoIpProvider::IpInfoLite => (Some("ipinfo-lite.mmdb"), None),
        GeoIpProvider::Sapics => (None, Some("sapics-asn-country.mmdb")),
        GeoIpProvider::Custom => (None, None),
    }
}

/// Check if a file exists and was modified within `max_age_secs`
fn is_fresh(path: &Path, max_age_secs: u64) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = metadata.modified() else {
        return false;
    };
    let Ok(age) = SystemTime::now().duration_since(modified) else {
        return false;
    };
    age.as_secs() < max_age_secs
}

/// Download city database for the given provider
async fn download_city_db(
    provider: &GeoIpProvider,
    auto: &AutoDownloadConfig,
    data_dir: &Path,
) -> Result<PathBuf, GeoIpDownloadError> {
    match provider {
        GeoIpProvider::DbIpLite => {
            let now = Utc::now();
            let url = format!(
                "https://download.db-ip.com/free/dbip-city-lite-{}.mmdb.gz",
                now.format("%Y-%m")
            );
            let dest = data_dir.join("dbip-city-lite.mmdb");
            download_gzipped(&url, &dest, None).await?;
            Ok(dest)
        }
        GeoIpProvider::MaxMindGeoLite2 => {
            let account_id = auto.maxmind_account_id.as_deref().ok_or(
                GeoIpDownloadError::MissingCredential {
                    provider: "MaxMindGeoLite2",
                    field: "maxmind_account_id",
                },
            )?;
            let license_key = auto.maxmind_license_key.as_deref().ok_or(
                GeoIpDownloadError::MissingCredential {
                    provider: "MaxMindGeoLite2",
                    field: "maxmind_license_key",
                },
            )?;
            let url =
                "https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz";
            let dest = data_dir.join("GeoLite2-City.mmdb");
            let auth = BasicAuth {
                username: account_id.to_string(),
                password: license_key.to_string(),
            };
            download_tar_gz(url, &dest, "GeoLite2-City.mmdb", Some(&auth)).await?;
            Ok(dest)
        }
        GeoIpProvider::IpInfoLite => {
            let token =
                auto.ipinfo_token
                    .as_deref()
                    .ok_or(GeoIpDownloadError::MissingCredential {
                        provider: "IpInfoLite",
                        field: "ipinfo_token",
                    })?;
            let url = format!("https://ipinfo.io/data/ipinfo_lite.mmdb?token={token}");
            let dest = data_dir.join("ipinfo-lite.mmdb");
            download_raw(&url, &dest, None).await?;
            Ok(dest)
        }
        // IpLocate and Sapics have no city database
        GeoIpProvider::IpLocate | GeoIpProvider::Sapics | GeoIpProvider::Custom => {
            Err(GeoIpDownloadError::NoDatabases(format!("{provider:?}")))
        }
    }
}

/// Download ASN database for the given provider
async fn download_asn_db(
    provider: &GeoIpProvider,
    auto: &AutoDownloadConfig,
    data_dir: &Path,
) -> Result<PathBuf, GeoIpDownloadError> {
    match provider {
        GeoIpProvider::DbIpLite => {
            let now = Utc::now();
            let url = format!(
                "https://download.db-ip.com/free/dbip-asn-lite-{}.mmdb.gz",
                now.format("%Y-%m")
            );
            let dest = data_dir.join("dbip-asn-lite.mmdb");
            download_gzipped(&url, &dest, None).await?;
            Ok(dest)
        }
        GeoIpProvider::MaxMindGeoLite2 => {
            let account_id = auto.maxmind_account_id.as_deref().ok_or(
                GeoIpDownloadError::MissingCredential {
                    provider: "MaxMindGeoLite2",
                    field: "maxmind_account_id",
                },
            )?;
            let license_key = auto.maxmind_license_key.as_deref().ok_or(
                GeoIpDownloadError::MissingCredential {
                    provider: "MaxMindGeoLite2",
                    field: "maxmind_license_key",
                },
            )?;
            let url =
                "https://download.maxmind.com/geoip/databases/GeoLite2-ASN/download?suffix=tar.gz";
            let dest = data_dir.join("GeoLite2-ASN.mmdb");
            let auth = BasicAuth {
                username: account_id.to_string(),
                password: license_key.to_string(),
            };
            download_tar_gz(url, &dest, "GeoLite2-ASN.mmdb", Some(&auth)).await?;
            Ok(dest)
        }
        GeoIpProvider::IpLocate => {
            let url = "https://github.com/sapics/ip-location-db/raw/main/dbip-asn/dbip-asn.mmdb";
            let dest = data_dir.join("iplocate-asn.mmdb");
            download_raw(url, &dest, None).await?;
            Ok(dest)
        }
        GeoIpProvider::Sapics => {
            let url = "https://github.com/sapics/ip-location-db/raw/main/geo-whois-asn-country/geo-whois-asn-country.mmdb";
            let dest = data_dir.join("sapics-asn-country.mmdb");
            download_raw(url, &dest, None).await?;
            Ok(dest)
        }
        // IpInfoLite has no separate ASN database
        GeoIpProvider::IpInfoLite | GeoIpProvider::Custom => {
            Err(GeoIpDownloadError::NoDatabases(format!("{provider:?}")))
        }
    }
}

/// HTTP Basic Auth credentials
struct BasicAuth {
    username: String,
    password: String,
}

/// Build a reqwest client with reasonable timeouts
fn http_client() -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .connect_timeout(std::time::Duration::from_secs(30))
        .user_agent("dfe-loader")
        .build()
}

/// Download a gzip-compressed file and decompress to dest
async fn download_gzipped(
    url: &str,
    dest: &Path,
    auth: Option<&BasicAuth>,
) -> Result<(), GeoIpDownloadError> {
    info!(url = %url, dest = %dest.display(), "Downloading GeoIP database (gzip)");

    // Ensure parent directory exists
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }

    let client = http_client()?;
    let mut req = client.get(url);
    if let Some(auth) = auth {
        req = req.basic_auth(&auth.username, Some(&auth.password));
    }

    let response = req.send().await?.error_for_status()?;
    let bytes = response.bytes().await?;

    // Decompress gzip
    let mut decoder = GzDecoder::new(&bytes[..]);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    // Atomic write: write to temp file then rename
    let tmp = dest.with_extension("mmdb.tmp");
    fs::write(&tmp, &decompressed)?;
    fs::rename(&tmp, dest)?;

    info!(
        dest = %dest.display(),
        size_mb = decompressed.len() / (1024 * 1024),
        "GeoIP database downloaded"
    );
    Ok(())
}

/// Download a tar.gz archive and extract a specific file to dest
async fn download_tar_gz(
    url: &str,
    dest: &Path,
    target_filename: &str,
    auth: Option<&BasicAuth>,
) -> Result<(), GeoIpDownloadError> {
    info!(url = %url, dest = %dest.display(), "Downloading GeoIP database (tar.gz)");

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }

    let client = http_client()?;
    let mut req = client.get(url);
    if let Some(auth) = auth {
        req = req.basic_auth(&auth.username, Some(&auth.password));
    }

    let response = req.send().await?.error_for_status()?;
    let bytes = response.bytes().await?;

    // Decompress gzip, then extract tar
    let decoder = GzDecoder::new(&bytes[..]);
    let mut archive = tar::Archive::new(decoder);

    let mut found = false;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;

        // MaxMind tar contains: GeoLite2-City_20241231/GeoLite2-City.mmdb
        if let Some(filename) = path.file_name()
            && filename == target_filename
        {
            let tmp = dest.with_extension("mmdb.tmp");
            let mut outfile = fs::File::create(&tmp)?;
            io::copy(&mut entry, &mut outfile)?;
            fs::rename(&tmp, dest)?;

            let size = fs::metadata(dest).map(|m| m.len()).unwrap_or(0);
            info!(
                dest = %dest.display(),
                size_mb = size / (1024 * 1024),
                "GeoIP database extracted from tar"
            );
            found = true;
            break;
        }
    }

    if !found {
        return Err(GeoIpDownloadError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{target_filename} not found in tar archive"),
        )));
    }

    Ok(())
}

/// Download a raw (uncompressed) MMDB file
async fn download_raw(
    url: &str,
    dest: &Path,
    auth: Option<&BasicAuth>,
) -> Result<(), GeoIpDownloadError> {
    info!(url = %url, dest = %dest.display(), "Downloading GeoIP database (raw)");

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }

    let client = http_client()?;
    let mut req = client.get(url);
    if let Some(auth) = auth {
        req = req.basic_auth(&auth.username, Some(&auth.password));
    }

    let response = req.send().await?.error_for_status()?;
    let bytes = response.bytes().await?;

    let tmp = dest.with_extension("mmdb.tmp");
    fs::write(&tmp, &bytes)?;
    fs::rename(&tmp, dest)?;

    info!(
        dest = %dest.display(),
        size_mb = bytes.len() / (1024 * 1024),
        "GeoIP database downloaded"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_provider_filenames() {
        let (city, asn) = provider_filenames(&GeoIpProvider::DbIpLite);
        assert_eq!(city, Some("dbip-city-lite.mmdb"));
        assert_eq!(asn, Some("dbip-asn-lite.mmdb"));

        let (city, asn) = provider_filenames(&GeoIpProvider::MaxMindGeoLite2);
        assert_eq!(city, Some("GeoLite2-City.mmdb"));
        assert_eq!(asn, Some("GeoLite2-ASN.mmdb"));

        let (city, asn) = provider_filenames(&GeoIpProvider::IpLocate);
        assert!(city.is_none());
        assert!(asn.is_some());

        let (city, asn) = provider_filenames(&GeoIpProvider::IpInfoLite);
        assert!(city.is_some());
        assert!(asn.is_none());

        let (city, asn) = provider_filenames(&GeoIpProvider::Sapics);
        assert!(city.is_none());
        assert!(asn.is_some());

        let (city, asn) = provider_filenames(&GeoIpProvider::Custom);
        assert!(city.is_none());
        assert!(asn.is_none());
    }

    #[test]
    fn test_is_fresh_nonexistent() {
        assert!(!is_fresh(Path::new("/nonexistent/file.mmdb"), 86400));
    }

    #[test]
    fn test_custom_provider_uses_explicit_paths() {
        let config = GeoIpConfig {
            provider: GeoIpProvider::Custom,
            city_db_path: Some("/data/city.mmdb".into()),
            asn_db_path: Some("/data/asn.mmdb".into()),
            ..Default::default()
        };

        // Custom provider returns paths synchronously (no download)
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let paths = rt.block_on(ensure_databases(&config)).unwrap();
        assert_eq!(paths.city, Some(PathBuf::from("/data/city.mmdb")));
        assert_eq!(paths.asn, Some(PathBuf::from("/data/asn.mmdb")));
    }

    #[test]
    fn test_explicit_paths_override_provider() {
        let config = GeoIpConfig {
            provider: GeoIpProvider::DbIpLite,
            city_db_path: Some("/custom/city.mmdb".into()),
            asn_db_path: Some("/custom/asn.mmdb".into()),
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let paths = rt.block_on(ensure_databases(&config)).unwrap();
        assert_eq!(paths.city, Some(PathBuf::from("/custom/city.mmdb")));
        assert_eq!(paths.asn, Some(PathBuf::from("/custom/asn.mmdb")));
    }

    #[test]
    fn test_disabled_auto_download_returns_existing_only() {
        let config = GeoIpConfig {
            provider: GeoIpProvider::DbIpLite,
            auto_download: AutoDownloadConfig {
                enabled: false,
                data_dir: "/nonexistent".into(),
                ..Default::default()
            },
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let paths = rt.block_on(ensure_databases(&config)).unwrap();
        // Files don't exist at /nonexistent, so both are None
        assert!(paths.city.is_none());
        assert!(paths.asn.is_none());
    }

    #[test]
    fn test_maxmind_requires_credentials() {
        let config = GeoIpConfig {
            enabled: true,
            provider: GeoIpProvider::MaxMindGeoLite2,
            auto_download: AutoDownloadConfig {
                enabled: true,
                data_dir: "/tmp/geoip-test".into(),
                maxmind_account_id: None,  // Missing!
                maxmind_license_key: None, // Missing!
                ..Default::default()
            },
            ..Default::default()
        };

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        // Should fail to download because credentials are missing
        // (the ensure_databases function will try to download since files don't exist)
        let paths = rt.block_on(ensure_databases(&config)).unwrap();
        // Downloads failed (missing creds), no existing files → both None
        assert!(paths.city.is_none());
        assert!(paths.asn.is_none());
    }

    #[test]
    fn test_default_config_disabled() {
        let config = GeoIpConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.provider, GeoIpProvider::DbIpLite);
        assert!(config.auto_download.enabled);
        assert_eq!(config.cache_capacity, 100_000);
    }
}
