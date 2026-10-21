// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! The loader's `geoip` settings provision databases through scalo's
//! `ensure_databases`.
//!
//! Each case builds the loader's own config, converts it the way
//! `GeoIpEnricher::from_config` does, and asserts the paths that come back.
//! None of them reach the network: the download itself is scalo's, tested there.

use std::fs;
use std::path::{Path, PathBuf};

use dfe_loader::config::{AutoDownloadConfig, GeoIpConfig, GeoIpProvider};
use scalo::geoip_download::{DatabasePaths, ensure_databases};

/// Resolve the databases the loader config names.
fn provision(config: &GeoIpConfig) -> DatabasePaths {
    let provisioning = scalo::geoip_download::GeoIpConfig::from(config);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(ensure_databases(&provisioning))
        .expect("ensure_databases is non-fatal")
}

/// GeoIP on, `provider`, downloading into `dir` when `download` is set.
fn config_in(
    dir: &Path,
    provider: GeoIpProvider,
    download: bool,
    max_age_days: u32,
) -> GeoIpConfig {
    GeoIpConfig {
        enabled: true,
        provider,
        auto_download: AutoDownloadConfig {
            enabled: download,
            data_dir: dir.to_path_buf(),
            max_age_days,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn touch(dir: &Path, name: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, b"mmdb").expect("write database file");
    path
}

#[test]
fn explicit_paths_bypass_the_provider() {
    for provider in [GeoIpProvider::Custom, GeoIpProvider::DbIpLite] {
        let config = GeoIpConfig {
            enabled: true,
            provider,
            city_db_path: Some("/explicit/city.mmdb".into()),
            asn_db_path: Some("/explicit/asn.mmdb".into()),
            ..Default::default()
        };
        let paths = provision(&config);
        assert_eq!(paths.city, Some(PathBuf::from("/explicit/city.mmdb")));
        assert_eq!(paths.asn, Some(PathBuf::from("/explicit/asn.mmdb")));
    }
}

#[test]
fn geoip_off_provisions_nothing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    touch(tmp.path(), "dbip-city-lite.mmdb");
    let mut config = config_in(tmp.path(), GeoIpProvider::DbIpLite, false, 30);
    config.enabled = false;
    assert_eq!(provision(&config), DatabasePaths::default());
}

#[test]
fn with_download_off_only_files_already_in_data_dir_are_used() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let city = touch(tmp.path(), "dbip-city-lite.mmdb");
    let paths = provision(&config_in(tmp.path(), GeoIpProvider::DbIpLite, false, 30));
    assert_eq!(paths.city, Some(city));
    assert_eq!(paths.asn, None, "no ASN file was on disk");
}

#[test]
fn sapics_reads_the_dbip_asn_file_and_has_no_city() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let asn = touch(tmp.path(), "sapics-dbip-asn.mmdb");
    touch(tmp.path(), "sapics-asn-country.mmdb");
    let paths = provision(&config_in(tmp.path(), GeoIpProvider::Sapics, false, 30));
    assert_eq!(paths.city, None);
    assert_eq!(paths.asn, Some(asn));
}

#[test]
fn a_fresh_file_is_used_without_a_download() {
    // MaxMind has no credentials here, so any download attempt would fail.
    let tmp = tempfile::tempdir().expect("tempdir");
    let city = touch(tmp.path(), "GeoLite2-City.mmdb");
    let asn = touch(tmp.path(), "GeoLite2-ASN.mmdb");
    let paths = provision(&config_in(
        tmp.path(),
        GeoIpProvider::MaxMindGeoLite2,
        true,
        365,
    ));
    assert_eq!(paths.city, Some(city));
    assert_eq!(paths.asn, Some(asn));
}

#[test]
fn a_failed_download_falls_back_to_the_stale_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let city = touch(tmp.path(), "GeoLite2-City.mmdb");
    let asn = touch(tmp.path(), "GeoLite2-ASN.mmdb");
    // max_age_days 0 makes every file stale, and the missing credentials fail the download.
    let paths = provision(&config_in(
        tmp.path(),
        GeoIpProvider::MaxMindGeoLite2,
        true,
        0,
    ));
    assert_eq!(paths.city, Some(city));
    assert_eq!(paths.asn, Some(asn));
}

#[test]
fn a_failed_download_with_nothing_on_disk_leaves_no_database() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let paths = provision(&config_in(
        tmp.path(),
        GeoIpProvider::MaxMindGeoLite2,
        true,
        0,
    ));
    assert_eq!(paths, DatabasePaths::default());
}

#[test]
fn the_conversion_carries_every_setting() {
    let yaml = r"
enabled: true
provider: max_mind_geo_lite2
city_db_path: /srv/city.mmdb
auto_download:
  enabled: false
  data_dir: /srv/geoip
  maxmind_account_id: account-1234
  maxmind_license_key: licence-abcd
  ipinfo_token: token-wxyz
  max_age_days: 7
  max_database_bytes: 1048576
";
    let config: GeoIpConfig =
        scalo::yaml::from_str(yaml, "geoip.yaml", scalo::yaml::Profile::Config)
            .expect("the documented shape parses");
    let provisioning = scalo::geoip_download::GeoIpConfig::from(&config);

    assert!(provisioning.enabled);
    assert_eq!(provisioning.provider, GeoIpProvider::MaxMindGeoLite2);
    assert_eq!(
        provisioning.city_db_path,
        Some(PathBuf::from("/srv/city.mmdb"))
    );
    assert_eq!(provisioning.asn_db_path, None);
    let auto = &provisioning.auto_download;
    assert!(!auto.enabled);
    assert_eq!(auto.data_dir, PathBuf::from("/srv/geoip"));
    assert_eq!(auto.max_age_days, 7);
    assert_eq!(auto.max_database_bytes, 1_048_576);
    let exposed =
        |value: &Option<scalo::SensitiveString>| value.as_ref().map(|s| s.expose().to_owned());
    assert_eq!(
        exposed(&auto.maxmind_account_id).as_deref(),
        Some("account-1234")
    );
    assert_eq!(
        exposed(&auto.maxmind_license_key).as_deref(),
        Some("licence-abcd")
    );
    assert_eq!(exposed(&auto.ipinfo_token).as_deref(), Some("token-wxyz"));
}

#[test]
fn the_defaults_keep_the_loader_data_dir_and_scalo_limits() {
    let auto = AutoDownloadConfig::default();
    let upstream = scalo::geoip_download::AutoDownloadConfig::default();
    assert_eq!(auto.data_dir, PathBuf::from("/var/lib/dfe/geoip"));
    assert_eq!(auto.max_age_days, upstream.max_age_days);
    assert_eq!(auto.max_database_bytes, upstream.max_database_bytes);
    assert!(auto.enabled);
    assert!(
        !GeoIpConfig::default().enabled,
        "GeoIP enrichment stays opt-in"
    );
}

#[test]
fn credentials_never_reach_a_config_dump() {
    let config = GeoIpConfig {
        auto_download: AutoDownloadConfig {
            maxmind_account_id: Some("account-1234".into()),
            maxmind_license_key: Some("licence-abcd".into()),
            ipinfo_token: Some("token-wxyz".into()),
            ..Default::default()
        },
        ..Default::default()
    };
    let dumped = serde_json::to_string(&config).expect("serialise");
    let debugged = format!("{config:?}");
    // The messages name the field, never the value or the dump.
    for (field, value) in [
        ("maxmind_account_id", "account-1234"),
        ("maxmind_license_key", "licence-abcd"),
        ("ipinfo_token", "token-wxyz"),
    ] {
        assert!(
            !dumped.contains(value),
            "{field} reached the serialised config"
        );
        assert!(
            !debugged.contains(value),
            "{field} reached the Debug output"
        );
    }
}
