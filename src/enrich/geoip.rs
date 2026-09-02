// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! `GeoIP` enrichment.
//!
//! An adapter over [`factbook::geoip`], which owns the database downloads, the
//! MMDB readers, the cache and the private-address fast path.
//!
//! What lives here is the shape the destination schema expects:
//! [`GeoIpResult`] and its [`to_schema_map`](GeoIpResult::to_schema_map) name
//! ClickHouse columns, so the field names are a contract with deployed tables
//! rather than an internal detail. The rename between factbook's names and
//! ours happens on that boundary and nowhere else.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;

use factbook::Secret;
use factbook::geoip::{GeoIp as FactbookGeoIp, GeoIpRecord as FactbookRecord};
use tracing::{debug, info, warn};

use crate::config::{GeoIpConfig, GeoIpProvider};

/// `GeoIP` lookup result with all available fields
#[derive(Debug, Clone, Default)]
pub struct GeoIpResult {
    /// Continent code (e.g., "NA", "EU", "AS")
    pub continent_code: Option<String>,
    /// Continent name in English (e.g., "North America", "Europe")
    pub continent_name: Option<String>,
    /// ISO 3166-1 alpha-2 country code (e.g., "US", "GB")
    pub country_code: Option<String>,
    /// Country name in English
    pub country_name: Option<String>,
    /// City name in English
    pub city: Option<String>,
    /// Latitude coordinate
    pub latitude: Option<f64>,
    /// Longitude coordinate
    pub longitude: Option<f64>,
    /// Timezone (IANA format, e.g., "`America/New_York`")
    pub timezone: Option<String>,
    /// Postal/ZIP code
    pub postal_code: Option<String>,
    /// Subdivision/state/province name
    pub subdivision: Option<String>,
    /// Subdivision ISO code
    pub subdivision_code: Option<String>,
    /// Autonomous System Number
    pub asn: Option<u32>,
    /// Autonomous System Organization name
    pub asn_org: Option<String>,
    /// Whether this is a private/internal IP address
    pub is_private: bool,
    /// Accuracy radius in kilometers
    pub accuracy_radius: Option<u16>,
}

impl From<&FactbookRecord> for GeoIpResult {
    /// Map factbook's record onto the shape `to_schema_map` writes from.
    ///
    /// Five fields are named differently on each side; the rest match. The
    /// names here are ClickHouse column names, so they stay as they are and
    /// the rename happens on this boundary.
    fn from(r: &FactbookRecord) -> Self {
        // `CompactString` is factbook's; the schema map hands out `String`.
        fn owned(v: Option<&compact_str::CompactString>) -> Option<String> {
            v.map(std::string::ToString::to_string)
        }

        Self {
            continent_code: owned(r.continent_code.as_ref()),
            continent_name: owned(r.continent_name.as_ref()),
            country_code: owned(r.country_code.as_ref()),
            country_name: owned(r.country_name.as_ref()),
            city: owned(r.city_name.as_ref()),
            latitude: r.latitude,
            longitude: r.longitude,
            timezone: owned(r.timezone.as_ref()),
            postal_code: owned(r.postal_code.as_ref()),
            subdivision: owned(r.region_name.as_ref()),
            subdivision_code: owned(r.region_code.as_ref()),
            asn: r.autonomous_system_number,
            asn_org: owned(r.autonomous_system_organization.as_ref()),
            is_private: r.is_private,
            accuracy_radius: r.accuracy_radius,
        }
    }
}

impl GeoIpResult {
    /// Convert result to a map, filtering to only requested fields
    pub fn to_schema_map(&self, schema_fields: &[&str]) -> HashMap<String, serde_json::Value> {
        use serde_json::Value;
        let mut out = HashMap::with_capacity(schema_fields.len());

        for &field in schema_fields {
            match field {
                "continent_code" | "continent" => {
                    if let Some(ref v) = self.continent_code {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "continent_name" => {
                    if let Some(ref v) = self.continent_name {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "country_code" => {
                    if let Some(ref v) = self.country_code {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "country_name" | "country" => {
                    if let Some(ref v) = self.country_name {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "city" => {
                    if let Some(ref v) = self.city {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "latitude" | "lat" => {
                    if let Some(v) = self.latitude {
                        out.insert(
                            field.to_string(),
                            Value::Number(
                                serde_json::Number::from_f64(v)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                    }
                }
                "longitude" | "lon" | "lng" => {
                    if let Some(v) = self.longitude {
                        out.insert(
                            field.to_string(),
                            Value::Number(
                                serde_json::Number::from_f64(v)
                                    .unwrap_or(serde_json::Number::from(0)),
                            ),
                        );
                    }
                }
                "timezone" | "tz" => {
                    if let Some(ref v) = self.timezone {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "postal_code" | "postal" | "zip" => {
                    if let Some(ref v) = self.postal_code {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "subdivision" | "state" | "region" => {
                    if let Some(ref v) = self.subdivision {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "subdivision_code" | "state_code" | "region_code" => {
                    if let Some(ref v) = self.subdivision_code {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "asn" => {
                    if let Some(v) = self.asn {
                        out.insert(field.to_string(), Value::Number(v.into()));
                    }
                }
                "asn_org" | "isp" | "org" => {
                    if let Some(ref v) = self.asn_org {
                        out.insert(field.to_string(), Value::String(v.clone()));
                    }
                }
                "is_private" | "private" => {
                    out.insert(field.to_string(), Value::Bool(self.is_private));
                }
                "accuracy_radius" | "accuracy" => {
                    if let Some(v) = self.accuracy_radius {
                        out.insert(field.to_string(), Value::Number(v.into()));
                    }
                }
                _ => {}
            }
        }

        out
    }
}

/// `GeoIP` enricher.
///
/// An adapter over [`factbook::geoip`], which owns the download, the MMDB
/// readers and the cache. What stays here is [`GeoIpResult`] and its
/// [`to_schema_map`](GeoIpResult::to_schema_map): those field names are
/// ClickHouse column names in deployed tables, so they are a contract rather
/// than an internal shape.
///
/// `None` is an enricher with no databases -- a download that failed, or
/// enrichment configured off. Lookups return `None` and the pipeline carries
/// on, which is the behaviour this had before.
pub struct GeoIpEnricher {
    inner: Option<FactbookGeoIp>,
}

impl GeoIpEnricher {
    /// An enricher with no databases. Every lookup returns `None`.
    #[must_use]
    pub fn new() -> Self {
        Self { inner: None }
    }

    /// Whether any database is loaded.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.inner.is_some()
    }

    /// Entries currently held in factbook's lookup cache.
    ///
    /// Hits and misses are not counted here any more. factbook emits
    /// `enrichment_cache_hits_total` and `enrichment_cache_misses_total`
    /// through the metrics facade, so they reach a scrape without this
    /// carrying the numbers.
    #[must_use]
    pub fn cached_entries(&self) -> usize {
        self.inner.as_ref().map_or(0, FactbookGeoIp::cached_entries)
    }

    /// Look up an IP address.
    ///
    /// Caching, private-address handling and the database reads all belong to
    /// factbook; this parses the string and maps the record onto the shape the
    /// ClickHouse schema expects.
    pub fn lookup(&self, ip: &str) -> Option<GeoIpResult> {
        let Ok(addr) = ip.parse::<IpAddr>() else {
            debug!(ip = %ip, "Invalid IP address");
            return None;
        };

        // Answered before the database is consulted, because the answer does
        // not need one. A deployment whose download failed still reports
        // internal traffic as internal rather than reporting nothing.
        if is_private_ip(&addr) {
            return Some(GeoIpResult {
                is_private: true,
                ..Default::default()
            });
        }

        let geoip = self.inner.as_ref()?;
        geoip.lookup(addr).map(|record| GeoIpResult::from(&*record))
    }

    /// Batch lookup with deduplication
    ///
    /// Returns a map of IP -> result. Each unique IP is looked up once.
    pub fn lookup_batch(&self, ips: &[&str]) -> HashMap<String, GeoIpResult> {
        let mut results = HashMap::with_capacity(ips.len());

        for &ip in ips {
            if results.contains_key(ip) {
                continue;
            }
            if let Some(result) = self.lookup(ip) {
                results.insert(ip.to_string(), result);
            }
        }

        results
    }

    /// Create enricher from config, downloading databases if needed.
    ///
    /// This is the primary constructor for production use. It resolves
    /// database paths (downloading if necessary) and loads them.
    /// Download failures are non-fatal — the enricher works with
    /// zero, one, or both databases.
    pub async fn from_config(config: &GeoIpConfig) -> Self {
        let fb_config = to_factbook_config(config);

        let databases = match factbook::geoip::ensure_databases(&fb_config).await {
            Ok(databases) => databases,
            Err(e) => {
                warn!(error = %e, "Failed to resolve GeoIP databases, enricher will be inactive");
                return Self::new();
            }
        };

        // factbook builds a reader over zero databases quite happily, so the
        // check has to happen here: `is_available` means there is something to
        // look in, and the pipeline logs on the strength of it.
        if databases.city.is_none() && databases.asn.is_none() {
            warn!(provider = ?config.provider, "No GeoIP databases available, enricher inactive");
            return Self::new();
        }

        let cache = factbook::geoip::CacheConfig {
            capacity: config.cache_capacity,
            ..factbook::geoip::CacheConfig::default()
        };

        match FactbookGeoIp::from_databases(&databases, cache) {
            Ok(geoip) => {
                info!(provider = ?config.provider, "Loaded GeoIP databases");
                Self { inner: Some(geoip) }
            }
            Err(e) => {
                warn!(error = %e, provider = ?config.provider,
                      "No GeoIP databases loaded, enricher inactive");
                Self::new()
            }
        }
    }
}

impl Default for GeoIpEnricher {
    fn default() -> Self {
        Self::new()
    }
}

/// Translate our config into factbook's.
///
/// The two are near enough field-for-field; what differs is that factbook
/// names the product line separately (`ProviderTier`) where we fold it into
/// the provider name, and wraps every credential in `Secret` where we wrapped
/// only the IPinfo token.
fn to_factbook_config(config: &GeoIpConfig) -> factbook::geoip::GeoIpConfig {
    use factbook::geoip::{GeoIpProvider as Fb, ProviderChoice, ProviderSelection};

    // Which factbook provider serves each half. Providers that publish no city
    // database leave that half on the default rather than pointing it at a
    // source that cannot answer.
    let selection = match config.provider {
        GeoIpProvider::DbIpLite => ProviderSelection::from(ProviderChoice::from(Fb::DbIp)),
        GeoIpProvider::MaxMindGeoLite2 => {
            ProviderSelection::from(ProviderChoice::from(Fb::MaxMind))
        }
        GeoIpProvider::IpInfoLite => ProviderSelection::from(ProviderChoice::from(Fb::IpInfo)),
        // ASN-only sources: leave the city half at the default, which is the
        // only free city source anyway.
        GeoIpProvider::Sapics => ProviderSelection {
            asn: ProviderChoice::from(Fb::SapicsOriginAsn),
            ..ProviderSelection::default()
        },
        // factbook carries no IPLocate source. The default pair keeps a
        // deployment answering rather than leaving it with no databases.
        GeoIpProvider::IpLocate => {
            warn!(
                "provider 'iplocate' has no factbook equivalent; \
                 falling back to the default free pair"
            );
            ProviderSelection::default()
        }
        GeoIpProvider::Custom => ProviderSelection::from(ProviderChoice::from(Fb::Custom)),
    };

    factbook::geoip::GeoIpConfig {
        enabled: config.enabled,
        provider: selection,
        city_db_path: config.city_db_path.as_ref().map(PathBuf::from),
        asn_db_path: config.asn_db_path.as_ref().map(PathBuf::from),
        auto_download: factbook::geoip::AutoDownloadConfig {
            enabled: config.auto_download.enabled,
            data_dir: PathBuf::from(&config.auto_download.data_dir),
            maxmind_account_id: config
                .auto_download
                .maxmind_account_id
                .as_ref()
                .map(|s| Secret::from(s.clone())),
            maxmind_license_key: config
                .auto_download
                .maxmind_license_key
                .as_ref()
                .map(|s| Secret::from(s.clone())),
            ipinfo_token: config
                .auto_download
                .ipinfo_token
                .as_ref()
                .map(|s| Secret::from(s.expose().to_string())),
            max_age_days: config.auto_download.max_age_days,
            ..factbook::geoip::AutoDownloadConfig::default()
        },
        ..factbook::geoip::GeoIpConfig::default()
    }
}

/// Check if an IP address is private/internal (RFC1918, loopback, link-local, etc.)
///
/// This is a fast path to skip MMDB lookups for internal addresses.
#[inline]
pub fn is_private_ip(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(ipv4) => is_private_ipv4(ipv4),
        IpAddr::V6(ipv6) => is_private_ipv6(ipv6),
    }
}

/// Check if IPv4 address is private
#[inline]
fn is_private_ipv4(addr: &Ipv4Addr) -> bool {
    // RFC1918 private ranges
    addr.is_private()
        // Loopback (127.0.0.0/8)
        || addr.is_loopback()
        // Link-local (169.254.0.0/16)
        || addr.is_link_local()
        // Broadcast
        || addr.is_broadcast()
        // Documentation/TEST-NET (192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24)
        || is_documentation_ipv4(addr)
        // Shared address space (100.64.0.0/10) - RFC6598 CGNAT
        || is_shared_address_space(addr)
        // Unspecified (0.0.0.0)
        || addr.is_unspecified()
}

/// Check if IPv4 is in documentation range
#[inline]
fn is_documentation_ipv4(addr: &Ipv4Addr) -> bool {
    let octets = addr.octets();
    // TEST-NET-1: 192.0.2.0/24
    (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        // TEST-NET-2: 198.51.100.0/24
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        // TEST-NET-3: 203.0.113.0/24
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113)
}

/// Check if IPv4 is in CGNAT shared address space (100.64.0.0/10)
#[inline]
fn is_shared_address_space(addr: &Ipv4Addr) -> bool {
    let octets = addr.octets();
    octets[0] == 100 && (octets[1] & 0xC0) == 64
}

/// Check if IPv6 address is private/internal
#[inline]
fn is_private_ipv6(addr: &Ipv6Addr) -> bool {
    // Loopback (::1)
    addr.is_loopback()
        // Unspecified (::)
        || addr.is_unspecified()
        // Unique local addresses (fc00::/7)
        || is_unique_local_ipv6(addr)
        // Link-local (fe80::/10)
        || is_link_local_ipv6(addr)
}

/// Check if IPv6 is unique local (`fc00::/7`)
#[inline]
fn is_unique_local_ipv6(addr: &Ipv6Addr) -> bool {
    let segments = addr.segments();
    (segments[0] & 0xFE00) == 0xFC00
}

/// Check if IPv6 is link-local (`fe80::/10`)
#[inline]
fn is_link_local_ipv6(addr: &Ipv6Addr) -> bool {
    let segments = addr.segments();
    (segments[0] & 0xFFC0) == 0xFE80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_private_ipv4() {
        // RFC1918 private ranges
        assert!(is_private_ip(&"10.0.0.1".parse().unwrap()));
        assert!(is_private_ip(&"172.16.0.1".parse().unwrap()));
        assert!(is_private_ip(&"192.168.1.1".parse().unwrap()));

        // Loopback
        assert!(is_private_ip(&"127.0.0.1".parse().unwrap()));

        // Link-local
        assert!(is_private_ip(&"169.254.1.1".parse().unwrap()));

        // CGNAT shared
        assert!(is_private_ip(&"100.64.0.1".parse().unwrap()));
        assert!(is_private_ip(&"100.127.255.255".parse().unwrap()));

        // Public IPs
        assert!(!is_private_ip(&"8.8.8.8".parse().unwrap()));
        assert!(!is_private_ip(&"1.1.1.1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6() {
        // Loopback
        assert!(is_private_ip(&"::1".parse().unwrap()));

        // Unique local
        assert!(is_private_ip(&"fc00::1".parse().unwrap()));
        assert!(is_private_ip(&"fd00::1".parse().unwrap()));

        // Link-local
        assert!(is_private_ip(&"fe80::1".parse().unwrap()));

        // Public
        assert!(!is_private_ip(&"2001:4860:4860::8888".parse().unwrap()));
    }

    #[test]
    fn test_enricher_without_db() {
        let enricher = GeoIpEnricher::new();
        assert!(!enricher.is_available());

        // Should handle gracefully
        assert!(enricher.lookup("8.8.8.8").is_none());

        // Private IPs should still work
        let result = enricher.lookup("192.168.1.1");
        assert!(result.is_some());
        assert!(result.unwrap().is_private);
    }

    #[test]
    fn test_result_to_schema_map() {
        let result = GeoIpResult {
            country_code: Some("US".to_string()),
            country_name: Some("United States".to_string()),
            city: Some("Mountain View".to_string()),
            latitude: Some(37.386),
            longitude: Some(-122.084),
            ..Default::default()
        };

        let fields = ["country_code", "city", "latitude"];
        let map = result.to_schema_map(&fields);

        assert_eq!(map.len(), 3);
        assert_eq!(map.get("country_code").unwrap(), "US");
        assert_eq!(map.get("city").unwrap(), "Mountain View");
    }

    // ========================================================================
    // Private IP fast path: every RFC1918 and special range
    // ========================================================================

    #[test]
    fn test_private_ipv4_class_a() {
        // 10.0.0.0/8 — every octet boundary
        assert!(is_private_ip(&"10.0.0.0".parse().unwrap()));
        assert!(is_private_ip(&"10.0.0.1".parse().unwrap()));
        assert!(is_private_ip(&"10.255.255.255".parse().unwrap()));
        assert!(is_private_ip(&"10.128.64.32".parse().unwrap()));

        // Just outside the range
        assert!(!is_private_ip(&"11.0.0.1".parse().unwrap()));
        assert!(!is_private_ip(&"9.255.255.255".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_class_b() {
        // 172.16.0.0/12
        assert!(is_private_ip(&"172.16.0.0".parse().unwrap()));
        assert!(is_private_ip(&"172.16.0.1".parse().unwrap()));
        assert!(is_private_ip(&"172.31.255.255".parse().unwrap()));
        assert!(is_private_ip(&"172.20.10.5".parse().unwrap()));

        // Just outside the range
        assert!(!is_private_ip(&"172.15.0.0".parse().unwrap()));
        assert!(!is_private_ip(&"172.32.0.0".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_class_c() {
        // 192.168.0.0/16
        assert!(is_private_ip(&"192.168.0.0".parse().unwrap()));
        assert!(is_private_ip(&"192.168.1.1".parse().unwrap()));
        assert!(is_private_ip(&"192.168.255.255".parse().unwrap()));

        // Just outside the range
        assert!(!is_private_ip(&"192.167.0.0".parse().unwrap()));
        assert!(!is_private_ip(&"192.169.0.0".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_loopback() {
        // 127.0.0.0/8
        assert!(is_private_ip(&"127.0.0.0".parse().unwrap()));
        assert!(is_private_ip(&"127.0.0.1".parse().unwrap()));
        assert!(is_private_ip(&"127.255.255.254".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_link_local() {
        // 169.254.0.0/16
        assert!(is_private_ip(&"169.254.0.0".parse().unwrap()));
        assert!(is_private_ip(&"169.254.1.1".parse().unwrap()));
        assert!(is_private_ip(&"169.254.255.255".parse().unwrap()));

        assert!(!is_private_ip(&"169.253.255.255".parse().unwrap()));
        assert!(!is_private_ip(&"169.255.0.0".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_cgnat() {
        // 100.64.0.0/10 — RFC 6598
        assert!(is_private_ip(&"100.64.0.0".parse().unwrap()));
        assert!(is_private_ip(&"100.64.0.1".parse().unwrap()));
        assert!(is_private_ip(&"100.127.255.255".parse().unwrap()));

        // 100.63.x.x is NOT CGNAT
        assert!(!is_private_ip(&"100.63.255.255".parse().unwrap()));
        // 100.128.x.x is NOT CGNAT
        assert!(!is_private_ip(&"100.128.0.0".parse().unwrap()));
        // 100.0.0.1 is public
        assert!(!is_private_ip(&"100.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_documentation() {
        // TEST-NET-1: 192.0.2.0/24
        assert!(is_private_ip(&"192.0.2.0".parse().unwrap()));
        assert!(is_private_ip(&"192.0.2.1".parse().unwrap()));
        assert!(is_private_ip(&"192.0.2.255".parse().unwrap()));
        // Just outside
        assert!(!is_private_ip(&"192.0.3.0".parse().unwrap()));
        assert!(!is_private_ip(&"192.1.2.0".parse().unwrap()));

        // TEST-NET-2: 198.51.100.0/24
        assert!(is_private_ip(&"198.51.100.0".parse().unwrap()));
        assert!(is_private_ip(&"198.51.100.1".parse().unwrap()));
        assert!(is_private_ip(&"198.51.100.255".parse().unwrap()));
        assert!(!is_private_ip(&"198.51.99.0".parse().unwrap()));
        assert!(!is_private_ip(&"198.52.100.0".parse().unwrap()));

        // TEST-NET-3: 203.0.113.0/24
        assert!(is_private_ip(&"203.0.113.0".parse().unwrap()));
        assert!(is_private_ip(&"203.0.113.1".parse().unwrap()));
        assert!(is_private_ip(&"203.0.113.255".parse().unwrap()));
        assert!(!is_private_ip(&"203.0.114.0".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_unspecified() {
        assert!(is_private_ip(&"0.0.0.0".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv4_broadcast() {
        assert!(is_private_ip(&"255.255.255.255".parse().unwrap()));
    }

    #[test]
    fn test_public_ipv4_various() {
        // Well-known public IPs
        assert!(!is_private_ip(&"8.8.8.8".parse().unwrap())); // Google DNS
        assert!(!is_private_ip(&"1.1.1.1".parse().unwrap())); // Cloudflare DNS
        assert!(!is_private_ip(&"9.9.9.9".parse().unwrap())); // Quad9
        assert!(!is_private_ip(&"208.67.222.222".parse().unwrap())); // OpenDNS
        assert!(!is_private_ip(&"142.250.80.46".parse().unwrap())); // google.com
        assert!(!is_private_ip(&"140.82.121.3".parse().unwrap())); // github.com
    }

    // ========================================================================
    // IPv6 private ranges
    // ========================================================================

    #[test]
    fn test_private_ipv6_loopback() {
        assert!(is_private_ip(&"::1".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6_unspecified() {
        assert!(is_private_ip(&"::".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6_unique_local_fc00() {
        // fc00::/7 — both fc00 and fd00 prefixes
        assert!(is_private_ip(&"fc00::".parse().unwrap()));
        assert!(is_private_ip(&"fc00::1".parse().unwrap()));
        assert!(is_private_ip(&"fcff:ffff:ffff:ffff::".parse().unwrap()));
        assert!(is_private_ip(&"fd00::".parse().unwrap()));
        assert!(is_private_ip(&"fd00::1".parse().unwrap()));
        assert!(is_private_ip(&"fdff:ffff::".parse().unwrap()));

        // Just outside the range
        assert!(!is_private_ip(&"fbff::".parse().unwrap()));
        assert!(!is_private_ip(&"fe00::".parse().unwrap()));
    }

    #[test]
    fn test_private_ipv6_link_local_fe80() {
        // fe80::/10
        assert!(is_private_ip(&"fe80::".parse().unwrap()));
        assert!(is_private_ip(&"fe80::1".parse().unwrap()));
        assert!(is_private_ip(&"febf:ffff::".parse().unwrap()));

        // Just outside the range
        assert!(!is_private_ip(&"fec0::".parse().unwrap()));
        assert!(!is_private_ip(&"fe7f::".parse().unwrap()));
    }

    #[test]
    fn test_public_ipv6_various() {
        // Well-known public IPv6 addresses
        assert!(!is_private_ip(&"2001:4860:4860::8888".parse().unwrap())); // Google DNS
        assert!(!is_private_ip(&"2606:4700:4700::1111".parse().unwrap())); // Cloudflare DNS
        assert!(!is_private_ip(&"2001:db8::1".parse().unwrap())); // Documentation (public-ish)
    }

    // ========================================================================
    // Private IP lookup returns private result
    // ========================================================================

    #[test]
    fn test_private_ip_lookup_returns_private_flag() {
        let enricher = GeoIpEnricher::new();
        let result = enricher.lookup("10.1.2.3").unwrap();
        assert!(result.is_private);
        assert!(result.country_code.is_none());
        assert!(result.city.is_none());
    }

    #[test]
    fn test_private_ipv6_lookup_returns_private_flag() {
        let enricher = GeoIpEnricher::new();
        let result = enricher.lookup("fe80::1").unwrap();
        assert!(result.is_private);
    }

    #[test]
    fn test_cgnat_ip_lookup_returns_private_flag() {
        let enricher = GeoIpEnricher::new();
        let result = enricher.lookup("100.64.10.20").unwrap();
        assert!(result.is_private);
    }

    // ========================================================================
    // Batch deduplication
    // ========================================================================

    #[test]
    fn test_batch_deduplication() {
        let enricher = GeoIpEnricher::new();

        // Same IP multiple times
        let ips = vec![
            "192.168.1.1",
            "192.168.1.1",
            "192.168.1.1",
            "192.168.1.2",
            "192.168.1.1",
        ];

        let results = enricher.lookup_batch(&ips);
        assert_eq!(results.len(), 2, "Should dedupe to 2 unique IPs");
        assert!(results.contains_key("192.168.1.1"));
        assert!(results.contains_key("192.168.1.2"));

        // All results should be private
        for r in results.values() {
            assert!(r.is_private);
        }
    }

    #[test]
    fn test_batch_lookup_empty() {
        let enricher = GeoIpEnricher::new();
        let results = enricher.lookup_batch(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_batch_lookup_invalid_ips_skipped() {
        let enricher = GeoIpEnricher::new();
        let ips = vec!["192.168.1.1", "not_an_ip", "10.0.0.1"];
        let results = enricher.lookup_batch(&ips);
        // Invalid IP should be skipped (lookup returns None)
        assert_eq!(results.len(), 2);
        assert!(!results.contains_key("not_an_ip"));
    }

    // ========================================================================
    // Cache hits/misses
    // ========================================================================

    // ========================================================================
    // Schema map selective fields
    // ========================================================================

    #[test]
    fn test_schema_map_only_country_code() {
        let result = GeoIpResult {
            continent_code: Some("NA".to_string()),
            continent_name: Some("North America".to_string()),
            country_code: Some("US".to_string()),
            country_name: Some("United States".to_string()),
            city: Some("Mountain View".to_string()),
            latitude: Some(37.386),
            longitude: Some(-122.084),
            timezone: Some("America/Los_Angeles".to_string()),
            postal_code: Some("94043".to_string()),
            subdivision: Some("California".to_string()),
            subdivision_code: Some("CA".to_string()),
            asn: Some(15169),
            asn_org: Some("Google LLC".to_string()),
            is_private: false,
            accuracy_radius: Some(1000),
        };

        let fields = ["country_code"];
        let map = result.to_schema_map(&fields);
        assert_eq!(map.len(), 1);
        assert!(map.contains_key("country_code"));
        assert!(!map.contains_key("city"));
        assert!(!map.contains_key("latitude"));
        assert!(!map.contains_key("asn"));
    }

    #[test]
    fn test_schema_map_aliases() {
        let result = GeoIpResult {
            continent_code: Some("EU".to_string()),
            country_name: Some("Germany".to_string()),
            latitude: Some(52.52),
            longitude: Some(13.405),
            timezone: Some("Europe/Berlin".to_string()),
            postal_code: Some("10115".to_string()),
            subdivision: Some("Berlin".to_string()),
            subdivision_code: Some("BE".to_string()),
            asn_org: Some("Deutsche Telekom".to_string()),
            ..Default::default()
        };

        // Test all aliases
        let aliases = [
            ("continent", "EU"),
            ("country", "Germany"),
            ("tz", "Europe/Berlin"),
            ("postal", "10115"),
            ("zip", "10115"),
            ("state", "Berlin"),
            ("region", "Berlin"),
            ("state_code", "BE"),
            ("region_code", "BE"),
            ("isp", "Deutsche Telekom"),
            ("org", "Deutsche Telekom"),
        ];

        for (alias, expected) in aliases {
            let fields = [alias];
            let map = result.to_schema_map(&fields);
            assert_eq!(
                map.get(alias).unwrap(),
                expected,
                "Alias {alias} should map correctly"
            );
        }
    }

    #[test]
    fn test_schema_map_numeric_aliases() {
        let result = GeoIpResult {
            latitude: Some(37.386),
            longitude: Some(-122.084),
            ..Default::default()
        };

        // lat / latitude
        let map = result.to_schema_map(&["lat"]);
        assert!(map.contains_key("lat"));

        // lon / longitude / lng
        let map = result.to_schema_map(&["lng"]);
        assert!(map.contains_key("lng"));
        let map = result.to_schema_map(&["lon"]);
        assert!(map.contains_key("lon"));
    }

    #[test]
    fn test_schema_map_none_fields_absent() {
        let result = GeoIpResult::default();
        let fields = [
            "continent_code",
            "country_code",
            "city",
            "latitude",
            "longitude",
            "timezone",
            "postal_code",
            "subdivision",
            "asn",
            "asn_org",
            "accuracy_radius",
        ];
        let map = result.to_schema_map(&fields);
        // None of these are set — all should be absent
        for field in fields {
            assert!(
                !map.contains_key(field),
                "None field {field} should be absent from map"
            );
        }
    }

    #[test]
    fn test_schema_map_is_private_always_present() {
        let result = GeoIpResult {
            is_private: true,
            ..Default::default()
        };
        let map = result.to_schema_map(&["is_private"]);
        assert_eq!(
            map.get("is_private").unwrap(),
            &serde_json::Value::Bool(true)
        );

        // Alias "private"
        let map = result.to_schema_map(&["private"]);
        assert_eq!(map.get("private").unwrap(), &serde_json::Value::Bool(true));
    }

    #[test]
    fn test_schema_map_unknown_field_ignored() {
        let result = GeoIpResult {
            country_code: Some("US".to_string()),
            ..Default::default()
        };
        let fields = ["country_code", "nonexistent_field"];
        let map = result.to_schema_map(&fields);
        assert_eq!(map.len(), 1);
        assert!(!map.contains_key("nonexistent_field"));
    }

    #[test]
    fn test_schema_map_asn_numeric() {
        let result = GeoIpResult {
            asn: Some(15169),
            accuracy_radius: Some(500),
            ..Default::default()
        };
        let map = result.to_schema_map(&["asn", "accuracy", "accuracy_radius"]);
        assert_eq!(
            map.get("asn").unwrap(),
            &serde_json::Value::Number(15169.into())
        );
        assert_eq!(
            map.get("accuracy").unwrap(),
            &serde_json::Value::Number(500.into())
        );
        assert_eq!(
            map.get("accuracy_radius").unwrap(),
            &serde_json::Value::Number(500.into())
        );
    }

    // ========================================================================
    // Invalid IP returns None
    // ========================================================================

    #[test]
    fn test_invalid_ip_lookup_returns_none() {
        let enricher = GeoIpEnricher::new();
        assert!(enricher.lookup("not_an_ip").is_none());
        assert!(enricher.lookup("").is_none());
        assert!(enricher.lookup("999.999.999.999").is_none());
        assert!(enricher.lookup("zzzz::yyyy").is_none());
    }

    #[test]
    fn test_public_ip_without_db_returns_none() {
        let enricher = GeoIpEnricher::new();
        // Public IP with no DB loaded — returns None
        let result = enricher.lookup("8.8.8.8");
        assert!(result.is_none());
    }

    // ========================================================================
    // from_config: invalid paths handled gracefully
    // ========================================================================

    #[tokio::test]
    async fn test_from_config_invalid_custom_paths_yields_inactive() {
        // Custom provider with paths pointing at non-existent MMDB files.
        // ensure_databases() returns these paths anyway (doesn't check existence
        // for Custom), then Reader::open_readfile fails and from_config logs
        // and returns an enricher with no readers — is_available() is false.
        let config = GeoIpConfig {
            enabled: true,
            provider: crate::config::GeoIpProvider::Custom,
            city_db_path: Some("/nonexistent/city.mmdb".into()),
            asn_db_path: Some("/nonexistent/asn.mmdb".into()),
            ..Default::default()
        };
        let enricher = GeoIpEnricher::from_config(&config).await;
        // Failed to open non-existent files → no readers available
        assert!(!enricher.is_available());
    }

    #[tokio::test]
    async fn test_from_config_disabled_autodownload_no_files() {
        // Auto-download disabled + no files on disk + DbIpLite provider →
        // enricher is inactive but constructor succeeds.
        let tmp = tempfile::tempdir().expect("tempdir");
        let config = GeoIpConfig {
            enabled: true,
            provider: crate::config::GeoIpProvider::DbIpLite,
            auto_download: crate::config::AutoDownloadConfig {
                enabled: false,
                data_dir: tmp.path().to_string_lossy().into_owned(),
                ..Default::default()
            },
            cache_capacity: 500,
            ..Default::default()
        };
        let enricher = GeoIpEnricher::from_config(&config).await;
        assert!(!enricher.is_available());
        // No databases means no cache to hold anything.
        assert_eq!(enricher.cached_entries(), 0);
    }

    #[tokio::test]
    async fn test_from_config_corrupt_mmdb_file_graceful() {
        // Custom provider pointing at a file that exists but is NOT a valid MMDB.
        // Reader::open_readfile should fail; from_config should log + return
        // an inactive enricher rather than panicking.
        let tmp = tempfile::tempdir().expect("tempdir");
        let bogus = tmp.path().join("bogus.mmdb");
        std::fs::write(&bogus, b"this is not an mmdb file, just text").expect("write");

        let config = GeoIpConfig {
            enabled: true,
            provider: crate::config::GeoIpProvider::Custom,
            city_db_path: Some(bogus.to_string_lossy().into_owned()),
            asn_db_path: None,
            ..Default::default()
        };
        let enricher = GeoIpEnricher::from_config(&config).await;
        // Reader failed to parse bogus file → inactive enricher
        assert!(!enricher.is_available());
    }

    // ========================================================================
    // Batch lookup: mixed private + public IPs
    // ========================================================================

    #[test]
    fn test_batch_lookup_mixed_private_public_no_db() {
        let enricher = GeoIpEnricher::new();
        let ips = vec!["10.0.0.1", "8.8.8.8", "192.168.1.1", "1.1.1.1"];
        let results = enricher.lookup_batch(&ips);
        // Private IPs resolve to private=true; public IPs return None → skipped
        // Only the private ones should be in results
        assert_eq!(results.len(), 2);
        assert!(results.contains_key("10.0.0.1"));
        assert!(results.contains_key("192.168.1.1"));
        assert!(!results.contains_key("8.8.8.8"));
        assert!(!results.contains_key("1.1.1.1"));
        for v in results.values() {
            assert!(v.is_private);
        }
    }

    #[test]
    fn test_batch_lookup_all_invalid_ips() {
        let enricher = GeoIpEnricher::new();
        let ips = vec!["bogus", "also_bogus", "not.an.ip.x"];
        let results = enricher.lookup_batch(&ips);
        assert!(results.is_empty());
    }

    // ========================================================================
    // Cache eviction at full capacity (25% evicted)
    // ========================================================================

    // ========================================================================
    // to_schema_map: empty schema fields
    // ========================================================================

    #[test]
    fn test_to_schema_map_empty_fields_returns_empty() {
        let result = GeoIpResult {
            country_code: Some("US".to_string()),
            city: Some("SF".to_string()),
            latitude: Some(37.7),
            ..Default::default()
        };
        let map = result.to_schema_map(&[]);
        assert!(map.is_empty());
    }

    #[test]
    fn test_to_schema_map_empty_fields_even_when_private() {
        let result = GeoIpResult {
            is_private: true,
            ..Default::default()
        };
        let map = result.to_schema_map(&[]);
        // is_private would be True, but the field is not requested
        assert!(map.is_empty());
    }

    // ========================================================================
    // IPv6 edge cases
    // ========================================================================

    #[test]
    fn test_ipv6_all_zeros_is_unspecified() {
        assert!(is_private_ip(&"::".parse().unwrap()));
    }

    #[test]
    fn test_ipv6_all_ones_is_public() {
        // All ones is NOT in fc00::/7, fe80::/10, ::1, or :: — so it's "public"
        // from the fast-path perspective.
        assert!(!is_private_ip(
            &"ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff".parse().unwrap()
        ));
    }

    #[test]
    fn test_ipv6_mapped_ipv4_private_counts_as_public() {
        // IPv4-mapped IPv6 addresses ::ffff:10.0.0.1 do NOT match any of our
        // IPv6 private patterns — they parse as IPv6 and fall through.
        // This is a known limitation documented by this test.
        let addr: IpAddr = "::ffff:10.0.0.1".parse().unwrap();
        // The is_private function does NOT unwrap mapped v4 → returns false
        assert!(!is_private_ip(&addr));
    }

    #[test]
    fn test_ipv6_mapped_ipv4_zero_is_unspecified() {
        // ::ffff:0.0.0.0 is NOT "::" unspecified — it's a mapped IPv4.
        // Parses as Ipv6 with non-zero segments (0xFFFF).
        let addr: IpAddr = "::ffff:0.0.0.0".parse().unwrap();
        assert!(!is_private_ip(&addr));
    }

    #[test]
    fn test_ipv6_compressed_forms_equivalent() {
        // Various notations for the same address
        let a: IpAddr = "fe80::".parse().unwrap();
        let b: IpAddr = "fe80:0:0:0:0:0:0:0".parse().unwrap();
        assert_eq!(a, b);
        assert!(is_private_ip(&a));
        assert!(is_private_ip(&b));
    }

    // ========================================================================
    // GeoIpResult struct methods and Default impl
    // ========================================================================

    #[test]
    fn test_geoip_result_default_is_all_none_not_private() {
        let r = GeoIpResult::default();
        assert!(r.continent_code.is_none());
        assert!(r.country_code.is_none());
        assert!(r.country_name.is_none());
        assert!(r.city.is_none());
        assert!(r.latitude.is_none());
        assert!(r.longitude.is_none());
        assert!(r.timezone.is_none());
        assert!(r.postal_code.is_none());
        assert!(r.subdivision.is_none());
        assert!(r.subdivision_code.is_none());
        assert!(r.asn.is_none());
        assert!(r.asn_org.is_none());
        assert!(r.accuracy_radius.is_none());
        assert!(
            !r.is_private,
            "Default GeoIpResult should NOT be marked private (private() constructor does that)"
        );
    }

    /// A private address carries the flag and nothing else. factbook answers
    /// these without reading a database, so this is the one record shape that
    /// reaches the schema map on every deployment, database or not.
    #[test]
    fn a_private_record_maps_to_the_flag_and_nothing_else() {
        let record = FactbookRecord::private_shared();
        let r = GeoIpResult::from(&*record);
        assert!(r.is_private);
        assert!(r.country_code.is_none());
        assert!(r.city.is_none());
        assert!(r.asn.is_none());
        assert!(r.latitude.is_none());
    }

    /// Five fields are named differently on each side, and a schema map built
    /// from the wrong one silently writes NULL into a populated column.
    #[test]
    fn the_renamed_fields_cross_the_boundary_intact() {
        let mut record = FactbookRecord::default();
        record.city_name = Some("Boxford".into());
        record.region_name = Some("West Berkshire".into());
        record.region_code = Some("WBK".into());
        record.autonomous_system_number = Some(13335);
        record.autonomous_system_organization = Some("Cloudflare".into());

        let r = GeoIpResult::from(&record);
        assert_eq!(r.city.as_deref(), Some("Boxford"));
        assert_eq!(r.subdivision.as_deref(), Some("West Berkshire"));
        assert_eq!(r.subdivision_code.as_deref(), Some("WBK"));
        assert_eq!(r.asn, Some(13335));
        assert_eq!(r.asn_org.as_deref(), Some("Cloudflare"));
    }

    #[test]
    fn test_geoip_result_clone_preserves_all_fields() {
        let r = GeoIpResult {
            continent_code: Some("NA".to_string()),
            country_code: Some("US".to_string()),
            city: Some("SF".to_string()),
            latitude: Some(37.7),
            longitude: Some(-122.4),
            asn: Some(15169),
            is_private: false,
            accuracy_radius: Some(100),
            ..Default::default()
        };
        let c = r.clone();
        assert_eq!(c.continent_code, r.continent_code);
        assert_eq!(c.country_code, r.country_code);
        assert_eq!(c.city, r.city);
        assert_eq!(c.latitude, r.latitude);
        assert_eq!(c.asn, r.asn);
        assert_eq!(c.accuracy_radius, r.accuracy_radius);
    }

    // ========================================================================
    // CacheStats accuracy
    // ========================================================================

    // ========================================================================
    // is_available: both readers absent
    // ========================================================================

    #[test]
    fn test_is_available_fresh_enricher() {
        let enricher = GeoIpEnricher::new();
        assert!(!enricher.is_available());
    }

    // ========================================================================
    // with_city_db / with_asn_db error paths
    // ========================================================================
}
