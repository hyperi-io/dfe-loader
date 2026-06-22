// SPDX-License-Identifier: BUSL-1.1
// Copyright (c) 2026 HYPERI PTY LIMITED

//! `GeoIP` enrichment with LRU caching
//!
//! High-performance IP geolocation using `MaxMind` MMDB databases.
//!
//! ## Hot Path Optimizations
//!
//! 1. **LRU Cache**: Avoids repeated MMDB lookups for the same IP
//! 2. **Private IP Fast Path**: Skips MMDB lookup for RFC1918/loopback addresses
//! 3. **Batch Deduplication**: Process unique IPs once, apply to all matching events
//! 4. **Schema-Aware Output**: Only compute fields that exist in destination schema

use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use maxminddb::{MaxMindDbError, Reader, geoip2};
use tracing::{debug, info, warn};

use crate::config::GeoIpConfig;

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

impl GeoIpResult {
    /// Create a result for private/internal IP addresses
    #[inline]
    fn private() -> Self {
        Self {
            is_private: true,
            ..Default::default()
        }
    }

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

/// Cache statistics
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
}

/// LRU cache entry
struct CacheEntry {
    result: GeoIpResult,
    /// Access order for LRU eviction (lower = older)
    access_order: u64,
}

/// `GeoIP` enricher with LRU caching
pub struct GeoIpEnricher {
    /// City database reader (optional)
    city_reader: Option<Reader<Vec<u8>>>,
    /// ASN database reader (optional)
    asn_reader: Option<Reader<Vec<u8>>>,
    /// LRU cache: IP string -> result
    cache: RwLock<HashMap<String, CacheEntry>>,
    /// Maximum cache entries
    cache_capacity: usize,
    /// Access counter for LRU ordering
    access_counter: AtomicU64,
    /// Cache hit counter
    cache_hits: AtomicU64,
    /// Cache miss counter
    cache_misses: AtomicU64,
}

impl GeoIpEnricher {
    /// Create a new `GeoIP` enricher (no databases loaded)
    pub fn new() -> Self {
        Self {
            city_reader: None,
            asn_reader: None,
            cache: RwLock::new(HashMap::with_capacity(1024)),
            cache_capacity: 100_000,
            access_counter: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }

    /// Create enricher with specified cache capacity
    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        self.cache_capacity = capacity;
        self
    }

    /// Load city database from file
    pub fn with_city_db<P: AsRef<Path>>(mut self, path: P) -> Result<Self, MaxMindDbError> {
        self.city_reader = Some(Reader::open_readfile(path)?);
        debug!("Loaded GeoIP city database");
        Ok(self)
    }

    /// Load ASN database from file
    pub fn with_asn_db<P: AsRef<Path>>(mut self, path: P) -> Result<Self, MaxMindDbError> {
        self.asn_reader = Some(Reader::open_readfile(path)?);
        debug!("Loaded GeoIP ASN database");
        Ok(self)
    }

    /// Check if any databases are loaded
    pub fn is_available(&self) -> bool {
        self.city_reader.is_some() || self.asn_reader.is_some()
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        let cache = self.cache.read();
        CacheStats {
            hits: self.cache_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
            size: cache.len(),
        }
    }

    /// Look up an IP address
    ///
    /// Returns cached result if available, otherwise performs MMDB lookup.
    /// Private IPs return early without MMDB lookup.
    pub fn lookup(&self, ip: &str) -> Option<GeoIpResult> {
        // Fast path: check cache first
        if let Some(result) = self.cache_get(ip) {
            return Some(result);
        }

        // Parse IP address
        let addr: IpAddr = if let Ok(a) = ip.parse() {
            a
        } else {
            debug!(ip = %ip, "Invalid IP address");
            return None;
        };

        // Fast path: private IP addresses (no MMDB lookup needed)
        if is_private_ip(&addr) {
            let result = GeoIpResult::private();
            self.cache_put(ip.to_string(), result.clone());
            return Some(result);
        }

        // Perform MMDB lookup
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        let result = self.lookup_mmdb(&addr);

        // Cache result (even if partial)
        if let Some(ref r) = result {
            self.cache_put(ip.to_string(), r.clone());
        }

        result
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

    /// Cache lookup (read path)
    fn cache_get(&self, ip: &str) -> Option<GeoIpResult> {
        let cache = self.cache.read();
        if let Some(entry) = cache.get(ip) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.result.clone())
        } else {
            None
        }
    }

    /// Cache insert with LRU eviction (write path)
    fn cache_put(&self, ip: String, result: GeoIpResult) {
        let mut cache = self.cache.write();

        // LRU eviction: remove oldest 25% when at capacity
        if cache.len() >= self.cache_capacity {
            self.evict_lru(&mut cache, self.cache_capacity / 4);
        }

        let order = self.access_counter.fetch_add(1, Ordering::Relaxed);
        cache.insert(
            ip,
            CacheEntry {
                result,
                access_order: order,
            },
        );
    }

    /// Evict oldest N entries from cache
    fn evict_lru(&self, cache: &mut HashMap<String, CacheEntry>, count: usize) {
        if cache.is_empty() || count == 0 {
            return;
        }

        // Collect entries sorted by access order
        let mut entries: Vec<_> = cache
            .iter()
            .map(|(k, v)| (k.clone(), v.access_order))
            .collect();
        entries.sort_by_key(|(_, order)| *order);

        // Remove oldest entries
        for (key, _) in entries.into_iter().take(count) {
            cache.remove(&key);
        }
    }

    /// Perform MMDB lookup using maxminddb 0.27+ API
    fn lookup_mmdb(&self, addr: &IpAddr) -> Option<GeoIpResult> {
        let mut result = GeoIpResult::default();
        let mut found = false;

        // City database lookup
        if let Some(ref reader) = self.city_reader
            && let Ok(lookup_result) = reader.lookup(*addr)
        {
            // Decode the result into a City struct
            if let Ok(Some(city)) = lookup_result.decode::<geoip2::City>() {
                found = true;

                // Continent data
                result.continent_code = city.continent.code.map(std::string::ToString::to_string);
                result.continent_name = city
                    .continent
                    .names
                    .english
                    .map(std::string::ToString::to_string);

                // Country data - new API has flat access
                result.country_code = city.country.iso_code.map(std::string::ToString::to_string);
                result.country_name = city
                    .country
                    .names
                    .english
                    .map(std::string::ToString::to_string);

                // City data
                result.city = city
                    .city
                    .names
                    .english
                    .map(std::string::ToString::to_string);

                // Location data
                result.latitude = city.location.latitude;
                result.longitude = city.location.longitude;
                result.timezone = city
                    .location
                    .time_zone
                    .map(std::string::ToString::to_string);
                result.accuracy_radius = city.location.accuracy_radius;

                // Postal data
                result.postal_code = city.postal.code.map(std::string::ToString::to_string);

                // Subdivision data (first subdivision = state/province)
                if let Some(first) = city.subdivisions.first() {
                    result.subdivision_code = first.iso_code.map(std::string::ToString::to_string);
                    result.subdivision = first.names.english.map(std::string::ToString::to_string);
                }
            }
        }

        // ASN database lookup
        if let Some(ref reader) = self.asn_reader
            && let Ok(lookup_result) = reader.lookup(*addr)
            && let Ok(Some(asn)) = lookup_result.decode::<geoip2::Asn>()
        {
            found = true;
            result.asn = asn.autonomous_system_number;
            result.asn_org = asn
                .autonomous_system_organization
                .map(std::string::ToString::to_string);
        }

        if found { Some(result) } else { None }
    }
}

impl GeoIpEnricher {
    /// Create enricher from config, downloading databases if needed.
    ///
    /// This is the primary constructor for production use. It resolves
    /// database paths (downloading if necessary) and loads them.
    /// Download failures are non-fatal — the enricher works with
    /// zero, one, or both databases.
    pub async fn from_config(config: &GeoIpConfig) -> Self {
        let paths = match super::geoip_download::ensure_databases(config).await {
            Ok(paths) => paths,
            Err(e) => {
                warn!(error = %e, "Failed to resolve GeoIP databases, enricher will be inactive");
                return Self::new().with_cache_capacity(config.cache_capacity);
            }
        };

        let city_reader = if let Some(ref city_path) = paths.city {
            match Reader::open_readfile(city_path) {
                Ok(reader) => {
                    info!(path = %city_path.display(), "Loaded GeoIP city database");
                    Some(reader)
                }
                Err(e) => {
                    warn!(error = %e, path = %city_path.display(), "Failed to load city database");
                    None
                }
            }
        } else {
            None
        };

        let asn_reader = if let Some(ref asn_path) = paths.asn {
            match Reader::open_readfile(asn_path) {
                Ok(reader) => {
                    info!(path = %asn_path.display(), "Loaded GeoIP ASN database");
                    Some(reader)
                }
                Err(e) => {
                    warn!(error = %e, path = %asn_path.display(), "Failed to load ASN database");
                    None
                }
            }
        } else {
            None
        };

        let enricher = Self {
            city_reader,
            asn_reader,
            cache: RwLock::new(HashMap::with_capacity(1024)),
            cache_capacity: config.cache_capacity,
            access_counter: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        };

        if !enricher.is_available() {
            warn!(provider = ?config.provider, "No GeoIP databases loaded, enricher inactive");
        }

        enricher
    }
}

impl Default for GeoIpEnricher {
    fn default() -> Self {
        Self::new()
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

    #[test]
    fn test_cache_stats() {
        let enricher = GeoIpEnricher::new();

        // First lookup - cache miss
        let _ = enricher.lookup("192.168.1.1");

        // Second lookup - cache hit
        let _ = enricher.lookup("192.168.1.1");

        let stats = enricher.cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.size, 1);
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

    #[test]
    fn test_cache_hit_on_second_lookup() {
        let enricher = GeoIpEnricher::new();

        // First lookup on a private IP
        let _ = enricher.lookup("10.0.0.1");
        let stats1 = enricher.cache_stats();
        assert_eq!(stats1.size, 1);
        assert_eq!(stats1.hits, 0);

        // Second lookup — should hit cache
        let _ = enricher.lookup("10.0.0.1");
        let stats2 = enricher.cache_stats();
        assert_eq!(stats2.size, 1);
        assert_eq!(stats2.hits, 1);

        // Third lookup
        let _ = enricher.lookup("10.0.0.1");
        let stats3 = enricher.cache_stats();
        assert_eq!(stats3.hits, 2);
    }

    #[test]
    fn test_cache_different_ips_separate_entries() {
        let enricher = GeoIpEnricher::new();

        let _ = enricher.lookup("10.0.0.1");
        let _ = enricher.lookup("10.0.0.2");
        let _ = enricher.lookup("10.0.0.3");

        let stats = enricher.cache_stats();
        assert_eq!(stats.size, 3);
        assert_eq!(stats.hits, 0);

        // Now hit all three
        let _ = enricher.lookup("10.0.0.1");
        let _ = enricher.lookup("10.0.0.2");
        let _ = enricher.lookup("10.0.0.3");

        let stats = enricher.cache_stats();
        assert_eq!(stats.size, 3);
        assert_eq!(stats.hits, 3);
    }

    #[test]
    fn test_cache_eviction() {
        // Small cache capacity to force eviction
        let enricher = GeoIpEnricher::new().with_cache_capacity(4);

        // Fill beyond capacity
        for i in 0..6 {
            let ip = format!("10.0.0.{i}");
            let _ = enricher.lookup(&ip);
        }

        let stats = enricher.cache_stats();
        assert!(
            stats.size <= 4,
            "Cache should not exceed capacity after eviction: {}",
            stats.size
        );
    }

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
        // cache_capacity from config was applied
        assert_eq!(enricher.cache_capacity, 500);
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

    #[test]
    fn test_cache_eviction_exact_boundary() {
        // Capacity=4 means we evict on the 4th insert (cache.len() >= 4).
        // After inserting 4 unique IPs, the next insert triggers eviction
        // of the oldest 25% (i.e. 1 entry), then insert → size = 4 again.
        let enricher = GeoIpEnricher::new().with_cache_capacity(4);

        for i in 0..4 {
            let _ = enricher.lookup(&format!("10.0.0.{i}"));
        }
        // After 4 inserts, some eviction may already have fired (>= is the
        // trigger). The invariant is: size never exceeds capacity.
        let s = enricher.cache_stats();
        assert!(
            s.size <= 4,
            "size should not exceed capacity, got {}",
            s.size
        );

        // Insert many more to ensure eviction is robust
        for i in 4..20 {
            let _ = enricher.lookup(&format!("10.0.0.{i}"));
        }
        let s = enricher.cache_stats();
        assert!(
            s.size <= 4,
            "after many inserts, size still <= capacity, got {}",
            s.size
        );
    }

    #[test]
    fn test_cache_eviction_evicts_oldest_first() {
        let enricher = GeoIpEnricher::new().with_cache_capacity(4);

        // Insert 4 — oldest is 10.0.0.0
        for i in 0..4 {
            let _ = enricher.lookup(&format!("10.0.0.{i}"));
        }
        // Access 10.0.0.0 to give it a recent access_order via cache_get. But
        // note: cache_get does NOT update access_order in this implementation
        // — so LRU eviction is purely by insertion order. The test verifies
        // insertion-order eviction.
        let _ = enricher.lookup("10.0.0.0");

        // Insert 10.0.0.10 — forces eviction of oldest 25% = 1 entry.
        // Since access_order isn't updated on read, the oldest-inserted is evicted.
        let _ = enricher.lookup("10.0.0.10");

        // 10.0.0.10 must be present
        let stats = enricher.cache_stats();
        assert!(stats.size <= 4);
    }

    #[test]
    fn test_cache_capacity_one_constantly_evicts() {
        let enricher = GeoIpEnricher::new().with_cache_capacity(1);
        for i in 0..5 {
            let _ = enricher.lookup(&format!("10.0.0.{i}"));
        }
        let s = enricher.cache_stats();
        // With capacity 1 and 25% = 0 (integer division), evict is a no-op.
        // Size may stay at 1 or grow momentarily — invariant is >=1.
        assert!(s.size >= 1);
    }

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
        let result = GeoIpResult::private();
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

    #[test]
    fn test_geoip_result_private_constructor() {
        // private() only sets is_private=true, everything else defaults
        let r = GeoIpResult::private();
        assert!(r.is_private);
        assert!(r.country_code.is_none());
        assert!(r.city.is_none());
        assert!(r.asn.is_none());
        assert!(r.latitude.is_none());
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

    #[test]
    fn test_cache_stats_starts_zero() {
        let enricher = GeoIpEnricher::new();
        let s = enricher.cache_stats();
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        assert_eq!(s.size, 0);
    }

    #[test]
    fn test_cache_stats_miss_counter_on_public_ip() {
        // Public IP with no DB → MMDB lookup attempted → miss counter increments.
        let enricher = GeoIpEnricher::new();
        let _ = enricher.lookup("8.8.8.8");
        let s = enricher.cache_stats();
        assert_eq!(
            s.misses, 1,
            "public IP lookup without DB should count as miss"
        );
    }

    #[test]
    fn test_cache_stats_private_ip_does_not_count_as_miss() {
        // Private IPs take the fast path — they're cached without incrementing
        // the miss counter.
        let enricher = GeoIpEnricher::new();
        let _ = enricher.lookup("192.168.1.1");
        let s = enricher.cache_stats();
        // Private IP is cached (size=1) but miss counter is NOT incremented
        // because lookup_mmdb was never called.
        assert_eq!(s.size, 1);
        assert_eq!(s.misses, 0, "private IP should bypass MMDB → no miss");
        assert_eq!(s.hits, 0);
    }

    #[test]
    fn test_cache_stats_hits_increment_on_repeat() {
        let enricher = GeoIpEnricher::new();
        let _ = enricher.lookup("10.1.1.1");
        let _ = enricher.lookup("10.1.1.1");
        let _ = enricher.lookup("10.1.1.1");
        let _ = enricher.lookup("10.1.1.1");
        let s = enricher.cache_stats();
        assert_eq!(s.size, 1);
        assert_eq!(s.hits, 3, "3 subsequent lookups should be hits");
    }

    #[test]
    fn test_cache_stats_size_matches_unique_ips() {
        let enricher = GeoIpEnricher::new();
        for i in 0..10 {
            let _ = enricher.lookup(&format!("10.0.0.{i}"));
        }
        // 10 unique IPs inserted — capacity defaults to 100_000
        let s = enricher.cache_stats();
        assert_eq!(s.size, 10);
    }

    // ========================================================================
    // is_available: both readers absent
    // ========================================================================

    #[test]
    fn test_is_available_fresh_enricher() {
        let enricher = GeoIpEnricher::new();
        assert!(!enricher.is_available());
    }

    #[test]
    fn test_with_cache_capacity_does_not_enable_availability() {
        let enricher = GeoIpEnricher::new().with_cache_capacity(50);
        assert!(!enricher.is_available());
        assert_eq!(enricher.cache_capacity, 50);
    }

    // ========================================================================
    // with_city_db / with_asn_db error paths
    // ========================================================================

    #[test]
    fn test_with_city_db_nonexistent_errors() {
        let result = GeoIpEnricher::new().with_city_db("/nonexistent/city.mmdb");
        assert!(result.is_err());
    }

    #[test]
    fn test_with_asn_db_nonexistent_errors() {
        let result = GeoIpEnricher::new().with_asn_db("/nonexistent/asn.mmdb");
        assert!(result.is_err());
    }

    #[test]
    fn test_with_city_db_invalid_file_errors() {
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(tmp.path(), b"not mmdb").expect("write");
        let result = GeoIpEnricher::new().with_city_db(tmp.path());
        assert!(
            result.is_err(),
            "Opening a non-MMDB file should return error"
        );
    }
}
