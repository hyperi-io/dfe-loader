// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! GeoIP enrichment with LRU caching
//!
//! High-performance IP geolocation using MaxMind MMDB databases.
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

/// GeoIP lookup result with all available fields
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
    /// Timezone (IANA format, e.g., "America/New_York")
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

/// GeoIP enricher with LRU caching
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
    /// Create a new GeoIP enricher (no databases loaded)
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
        let addr: IpAddr = match ip.parse() {
            Ok(a) => a,
            Err(_) => {
                debug!(ip = %ip, "Invalid IP address");
                return None;
            }
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
                result.continent_code = city.continent.code.map(|s| s.to_string());
                result.continent_name = city.continent.names.english.map(|s| s.to_string());

                // Country data - new API has flat access
                result.country_code = city.country.iso_code.map(|s| s.to_string());
                result.country_name = city.country.names.english.map(|s| s.to_string());

                // City data
                result.city = city.city.names.english.map(|s| s.to_string());

                // Location data
                result.latitude = city.location.latitude;
                result.longitude = city.location.longitude;
                result.timezone = city.location.time_zone.map(|s| s.to_string());
                result.accuracy_radius = city.location.accuracy_radius;

                // Postal data
                result.postal_code = city.postal.code.map(|s| s.to_string());

                // Subdivision data (first subdivision = state/province)
                if let Some(first) = city.subdivisions.first() {
                    result.subdivision_code = first.iso_code.map(|s| s.to_string());
                    result.subdivision = first.names.english.map(|s| s.to_string());
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
            result.asn_org = asn.autonomous_system_organization.map(|s| s.to_string());
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

/// Check if IPv6 is unique local (fc00::/7)
#[inline]
fn is_unique_local_ipv6(addr: &Ipv6Addr) -> bool {
    let segments = addr.segments();
    (segments[0] & 0xFE00) == 0xFC00
}

/// Check if IPv6 is link-local (fe80::/10)
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
}
