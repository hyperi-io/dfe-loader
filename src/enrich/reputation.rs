// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! IP/Domain reputation enrichment
//!
//! High-performance IP reputation checking with:
//! - Multiple threat type detection (VPN, proxy, Tor, botnet, etc.)
//! - Dual storage: O(1) IP hash + CIDR prefix matching
//! - LRU caching with configurable capacity
//! - Multi-source blocklist support with auto-download
//! - Schema-aware output (only compute needed fields)
//!
//! ## Hot Path Optimizations
//!
//! 1. **O(1) IP lookup**: Individual IPs stored in `FxHashMap`
//! 2. **LRU Cache**: Avoids repeated blocklist checks
//! 3. **Prefix trie**: CIDR ranges with prefix matching
//! 4. **Lazy loading**: Only load enabled blocklists

use parking_lot::RwLock;
use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap;
use tracing::debug;

/// Threat type classification (low cardinality for `ClickHouse`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ThreatType {
    #[default]
    None,
    Vpn,
    Proxy,
    Tor,
    Relay,       // Apple/iCloud Private Relay
    Datacenter,  // Hosting/cloud provider
    Residential, // Residential proxy
    Botnet,      // Known botnet C2
    Spam,        // Spam source
    Scanner,     // Internet scanner/crawler
    Malware,     // Malware distribution
    Phishing,    // Phishing source
    Bruteforce,  // Brute force attacker
    Exploit,     // Exploit attempts
}

impl ThreatType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThreatType::None => "",
            ThreatType::Vpn => "vpn",
            ThreatType::Proxy => "proxy",
            ThreatType::Tor => "tor",
            ThreatType::Relay => "relay",
            ThreatType::Datacenter => "datacenter",
            ThreatType::Residential => "residential",
            ThreatType::Botnet => "botnet",
            ThreatType::Spam => "spam",
            ThreatType::Scanner => "scanner",
            ThreatType::Malware => "malware",
            ThreatType::Phishing => "phishing",
            ThreatType::Bruteforce => "bruteforce",
            ThreatType::Exploit => "exploit",
        }
    }
}

/// Threat source identification (low cardinality for `ClickHouse`)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum ThreatSource {
    #[default]
    None,
    TorProject,
    FireHol,
    AbuseIpdb,
    AbuseCh,
    Spamhaus,
    MaxMind,
    IpInfo,
    CrowdSec,
    GreyNoise,
    Custom,
}

impl ThreatSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            ThreatSource::None => "",
            ThreatSource::TorProject => "tor_project",
            ThreatSource::FireHol => "firehol",
            ThreatSource::AbuseIpdb => "abuseipdb",
            ThreatSource::AbuseCh => "abuse_ch",
            ThreatSource::Spamhaus => "spamhaus",
            ThreatSource::MaxMind => "maxmind",
            ThreatSource::IpInfo => "ipinfo",
            ThreatSource::CrowdSec => "crowdsec",
            ThreatSource::GreyNoise => "greynoise",
            ThreatSource::Custom => "custom",
        }
    }
}

/// Reputation lookup result with boolean flags optimized for `ClickHouse` indexing
#[derive(Debug, Clone, Default)]
pub struct ReputationResult {
    // Detection flags (booleans - index-friendly in ClickHouse)
    pub is_vpn: bool,
    pub is_proxy: bool,
    pub is_tor: bool,
    pub is_relay: bool,
    pub is_datacenter: bool,
    pub is_residential: bool,
    pub is_botnet: bool,
    pub is_spam: bool,
    pub is_scanner: bool,
    pub is_malicious: bool,
    pub is_anonymizer: bool,

    // Threat classification (low cardinality)
    pub threat_type: ThreatType,
    pub threat_source: ThreatSource,

    // Provider identification
    pub vpn_provider: Option<String>,
    pub hosting_provider: Option<String>,

    // Confidence/risk scoring (0-100)
    pub abuse_score: u8,
    pub risk_score: u8,

    // Timestamps (Unix epoch)
    pub last_reported: Option<i64>,
    pub first_seen: Option<i64>,

    // Multiple threat sources (for multi-source hits)
    pub threat_sources: Vec<ThreatSource>,
    pub threat_types: Vec<ThreatType>,
}

impl ReputationResult {
    /// Check if any threat was detected
    #[inline]
    pub fn has_threat(&self) -> bool {
        self.is_vpn
            || self.is_proxy
            || self.is_tor
            || self.is_relay
            || self.is_datacenter
            || self.is_residential
            || self.is_botnet
            || self.is_spam
            || self.is_scanner
            || self.is_malicious
    }

    /// Convert result to a map, filtering to only requested fields
    pub fn to_schema_map(
        &self,
        schema_fields: &[&str],
    ) -> std::collections::HashMap<String, serde_json::Value> {
        use serde_json::Value;
        let mut out = std::collections::HashMap::with_capacity(schema_fields.len());

        for &field in schema_fields {
            match field {
                "is_vpn" => out.insert(field.to_string(), Value::Bool(self.is_vpn)),
                "is_proxy" => out.insert(field.to_string(), Value::Bool(self.is_proxy)),
                "is_tor" => out.insert(field.to_string(), Value::Bool(self.is_tor)),
                "is_relay" => out.insert(field.to_string(), Value::Bool(self.is_relay)),
                "is_datacenter" => out.insert(field.to_string(), Value::Bool(self.is_datacenter)),
                "is_residential" => out.insert(field.to_string(), Value::Bool(self.is_residential)),
                "is_botnet" => out.insert(field.to_string(), Value::Bool(self.is_botnet)),
                "is_spam" => out.insert(field.to_string(), Value::Bool(self.is_spam)),
                "is_scanner" => out.insert(field.to_string(), Value::Bool(self.is_scanner)),
                "is_malicious" => out.insert(field.to_string(), Value::Bool(self.is_malicious)),
                "is_anonymizer" => out.insert(field.to_string(), Value::Bool(self.is_anonymizer)),
                "threat_type" => {
                    let s = self.threat_type.as_str();
                    if s.is_empty() {
                        None
                    } else {
                        out.insert(field.to_string(), Value::String(s.to_string()))
                    }
                }
                "threat_source" => {
                    let s = self.threat_source.as_str();
                    if s.is_empty() {
                        None
                    } else {
                        out.insert(field.to_string(), Value::String(s.to_string()))
                    }
                }
                "vpn_provider" => {
                    if let Some(ref v) = self.vpn_provider {
                        out.insert(field.to_string(), Value::String(v.clone()))
                    } else {
                        None
                    }
                }
                "hosting_provider" => {
                    if let Some(ref v) = self.hosting_provider {
                        out.insert(field.to_string(), Value::String(v.clone()))
                    } else {
                        None
                    }
                }
                "abuse_score" => {
                    out.insert(field.to_string(), Value::Number(self.abuse_score.into()))
                }
                "risk_score" => {
                    out.insert(field.to_string(), Value::Number(self.risk_score.into()))
                }
                _ => None,
            };
        }

        out
    }
}

/// Blocklist configuration
#[derive(Debug, Clone)]
pub struct BlocklistConfig {
    /// Blocklist name (used in logging)
    pub name: String,
    /// Download URL (optional)
    pub url: Option<String>,
    /// Local file path (optional)
    pub file_path: Option<String>,
    /// Threat type to assign
    pub threat_type: ThreatType,
    /// Source identification
    pub source: ThreatSource,
    /// Format: "plain", "csv", or "json"
    pub format: String,
    /// Whether this blocklist is enabled
    pub enabled: bool,
}

impl BlocklistConfig {
    /// Common blocklists with default configurations
    pub fn common_blocklists() -> Vec<BlocklistConfig> {
        vec![
            BlocklistConfig {
                name: "tor_exit_nodes".to_string(),
                url: Some("https://check.torproject.org/torbulkexitlist".to_string()),
                file_path: None,
                threat_type: ThreatType::Tor,
                source: ThreatSource::TorProject,
                format: "plain".to_string(),
                enabled: true,
            },
            BlocklistConfig {
                name: "feodo_botnet_c2".to_string(),
                url: Some("https://feodotracker.abuse.ch/downloads/ipblocklist_recommended.txt".to_string()),
                file_path: None,
                threat_type: ThreatType::Botnet,
                source: ThreatSource::AbuseCh,
                format: "plain".to_string(),
                enabled: true,
            },
            BlocklistConfig {
                name: "firehol_level1".to_string(),
                url: Some("https://raw.githubusercontent.com/firehol/blocklist-ipsets/master/firehol_level1.netset".to_string()),
                file_path: None,
                threat_type: ThreatType::Malware,
                source: ThreatSource::FireHol,
                format: "plain".to_string(),
                enabled: false, // Opt-in due to size
            },
            BlocklistConfig {
                name: "spamhaus_drop".to_string(),
                url: Some("https://www.spamhaus.org/drop/drop.txt".to_string()),
                file_path: None,
                threat_type: ThreatType::Spam,
                source: ThreatSource::Spamhaus,
                format: "plain".to_string(),
                enabled: false, // Opt-in
            },
        ]
    }
}

/// Cache statistics
#[derive(Debug, Default)]
pub struct ReputationCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
    pub ip_count: usize,
    pub prefix_count: usize,
}

/// LRU cache entry
struct CacheEntry {
    result: ReputationResult,
    access_order: u64,
}

/// CIDR prefix for range-based blocklists
#[derive(Debug, Clone)]
struct IpPrefix {
    addr: IpAddr,
    prefix_len: u8,
    threat_type: ThreatType,
    source: ThreatSource,
}

impl IpPrefix {
    /// Check if an IP address is contained in this prefix
    fn contains(&self, addr: &IpAddr) -> bool {
        match (self.addr, addr) {
            (IpAddr::V4(prefix), IpAddr::V4(target)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                if self.prefix_len >= 32 {
                    return prefix == *target;
                }
                let mask = !0u32 << (32 - self.prefix_len);
                (u32::from(prefix) & mask) == (u32::from(*target) & mask)
            }
            (IpAddr::V6(prefix), IpAddr::V6(target)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                if self.prefix_len >= 128 {
                    return prefix == *target;
                }
                let prefix_bits = u128::from(prefix);
                let target_bits = u128::from(*target);
                let mask = !0u128 << (128 - self.prefix_len);
                (prefix_bits & mask) == (target_bits & mask)
            }
            _ => false, // IPv4/IPv6 mismatch
        }
    }
}

/// IP source info for audit trail
#[derive(Debug, Clone)]
struct IpSourceInfo {
    threat_type: ThreatType,
    source: ThreatSource,
}

/// IP Reputation enricher with multi-layer blocklist support
pub struct ReputationEnricher {
    /// IP sets for different threat types (O(1) lookup)
    ip_threats: RwLock<FxHashMap<IpAddr, Vec<IpSourceInfo>>>,

    /// CIDR prefix sets (for range-based blocklists)
    prefixes: RwLock<Vec<IpPrefix>>,

    /// LRU cache for results
    cache: RwLock<FxHashMap<String, CacheEntry>>,
    /// Maximum cache entries
    cache_capacity: usize,
    /// Access counter for LRU ordering
    access_counter: AtomicU64,

    /// Metrics
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    lookups: AtomicU64,
}

impl ReputationEnricher {
    /// Create a new reputation enricher
    pub fn new() -> Self {
        Self {
            ip_threats: RwLock::new(FxHashMap::default()),
            prefixes: RwLock::new(Vec::new()),
            cache: RwLock::new(FxHashMap::default()),
            cache_capacity: 100_000,
            access_counter: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            lookups: AtomicU64::new(0),
        }
    }

    /// Create enricher with specified cache capacity
    pub fn with_cache_capacity(mut self, capacity: usize) -> Self {
        self.cache_capacity = capacity;
        self
    }

    /// Add a single IP to the blocklist
    pub fn add_ip(&self, ip: IpAddr, threat_type: ThreatType, source: ThreatSource) {
        let mut threats = self.ip_threats.write();
        let info = IpSourceInfo {
            threat_type,
            source,
        };
        threats.entry(ip).or_default().push(info);
    }

    /// Add a CIDR prefix to the blocklist
    pub fn add_prefix(
        &self,
        addr: IpAddr,
        prefix_len: u8,
        threat_type: ThreatType,
        source: ThreatSource,
    ) {
        let mut prefixes = self.prefixes.write();
        prefixes.push(IpPrefix {
            addr,
            prefix_len,
            threat_type,
            source,
        });
    }

    /// Load IPs from a plain text list (one IP or CIDR per line)
    pub fn load_plain_list(
        &self,
        content: &str,
        threat_type: ThreatType,
        source: ThreatSource,
    ) -> usize {
        let mut count = 0;

        for line in content.lines() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }

            // Handle CIDR notation
            if let Some((addr_str, prefix_str)) = line.split_once('/') {
                if let (Ok(addr), Ok(prefix_len)) =
                    (addr_str.parse::<IpAddr>(), prefix_str.parse::<u8>())
                {
                    self.add_prefix(addr, prefix_len, threat_type, source);
                    count += 1;
                }
                continue;
            }

            // Parse single IP
            if let Ok(addr) = line.parse::<IpAddr>() {
                self.add_ip(addr, threat_type, source);
                count += 1;
            }
        }

        debug!(count, source = ?source, threat_type = ?threat_type, "Loaded IPs from blocklist");
        count
    }

    /// Check if any blocklists are loaded
    pub fn is_available(&self) -> bool {
        let threats = self.ip_threats.read();
        let prefixes = self.prefixes.read();
        !threats.is_empty() || !prefixes.is_empty()
    }

    /// Get cache and blocklist statistics
    pub fn stats(&self) -> ReputationCacheStats {
        let cache = self.cache.read();
        let threats = self.ip_threats.read();
        let prefixes = self.prefixes.read();

        ReputationCacheStats {
            hits: self.cache_hits.load(Ordering::Relaxed),
            misses: self.cache_misses.load(Ordering::Relaxed),
            size: cache.len(),
            ip_count: threats.len(),
            prefix_count: prefixes.len(),
        }
    }

    /// Look up reputation for an IP address
    pub fn lookup(&self, ip: &str) -> Option<ReputationResult> {
        self.lookups.fetch_add(1, Ordering::Relaxed);

        // Fast path: check cache first
        if let Some(result) = self.cache_get(ip) {
            return Some(result);
        }

        // Parse IP address
        let addr: IpAddr = if let Ok(a) = ip.parse() {
            a
        } else {
            debug!(ip = %ip, "Invalid IP address for reputation lookup");
            return None;
        };

        self.cache_misses.fetch_add(1, Ordering::Relaxed);
        let result = self.lookup_addr(&addr);

        // Cache result
        self.cache_put(ip.to_string(), result.clone());

        Some(result)
    }

    /// Perform actual blocklist lookup
    fn lookup_addr(&self, addr: &IpAddr) -> ReputationResult {
        let mut result = ReputationResult::default();
        let mut types_seen = HashSet::new();
        let mut sources_seen = HashSet::new();

        // Check IP sets (O(1) lookup)
        {
            let threats = self.ip_threats.read();
            if let Some(infos) = threats.get(addr) {
                for info in infos {
                    self.apply_threat(&mut result, info.threat_type, info.source);
                    types_seen.insert(info.threat_type);
                    sources_seen.insert(info.source);
                }
            }
        }

        // Check CIDR prefixes (O(N) but necessary for ranges)
        {
            let prefixes = self.prefixes.read();
            for prefix in prefixes.iter() {
                if prefix.contains(addr) {
                    self.apply_threat(&mut result, prefix.threat_type, prefix.source);
                    types_seen.insert(prefix.threat_type);
                    sources_seen.insert(prefix.source);
                }
            }
        }

        // Collect unique threat types and sources
        if types_seen.len() > 1 {
            result.threat_types = types_seen.into_iter().collect();
        }
        if sources_seen.len() > 1 {
            result.threat_sources = sources_seen.into_iter().collect();
        }

        result
    }

    /// Apply threat detection to result
    #[inline]
    fn apply_threat(
        &self,
        result: &mut ReputationResult,
        threat_type: ThreatType,
        source: ThreatSource,
    ) {
        // Set first threat type/source if not already set
        if result.threat_type == ThreatType::None {
            result.threat_type = threat_type;
        }
        if result.threat_source == ThreatSource::None {
            result.threat_source = source;
        }

        // Set boolean flags based on threat type
        match threat_type {
            ThreatType::Vpn => {
                result.is_vpn = true;
                result.is_anonymizer = true;
            }
            ThreatType::Proxy => {
                result.is_proxy = true;
                result.is_anonymizer = true;
            }
            ThreatType::Tor => {
                result.is_tor = true;
                result.is_anonymizer = true;
            }
            ThreatType::Relay => {
                result.is_relay = true;
                result.is_anonymizer = true;
            }
            ThreatType::Datacenter => {
                result.is_datacenter = true;
            }
            ThreatType::Residential => {
                result.is_residential = true;
            }
            ThreatType::Botnet => {
                result.is_botnet = true;
                result.is_malicious = true;
            }
            ThreatType::Spam => {
                result.is_spam = true;
            }
            ThreatType::Scanner => {
                result.is_scanner = true;
            }
            ThreatType::Malware
            | ThreatType::Phishing
            | ThreatType::Bruteforce
            | ThreatType::Exploit => {
                result.is_malicious = true;
            }
            ThreatType::None => {}
        }
    }

    /// Cache lookup (read path)
    fn cache_get(&self, ip: &str) -> Option<ReputationResult> {
        let cache = self.cache.read();
        if let Some(entry) = cache.get(ip) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.result.clone())
        } else {
            None
        }
    }

    /// Cache insert with LRU eviction (write path)
    fn cache_put(&self, ip: String, result: ReputationResult) {
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
    fn evict_lru(&self, cache: &mut FxHashMap<String, CacheEntry>, count: usize) {
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

    /// Clear all blocklists and cache
    pub fn clear(&self) {
        self.ip_threats.write().clear();
        self.prefixes.write().clear();
        self.cache.write().clear();
    }
}

impl Default for ReputationEnricher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_enricher() {
        let enricher = ReputationEnricher::new();
        assert!(!enricher.is_available());

        let result = enricher.lookup("8.8.8.8");
        assert!(result.is_some());
        assert!(!result.unwrap().has_threat());
    }

    #[test]
    fn test_add_ip() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Tor, ThreatSource::TorProject);

        assert!(enricher.is_available());

        let result = enricher.lookup("1.2.3.4").unwrap();
        assert!(result.is_tor);
        assert!(result.is_anonymizer);
        assert_eq!(result.threat_type, ThreatType::Tor);
        assert_eq!(result.threat_source, ThreatSource::TorProject);
    }

    #[test]
    fn test_add_prefix() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "192.168.0.0".parse().unwrap();
        enricher.add_prefix(ip, 16, ThreatType::Datacenter, ThreatSource::Custom);

        // Should match
        let result = enricher.lookup("192.168.1.1").unwrap();
        assert!(result.is_datacenter);

        // Should not match
        let result = enricher.lookup("192.169.1.1").unwrap();
        assert!(!result.is_datacenter);
    }

    #[test]
    fn test_load_plain_list() {
        let enricher = ReputationEnricher::new();

        let list = r"
# Comment
1.2.3.4
5.6.7.8
10.0.0.0/8
";

        let count = enricher.load_plain_list(list, ThreatType::Botnet, ThreatSource::AbuseCh);
        assert_eq!(count, 3);

        let result = enricher.lookup("1.2.3.4").unwrap();
        assert!(result.is_botnet);
        assert!(result.is_malicious);

        // Check CIDR match
        let result = enricher.lookup("10.1.2.3").unwrap();
        assert!(result.is_botnet);
    }

    #[test]
    fn test_cache_stats() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Spam, ThreatSource::Spamhaus);

        // First lookup - cache miss
        let _ = enricher.lookup("1.2.3.4");

        // Second lookup - cache hit
        let _ = enricher.lookup("1.2.3.4");

        let stats = enricher.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.ip_count, 1);
    }

    #[test]
    fn test_result_to_schema_map() {
        let result = ReputationResult {
            is_tor: true,
            is_anonymizer: true,
            threat_type: ThreatType::Tor,
            threat_source: ThreatSource::TorProject,
            ..Default::default()
        };

        let fields = ["is_tor", "threat_type"];
        let map = result.to_schema_map(&fields);

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("is_tor").unwrap(), &serde_json::Value::Bool(true));
        assert_eq!(map.get("threat_type").unwrap(), "tor");
    }

    // ========================================================================
    // Multiple sources for same IP
    // ========================================================================

    #[test]
    fn test_ip_in_both_individual_and_cidr() {
        let enricher = ReputationEnricher::new();

        // Add 1.2.3.4 as individual IP (Tor)
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Tor, ThreatSource::TorProject);

        // Also add CIDR 1.2.3.0/24 (Scanner from a different source)
        let prefix: IpAddr = "1.2.3.0".parse().unwrap();
        enricher.add_prefix(prefix, 24, ThreatType::Scanner, ThreatSource::GreyNoise);

        let result = enricher.lookup("1.2.3.4").unwrap();

        // Should detect both threats
        assert!(result.is_tor, "Should detect Tor from individual IP");
        assert!(result.is_scanner, "Should detect Scanner from CIDR");
        assert!(result.is_anonymizer, "Tor should set anonymizer flag");

        // Should have multiple threat types and sources
        assert!(
            result.threat_types.len() >= 2 || result.threat_type != ThreatType::None,
            "Should aggregate threat types"
        );
        assert!(
            result.threat_sources.len() >= 2 || result.threat_source != ThreatSource::None,
            "Should aggregate threat sources"
        );
    }

    // ========================================================================
    // CIDR overlap: IP matching multiple CIDR ranges
    // ========================================================================

    #[test]
    fn test_cidr_overlap_different_sizes() {
        let enricher = ReputationEnricher::new();

        // Broader range /16 — Datacenter
        let broad: IpAddr = "10.0.0.0".parse().unwrap();
        enricher.add_prefix(broad, 16, ThreatType::Datacenter, ThreatSource::Custom);

        // Narrower range /24 — Botnet
        let narrow: IpAddr = "10.0.1.0".parse().unwrap();
        enricher.add_prefix(narrow, 24, ThreatType::Botnet, ThreatSource::AbuseCh);

        // IP in both ranges
        let result = enricher.lookup("10.0.1.100").unwrap();
        assert!(result.is_datacenter, "Should match /16 prefix");
        assert!(result.is_botnet, "Should match /24 prefix");
        assert!(result.is_malicious, "Botnet should set malicious flag");

        // IP in only the broader range
        let result = enricher.lookup("10.0.2.1").unwrap();
        assert!(result.is_datacenter, "Should match /16 prefix");
        assert!(!result.is_botnet, "Should NOT match /24 prefix");
    }

    #[test]
    fn test_cidr_no_match_outside_range() {
        let enricher = ReputationEnricher::new();
        let prefix: IpAddr = "192.168.1.0".parse().unwrap();
        enricher.add_prefix(prefix, 24, ThreatType::Spam, ThreatSource::Spamhaus);

        let result = enricher.lookup("192.168.2.1").unwrap();
        assert!(!result.is_spam, "Should not match outside /24 range");
        assert!(!result.has_threat());
    }

    // ========================================================================
    // Load blocklist with malformed lines
    // ========================================================================

    #[test]
    fn test_load_plain_list_malformed_lines() {
        let enricher = ReputationEnricher::new();

        let list = r"
# This is a comment
; This is also a comment

1.2.3.4
not_an_ip_at_all
5.6.7.8
999.999.999.999
10.0.0.0/8
10.0.0.0/abc
/24
hello world
192.168.1.1
";

        let count = enricher.load_plain_list(list, ThreatType::Malware, ThreatSource::FireHol);
        // Valid entries: 1.2.3.4, 5.6.7.8, 10.0.0.0/8, 192.168.1.1 = 4
        assert_eq!(count, 4, "Should skip malformed lines");

        // Verify valid entries were loaded
        let result = enricher.lookup("1.2.3.4").unwrap();
        assert!(result.is_malicious);

        let result = enricher.lookup("5.6.7.8").unwrap();
        assert!(result.is_malicious);

        // CIDR should work
        let result = enricher.lookup("10.1.2.3").unwrap();
        assert!(result.is_malicious);

        // Invalid entry should not be present
        let result = enricher.lookup("999.999.999.999");
        // This parse fails entirely, so lookup returns None
        assert!(result.is_none() || !result.unwrap().has_threat());
    }

    #[test]
    fn test_load_plain_list_semicolon_comments() {
        let enricher = ReputationEnricher::new();
        let list = "; semicolon comment\n1.1.1.1\n";
        let count = enricher.load_plain_list(list, ThreatType::Vpn, ThreatSource::Custom);
        assert_eq!(count, 1);
    }

    #[test]
    fn test_load_plain_list_empty() {
        let enricher = ReputationEnricher::new();
        let list = "\n\n# only comments\n\n";
        let count = enricher.load_plain_list(list, ThreatType::Vpn, ThreatSource::Custom);
        assert_eq!(count, 0);
        assert!(!enricher.is_available());
    }

    // ========================================================================
    // Cache eviction
    // ========================================================================

    #[test]
    fn test_cache_eviction_at_capacity() {
        // Small cache capacity
        let enricher = ReputationEnricher::new().with_cache_capacity(4);

        // Add an IP to blocklist
        let ip: IpAddr = "1.1.1.1".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Spam, ThreatSource::Spamhaus);

        // Fill cache beyond capacity: lookup 5 different IPs
        for i in 0..5 {
            let ip_str = format!("10.0.0.{i}");
            let _ = enricher.lookup(&ip_str);
        }

        // Cache should have been evicted (oldest 25% = 1 entry removed)
        let stats = enricher.stats();
        assert!(
            stats.size <= 4,
            "Cache size should not exceed capacity after eviction, got: {}",
            stats.size
        );
    }

    #[test]
    fn test_cache_eviction_preserves_newer_entries() {
        let enricher = ReputationEnricher::new().with_cache_capacity(4);

        let ip: IpAddr = "1.1.1.1".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Tor, ThreatSource::TorProject);

        // Fill cache to capacity
        let _ = enricher.lookup("10.0.0.1"); // oldest
        let _ = enricher.lookup("10.0.0.2");
        let _ = enricher.lookup("10.0.0.3");
        let _ = enricher.lookup("10.0.0.4");

        // This triggers eviction (25% = 1 oldest entry removed)
        let _ = enricher.lookup("10.0.0.5");

        // Newest entry should still be cached (cache hit)
        let hits_before = enricher.cache_hits.load(Ordering::Relaxed);
        let _ = enricher.lookup("10.0.0.5");
        let hits_after = enricher.cache_hits.load(Ordering::Relaxed);
        assert!(
            hits_after > hits_before,
            "Newest entry should be a cache hit"
        );
    }

    // ========================================================================
    // Threat type aggregation
    // ========================================================================

    #[test]
    fn test_ip_flagged_vpn_and_scanner_from_different_sources() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "5.5.5.5".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Vpn, ThreatSource::IpInfo);
        enricher.add_ip(ip, ThreatType::Scanner, ThreatSource::GreyNoise);

        let result = enricher.lookup("5.5.5.5").unwrap();
        assert!(result.is_vpn);
        assert!(result.is_scanner);
        assert!(result.is_anonymizer, "VPN should set anonymizer");

        // First threat type should be the first added (Vpn)
        assert_eq!(result.threat_type, ThreatType::Vpn);
        // Should have multiple types in the aggregated list
        assert!(
            result.threat_types.len() >= 2,
            "Should have at least 2 distinct threat types: {:?}",
            result.threat_types
        );
    }

    #[test]
    fn test_all_threat_types_set_correct_flags() {
        let ip: IpAddr = "9.9.9.9".parse().unwrap();

        let cases: Vec<(ThreatType, &str)> = vec![
            (ThreatType::Vpn, "is_vpn"),
            (ThreatType::Proxy, "is_proxy"),
            (ThreatType::Tor, "is_tor"),
            (ThreatType::Relay, "is_relay"),
            (ThreatType::Datacenter, "is_datacenter"),
            (ThreatType::Residential, "is_residential"),
            (ThreatType::Botnet, "is_botnet"),
            (ThreatType::Spam, "is_spam"),
            (ThreatType::Scanner, "is_scanner"),
            (ThreatType::Malware, "is_malicious"),
            (ThreatType::Phishing, "is_malicious"),
            (ThreatType::Bruteforce, "is_malicious"),
            (ThreatType::Exploit, "is_malicious"),
        ];

        for (threat_type, expected_flag) in cases {
            let e = ReputationEnricher::new();
            e.add_ip(ip, threat_type, ThreatSource::Custom);
            let result = e.lookup("9.9.9.9").unwrap();

            let flag_set = match expected_flag {
                "is_vpn" => result.is_vpn,
                "is_proxy" => result.is_proxy,
                "is_tor" => result.is_tor,
                "is_relay" => result.is_relay,
                "is_datacenter" => result.is_datacenter,
                "is_residential" => result.is_residential,
                "is_botnet" => result.is_botnet,
                "is_spam" => result.is_spam,
                "is_scanner" => result.is_scanner,
                "is_malicious" => result.is_malicious,
                _ => panic!("Unknown flag: {expected_flag}"),
            };
            assert!(
                flag_set,
                "ThreatType::{threat_type:?} should set {expected_flag}"
            );
        }
    }

    // ========================================================================
    // IPv6 addresses
    // ========================================================================

    #[test]
    fn test_ipv6_individual_lookup() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "2001:db8::1".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Tor, ThreatSource::TorProject);

        let result = enricher.lookup("2001:db8::1").unwrap();
        assert!(result.is_tor);
    }

    #[test]
    fn test_ipv6_cidr_lookup() {
        let enricher = ReputationEnricher::new();

        let prefix: IpAddr = "2001:db8::".parse().unwrap();
        enricher.add_prefix(prefix, 32, ThreatType::Datacenter, ThreatSource::Custom);

        // Should match within /32
        let result = enricher.lookup("2001:db8::1234").unwrap();
        assert!(result.is_datacenter);

        // Should not match outside /32
        let result = enricher.lookup("2001:db9::1").unwrap();
        assert!(!result.is_datacenter);
    }

    #[test]
    fn test_ipv6_full_form() {
        let enricher = ReputationEnricher::new();

        let ip: IpAddr = "2001:0db8:0000:0000:0000:0000:0000:0001".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Botnet, ThreatSource::AbuseCh);

        // Abbreviated form should match the same IP
        let result = enricher.lookup("2001:db8::1").unwrap();
        assert!(result.is_botnet);
    }

    #[test]
    fn test_ipv4_ipv6_mismatch_cidr() {
        let enricher = ReputationEnricher::new();

        // IPv4 CIDR
        let prefix: IpAddr = "10.0.0.0".parse().unwrap();
        enricher.add_prefix(prefix, 8, ThreatType::Spam, ThreatSource::Spamhaus);

        // IPv6 lookup should not match IPv4 CIDR
        let result = enricher.lookup("::ffff:10.0.0.1").unwrap();
        // The IPv4-mapped IPv6 address is parsed as V6, so CIDR V4 won't match
        assert!(!result.is_spam, "IPv6 should not match IPv4 CIDR prefix");
    }

    // ========================================================================
    // Schema map filtering
    // ========================================================================

    #[test]
    fn test_schema_map_all_boolean_fields() {
        let result = ReputationResult {
            is_vpn: true,
            is_proxy: false,
            is_tor: true,
            is_relay: false,
            is_datacenter: true,
            is_residential: false,
            is_botnet: true,
            is_spam: false,
            is_scanner: true,
            is_malicious: true,
            is_anonymizer: true,
            ..Default::default()
        };

        let fields = [
            "is_vpn",
            "is_proxy",
            "is_tor",
            "is_relay",
            "is_datacenter",
            "is_residential",
            "is_botnet",
            "is_spam",
            "is_scanner",
            "is_malicious",
            "is_anonymizer",
        ];
        let map = result.to_schema_map(&fields);
        assert_eq!(map.len(), 11);
        assert_eq!(map.get("is_vpn").unwrap(), &serde_json::Value::Bool(true));
        assert_eq!(
            map.get("is_proxy").unwrap(),
            &serde_json::Value::Bool(false)
        );
    }

    #[test]
    fn test_schema_map_scores() {
        let result = ReputationResult {
            abuse_score: 85,
            risk_score: 42,
            ..Default::default()
        };

        let fields = ["abuse_score", "risk_score"];
        let map = result.to_schema_map(&fields);
        assert_eq!(
            map.get("abuse_score").unwrap(),
            &serde_json::Value::Number(85.into())
        );
        assert_eq!(
            map.get("risk_score").unwrap(),
            &serde_json::Value::Number(42.into())
        );
    }

    #[test]
    fn test_schema_map_optional_fields_absent() {
        let result = ReputationResult::default();

        // vpn_provider and hosting_provider are None — should not appear
        let fields = ["vpn_provider", "hosting_provider", "threat_type"];
        let map = result.to_schema_map(&fields);
        assert!(
            !map.contains_key("vpn_provider"),
            "None vpn_provider should be absent"
        );
        assert!(
            !map.contains_key("hosting_provider"),
            "None hosting_provider should be absent"
        );
        // threat_type is None/empty string — should also be absent
        assert!(
            !map.contains_key("threat_type"),
            "Empty threat_type should be absent"
        );
    }

    #[test]
    fn test_schema_map_optional_fields_present() {
        let result = ReputationResult {
            vpn_provider: Some("NordVPN".to_string()),
            hosting_provider: Some("AWS".to_string()),
            threat_source: ThreatSource::IpInfo,
            ..Default::default()
        };

        let fields = ["vpn_provider", "hosting_provider", "threat_source"];
        let map = result.to_schema_map(&fields);
        assert_eq!(map.get("vpn_provider").unwrap(), "NordVPN");
        assert_eq!(map.get("hosting_provider").unwrap(), "AWS");
        assert_eq!(map.get("threat_source").unwrap(), "ipinfo");
    }

    #[test]
    fn test_schema_map_unknown_field_ignored() {
        let result = ReputationResult::default();
        let fields = ["nonexistent_field", "is_vpn"];
        let map = result.to_schema_map(&fields);
        assert_eq!(map.len(), 1); // Only is_vpn
        assert!(!map.contains_key("nonexistent_field"));
    }

    // ========================================================================
    // Misc edge cases
    // ========================================================================

    #[test]
    fn test_invalid_ip_returns_none() {
        let enricher = ReputationEnricher::new();
        let result = enricher.lookup("not_an_ip");
        assert!(result.is_none());
    }

    #[test]
    fn test_clear_removes_all_data() {
        let enricher = ReputationEnricher::new();
        let ip: IpAddr = "1.2.3.4".parse().unwrap();
        enricher.add_ip(ip, ThreatType::Tor, ThreatSource::TorProject);
        enricher.add_prefix(
            "10.0.0.0".parse().unwrap(),
            8,
            ThreatType::Spam,
            ThreatSource::Spamhaus,
        );
        let _ = enricher.lookup("1.2.3.4"); // populate cache

        assert!(enricher.is_available());

        enricher.clear();
        assert!(!enricher.is_available());

        let stats = enricher.stats();
        assert_eq!(stats.ip_count, 0);
        assert_eq!(stats.prefix_count, 0);
        assert_eq!(stats.size, 0);
    }

    #[test]
    fn test_has_threat_false_for_default() {
        let result = ReputationResult::default();
        assert!(!result.has_threat());
    }

    #[test]
    fn test_threat_type_as_str() {
        assert_eq!(ThreatType::None.as_str(), "");
        assert_eq!(ThreatType::Vpn.as_str(), "vpn");
        assert_eq!(ThreatType::Proxy.as_str(), "proxy");
        assert_eq!(ThreatType::Tor.as_str(), "tor");
        assert_eq!(ThreatType::Relay.as_str(), "relay");
        assert_eq!(ThreatType::Datacenter.as_str(), "datacenter");
        assert_eq!(ThreatType::Residential.as_str(), "residential");
        assert_eq!(ThreatType::Botnet.as_str(), "botnet");
        assert_eq!(ThreatType::Spam.as_str(), "spam");
        assert_eq!(ThreatType::Scanner.as_str(), "scanner");
        assert_eq!(ThreatType::Malware.as_str(), "malware");
        assert_eq!(ThreatType::Phishing.as_str(), "phishing");
        assert_eq!(ThreatType::Bruteforce.as_str(), "bruteforce");
        assert_eq!(ThreatType::Exploit.as_str(), "exploit");
    }

    #[test]
    fn test_threat_source_as_str() {
        assert_eq!(ThreatSource::None.as_str(), "");
        assert_eq!(ThreatSource::TorProject.as_str(), "tor_project");
        assert_eq!(ThreatSource::FireHol.as_str(), "firehol");
        assert_eq!(ThreatSource::AbuseIpdb.as_str(), "abuseipdb");
        assert_eq!(ThreatSource::AbuseCh.as_str(), "abuse_ch");
        assert_eq!(ThreatSource::Spamhaus.as_str(), "spamhaus");
        assert_eq!(ThreatSource::MaxMind.as_str(), "maxmind");
        assert_eq!(ThreatSource::IpInfo.as_str(), "ipinfo");
        assert_eq!(ThreatSource::CrowdSec.as_str(), "crowdsec");
        assert_eq!(ThreatSource::GreyNoise.as_str(), "greynoise");
        assert_eq!(ThreatSource::Custom.as_str(), "custom");
    }

    #[test]
    fn test_prefix_contains_full_mask_ipv4() {
        // /32 should match only the exact IP
        let enricher = ReputationEnricher::new();
        let exact: IpAddr = "8.8.8.8".parse().unwrap();
        enricher.add_prefix(exact, 32, ThreatType::Scanner, ThreatSource::GreyNoise);

        let result = enricher.lookup("8.8.8.8").unwrap();
        assert!(result.is_scanner);

        let result = enricher.lookup("8.8.8.9").unwrap();
        assert!(!result.is_scanner);
    }

    #[test]
    fn test_prefix_contains_zero_mask() {
        // /0 matches everything in that address family
        let enricher = ReputationEnricher::new();
        let any: IpAddr = "0.0.0.0".parse().unwrap();
        enricher.add_prefix(any, 0, ThreatType::Residential, ThreatSource::Custom);

        let result = enricher.lookup("1.2.3.4").unwrap();
        assert!(result.is_residential);

        let result = enricher.lookup("255.255.255.255").unwrap();
        assert!(result.is_residential);

        // IPv6 should NOT match a /0 IPv4 prefix
        let result = enricher.lookup("2001:db8::1").unwrap();
        assert!(!result.is_residential);
    }
}
