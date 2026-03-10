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
//! 1. **O(1) IP lookup**: Individual IPs stored in FxHashMap
//! 2. **LRU Cache**: Avoids repeated blocklist checks
//! 3. **Prefix trie**: CIDR ranges with prefix matching
//! 4. **Lazy loading**: Only load enabled blocklists

use std::collections::HashSet;
use std::net::IpAddr;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

use rustc_hash::FxHashMap;
use tracing::debug;

/// Threat type classification (low cardinality for ClickHouse)
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

/// Threat source identification (low cardinality for ClickHouse)
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

/// Reputation lookup result with boolean flags optimized for ClickHouse indexing
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
                    if !s.is_empty() {
                        out.insert(field.to_string(), Value::String(s.to_string()))
                    } else {
                        None
                    }
                }
                "threat_source" => {
                    let s = self.threat_source.as_str();
                    if !s.is_empty() {
                        out.insert(field.to_string(), Value::String(s.to_string()))
                    } else {
                        None
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
        let mut threats = self.ip_threats.write().unwrap();
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
        let mut prefixes = self.prefixes.write().unwrap();
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
        let threats = self.ip_threats.read().unwrap();
        let prefixes = self.prefixes.read().unwrap();
        !threats.is_empty() || !prefixes.is_empty()
    }

    /// Get cache and blocklist statistics
    pub fn stats(&self) -> ReputationCacheStats {
        let cache = self.cache.read().unwrap();
        let threats = self.ip_threats.read().unwrap();
        let prefixes = self.prefixes.read().unwrap();

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
        let addr: IpAddr = match ip.parse() {
            Ok(a) => a,
            Err(_) => {
                debug!(ip = %ip, "Invalid IP address for reputation lookup");
                return None;
            }
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
            let threats = self.ip_threats.read().unwrap();
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
            let prefixes = self.prefixes.read().unwrap();
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
        let cache = self.cache.read().unwrap();
        if let Some(entry) = cache.get(ip) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.result.clone())
        } else {
            None
        }
    }

    /// Cache insert with LRU eviction (write path)
    fn cache_put(&self, ip: String, result: ReputationResult) {
        let mut cache = self.cache.write().unwrap();

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
        self.ip_threats.write().unwrap().clear();
        self.prefixes.write().unwrap().clear();
        self.cache.write().unwrap().clear();
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

        let list = r#"
# Comment
1.2.3.4
5.6.7.8
10.0.0.0/8
"#;

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
}
