// SPDX-License-Identifier: FSL-1.1-ALv2
// Copyright (c) 2026 HYPERI PTY LIMITED

//! Enrichment module benchmarks
//!
//! Measures CPU cost per operation for GeoIP, Reputation, and Risk scoring.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use dfe_loader::enrich::geoip::{GeoIpEnricher, GeoIpResult};
use dfe_loader::enrich::reputation::{ReputationEnricher, ThreatSource, ThreatType};
use dfe_loader::enrich::risk::{RiskInput, RiskPreset, RiskScorer};
use std::hint::black_box;
use std::net::IpAddr;

// Sample IPs for benchmarking
const SAMPLE_IPS: &[&str] = &[
    "8.8.8.8",
    "1.1.1.1",
    "208.67.222.222",
    "9.9.9.9",
    "185.228.168.9",
    "76.76.19.19",
    "94.140.14.14",
    "77.88.8.8",
    "223.5.5.5",
    "119.29.29.29",
];

const PRIVATE_IPS: &[&str] = &[
    "192.168.1.1",
    "10.0.0.1",
    "172.16.0.1",
    "127.0.0.1",
    "192.168.0.100",
];

fn bench_geoip_private_ip_fast_path(c: &mut Criterion) {
    let enricher = GeoIpEnricher::new();

    c.bench_function("geoip/private_ip_fast_path", |b| {
        b.iter(|| {
            for ip in PRIVATE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_geoip_cache_hit(c: &mut Criterion) {
    let enricher = GeoIpEnricher::new();

    // Prime the cache
    for ip in SAMPLE_IPS {
        enricher.lookup(ip);
    }

    c.bench_function("geoip/cache_hit", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_geoip_cache_miss_no_db(c: &mut Criterion) {
    // Fresh enricher each time (no cache, no DB)
    c.bench_function("geoip/cache_miss_no_db", |b| {
        b.iter(|| {
            let enricher = GeoIpEnricher::new();
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_reputation_empty(c: &mut Criterion) {
    let enricher = ReputationEnricher::new();

    c.bench_function("reputation/empty_blocklist", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_reputation_cache_hit(c: &mut Criterion) {
    let enricher = ReputationEnricher::new();

    // Add some IPs to blocklist
    for ip in SAMPLE_IPS {
        if let Ok(addr) = ip.parse::<IpAddr>() {
            enricher.add_ip(addr, ThreatType::Scanner, ThreatSource::Custom);
        }
    }

    // Prime cache
    for ip in SAMPLE_IPS {
        enricher.lookup(ip);
    }

    c.bench_function("reputation/cache_hit", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_reputation_with_blocklist(c: &mut Criterion) {
    let enricher = ReputationEnricher::new();

    // Load a simulated blocklist (1000 IPs + 100 CIDR prefixes)
    for i in 0..1000u32 {
        let ip: IpAddr = format!("1.2.{}.{}", i / 256, i % 256).parse().unwrap();
        enricher.add_ip(ip, ThreatType::Botnet, ThreatSource::AbuseCh);
    }
    for i in 0..100u8 {
        let prefix: IpAddr = format!("10.{}.0.0", i).parse().unwrap();
        enricher.add_prefix(prefix, 16, ThreatType::Datacenter, ThreatSource::Custom);
    }

    c.bench_function("reputation/1000_ips_100_prefixes", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_reputation_prefix_match(c: &mut Criterion) {
    let enricher = ReputationEnricher::new();

    // Add prefixes that will match
    enricher.add_prefix(
        "8.0.0.0".parse().unwrap(),
        8,
        ThreatType::Datacenter,
        ThreatSource::Custom,
    );
    enricher.add_prefix(
        "1.0.0.0".parse().unwrap(),
        8,
        ThreatType::Vpn,
        ThreatSource::Custom,
    );

    c.bench_function("reputation/prefix_match", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                black_box(enricher.lookup(ip));
            }
        })
    });
}

fn bench_risk_minimal(c: &mut Criterion) {
    let scorer = RiskScorer::new();

    c.bench_function("risk/minimal_input", |b| {
        b.iter(|| {
            let input = RiskInput::default();
            black_box(scorer.score(&input))
        })
    });
}

fn bench_risk_full_input(c: &mut Criterion) {
    let scorer = RiskScorer::new();

    c.bench_function("risk/full_input", |b| {
        b.iter(|| {
            let input = RiskInput {
                country_code: Some("CN"),
                is_vpn: true,
                is_tor: true,
                is_botnet: true,
                is_malicious: true,
                abuse_score: 85,
                ..Default::default()
            };
            black_box(scorer.score(&input))
        })
    });
}

fn bench_risk_with_preset(c: &mut Criterion) {
    let scorer = RiskScorer::from_preset(RiskPreset::HighSecurity);

    c.bench_function("risk/high_security_preset", |b| {
        b.iter(|| {
            let input = RiskInput {
                country_code: Some("RU"),
                is_proxy: true,
                ..Default::default()
            };
            black_box(scorer.score(&input))
        })
    });
}

fn bench_risk_from_enrichment(c: &mut Criterion) {
    let scorer = RiskScorer::new();

    // Simulate results from GeoIP and Reputation
    let geo_result = GeoIpResult {
        country_code: Some("US".to_string()),
        city: Some("Mountain View".to_string()),
        latitude: Some(37.386),
        longitude: Some(-122.084),
        is_private: false,
        ..Default::default()
    };

    let rep_result = dfe_loader::enrich::reputation::ReputationResult {
        is_vpn: true,
        is_anonymizer: true,
        threat_type: ThreatType::Vpn,
        ..Default::default()
    };

    c.bench_function("risk/from_enrichment_results", |b| {
        b.iter(|| {
            let input = RiskInput::from_enrichment(Some(&geo_result), Some(&rep_result));
            black_box(scorer.score(&input))
        })
    });
}

fn bench_full_enrichment_pipeline(c: &mut Criterion) {
    let geoip = GeoIpEnricher::new();
    let reputation = ReputationEnricher::new();
    let risk = RiskScorer::new();

    // Add some blocklist entries
    reputation.add_ip(
        "8.8.8.8".parse().unwrap(),
        ThreatType::Scanner,
        ThreatSource::GreyNoise,
    );

    let mut group = c.benchmark_group("enrichment_pipeline");
    group.throughput(Throughput::Elements(10)); // 10 IPs per iteration

    group.bench_function("full_pipeline_10_ips", |b| {
        b.iter(|| {
            for ip in SAMPLE_IPS {
                // GeoIP lookup
                let geo = geoip.lookup(ip);

                // Reputation lookup
                let rep = reputation.lookup(ip);

                // Risk scoring
                let input = RiskInput::from_enrichment(geo.as_ref(), rep.as_ref());
                let _risk = risk.score(&input);

                black_box((&geo, &rep, &_risk));
            }
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_geoip_private_ip_fast_path,
    bench_geoip_cache_hit,
    bench_geoip_cache_miss_no_db,
    bench_reputation_empty,
    bench_reputation_cache_hit,
    bench_reputation_with_blocklist,
    bench_reputation_prefix_match,
    bench_risk_minimal,
    bench_risk_full_input,
    bench_risk_with_preset,
    bench_risk_from_enrichment,
    bench_full_enrichment_pipeline,
);

criterion_main!(benches);
