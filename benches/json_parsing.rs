//! JSON parsing benchmarks

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use sonic_rs::JsonValueTrait;

fn bench_sonic_rs_parse(c: &mut Criterion) {
    let json = r#"{"event_category":"auth","timestamp":"2025-12-24T00:00:00Z","user_id":"123"}"#;

    c.bench_function("sonic_rs_parse", |b| {
        b.iter(|| {
            let _: sonic_rs::Value = sonic_rs::from_str(black_box(json)).unwrap();
        })
    });
}

fn bench_sonic_rs_get_unchecked(c: &mut Criterion) {
    let json = r#"{"event_category":"auth","timestamp":"2025-12-24T00:00:00Z","user_id":"123"}"#;

    c.bench_function("sonic_rs_get_event_category", |b| {
        b.iter(|| {
            let value: sonic_rs::Value = sonic_rs::from_str(black_box(json)).unwrap();
            let _ = value.get("event_category");
        })
    });
}

criterion_group!(benches, bench_sonic_rs_parse, bench_sonic_rs_get_unchecked);
criterion_main!(benches);
