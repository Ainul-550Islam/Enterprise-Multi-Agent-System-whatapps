// Throughput floor for the worker retry policy: full-jitter delay
// computation must stay in nanosecond territory — it runs on every failed
// message settlement, inline with the consumer loop.
//
// Anchored from `crates/worker/benches/worker_benches.rs`; source of truth
// is this root file so the bench tree mirrors the spec layout.

use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mas_worker::backoff::RetryPolicy;

fn bench_delay_for(c: &mut Criterion) {
    let policy = RetryPolicy {
        base: Duration::from_millis(100),
        cap: Duration::from_millis(60_000),
        max_deliver: 10,
    };
    c.bench_function("retry_policy/delay_for_full_jitter", |b| {
        b.iter(|| {
            for deliveries in 1..=8u32 {
                black_box(policy.delay_for(black_box(deliveries)));
            }
        })
    });
}

fn bench_bound_for(c: &mut Criterion) {
    let policy = RetryPolicy {
        base: Duration::from_millis(100),
        cap: Duration::from_millis(60_000),
        max_deliver: 10,
    };
    c.bench_function("retry_policy/bound_for_saturating_growth", |b| {
        b.iter(|| {
            for deliveries in 1..=30u32 {
                black_box(policy.bound_for(black_box(deliveries)));
            }
        })
    });
}

criterion_group!(benches, bench_delay_for, bench_bound_for);
criterion_main!(benches);
