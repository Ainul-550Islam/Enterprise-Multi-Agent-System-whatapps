// Wire-path codec throughput: every broker publish/subscription pays this —
// the delay target for the worker loop's decode pre-check hinges on it.
//
// Anchored from `crates/messaging/benches/messaging_benches.rs`.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mas_messaging::codec::{CodecConfig, JsonEventCodec};

fn realistic_frame() -> serde_json::Value {
    serde_json::json!({
        "task_id": "9df51c0a-3e0c-4a0a-a970-9b16f2a8a001",
        "tenant_id": "9df51c0a-3e0c-4a0a-a970-9b16f2a8a002",
        "organization_id": "9df51c0a-3e0c-4a0a-a970-9b16f2a8a003",
        "project_id": "9df51c0a-3e0c-4a0a-a970-9b16f2a8a004",
        "operation": "execution.run",
        "input": {
            "workflow_id": "9df51c0a-3e0c-4a0a-a970-9b16f2a8a005",
            "question": "Summarize today's usage report",
            "budget": {"tokens": 4096, "time_ms": 30000},
            "history": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": "hello"}
            ]
        },
        "idempotency_key": "7f8a9b0c-1d2e-3f4a-5b6c-7d8e9f0a1b2c",
        "priority": "high",
        "attempt_count": 1,
        "max_attempts": 5,
        "correlation_id": "bench-corr",
        "enqueued_at": "2026-09-30T00:00:00.000Z"
    })
}

fn bench_round_trip(c: &mut Criterion) {
    let codec = JsonEventCodec::new(CodecConfig::default());
    let value = realistic_frame();
    c.bench_function("json_event_codec/round_trip_realistic_task", |b| {
        b.iter(|| {
            let frame = codec.encode_value(black_box(&value)).expect("encode");
            let decoded: serde_json::Value = codec.decode_value(black_box(&frame)).expect("decode");
            black_box(decoded)
        })
    });

    let pre_encoded_once = codec.encode_value(&value).expect("pre-encode");
    c.bench_function("json_event_codec/decode_preencoded", |b| {
        b.iter(|| {
            let decoded: serde_json::Value = codec
                .decode_value(black_box(&pre_encoded_once))
                .expect("decode");
            black_box(decoded)
        })
    });
}

criterion_group!(benches, bench_round_trip);
criterion_main!(benches);
