//! W3C `traceparent`-compatible trace plumbing with an OTLP-shaped export
//! pipeline.
//!
//! This module is deliberately dependency-light: it owns the wire formats
//! (trace ids, span ids, traceparent, span records) and the *port* real
//! OTLP exporters bind to ([`SpanExporterPort`]), plus a deterministic
//! tail sampler and a batching [`SpanProcessor`] with bounded memory.
//!
//! Determinism: sampling decisions derive from the trace id itself, not a
//! RNG, so the same trace is sampled (or not) at every hop.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde_json::{Map, Value};
use std::collections::VecDeque;
use std::fmt;
use std::sync::Mutex;

/// A 128-bit OpenTelemetry trace id (32 lowercase hex chars, non-zero).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TraceId([u8; 16]);

impl TraceId {
    /// Fresh random trace id (CSPRNG; zero ids are rejected by construction).
    #[must_use]
    pub fn new() -> Self {
        use rand::RngCore as _;
        loop {
            let mut bytes = [0u8; 16];
            rand::rng().fill_bytes(&mut bytes);
            if bytes.iter().any(|b| *b != 0) {
                return Self(bytes);
            }
        }
    }

    /// Encodes as 32 lowercase hex chars.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }

    /// Deterministic 16-bit signature used by the tail sampler.
    #[must_use]
    fn sampling_key(&self) -> u16 {
        u16::from_be_bytes([self.0[14], self.0[15]])
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for TraceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TraceId({self})")
    }
}

/// A 64-bit OpenTelemetry span id (16 lowercase hex chars, non-zero).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SpanId([u8; 8]);

impl SpanId {
    /// Fresh random span id.
    #[must_use]
    pub fn new() -> Self {
        use rand::RngCore as _;
        loop {
            let mut bytes = [0u8; 8];
            rand::rng().fill_bytes(&mut bytes);
            if bytes.iter().any(|b| *b != 0) {
                return Self(bytes);
            }
        }
    }

    /// Encodes as 16 lowercase hex chars.
    #[must_use]
    pub fn to_hex(self) -> String {
        hex::encode(self.0)
    }
}

impl Default for SpanId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for SpanId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpanId({self})")
    }
}

fn parse_hex<const N: usize>(raw: &str, what: &'static str) -> Result<[u8; N]> {
    if raw.len() != N * 2 || !raw.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(AppError::invalid_field(
            what,
            "invalid_format",
            format!("{what} must be exactly {} lowercase hex characters", N * 2),
        ));
    }
    let bytes = hex::decode(raw)
        .map_err(|_| AppError::invalid_field(what, "invalid_hex", "not valid hex"))?;
    if bytes.iter().all(|b| *b == 0) {
        return Err(AppError::invalid_field(
            what,
            "forbidden_zero",
            format!("{what} must not be all zeros"),
        ));
    }
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes);
    Ok(out)
}

/// A span's trace context — local, or parsed from a remote `traceparent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub sampled: bool,
}

impl SpanContext {
    /// A fresh root context (unsampled until a sampler marks it).
    #[must_use]
    pub fn root() -> Self {
        Self {
            trace_id: TraceId::new(),
            span_id: SpanId::new(),
            sampled: false,
        }
    }

    /// The W3C `traceparent` header value (`00-…-…-01|00`).
    #[must_use]
    pub fn traceparent(&self) -> String {
        format!(
            "00-{}-{}-{}",
            self.trace_id,
            self.span_id,
            if self.sampled { "01" } else { "00" }
        )
    }

    /// Parses a `traceparent` header. Accepts only version `00` with
    /// exactly four fields, 32/16-char ids and a 2-hex flags byte.
    pub fn parse_traceparent(raw: &str) -> Result<Self> {
        let invalid = || {
            AppError::invalid_field(
                "traceparent",
                "malformed",
                "expected version-traceid-spanid-flags",
            )
        };
        let fields: Vec<&str> = raw.split('-').collect();
        if fields.len() != 4 {
            return Err(invalid());
        }
        if fields[0] != "00" {
            return Err(AppError::invalid_field(
                "traceparent",
                "unsupported_version",
                format!("unsupported traceparent version {:?}", fields[0]),
            ));
        }
        let trace_bytes = parse_hex::<16>(fields[1], "traceparent")?;
        let span_bytes = parse_hex::<8>(fields[2], "traceparent")?;
        let trace_id = TraceId(trace_bytes);
        let span_id = SpanId(span_bytes);
        if fields[3].len() != 2 || !fields[3].bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(invalid());
        }
        let flags = u8::from_str_radix(fields[3], 16)
            .map_err(|_| AppError::invalid_field("traceparent", "malformed", "bad flags"))?;
        Ok(Self {
            trace_id,
            span_id,
            sampled: flags & 0x01 == 0x01,
        })
    }

    /// A child context sharing the trace id with a fresh span id.
    #[must_use]
    pub fn child(&self) -> SpanContext {
        SpanContext {
            trace_id: self.trace_id,
            span_id: SpanId::new(),
            sampled: self.sampled,
        }
    }

    /// Copy marked with the sampling decision.
    #[must_use]
    pub fn with_sampling(mut self, sampled: bool) -> Self {
        self.sampled = sampled;
        self
    }
}

/// Maximum attributes per span.
pub const MAX_SPAN_ATTRIBUTES: usize = 32;

/// Final status of a finished span.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpanStatus {
    /// Not explicitly set.
    Unset,
    /// Completed successfully.
    Ok,
    /// Completed with an error description (single-line, ≤ 256 chars).
    Error(String),
}

/// One completed-or-in-flight span record.
#[derive(Debug, Clone)]
pub struct Span {
    pub name: String,
    pub context: SpanContext,
    pub parent: Option<SpanId>,
    pub started_at: Timestamp,
    pub ended_at: Option<Timestamp>,
    pub attributes: Map<String, Value>,
    pub status: SpanStatus,
}

impl Span {
    /// Starts a span.
    pub fn start(
        name: impl Into<String>,
        context: SpanContext,
        parent: Option<SpanId>,
        at: &Timestamp,
    ) -> Result<Self> {
        let name = name.into();
        mas_common::validation::validate_length("span.name", &name, 1, 128)?;
        Ok(Self {
            name,
            context,
            parent,
            started_at: *at,
            ended_at: None,
            attributes: Map::new(),
            status: SpanStatus::Unset,
        })
    }

    /// Adds an attribute (bounded keys, must not be a sensitive key —
    /// tracing payloads flow to third-party collectors).
    pub fn set_attribute(&mut self, key: &str, value: Value) -> Result<()> {
        if mas_common::redaction::is_sensitive_key(key) {
            return Err(AppError::invalid_field(
                "span.attributes",
                "sensitive_key",
                format!("attribute key {key:?} looks sensitive and must never be traced"),
            ));
        }
        if key.is_empty() || key.len() > 128 {
            return Err(AppError::invalid_field(
                "span.attributes",
                "invalid_key",
                "attribute keys are 1..=128 chars",
            ));
        }
        if self.attributes.len() >= MAX_SPAN_ATTRIBUTES {
            return Err(AppError::invalid_field(
                "span.attributes",
                "too_many",
                format!("spans carry at most {MAX_SPAN_ATTRIBUTES} attributes"),
            ));
        }
        self.attributes.insert(key.to_owned(), value);
        Ok(())
    }

    /// Marks the span errored with a sanitized message, then finishes it.
    pub fn fail(&mut self, message: &str, at: &Timestamp) -> Result<()> {
        let sanitized: String = message
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .take(256)
            .collect();
        self.status = SpanStatus::Error(sanitized);
        self.finish(at)
    }

    /// Finishes the span. Finishing twice rejects.
    pub fn finish(&mut self, at: &Timestamp) -> Result<()> {
        if self.ended_at.is_some() {
            return Err(AppError::conflict("span already finished"));
        }
        if at.is_before(&self.started_at) {
            return Err(AppError::invalid_field(
                "span.end",
                "before_start",
                "spans cannot end before they start",
            ));
        }
        if matches!(self.status, SpanStatus::Unset) {
            self.status = SpanStatus::Ok;
        }
        self.ended_at = Some(*at);
        Ok(())
    }
}

/// Deterministic tail sampler based on the trace id's last two bytes.
/// `ratio_permille` in `0..=1000`: the same trace is sampled everywhere.
#[derive(Debug, Clone, Copy)]
pub struct TailSampler {
    ratio_permille: u16,
}

impl TailSampler {
    /// Builds a sampler; ratios above 1000 permille clamp at 1000.
    #[must_use]
    pub fn new(ratio_permille: u16) -> Self {
        Self {
            ratio_permille: ratio_permille.min(1000),
        }
    }

    /// Whether `trace` is sampled under this ratio.
    #[must_use]
    pub fn is_sampled(&self, trace: &TraceId) -> bool {
        (trace.sampling_key() % 1000) < self.ratio_permille
    }

    /// The configured ratio.
    #[must_use]
    pub const fn ratio_permille(&self) -> u16 {
        self.ratio_permille
    }
}

/// Drain for finished spans (OTLP-shaped; real exporters bind here).
#[async_trait]
pub trait SpanExporterPort: std::fmt::Debug + Send + Sync {
    /// Exports one batch. Implementations MUST treat the batch as immutable
    /// and return transport-faithful errors (processor requeues on error).
    async fn export(&self, spans: &[Span]) -> Result<()>;
}

/// Bounded in-memory exporter for tests / shutdown drains.
#[derive(Debug, Default)]
pub struct InMemorySpanExporter {
    exported: Mutex<Vec<Span>>,
}

impl InMemorySpanExporter {
    /// Empty exporter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// All exported spans, oldest first.
    #[must_use]
    pub fn exported(&self) -> Vec<Span> {
        self.exported
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Number of exported spans.
    #[must_use]
    pub fn len(&self) -> usize {
        self.exported
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Whether nothing was exported yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl SpanExporterPort for InMemorySpanExporter {
    async fn export(&self, spans: &[Span]) -> Result<()> {
        self.exported
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(spans.iter().cloned());
        Ok(())
    }
}

/// Maximum buffered finished spans awaiting export.
pub const MAX_BUFFERED_SPANS: usize = 10_000;
/// Maximum spans per export call.
pub const MAX_EXPORT_BATCH: usize = 512;

/// Batching processor: sampling filter on end, bounded FIFO buffer,
/// batch exports, requeue-on-failure with drop accounting.
#[derive(Debug)]
pub struct SpanProcessor<E: SpanExporterPort> {
    exporter: E,
    buffer: Mutex<VecDeque<Span>>,
    dropped: Mutex<u64>,
    max_buffer: usize,
    batch_size: usize,
}

impl<E: SpanExporterPort> SpanProcessor<E> {
    /// Builds a processor over `exporter`.
    #[must_use]
    pub fn new(exporter: E) -> Self {
        Self {
            exporter,
            buffer: Mutex::new(VecDeque::new()),
            dropped: Mutex::new(0),
            max_buffer: MAX_BUFFERED_SPANS,
            batch_size: MAX_EXPORT_BATCH,
        }
    }

    /// Quick combined check used by instrumenting code: should a span be
    /// emitted at all?
    #[must_use]
    pub fn should_emit(sampler: &TailSampler, context: &SpanContext) -> bool {
        sampler.is_sampled(&context.trace_id) || context.sampled
    }

    /// Span finished: enqueue when marked sampled by the caller
    /// (`context.sampled`) — the instrumentation site applies
    /// [`SpanProcessor::should_emit`] with its sampler. Back-pressure:
    /// oldest spans are evicted past `max_buffer` and counted.
    pub fn on_end(&self, span: Span) -> Result<()> {
        if !span.context.sampled {
            return Ok(());
        }
        let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        if buffer.len() >= self.max_buffer {
            buffer.pop_front();
            let mut dropped = self.dropped.lock().unwrap_or_else(|e| e.into_inner());
            *dropped = dropped.saturating_add(1);
            tracing::warn!("span buffer full; evicted the oldest span (memory bound)");
        }
        buffer.push_back(span);
        Ok(())
    }

    /// Spans evicted by back-pressure since start.
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        *self.dropped.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Currently buffered span count.
    #[must_use]
    pub fn buffered(&self) -> usize {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Exports buffered spans in batches of ≤ `batch_size`. On exporter
    /// failure, unexported spans are requeued (preserving order) and the
    /// error surfaces — no span is silently lost.
    pub async fn flush(&self) -> Result<u64> {
        let mut exported_total = 0_u64;
        loop {
            let batch: Vec<Span> = {
                let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
                let take = buffer.len().min(self.batch_size);
                buffer.drain(..take).collect()
            };
            if batch.is_empty() {
                return Ok(exported_total);
            }
            let batch_len = batch.len() as u64;
            match self.exporter.export(&batch).await {
                Ok(()) => exported_total += batch_len,
                Err(err) => {
                    let mut buffer = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
                    for span in batch.into_iter().rev() {
                        buffer.push_front(span);
                    }
                    return Err(err.with_context(format!(
                        "span export failed after {exported_total} spans; {batch_len} requeued"
                    )));
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000 + secs).expect("ts")
    }

    #[test]
    fn traceparent_roundtrips_and_rejects_malformed() {
        let context = SpanContext::root().with_sampling(true);
        let header = context.traceparent();
        assert!(header.starts_with("00-"));
        let parsed = SpanContext::parse_traceparent(&header).expect("parse");
        assert_eq!(parsed.trace_id, context.trace_id);
        assert_eq!(parsed.span_id, context.span_id);
        assert!(parsed.sampled);

        assert!(
            SpanContext::parse_traceparent("00-abc-00-01").is_err(),
            "short fields impossible"
        );
        assert!(
            SpanContext::parse_traceparent(
                "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
            )
            .is_err(),
            "version 01 rejected"
        );
        assert!(
            SpanContext::parse_traceparent(
                "00-0000000000000000000000000000000000-00f067aa0ba902b7-01"
            )
            .is_err(),
            "zero trace id rejected"
        );
        assert!(
            SpanContext::parse_traceparent(
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff"
            )
            .is_ok(),
            "non-zero flags accepted (sampled bit evaluated)"
        );
    }

    #[test]
    fn spans_enforce_lifecycle_and_sanitization() {
        let context = SpanContext::root().with_sampling(true);
        let mut span = Span::start("worker.execution", context, None, &at(0)).expect("span");
        assert!(Span::start("", context, None, &at(0)).is_err());
        span.set_attribute("execution.id", Value::from("e-7"))
            .expect("attr");
        assert!(
            span.set_attribute("access_token", Value::from("nope"))
                .is_err(),
            "sensitive keys never traced"
        );

        assert!(span.finish(&at(-5)).is_err(), "cannot end before start");
        span.finish(&at(9)).expect("finish");
        assert_eq!(
            span.status,
            SpanStatus::Ok,
            "unset status defaults to Ok on finish"
        );
        assert!(span.finish(&at(10)).is_err(), "double finish rejects");

        let mut failing = Span::start(
            "http.request",
            context.child(),
            Some(context.span_id),
            &at(0),
        )
        .expect("span");
        failing
            .fail("upstream timeout\nwith-newline", &at(3))
            .expect("fail");
        match &failing.status {
            SpanStatus::Error(msg) => assert_eq!(msg, "upstream timeout with-newline"),
            other => panic!("expected error status, got {other:?}"),
        }
        assert_eq!(failing.parent, Some(context.span_id));
    }

    #[test]
    fn tail_sampler_is_deterministic_and_ratio_bound() {
        for id_hex in [
            "4bf92f3577b34da6a3ce929d0e0e0000",
            "4bf92f3577b34da6a3ce929d0e0e03e7", // 999
            "4bf92f3577b34da6a3ce929d0e0e01f4", // 500
        ] {
            let bytes = parse_hex::<16>(id_hex, "trace").expect("hex");
            let trace = TraceId(bytes);
            let half = TailSampler::new(500);
            let decision_a = half.is_sampled(&trace);
            let decision_b = half.is_sampled(&trace);
            assert_eq!(decision_a, decision_b, "deterministic per trace");
        }
        let bytes = parse_hex::<16>("4bf92f3577b34da6a3ce929d0e0e0000", "trace").expect("hex");
        let zero_tail = TraceId(bytes);
        assert!(TailSampler::new(1).is_sampled(&zero_tail), "0 % 1000 < 1");
        assert!(
            !TailSampler::new(0).is_sampled(&zero_tail),
            "ratio 0 samples nothing"
        );
        let bytes = parse_hex::<16>("4bf92f3577b34da6a3ce929d0e0e03e7", "trace").expect("hex");
        let nine_nine_nine = TraceId(bytes);
        assert!(
            !TailSampler::new(999).is_sampled(&nine_nine_nine),
            "999 !< 999"
        );
        assert!(
            TailSampler::new(1000).is_sampled(&nine_nine_nine),
            "999 < 1000"
        );
        assert!(
            TailSampler::new(1500).ratio_permille() == 1000,
            "ratio clamps"
        );
    }

    #[tokio::test]
    async fn processor_batches_flushes_and_requeues_on_failure() {
        let exporter = InMemorySpanExporter::new();
        let processor = SpanProcessor::new(exporter);

        for i in 0..1200_u32 {
            let context = SpanContext::root().with_sampling(true);
            let mut span = Span::start(format!("span-{i}"), context, None, &at(0)).expect("span");
            span.finish(&at(1)).expect("finish");
            processor.on_end(span).expect("on_end");
        }
        assert_eq!(processor.buffered(), 1200);
        let flushed = processor.flush().await.expect("flush");
        assert_eq!(flushed, 1200);
        assert_eq!(processor.exporter.len(), 1200, "3 batches of ≤512 exported");
        assert!(processor.buffered() == 0);

        // Unsampled spans are dropped at the gate.
        let mut quiet = Span::start("quiet", SpanContext::root(), None, &at(0)).expect("span");
        quiet.finish(&at(1)).expect("finish");
        processor.on_end(quiet).expect("on_end");
        assert_eq!(processor.buffered(), 0);
    }

    #[tokio::test]
    async fn failing_exporter_requeues_without_loss() {
        #[derive(Debug)]
        struct Flaky {
            fail_first: Mutex<bool>,
            exported: Mutex<usize>,
        }
        #[async_trait]
        impl SpanExporterPort for Flaky {
            async fn export(&self, spans: &[Span]) -> Result<()> {
                let mut guard = self.fail_first.lock().unwrap_or_else(|e| e.into_inner());
                if *guard {
                    *guard = false;
                    drop(guard);
                    return Err(AppError::timeout("collector unavailable"));
                }
                drop(guard);
                *self.exported.lock().unwrap_or_else(|e| e.into_inner()) += spans.len();
                Ok(())
            }
        }

        let processor = SpanProcessor::new(Flaky {
            fail_first: Mutex::new(true),
            exported: Mutex::new(0),
        });
        for i in 0..5_u32 {
            let mut span = Span::start(
                format!("s-{i}"),
                SpanContext::root().with_sampling(true),
                None,
                &at(0),
            )
            .expect("span");
            span.finish(&at(1)).expect("finish");
            processor.on_end(span).expect("on_end");
        }
        let _ = processor.flush().await.expect_err("first flush fails");
        assert_eq!(processor.buffered(), 5, "failure preserved every span");
        let second = processor.flush().await.expect("second flush succeeds");
        assert_eq!(second, 5);
        assert_eq!(
            *processor
                .exporter
                .exported
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
            5
        );
    }

    #[tokio::test]
    async fn buffer_overflow_evicts_oldest_with_accounting() {
        let exporter = InMemorySpanExporter::new();
        let mut processor = SpanProcessor::new(exporter);
        processor.max_buffer = 4;
        for i in 0..6_u32 {
            let mut span = Span::start(
                format!("s-{i}"),
                SpanContext::root().with_sampling(true),
                None,
                &at(0),
            )
            .expect("span");
            span.finish(&at(1)).expect("finish");
            processor.on_end(span).expect("on_end");
        }
        assert_eq!(processor.buffered(), 4);
        assert_eq!(processor.dropped_total(), 2);
        processor.flush().await.expect("flush");
        let names: Vec<String> = processor
            .exporter
            .exported()
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(
            names,
            vec!["s-2", "s-3", "s-4", "s-5"],
            "oldest evicted, order preserved"
        );
    }
}
