//! Rate limiting for `rate_limit` rules: spec parsing, key resolution and a
//! thread-safe in-memory token bucket behind the [`RateLimiterPort`].

use crate::expression::Path;
use crate::request::EvaluationRequest;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;

/// Rate-limit configuration attached to a rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RateLimitSpec {
    /// Bucket capacity (burst allowance).
    pub max_tokens: u64,
    /// Tokens refilled per minute.
    pub refill_per_minute: u64,
    /// Attribute path whose value keys buckets (`subject.actor`, `tenant_id`,
    /// `attributes.x.y`). Resolved per request; absence falls back to a
    /// resolved literal meaning "whole policy".
    pub key: Path,
}

impl RateLimitSpec {
    const MAX_CAPACITY: u64 = 1_000_000_000;

    pub fn parse(raw: &Value) -> Result<Self> {
        let object = raw.as_object().ok_or_else(|| {
            AppError::invalid_field("rate_limit", "invalid_format", "expects an object")
        })?;
        for key in object.keys() {
            if !["max_tokens", "refill_per_minute", "key"].contains(&key.as_str()) {
                return Err(AppError::invalid_field(
                    format!("rate_limit.{key}"),
                    "unknown_key",
                    "unknown rate_limit key",
                ));
            }
        }
        let max_tokens = u64_field(object, "max_tokens", 1..=Self::MAX_CAPACITY)?;
        let refill_per_minute = u64_field(object, "refill_per_minute", 1..=Self::MAX_CAPACITY)?;
        let key = object
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or("subject.actor");
        let key = Path::parse(key)?;
        Ok(Self {
            max_tokens,
            refill_per_minute,
            key,
        })
    }

    /// Resolves the bucket key for a request.
    #[must_use]
    pub fn key_for(&self, request: &EvaluationRequest) -> RateLimitKey {
        let value = request
            .lookup(self.key.as_str())
            .map(|v| match v {
                Value::String(s) => s,
                other => other.to_string(),
            })
            .unwrap_or_else(|| "<policy-wide>".to_owned());
        RateLimitKey(value)
    }
}

fn u64_field(
    object: &serde_json::Map<String, Value>,
    key: &str,
    range: std::ops::RangeInclusive<u64>,
) -> Result<u64> {
    let value = object.get(key).and_then(Value::as_u64).ok_or_else(|| {
        AppError::invalid_field(
            format!("rate_limit.{key}"),
            "required",
            "expects an integer",
        )
    })?;
    if !range.contains(&value) {
        return Err(AppError::invalid_field(
            format!("rate_limit.{key}"),
            "out_of_range",
            format!("must be within {range:?}"),
        ));
    }
    Ok(value)
}

/// A concrete bucket identity (rule + resolved key + tenant).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RateLimitKey(pub String);

/// Enforcement port: `true` = token granted, `false` = exhausted.
#[async_trait::async_trait]
pub trait RateLimiterPort: Send + Sync + fmt::Debug {
    /// Attempts to take `cost` tokens; refills before testing.
    async fn try_consume(
        &self,
        bucket: &RateLimitKey,
        spec: &RateLimitSpec,
        cost: u64,
    ) -> Result<bool>;

    /// Current approximate tokens (observability).
    async fn available(&self, bucket: &RateLimitKey, spec: &RateLimitSpec) -> Result<u64>;
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens_milli: u64, // milli-precision to smooth slow refills
    last_refill_ms: i64,
}

/// In-memory token buckets per key. Production swap: Redis-backed limiter
/// implementing the same port; keying stays identical.
#[derive(Debug, Default)]
pub struct TokenBucketRateLimiter {
    buckets: Mutex<HashMap<(String, String), Bucket>>, // (rule scope, key)
    max_buckets: usize,
}

impl TokenBucketRateLimiter {
    pub fn new() -> Self {
        Self {
            buckets: Mutex::new(HashMap::new()),
            max_buckets: 100_000,
        }
    }

    #[must_use]
    pub const fn with_capacity(mut self, max_buckets: usize) -> Self {
        self.max_buckets = max_buckets;
        self
    }

    /// Expected bucket (re)fill, returns whether `cost` was granted.
    fn consume_locked(
        &self,
        namespace: &str,
        key: &str,
        spec: &RateLimitSpec,
        cost: u64,
        now_ms: i64,
    ) -> bool {
        let mut buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        let bucket_key = (namespace.to_owned(), key.to_owned());
        let cap_milli = spec.max_tokens.saturating_mul(1_000);
        let bucket = buckets.entry(bucket_key).or_insert_with(|| Bucket {
            tokens_milli: cap_milli, // fresh buckets start full
            last_refill_ms: now_ms,
        });
        // Refill straight-line for elapsed time; clamp at capacity.
        let elapsed_ms = u64::try_from((now_ms - bucket.last_refill_ms).max(0)).unwrap_or(u64::MAX);
        let refill_milli = u128::from(spec.refill_per_minute)
            .saturating_mul(1_000)
            .saturating_mul(u128::from(elapsed_ms))
            / 60_000u128;
        bucket.tokens_milli = bucket
            .tokens_milli
            .saturating_add(u64::try_from(refill_milli).unwrap_or(u64::MAX))
            .min(cap_milli);
        bucket.last_refill_ms = now_ms;
        let cost_milli = cost.saturating_mul(1_000);
        if bucket.tokens_milli < cost_milli {
            return false;
        }
        bucket.tokens_milli -= cost_milli;
        // Evict hard if the map blew up (keys are per-actor, so abuse is possible).
        if buckets.len() > self.max_buckets {
            let cutoff = now_ms - 3_600_000; // older than an hour since last refill
            buckets.retain(|_, bucket| bucket.last_refill_ms >= cutoff);
        }
        true
    }

    fn available_locked(&self, namespace: &str, key: &str, spec: &RateLimitSpec) -> u64 {
        let buckets = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
        buckets
            .get(&(namespace.to_owned(), key.to_owned()))
            .map(|bucket| bucket.tokens_milli / 1_000)
            .unwrap_or(spec.max_tokens)
    }
}

#[async_trait::async_trait]
impl RateLimiterPort for TokenBucketRateLimiter {
    async fn try_consume(
        &self,
        bucket: &RateLimitKey,
        spec: &RateLimitSpec,
        cost: u64,
    ) -> Result<bool> {
        let now_ms = Timestamp::now().to_unix_ms();
        Ok(self.consume_locked("rule", &bucket.0, spec, cost, now_ms))
    }

    async fn available(&self, bucket: &RateLimitKey, spec: &RateLimitSpec) -> Result<u64> {
        Ok(self.available_locked("rule", &bucket.0, spec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{EvaluationContext, EvaluationRequest, EvaluationSubject};
    use mas_common::enums::Environment;
    use mas_common::ids::TenantId;
    use serde_json::json;

    fn spec() -> RateLimitSpec {
        RateLimitSpec::parse(&json!({
            "max_tokens": 4,
            "refill_per_minute": 0, // will fail: min 1 → use fixture below
        }))
        .unwrap_err();
        RateLimitSpec::parse(&json!({
            "max_tokens": 4,
            "refill_per_minute": 60,
            "key": "subject.actor"
        }))
        .expect("spec")
    }

    fn request(actor: &str) -> EvaluationRequest {
        EvaluationRequest::new(
            EvaluationSubject::new(actor).expect("subject"),
            "tool.invoke",
            "tool:x",
            json!({}),
            EvaluationContext::new(TenantId::new(), Environment::Development),
        )
        .expect("request")
    }

    #[tokio::test]
    async fn buckets_are_per_key_and_fill_down() {
        let limiter = TokenBucketRateLimiter::new();
        let spec = spec();
        let alice = spec.key_for(&request("alice"));
        let bob = spec.key_for(&request("bob"));
        for _ in 0..4 {
            assert!(limiter
                .try_consume(&alice, &spec, 1)
                .await
                .expect("consume"));
        }
        assert!(!limiter.try_consume(&alice, &spec, 1).await.expect("deny"));
        assert!(limiter
            .try_consume(&bob, &spec, 1)
            .await
            .expect("bob fresh"));
        assert_eq!(limiter.available(&alice, &spec,).await.expect("avail"), 0);
    }

    #[tokio::test]
    async fn refill_happens_over_elapsed_time() {
        let limiter = TokenBucketRateLimiter::new();
        let spec = spec();
        let alice = spec.key_for(&request("alice"));
        // Drain.
        for _ in 0..4 {
            assert!(limiter.try_consume(&alice, &spec, 1).await.expect("drain"));
        }
        // Simulate 3 seconds: 60/min = 1 token/sec → 3 tokens back.
        {
            let mut buckets = limiter.buckets.lock().expect("lock");
            let bucket = buckets
                .get_mut(&("rule".to_owned(), alice.0.clone()))
                .expect("bucket");
            bucket.last_refill_ms -= 3_000;
        }
        assert!(limiter
            .try_consume(&alice, &spec, 1)
            .await
            .expect("refilled"));
    }

    #[test]
    fn spec_parsing_is_strict() {
        for bad in [
            json!({"max_tokens": 0, "refill_per_minute": 60}),
            json!({"max_tokens": 10}),
            json!({"max_tokens": 10, "refill_per_minute": 60, "key": "bogus.path"}),
            json!({"max_tokens": 10, "refill_per_minute": 60, "extra": 1}),
        ] {
            assert!(RateLimitSpec::parse(&bad).is_err(), "reject {bad}");
        }
        // Default key = subject.actor.
        let plain =
            RateLimitSpec::parse(&json!({"max_tokens": 1, "refill_per_minute": 1})).expect("parse");
        assert_eq!(plain.key.as_str(), "subject.actor");
    }
}
