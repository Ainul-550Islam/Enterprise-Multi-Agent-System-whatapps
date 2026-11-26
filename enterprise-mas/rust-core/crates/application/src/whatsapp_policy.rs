//! Shared outbound WhatsApp policy: opt-out, customer-care window, template
//! requirements, and a bounded per-channel/recipient token bucket.
//!
//! This module is deliberately provider-agnostic. The application service must
//! call it before every text, template, or media send so no send path bypasses
//! contact preferences or channel throttling.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use mas_common::ids::{TenantId, WhatsAppChannelId};
use mas_common::timestamps::Timestamp;
use mas_domain::whatsapp::WhatsAppConversation;

/// Result of one outbound policy check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendDecision {
    /// The send may proceed.
    Allow,
    /// The customer-care window is closed; an approved template is required.
    TemplateRequired,
    /// The contact opted out; no outbound message is allowed.
    OptedOut,
    /// The shared channel/recipient budget is exhausted.
    RateLimited { retry_after_ms: u64 },
}

/// Per-destination outbound rate limiter contract.
pub trait OutboundRateLimiter: Send + Sync {
    /// Consume one token or return the time until one is available.
    fn take(
        &self,
        tenant: TenantId,
        channel: WhatsAppChannelId,
        recipient: &str,
    ) -> Result<(), u64>;
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    updated_at: Instant,
}

/// Thread-safe token bucket shared by all outbound WhatsApp message types.
///
/// `capacity` is the burst ceiling; `refill_per_second` is sustained throughput.
/// The key includes tenant, channel, and recipient to prevent unrelated tenants
/// or channels from consuming one another's budget.
#[derive(Debug)]
pub struct TokenBucket {
    capacity: f64,
    refill_per_second: f64,
    buckets: Mutex<HashMap<(TenantId, WhatsAppChannelId, String), Bucket>>,
}

impl TokenBucket {
    /// Construct a bounded token bucket. Invalid limits are rejected rather
    /// than silently disabling the safety control.
    pub fn new(capacity: u32, refill_per_second: f64) -> Result<Self, &'static str> {
        if capacity == 0 || !refill_per_second.is_finite() || refill_per_second <= 0.0 {
            return Err("rate bucket capacity and refill rate must be positive and finite");
        }
        Ok(Self {
            capacity: f64::from(capacity),
            refill_per_second,
            buckets: Mutex::new(HashMap::new()),
        })
    }
}

impl OutboundRateLimiter for TokenBucket {
    fn take(
        &self,
        tenant: TenantId,
        channel: WhatsAppChannelId,
        recipient: &str,
    ) -> Result<(), u64> {
        let now = Instant::now();
        let key = (tenant, channel, recipient.to_owned());
        let Ok(mut buckets) = self.buckets.lock() else {
            // A poisoned synchronization primitive must fail closed.
            return Err(1000);
        };
        let bucket = buckets.entry(key).or_insert(Bucket {
            tokens: self.capacity,
            updated_at: now,
        });
        let elapsed = now.duration_since(bucket.updated_at).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.refill_per_second).min(self.capacity);
        bucket.updated_at = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let millis = ((1.0 - bucket.tokens) / self.refill_per_second * 1000.0).ceil();
            Err(millis.max(1.0).min(u64::MAX as f64) as u64)
        }
    }
}

/// Enforce conversation-level rules and then consume the shared rate budget.
/// `template_send` should be true only after the caller has confirmed the
/// requested template is approved for the channel and locale.
pub fn authorize_outbound(
    limiter: &dyn OutboundRateLimiter,
    conversation: &WhatsAppConversation,
    channel_id: WhatsAppChannelId,
    now: Timestamp,
    template_send: bool,
) -> SendDecision {
    if conversation.opt_out {
        return SendDecision::OptedOut;
    }
    let window_open = conversation
        .window_expires_at
        .is_some_and(|expires_at| expires_at > now);
    if !window_open && !template_send {
        return SendDecision::TemplateRequired;
    }
    match limiter.take(conversation.tenant_id, channel_id, &conversation.contact) {
        Ok(()) => SendDecision::Allow,
        Err(retry_after_ms) => SendDecision::RateLimited { retry_after_ms },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{TenantId, WhatsAppChannelId, WhatsAppConversationId};

    #[derive(Debug)]
    struct AlwaysAllow;
    impl OutboundRateLimiter for AlwaysAllow {
        fn take(&self, _: TenantId, _: WhatsAppChannelId, _: &str) -> Result<(), u64> {
            Ok(())
        }
    }

    fn conversation(opt_out: bool, expires: Option<Timestamp>) -> WhatsAppConversation {
        WhatsAppConversation {
            id: WhatsAppConversationId::nil(),
            tenant_id: TenantId::nil(),
            channel_id: WhatsAppChannelId::nil(),
            contact: "+15555550123".to_owned(),
            handoff: false,
            opt_out,
            window_expires_at: expires,
            last_inbound_at: None,
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        }
    }

    #[test]
    fn opt_out_blocks_even_template_sends() {
        let c = conversation(true, None);
        assert_eq!(
            authorize_outbound(&AlwaysAllow, &c, c.channel_id, Timestamp::now(), true),
            SendDecision::OptedOut
        );
    }

    #[test]
    fn closed_window_requires_template() {
        let now = Timestamp::now();
        let c = conversation(false, None);
        assert_eq!(
            authorize_outbound(&AlwaysAllow, &c, c.channel_id, now, false),
            SendDecision::TemplateRequired
        );
    }

    #[test]
    fn open_window_allows_text_and_closed_window_allows_template() {
        let now = Timestamp::now();
        let future = Timestamp::from_datetime(*now.as_datetime() + chrono::Duration::minutes(1));
        let c = conversation(false, Some(future));
        assert_eq!(
            authorize_outbound(&AlwaysAllow, &c, c.channel_id, now, false),
            SendDecision::Allow
        );
        let expired = conversation(false, None);
        assert_eq!(
            authorize_outbound(&AlwaysAllow, &expired, expired.channel_id, now, true),
            SendDecision::Allow
        );
    }

    #[test]
    fn token_bucket_refuses_after_burst_and_scopes_buckets() {
        let limiter = TokenBucket::new(1, 0.001).expect("valid limits");
        let tenant = TenantId::nil();
        let channel = WhatsAppChannelId::nil();
        assert_eq!(limiter.take(tenant, channel, "a"), Ok(()));
        assert!(limiter.take(tenant, channel, "a").is_err());
        assert_eq!(limiter.take(tenant, channel, "b"), Ok(()));
    }

    #[test]
    fn bucket_rejects_invalid_limits() {
        assert!(TokenBucket::new(0, 1.0).is_err());
        assert!(TokenBucket::new(1, f64::NAN).is_err());
        assert!(TokenBucket::new(1, 0.0).is_err());
    }
}
