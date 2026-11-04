//! Stripe-specific raw-body authentication, not a complete billing endpoint.
//!
//! This module verifies before parsing. It does not resolve tenants, persist a
//! receipt, schedule processing, or make webhook delivery idempotent. A real
//! endpoint must do those things with durable, crash-safe transactions.

use std::fmt;

use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Max untrusted Stripe request body, before JSON parsing.
pub const MAX_STRIPE_WEBHOOK_BYTES: usize = 128 * 1024;
/// Fixed two-sided tolerance for a signed delivery timestamp.
pub const STRIPE_SIGNATURE_TOLERANCE_SECONDS: u64 = 300;
const MAX_HEADER_BYTES: usize = 2048;
const MAX_SIGNATURES: usize = 8;
const MAX_SECRETS: usize = 3;
type HmacSha256 = Hmac<Sha256>;

/// A Stripe envelope that was authenticated against its exact raw bytes.
///
/// The account and event fields are signed provider input, not proof of a
/// platform tenant binding. The raw body can contain customer data, so Debug
/// never renders it. This value does not imply that an event was persisted.
pub struct VerifiedStripeEvent {
    id: String,
    event_type: String,
    livemode: bool,
    account: Option<String>,
    payload_hash: String,
    raw_body: Vec<u8>,
}

impl fmt::Debug for VerifiedStripeEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VerifiedStripeEvent")
            .field("id", &self.id)
            .field("event_type", &self.event_type)
            .field("livemode", &self.livemode)
            .field("account", &self.account)
            .field("raw_body", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl VerifiedStripeEvent {
    /// Provider-assigned event identifier (not a tenant identifier).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Provider event type.
    #[must_use]
    pub fn event_type(&self) -> &str {
        &self.event_type
    }
    /// Whether the provider declared this as live rather than test mode.
    #[must_use]
    pub const fn livemode(&self) -> bool {
        self.livemode
    }
    /// Optional signed provider account selector; resolve ownership separately.
    #[must_use]
    pub fn account(&self) -> Option<&str> {
        self.account.as_deref()
    }
    /// Lowercase SHA-256 of the authenticated raw payload.
    #[must_use]
    pub fn payload_hash(&self) -> &str {
        &self.payload_hash
    }
    /// Exact authenticated body; treat as sensitive customer data.
    #[must_use]
    pub fn raw_body(&self) -> &[u8] {
        &self.raw_body
    }
}

#[derive(Deserialize)]
struct StripeEnvelope {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    livemode: bool,
    account: Option<String>,
}

/// Authenticates Stripe's `t=...,v1=...` header over `timestamp.raw_body`,
/// checks the configured mode, then parses only the signed envelope fields.
/// Multiple `v1` signatures and up to three server-owned signing secrets are
/// supported for key rotation. Unknown schemes are ignored, never accepted.
///
/// `secrets` and `expected_livemode` must come from operator-owned
/// configuration, not the request; `now` must come from a synchronized trusted
/// clock. Timestamp tolerance is not event deduplication: a valid event can be
/// retried and must be processed idempotently from durable state.
///
/// # Errors
/// Rejects malformed, oversized, stale, incorrectly signed, wrong-mode or
/// malformed-envelope requests without including secrets or payload in errors.
pub fn verify_stripe_event(
    header: &str,
    raw_body: &[u8],
    secrets: &[&[u8]],
    now: &Timestamp,
    expected_livemode: bool,
) -> Result<VerifiedStripeEvent> {
    if raw_body.is_empty() || raw_body.len() > MAX_STRIPE_WEBHOOK_BYTES {
        return Err(AppError::validation("Stripe webhook body has invalid size"));
    }
    if secrets.is_empty()
        || secrets.len() > MAX_SECRETS
        || secrets
            .iter()
            .any(|secret| secret.len() < 16 || secret.len() > 512)
    {
        return Err(AppError::internal(
            "Stripe signing secret configuration is invalid",
        ));
    }
    if header.is_empty() || header.len() > MAX_HEADER_BYTES || !header.is_ascii() {
        return Err(AppError::unauthorized("invalid Stripe signature header"));
    }
    let mut timestamp: Option<i64> = None;
    let mut signatures = Vec::new();
    for part in header.split(',') {
        let (key, value) = part
            .trim()
            .split_once('=')
            .ok_or_else(|| AppError::unauthorized("invalid Stripe signature header"))?;
        match key {
            "t" => {
                if timestamp.is_some()
                    || value.is_empty()
                    || value.len() > 19
                    || !value.bytes().all(|b| b.is_ascii_digit())
                    || (value.len() > 1 && value.starts_with('0'))
                {
                    return Err(AppError::unauthorized("invalid Stripe signature timestamp"));
                }
                timestamp =
                    Some(value.parse::<i64>().map_err(|_| {
                        AppError::unauthorized("invalid Stripe signature timestamp")
                    })?);
            },
            "v1" => {
                if signatures.len() >= MAX_SIGNATURES || value.len() != 64 {
                    return Err(AppError::unauthorized("invalid Stripe signature"));
                }
                let bytes = hex::decode(value)
                    .map_err(|_| AppError::unauthorized("invalid Stripe signature"))?;
                signatures.push(bytes);
            },
            _ => {},
        }
    }
    let timestamp =
        timestamp.ok_or_else(|| AppError::unauthorized("missing Stripe signature timestamp"))?;
    if signatures.is_empty()
        || now.to_unix_seconds().abs_diff(timestamp) > STRIPE_SIGNATURE_TOLERANCE_SECONDS
    {
        return Err(AppError::unauthorized(
            "invalid or expired Stripe signature",
        ));
    }
    let prefix = format!("{timestamp}.");
    let mut verified = false;
    for secret in secrets {
        for candidate in &signatures {
            let mut mac = <HmacSha256 as Mac>::new_from_slice(secret)
                .map_err(|_| AppError::internal("invalid Stripe signing secret"))?;
            mac.update(prefix.as_bytes());
            mac.update(raw_body);
            verified |= mac.verify_slice(candidate).is_ok();
        }
    }
    if !verified {
        return Err(AppError::unauthorized(
            "Stripe signature verification failed",
        ));
    }
    // Typed deserialization rejects repeated top-level security fields. No
    // provider data is parsed before authenticating the exact raw bytes.
    let envelope: StripeEnvelope = serde_json::from_slice(raw_body)
        .map_err(|_| AppError::validation("invalid Stripe event envelope"))?;
    if !safe_token(&envelope.id, 256)
        || !envelope.id.starts_with("evt_")
        || !safe_token(&envelope.event_type, 256)
        || envelope
            .account
            .as_deref()
            .is_some_and(|account| !safe_token(account, 256))
        || envelope.livemode != expected_livemode
    {
        return Err(AppError::validation("invalid Stripe event envelope"));
    }
    Ok(VerifiedStripeEvent {
        id: envelope.id,
        event_type: envelope.event_type,
        livemode: envelope.livemode,
        account: envelope.account,
        payload_hash: hex::encode(Sha256::digest(raw_body)),
        raw_body: raw_body.to_vec(),
    })
}

fn safe_token(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Independently generated using Python hmac/hashlib, not the verifier.
    const BODY: &[u8] = br#"{"id":"evt_test_123","type":"invoice.paid","livemode":false,"data":{"object":{"id":"in_123"}}}"#;
    const SECRET: &[u8] = b"whsec_test_0123456789abcdef";
    const OTHER: &[u8] = b"whsec_rotation_0123456789abcdef";
    const SIGNATURE: &str = "2d25dc9cce7eec5508c94e5eafb759d0aab8c3f7ee5e578c80d26fb400bc9efa";
    const ROTATION: &str = "c283da1767df200cf7ed050bd6ee3870c2c1c40fca2df1172eb5704a94284a48";
    fn time(value: i64) -> Timestamp {
        Timestamp::from_unix_seconds(value).expect("timestamp")
    }
    fn header() -> String {
        format!("t=1700000000,v1={SIGNATURE}")
    }
    fn sign(payload: &[u8]) -> String {
        let mut mac = <HmacSha256 as Mac>::new_from_slice(SECRET).expect("key");
        mac.update(b"1700000000.");
        mac.update(payload);
        format!(
            "t=1700000000,v1={}",
            hex::encode(mac.finalize().into_bytes())
        )
    }
    fn verify(header: &str, payload: &[u8], now: i64) -> Result<VerifiedStripeEvent> {
        verify_stripe_event(header, payload, &[SECRET], &time(now), false)
    }

    #[test]
    fn independent_vector_and_multiple_signatures_support_rotation() {
        let signed = format!("t=1700000000,v0=ignored,v1={ROTATION},v1={SIGNATURE}");
        let result = verify_stripe_event(&signed, BODY, &[OTHER, SECRET], &time(1700000000), false)
            .expect("verified");
        assert_eq!(result.id(), "evt_test_123");
        assert_eq!(result.event_type(), "invoice.paid");
        assert!(!result.livemode());
        assert_eq!(result.account(), None);
        assert_eq!(result.raw_body(), BODY);
        assert_eq!(result.payload_hash(), hex::encode(Sha256::digest(BODY)));
        let debug = format!("{result:?}");
        assert!(!debug.contains("in_123"));
        assert!(!debug.contains("whsec"));
        verify_stripe_event(&signed, BODY, &[OTHER], &time(1700000000), false)
            .expect("previous key");
    }

    #[test]
    fn rejects_tampering_modes_bad_headers_and_stale_or_future_signatures() {
        assert!(verify(&header(), b"{}", 1700000000).is_err());
        let forged = br#"{"id":"evt_test_123","type":"invoice.failed","livemode":false}"#;
        let error = verify(&header(), forged, 1700000000)
            .expect_err("valid JSON must not bypass signature");
        assert!(error.to_string().contains("signature verification"));
        assert!(verify_stripe_event(&header(), BODY, &[SECRET], &time(1700000000), true).is_err());
        for malformed in [
            format!("t=1700000000,t=1700000000,v1={SIGNATURE}"),
            format!("t=01700000000,v1={SIGNATURE}"),
            format!("t=-1700000000,v1={SIGNATURE}"),
            format!("t=1700000000,v0={SIGNATURE}"),
            "t=1700000000,v1=nothex".to_owned(),
            format!(
                "t=1700000000,{}",
                vec![format!("v1={SIGNATURE}"); 9].join(",")
            ),
        ] {
            assert!(verify(&malformed, BODY, 1700000000).is_err(), "{malformed}");
        }
        for valid in [1699999700, 1700000300] {
            assert!(verify(&header(), BODY, valid).is_ok());
        }
        for invalid in [1699999699, 1700000301] {
            assert!(verify(&header(), BODY, invalid).is_err());
        }
        assert!(verify_stripe_event(&header(), BODY, &[], &time(1700000000), false).is_err());
        assert!(
            verify_stripe_event(&header(), BODY, &[b"short"], &time(1700000000), false).is_err()
        );
        assert!(verify(
            &header(),
            &vec![b'x'; MAX_STRIPE_WEBHOOK_BYTES + 1],
            1700000000
        )
        .is_err());
    }

    #[test]
    fn authenticates_before_parsing_and_rejects_duplicate_envelope_fields() {
        let invalid_json = b"not-json";
        // Invalid signatures are refused at the signature boundary, even if
        // the untrusted payload is malformed; the body is never echoed.
        let error = verify(&header(), invalid_json, 1700000000).expect_err("bad signature");
        assert!(error.to_string().contains("signature verification"));
        assert!(!error.to_string().contains("not-json"));
        assert!(verify(&sign(invalid_json), invalid_json, 1700000000).is_err());
        let duplicate = br#"{"id":"evt_1","id":"evt_2","type":"invoice.paid","livemode":false}"#;
        assert!(verify(&sign(duplicate), duplicate, 1700000000).is_err());
        let missing = br#"{"id":"evt_1","type":"invoice.paid"}"#;
        assert!(verify(&sign(missing), missing, 1700000000).is_err());
    }
}
