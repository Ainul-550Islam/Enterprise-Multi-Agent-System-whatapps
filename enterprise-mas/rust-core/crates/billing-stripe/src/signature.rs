//! `Stripe-Signature` verification over the raw request body.
//!
//! Stripe signs every webhook delivery with an HMAC-SHA256 tag over
//! `"{timestamp}.{raw_payload}"`, sent as a header of the form
//!
//! ```text
//! Stripe-Signature: t=1492774577,v1=5257a869...,v1=...
//! ```
//!
//! This module implements that scheme with three non-negotiable properties:
//!
//! * **Tolerance.** The timestamp must lie within `+/-tolerance` of the server
//!   clock. A stale delivery is a replay; a future one is a forged or skewed
//!   clock. Both directions are rejected.
//! * **Constant-time comparison.** Tag comparison goes through
//!   [`hmac::Mac::verify_slice`], which uses a constant-time equality. A `==`
//!   on hex strings would leak the expected tag one byte at a time.
//! * **Rotation.** A header may carry several `v1` tags (Stripe emits one per
//!   active secret) and the caller may configure several secrets (old + new
//!   during rotation). A delivery is authentic when **any** configured secret
//!   matches **any** `v1` tag.
//!
//! The payload bytes are used exactly as received. Re-encoding the body
//! (re-serialised JSON, for example) would change the signed bytes and reject
//! every legitimate delivery.

use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::result::Result;
use sha2::Sha256;

/// Canonical header name Stripe sends. Handlers must read this exact name
/// (`Stripe-Signature`), never `X-Stripe-Signature`.
pub const STRIPE_SIGNATURE_HEADER: &str = "Stripe-Signature";

/// Default timestamp tolerance, matching Stripe's own recommended window.
pub const DEFAULT_TOLERANCE_SECS: u64 = 300;

/// Upper bound on `v1` tags accepted in one header.
///
/// Verification is linear in (secrets x tags); without a bound, an attacker
/// could attach thousands of tags and amplify the HMAC work per request.
pub const MAX_SIGNATURES_PER_HEADER: usize = 8;

/// A parsed `Stripe-Signature` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedHeader {
    /// The `t=` value: signature timestamp, whole unix seconds.
    pub timestamp: u64,
    /// Decoded `v1=` HMAC-SHA256 tags, in header order.
    pub v1_signatures: Vec<Vec<u8>>,
}

/// Computes the expected `v1` tag for one secret, timestamp and raw payload.
///
/// Exposed for tests and for tooling that needs to produce a valid header;
/// verification itself goes through [`verify_signature`].
#[must_use]
pub fn compute_v1_signature(secret: &[u8], timestamp: u64, payload: &[u8]) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(payload);
    mac.finalize().into_bytes().to_vec()
}

/// Parses a `Stripe-Signature` header value.
///
/// # Errors
///
/// [`AppError::validation`] when the header is not `k=v` pairs, when `t` is
/// missing or not a whole number, when any `v1` value is not hex, when no
/// `v1` tag is present, or when more than [`MAX_SIGNATURES_PER_HEADER`] tags
/// are supplied.
pub fn parse_header(header: &str) -> Result<ParsedHeader> {
    let mut timestamp: Option<u64> = None;
    let mut v1_signatures: Vec<Vec<u8>> = Vec::new();

    for item in header.split(',') {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        let (key, value) = item
            .split_once('=')
            .ok_or_else(|| AppError::validation("malformed Stripe-Signature entry"))?;
        match key.trim() {
            "t" => {
                if timestamp.is_some() {
                    return Err(AppError::validation(
                        "duplicate timestamp in Stripe-Signature",
                    ));
                }
                let parsed = value
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| AppError::validation("invalid timestamp in Stripe-Signature"))?;
                timestamp = Some(parsed);
            },
            "v1" => {
                if v1_signatures.len() >= MAX_SIGNATURES_PER_HEADER {
                    return Err(AppError::validation("too many v1 tags in Stripe-Signature"));
                }
                let tag = hex::decode(value.trim())
                    .map_err(|_| AppError::validation("invalid v1 tag in Stripe-Signature"))?;
                v1_signatures.push(tag);
            },
            // Unknown scheme names (a future `v2`) are ignored, not fatal.
            _ => {},
        }
    }

    let timestamp =
        timestamp.ok_or_else(|| AppError::validation("missing timestamp in Stripe-Signature"))?;
    if v1_signatures.is_empty() {
        return Err(AppError::validation("missing v1 tag in Stripe-Signature"));
    }
    Ok(ParsedHeader {
        timestamp,
        v1_signatures,
    })
}

/// Verifies a `Stripe-Signature` header against the raw body.
///
/// * `payload` -- the request body **exactly as received**, before any parsing.
/// * `header` -- the `Stripe-Signature` header value.
/// * `secrets` -- active signing secrets; pass old and new during rotation.
/// * `tolerance_secs` -- maximum accepted clock skew in either direction.
/// * `now_unix_secs` -- the server's current unix time, injected so tests do
///   not sleep and callers cannot accidentally use a value derived from the
///   request.
///
/// # Errors
///
/// * [`AppError::validation`] for a malformed header (see [`parse_header`]).
/// * [`AppError::unauthorized`] when the timestamp is outside the tolerance
///   window, when no secret matches any tag, or when `secrets` is empty.
pub fn verify_signature(
    payload: &[u8],
    header: &str,
    secrets: &[&[u8]],
    tolerance_secs: u64,
    now_unix_secs: u64,
) -> Result<()> {
    if secrets.is_empty() {
        return Err(AppError::unauthorized(
            "stripe signature cannot be verified: no signing secret configured",
        ));
    }
    let parsed = parse_header(header)?;

    // Reject stale AND future-skewed timestamps. A stale delivery is a
    // replay; a future one is forged or a badly skewed clock. The window is
    // inclusive at the boundary.
    let skew = now_unix_secs.abs_diff(parsed.timestamp);
    if skew > tolerance_secs {
        return Err(AppError::unauthorized(
            "stripe signature timestamp outside the tolerance window",
        ));
    }

    // Build the signed bytes: "{t}." followed by the raw payload.
    let mut signed = Vec::with_capacity(parsed.timestamp.to_string().len() + 1 + payload.len());
    signed.extend_from_slice(parsed.timestamp.to_string().as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(payload);

    for secret in secrets {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
        mac.update(&signed);
        for tag in &parsed.v1_signatures {
            // Constant-time comparison; also rejects tags of the wrong length.
            if mac.clone().verify_slice(tag).is_ok() {
                return Ok(());
            }
        }
    }

    Err(AppError::unauthorized(
        "stripe signature verification failed",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"whsec_test_secret";
    const OTHER_SECRET: &[u8] = b"whsec_rotated_secret";
    const PAYLOAD: &[u8] = br#"{"id":"evt_1","type":"checkout.session.completed"}"#;
    const NOW: u64 = 1_700_000_000;

    fn header_for(secret: &[u8], timestamp: u64, payload: &[u8]) -> String {
        let tag = compute_v1_signature(secret, timestamp, payload);
        format!("t={timestamp},v1={}", hex::encode(tag))
    }

    #[test]
    fn accepts_a_valid_signature() {
        let header = header_for(SECRET, NOW, PAYLOAD);
        assert!(verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_ok());
    }

    #[test]
    fn rejects_a_wrong_payload() {
        let header = header_for(SECRET, NOW, PAYLOAD);
        let tampered = br#"{"id":"evt_1","type":"invoice.paid"}"#;
        let error = verify_signature(tampered, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW)
            .expect_err("tampered payload must fail");
        assert_eq!(error.error_code(), "UNAUTHENTICATED");
    }

    #[test]
    fn rejects_a_wrong_secret() {
        let header = header_for(OTHER_SECRET, NOW, PAYLOAD);
        assert!(
            verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_err()
        );
    }

    #[test]
    fn accepts_rotation_when_the_new_secret_matches() {
        let header = header_for(OTHER_SECRET, NOW, PAYLOAD);
        // Old + new configured during rotation; the new one matches.
        assert!(verify_signature(
            PAYLOAD,
            &header,
            &[SECRET, OTHER_SECRET],
            DEFAULT_TOLERANCE_SECS,
            NOW
        )
        .is_ok());
    }

    #[test]
    fn accepts_rotation_when_the_old_secret_matches() {
        let header = header_for(SECRET, NOW, PAYLOAD);
        assert!(verify_signature(
            PAYLOAD,
            &header,
            &[SECRET, OTHER_SECRET],
            DEFAULT_TOLERANCE_SECS,
            NOW
        )
        .is_ok());
    }

    #[test]
    fn accepts_any_matching_v1_tag() {
        // Stripe emits one v1 tag per active secret: first bogus, second real.
        let bogus = hex::encode([0u8; 32]);
        let real = hex::encode(compute_v1_signature(SECRET, NOW, PAYLOAD));
        let header = format!("t={NOW},v1={bogus},v1={real}");
        assert!(verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_ok());
    }

    #[test]
    fn rejects_a_stale_timestamp() {
        let old = NOW - DEFAULT_TOLERANCE_SECS - 1;
        let header = header_for(SECRET, old, PAYLOAD);
        let error = verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW)
            .expect_err("stale delivery must fail");
        assert_eq!(error.error_code(), "UNAUTHENTICATED");
    }

    #[test]
    fn rejects_a_future_timestamp() {
        let future = NOW + DEFAULT_TOLERANCE_SECS + 1;
        let header = header_for(SECRET, future, PAYLOAD);
        assert!(
            verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_err()
        );
    }

    #[test]
    fn accepts_the_exact_tolerance_boundary() {
        let edge = NOW + DEFAULT_TOLERANCE_SECS;
        let header = header_for(SECRET, edge, PAYLOAD);
        assert!(verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_ok());
    }

    #[test]
    fn rejects_when_no_secrets_are_configured() {
        let header = header_for(SECRET, NOW, PAYLOAD);
        let error = verify_signature(PAYLOAD, &header, &[], DEFAULT_TOLERANCE_SECS, NOW)
            .expect_err("no secret must fail closed");
        assert_eq!(error.error_code(), "UNAUTHENTICATED");
    }

    #[test]
    fn rejects_a_malformed_header() {
        for bad in [
            "",
            "garbage",
            "t=abc,v1=00",
            "v1=00",
            "t=1",
            "t=1,v1=zz",
            "t=1,t=2,v1=00",
        ] {
            assert!(
                verify_signature(PAYLOAD, bad, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_err(),
                "header {bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_too_many_v1_tags() {
        let tag = hex::encode([1u8; 32]);
        let mut header = format!("t={NOW}");
        for _ in 0..=MAX_SIGNATURES_PER_HEADER {
            header.push_str(&format!(",v1={tag}"));
        }
        let error = verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW)
            .expect_err("tag flood must be rejected");
        assert_eq!(error.error_code(), "VALIDATION_FAILED");
    }

    #[test]
    fn rejects_a_tag_of_the_wrong_length() {
        let header = format!("t={NOW},v1={}", hex::encode([7u8; 16]));
        assert!(
            verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_err()
        );
    }

    #[test]
    fn parse_header_round_trips() {
        let header = header_for(SECRET, NOW, PAYLOAD);
        let parsed = parse_header(&header).expect("valid header");
        assert_eq!(parsed.timestamp, NOW);
        assert_eq!(parsed.v1_signatures.len(), 1);
        assert_eq!(
            parsed.v1_signatures[0],
            compute_v1_signature(SECRET, NOW, PAYLOAD)
        );
    }

    #[test]
    fn unknown_scheme_names_are_ignored() {
        let tag = hex::encode(compute_v1_signature(SECRET, NOW, PAYLOAD));
        let header = format!("t={NOW},v1={tag},v2=deadbeef");
        assert!(verify_signature(PAYLOAD, &header, &[SECRET], DEFAULT_TOLERANCE_SECS, NOW).is_ok());
    }
}
