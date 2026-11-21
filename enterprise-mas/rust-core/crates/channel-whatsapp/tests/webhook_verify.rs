//! Webhook signature verification: real HMAC checks against real headers.
//!
//! The tests construct signatures exactly as Meta does (`sha256=` + hex
//! HMAC-SHA256 over the raw body) and assert both directions: a genuine
//! delivery verifies, and every tampering path is refused.

use hmac::{Hmac, Mac};
use mas_channel_whatsapp::{
    verify_inbound_signature, verify_webhook_challenge, DEFAULT_TOLERANCE_SECS,
    MAX_WEBHOOK_BODY_BYTES, SIG_PREFIX,
};
use sha2::Sha256;

fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(body);
    format!("{SIG_PREFIX}{}", hex::encode(mac.finalize().into_bytes()))
}

const BODY: &[u8] = br#"{"object":"whatsapp_business_account","entry":[]}"#;

#[test]
fn accepts_a_genuine_delivery() {
    let header = sign("app-secret", BODY);
    verify_inbound_signature(BODY, &header, "app-secret", None, 0).expect("genuine must verify");
}

#[test]
fn rejects_mismatched_signatures() {
    let header = sign("attacker-secret", BODY);
    let err = verify_inbound_signature(BODY, &header, "app-secret", None, 0)
        .expect_err("foreign signature must fail");
    assert!(err.to_string().contains("mismatch"), "got: {err}");

    // tampered body under a genuine tag
    let header = sign("app-secret", BODY);
    let tampered = br#"{"object":"whatsapp_business_account","entry":[1]}"#;
    assert!(verify_inbound_signature(tampered, &header, "app-secret", None, 0).is_err());
}

#[test]
fn rejects_malformed_headers() {
    // no sha256= prefix
    assert!(verify_inbound_signature(BODY, "deadbeef", "app-secret", None, 0).is_err());
    // non-hex tag
    assert!(verify_inbound_signature(BODY, "sha256=zz", "app-secret", None, 0).is_err());
    // empty tag
    assert!(verify_inbound_signature(BODY, "sha256=", "app-secret", None, 0).is_err());
}

#[test]
fn rejects_replayed_deliveries() {
    let header = sign("app-secret", BODY);
    let now = 1_800_000_000_i64;
    let too_old = now - DEFAULT_TOLERANCE_SECS - 1;
    assert!(verify_inbound_signature(BODY, &header, "app-secret", Some(too_old), now).is_err());
    // just inside the window is fine
    let fresh = now - DEFAULT_TOLERANCE_SECS + 1;
    assert!(verify_inbound_signature(BODY, &header, "app-secret", Some(fresh), now).is_ok());
}

#[test]
fn rejects_a_missing_header() {
    assert!(verify_inbound_signature(BODY, "", "app-secret", None, 0).is_err());
}

#[test]
fn rejects_an_oversized_body() {
    let body = vec![b'a'; MAX_WEBHOOK_BODY_BYTES + 1];
    let header = sign("app-secret", &body);
    let err = verify_inbound_signature(&body, &header, "app-secret", None, 0)
        .expect_err("oversized body must fail");
    assert!(err.to_string().contains("size limit"), "got: {err}");
}

#[test]
fn handshake_echoes_the_challenge_on_a_matching_token() {
    let challenge = verify_webhook_challenge(
        Some("subscribe"),
        Some("verify-token"),
        Some("challenge-42"),
        "verify-token",
    )
    .expect("matching handshake");
    assert_eq!(challenge, "challenge-42");
}

#[test]
fn handshake_rejects_wrong_mode_wrong_token_and_missing_challenge() {
    assert!(verify_webhook_challenge(
        Some("unsubscribe"),
        Some("verify-token"),
        Some("c"),
        "verify-token"
    )
    .is_err());
    assert!(verify_webhook_challenge(
        Some("subscribe"),
        Some("attacker"),
        Some("c"),
        "verify-token"
    )
    .is_err());
    assert!(verify_webhook_challenge(
        Some("subscribe"),
        Some("verify-token"),
        None,
        "verify-token"
    )
    .is_err());
}
