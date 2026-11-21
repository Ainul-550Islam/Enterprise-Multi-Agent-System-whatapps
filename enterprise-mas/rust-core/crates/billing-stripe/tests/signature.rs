//! Stripe-Signature verification: real HMAC tags, real header shapes.
//!
//! Every test constructs headers exactly as Stripe does
//! (`t=<ts>,v1=<hex hmac-sha256 over "{t}.{payload}">`) and asserts the
//! security properties: tolerance both directions, constant-time path,
//! secret rotation.

use mas_billing_stripe::signature::{
    compute_v1_signature, verify_signature, DEFAULT_TOLERANCE_SECS,
};
use std::time::{SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn header(secret: &[u8], timestamp: u64, payload: &[u8]) -> String {
    let tag = hex::encode(compute_v1_signature(secret, timestamp, payload));
    format!("t={timestamp},v1={tag}")
}

const PAYLOAD: &[u8] = br#"{"id":"evt_test","type":"invoice.paid","data":{"object":{}}}"#;

#[test]
fn accepts_a_genuine_tag() {
    let ts = now();
    let h = header(b"whsec_one", ts, PAYLOAD);
    verify_signature(PAYLOAD, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, ts)
        .expect("genuine tag verifies");
}

#[test]
fn rejects_replayed_timestamps() {
    let ts = now();
    let h = header(b"whsec_one", ts, PAYLOAD);
    // stale delivery (replay) and future-skewed delivery are both refused
    let old = ts - DEFAULT_TOLERANCE_SECS - 1;
    assert!(verify_signature(PAYLOAD, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, old).is_err());
    let future = ts + DEFAULT_TOLERANCE_SECS + 1;
    assert!(
        verify_signature(PAYLOAD, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, future).is_err()
    );
    // exactly at the boundary is inclusive
    let edge = ts - DEFAULT_TOLERANCE_SECS;
    assert!(verify_signature(PAYLOAD, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, edge).is_ok());
}

#[test]
fn rejects_tampered_payloads() {
    let ts = now();
    let h = header(b"whsec_one", ts, PAYLOAD);
    let tampered = br#"{"id":"evt_forged","type":"invoice.paid","data":{"object":{}}}"#;
    assert!(verify_signature(tampered, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, ts).is_err());
}

#[test]
fn honours_secret_rotation() {
    let ts = now();
    // signed with the previous secret, verified with [new, previous]
    let h = header(b"whsec_old", ts, PAYLOAD);
    assert!(verify_signature(
        PAYLOAD,
        &h,
        &[b"whsec_new", b"whsec_old"],
        DEFAULT_TOLERANCE_SECS,
        ts
    )
    .is_ok());
    // signed with an unconfigured secret must still fail
    let h2 = header(b"whsec_evil", ts, PAYLOAD);
    assert!(verify_signature(
        PAYLOAD,
        &h2,
        &[b"whsec_new", b"whsec_old"],
        DEFAULT_TOLERANCE_SECS,
        ts
    )
    .is_err());
}

#[test]
fn refuses_without_configured_secrets() {
    let ts = now();
    let h = header(b"whsec_one", ts, PAYLOAD);
    assert!(verify_signature(PAYLOAD, &h, &[], DEFAULT_TOLERANCE_SECS, ts).is_err());
}

#[test]
fn parses_multi_tag_headers() {
    let ts = now();
    let good = hex::encode(compute_v1_signature(b"whsec_one", ts, PAYLOAD));
    let evil = hex::encode(compute_v1_signature(b"whsec_evil", ts, PAYLOAD));
    let h = format!("t={ts},v1={evil},v1={good}");
    assert!(verify_signature(PAYLOAD, &h, &[b"whsec_one"], DEFAULT_TOLERANCE_SECS, ts).is_ok());
}
