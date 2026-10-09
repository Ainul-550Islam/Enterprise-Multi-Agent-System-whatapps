//! Versioned body-bound authentication for calls between platform services.
//!
//! Version 2 fixes the replay hole in the former timestamp-only scheme. Every
//! signature contains a cryptographically random nonce that is included in the
//! MAC. A receiver must atomically consume that nonce in its durable receipt
//! store before accepting work; [`verify`] validates cryptographic facts and
//! returns the nonce for that receipt boundary.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::secret_string::constant_time_eq;

/// Header carrying the internal signature.
pub const SIGNATURE_HEADER: &str = "x-mas-internal-signature";
/// Header naming the calling service.
pub const SERVICE_HEADER: &str = "x-mas-internal-service";
/// Maximum accepted clock skew between services.
pub const MAX_SKEW: std::time::Duration = std::time::Duration::from_secs(60);
/// Signature version. Version one is deliberately refused rather than retained
/// as an insecure downgrade path.
const VERSION: &str = "v2";
const MIN_NONCE_LEN: usize = 22;
const MAX_NONCE_LEN: usize = 128;

/// A cryptographically verified request identity awaiting one-time receipt
/// consumption by the process edge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalServiceIdentity {
    /// Server-allowlisted service identity.
    pub service: String,
    /// Signing timestamp after freshness validation.
    pub signed_at: Timestamp,
    /// Signed random request identity; consume it exactly once durably.
    pub nonce: String,
}

impl InternalServiceIdentity {
    /// Whether this identity is the named service.
    #[must_use]
    pub fn is(&self, service: &str) -> bool {
        self.service == service
    }

    /// Expiry used by receipt stores. A nonce is useful only through the
    /// signing timestamp's accepted skew window.
    #[must_use]
    pub fn expires_at_ms(&self) -> i64 {
        self.signed_at
            .to_unix_ms()
            .saturating_add(i64::try_from(MAX_SKEW.as_millis()).unwrap_or(i64::MAX))
    }
}

fn valid_service(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

fn valid_nonce(value: &str) -> bool {
    (MIN_NONCE_LEN..=MAX_NONCE_LEN).contains(&value.len())
        && value
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '-' | '_'))
}

/// Computes the v2 MAC over service, timestamp, nonce, and body digest.
fn mac(secret: &[u8], service: &str, millis: i64, nonce: &str, body: &[u8]) -> String {
    let body_digest = format!("{:x}", Sha256::digest(body));
    let message = format!("{VERSION}:{service}:{millis}:{nonce}:{body_digest}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
    Mac::update(&mut mac, message.as_bytes());
    hex::encode(Mac::finalize(mac).into_bytes())
}

fn new_nonce() -> String {
    let mut entropy = [0_u8; 24];
    rand::rng().fill_bytes(&mut entropy);
    URL_SAFE_NO_PAD.encode(entropy)
}

/// Signs an internal request with a fresh random nonce.
#[must_use]
pub fn sign(secret: &[u8], service: &str, now: &Timestamp, body: &[u8]) -> String {
    sign_with_nonce(secret, service, now, body, &new_nonce())
}

/// Signs with a supplied nonce for deterministic cross-language vectors.
///
/// Production callers should use [`sign`].
#[must_use]
pub fn sign_with_nonce(
    secret: &[u8],
    service: &str,
    now: &Timestamp,
    body: &[u8],
    nonce: &str,
) -> String {
    let millis = now.to_unix_ms();
    format!(
        "{VERSION}:{service}:{millis}:{nonce}:{}",
        mac(secret, service, millis, nonce, body)
    )
}

/// Verifies authenticity and freshness of a v2 signature.
///
/// The caller must atomically insert `(identity.service, identity.nonce)` into
/// its durable receipt store before starting work. Every malformed, stale,
/// mismatched, or unallowlisted value has the same unauthorized result.
pub fn verify(
    secret: &[u8],
    header: Option<&str>,
    service_header: Option<&str>,
    now: &Timestamp,
    body: &[u8],
    allowed_services: &[&str],
) -> Result<InternalServiceIdentity> {
    let unauthenticated = || AppError::unauthorized("unauthenticated");
    if secret.is_empty() {
        return Err(unauthenticated());
    }
    let Some(header) = header.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(unauthenticated());
    };
    let Some(claimed_service) = service_header
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(unauthenticated());
    };

    let mut parts = header.splitn(5, ':');
    let (Some(version), Some(service), Some(millis_raw), Some(nonce), Some(signature)) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(unauthenticated());
    };
    if version != VERSION
        || service != claimed_service
        || !valid_service(service)
        || !valid_nonce(nonce)
        || !allowed_services.contains(&service)
        || signature.len() != 64
        || !signature
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(unauthenticated());
    }
    let Ok(millis) = millis_raw.parse::<i64>() else {
        return Err(unauthenticated());
    };
    let expected = mac(secret, service, millis, nonce, body);
    if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
        return Err(unauthenticated());
    }
    let signed_at = Timestamp::from_unix_ms(millis).map_err(|_| unauthenticated())?;
    let skew = now
        .duration_since(&signed_at)
        .unwrap_or(std::time::Duration::MAX);
    if skew > MAX_SKEW {
        return Err(unauthenticated());
    }
    Ok(InternalServiceIdentity {
        service: service.to_owned(),
        signed_at,
        nonce: nonce.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"s3cr3t";
    const NONCE: &str = "0123456789abcdefghijklmnopqrstuv";

    fn now() -> Timestamp {
        Timestamp::from_unix_ms(1_700_000_000_000).expect("timestamp")
    }

    #[test]
    fn deterministic_v2_vector_verifies() {
        let signature = sign_with_nonce(SECRET, "mas-api", &now(), b"{\"a\":1}", NONCE);
        assert_eq!(
            signature,
            "v2:mas-api:1700000000000:0123456789abcdefghijklmnopqrstuv:ad29671c18b6926ff5c86fa205004b7b9e8675b6324bef6459aedead1e775766"
        );
        let identity = verify(
            SECRET,
            Some(&signature),
            Some("mas-api"),
            &now(),
            b"{\"a\":1}",
            &["mas-api"],
        )
        .expect("signature verifies");
        assert_eq!(identity.nonce, NONCE);
    }

    #[test]
    fn random_signatures_are_distinct_and_both_valid() {
        let first = sign(SECRET, "mas-api", &now(), b"body");
        let second = sign(SECRET, "mas-api", &now(), b"body");
        assert_ne!(first, second);
        assert!(verify(
            SECRET,
            Some(&first),
            Some("mas-api"),
            &now(),
            b"body",
            &["mas-api"]
        )
        .is_ok());
        assert!(verify(
            SECRET,
            Some(&second),
            Some("mas-api"),
            &now(),
            b"body",
            &["mas-api"]
        )
        .is_ok());
    }

    #[test]
    fn v1_and_changed_facts_are_refused() {
        let signature = sign_with_nonce(SECRET, "mas-api", &now(), b"body", NONCE);
        for (header, service, body) in [
            (
                "v1:mas-api:1700000000000:deadbeef",
                "mas-api",
                b"body".as_slice(),
            ),
            (&signature, "mas-worker", b"body".as_slice()),
            (&signature, "mas-api", b"other".as_slice()),
        ] {
            assert!(verify(
                SECRET,
                Some(header),
                Some(service),
                &now(),
                body,
                &["mas-api"]
            )
            .is_err());
        }
    }

    #[test]
    fn stale_and_future_signatures_are_refused() {
        let old = Timestamp::from_unix_ms(now().to_unix_ms() - 61_000).expect("timestamp");
        let future = Timestamp::from_unix_ms(now().to_unix_ms() + 61_000).expect("timestamp");
        for when in [old, future] {
            let signature = sign_with_nonce(SECRET, "mas-api", &when, b"body", NONCE);
            assert!(verify(
                SECRET,
                Some(&signature),
                Some("mas-api"),
                &now(),
                b"body",
                &["mas-api"],
            )
            .is_err());
        }
    }
}
