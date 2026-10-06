// PURPOSE: Authentication for calls between the platform's own services.
//
// Owning layer: `mas-security`.
//
// Production responsibility: spec Phase 9 ("network access is not trust") and
// Phase 25 (worker/scheduler identities). Before this module existed, the
// scheduler's completion endpoint accepted any POST from anything that could
// reach the port: an unauthenticated caller could clear the in-flight ledger
// and make the scheduler believe a run had finished, which defeats the
// overlap guard and lets a schedule be dispatched again immediately.
//
// The scheme is a keyed HMAC over a bounded set of facts:
//
// ```text
// v1:<service id>:<unix millis>:<hex HMAC-SHA256>
// ```
//
// where the MAC covers `service id`, the timestamp and the SHA-256 of the
// request **body**. Binding the body is what stops a valid signature from one
// request being replayed against a different one; binding the timestamp is
// what stops an old signature being replayed forever.
//
// Security boundary: this is an authentication boundary between processes, not
// an authorization one. A verified internal identity says "this really is the
// scheduler", not "the scheduler may do anything" — the caller still resolves
// a tenant scope and still goes through `AuthorizationService`.
//
// It must NOT:
//   • accept a signature without a freshness bound (replay);
//   • verify a MAC with `==` (timing oracle on the MAC);
//   • let the claimed service id be anything but a comparison against a
//     server-side allowlist. The id in the header is a claim until it matches;
//   • carry a tenant, organization or role. Internal calls state which tenant
//     they are acting on in the body, and the server resolves it — an internal
//     header that simply asserted a tenant would be exactly the
//     caller-supplied identity this phase removes.

use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use sha2::{Digest, Sha256};

use crate::secret_string::constant_time_eq;

/// Header carrying the internal signature.
pub const SIGNATURE_HEADER: &str = "x-mas-internal-signature";
/// Header naming the calling service.
pub const SERVICE_HEADER: &str = "x-mas-internal-service";

/// Maximum accepted clock skew between the two services.
///
///_short enough that a captured signature is worthless within a minute, long
/// enough for ordinary clock drift between hosts in one cluster.
pub const MAX_SKEW: std::time::Duration = std::time::Duration::from_secs(60);

/// Signature version prefix. Bumping it invalidates every outstanding
/// signature, which is the intended way to retire a scheme.
const VERSION: &str = "v1";

/// A verified internal caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalServiceIdentity {
    /// Which service this is, as matched against the server's allowlist.
    pub service: String,
    /// When the caller claims it signed. Recorded for audit; the freshness
    /// check has already passed by the time one of these exists.
    pub signed_at: Timestamp,
}

impl InternalServiceIdentity {
    /// Whether this identity is the named service.
    #[must_use]
    pub fn is(&self, service: &str) -> bool {
        self.service == service
    }
}

/// Computes the MAC over `(service, millis, body digest)`.
fn mac(secret: &[u8], service: &str, millis: i64, body: &[u8]) -> String {
    let body_digest = Sha256::digest(body);
    let body_digest = format!("{body_digest:x}");
    let message = format!("{VERSION}:{service}:{millis}:{body_digest}");
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret).expect("HMAC-SHA256 accepts keys of any length");
    Mac::update(&mut mac, message.as_bytes());
    let result = Mac::finalize(mac).into_bytes();
    hex::encode(result)
}

/// Signs an internal request.
///
/// `now` is supplied by the caller so signing is deterministic and testable;
/// in production it is `Timestamp::now()`.
#[must_use]
pub fn sign(secret: &[u8], service: &str, now: &Timestamp, body: &[u8]) -> String {
    let millis = now.to_unix_ms();
    format!(
        "{VERSION}:{service}:{millis}:{}",
        mac(secret, service, millis, body)
    )
}

/// Verifies an internal request signature.
///
/// `allowed_services` is the server-side allowlist: a syntactically perfect
/// signature from a service the receiver does not expect is still a failure.
/// `now` is the *receiver's* clock, so skew is measured here rather than
/// trusted from the header.
///
/// # Errors
/// * `UNAUTHENTICATED` — malformed header, unknown version, wrong MAC,
///   service not allow-listed, or the timestamp is outside the skew window.
///   Every one of these produces the same error body, so a caller cannot learn
///   which part failed.
pub fn verify(
    secret: &[u8],
    header: Option<&str>,
    service_header: Option<&str>,
    now: &Timestamp,
    body: &[u8],
    allowed_services: &[&str],
) -> Result<InternalServiceIdentity> {
    let unauthenticated = || AppError::unauthorized("unauthenticated");

    let Some(header) = header.map(str::trim).filter(|v| !v.is_empty()) else {
        return Err(unauthenticated());
    };
    let Some(claimed_service) = service_header.map(str::trim).filter(|v| !v.is_empty()) else {
        return Err(unauthenticated());
    };

    let mut parts = header.splitn(4, ':');
    let (Some(version), Some(service), Some(millis_raw), Some(signature)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(unauthenticated());
    };
    if version != VERSION {
        return Err(unauthenticated());
    }
    // The header's service must agree with the service header. Both are
    // claims; only the allowlist below makes one of them a fact.
    if service != claimed_service {
        return Err(unauthenticated());
    }
    if !allowed_services.contains(&service) {
        return Err(unauthenticated());
    }
    if !service
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(unauthenticated());
    }
    let Ok(millis) = millis_raw.parse::<i64>() else {
        return Err(unauthenticated());
    };
    // The signature is over the service, the timestamp and the body digest,
    // so it cannot be moved to another service, another time, or another body.
    let expected = mac(secret, service, millis, body);
    if !constant_time_eq(expected.as_bytes(), signature.as_bytes()) {
        return Err(unauthenticated());
    }

    let signed_at = Timestamp::from_unix_ms(millis)?;
    let skew = now
        .duration_since(&signed_at)
        .unwrap_or(std::time::Duration::MAX);
    if skew > MAX_SKEW {
        return Err(unauthenticated());
    }

    Ok(InternalServiceIdentity {
        service: service.to_owned(),
        signed_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"internal-shared-secret";

    fn now() -> Timestamp {
        Timestamp::from_unix_ms(1_700_000_000_000).expect("ts")
    }

    #[test]
    fn a_valid_signature_verifies() {
        let signature = sign(SECRET, "mas-scheduler", &now(), b"body");
        let identity = verify(
            SECRET,
            Some(&signature),
            Some("mas-scheduler"),
            &now(),
            b"body",
            &["mas-scheduler"],
        )
        .expect("valid");
        assert!(identity.is("mas-scheduler"));
    }

    /// The body is part of the MAC: a signature for one body must not work
    /// for another, or an attacker could replay a legitimate call with
    /// different content.
    #[test]
    fn a_signature_does_not_transfer_to_another_body() {
        let signature = sign(SECRET, "mas-scheduler", &now(), b"original");
        assert!(verify(
            SECRET,
            Some(&signature),
            Some("mas-scheduler"),
            &now(),
            b"tampered",
            &["mas-scheduler"],
        )
        .is_err());
    }

    /// Freshness: an old signature is worthless.
    #[test]
    fn a_stale_signature_is_refused() {
        let old = Timestamp::from_unix_ms(1_700_000_000_000 - 600_000).expect("ts");
        let signature = sign(SECRET, "mas-scheduler", &old, b"body");
        assert!(verify(
            SECRET,
            Some(&signature),
            Some("mas-scheduler"),
            &now(),
            b"body",
            &["mas-scheduler"],
        )
        .is_err());
    }

    #[test]
    fn a_signature_from_an_unexpected_service_is_refused() {
        let signature = sign(SECRET, "mas-something-else", &now(), b"body");
        assert!(verify(
            SECRET,
            Some(&signature),
            Some("mas-something-else"),
            &now(),
            b"body",
            &["mas-scheduler"],
        )
        .is_err());
    }

    #[test]
    fn a_wrong_secret_is_refused() {
        let signature = sign(b"attacker-guess", "mas-scheduler", &now(), b"body");
        assert!(verify(
            SECRET,
            Some(&signature),
            Some("mas-scheduler"),
            &now(),
            b"body",
            &["mas-scheduler"],
        )
        .is_err());
    }

    #[test]
    fn missing_or_malformed_headers_are_refused() {
        assert!(verify(SECRET, None, Some("s"), &now(), b"b", &["s"]).is_err());
        assert!(verify(SECRET, Some(""), Some("s"), &now(), b"b", &["s"]).is_err());
        assert!(verify(
            SECRET,
            Some("v1:mas-scheduler"),
            Some("mas-scheduler"),
            &now(),
            b"b",
            &["mas-scheduler"]
        )
        .is_err());
        assert!(verify(
            SECRET,
            Some("v2:mas-scheduler:1:x"),
            Some("mas-scheduler"),
            &now(),
            b"b",
            &["mas-scheduler"]
        )
        .is_err());
        assert!(verify(
            SECRET,
            Some("v1:mas-scheduler:notanumber:x"),
            Some("mas-scheduler"),
            &now(),
            b"b",
            &["mas-scheduler"]
        )
        .is_err());
    }

    /// The two service identifiers must agree; a mismatch is not a reason to
    /// prefer one over the other.
    #[test]
    fn disagreeing_service_identifiers_are_refused() {
        let signature = sign(SECRET, "mas-scheduler", &now(), b"body");
        assert!(verify(
            SECRET,
            Some(&signature),
            Some("mas-worker"),
            &now(),
            b"body",
            &["mas-scheduler", "mas-worker"],
        )
        .is_err());
    }

    /// Every rejection is the same error, so none of them can be used as an
    /// oracle for which part of the credential was wrong.
    #[test]
    fn every_rejection_is_indistinguishable() {
        let codes = [
            verify(SECRET, None, Some("s"), &now(), b"b", &["s"]),
            verify(SECRET, Some("junk"), Some("s"), &now(), b"b", &["s"]),
            verify(
                SECRET,
                Some("v1:s:1:deadbeef"),
                Some("s"),
                &now(),
                b"b",
                &["s"],
            ),
        ];
        for outcome in codes {
            let err = outcome.expect_err("must fail");
            assert_eq!(err.error_code(), "UNAUTHENTICATED");
            assert!(err.public_message().contains("unauthenticated"));
        }
    }
}

/// Cross-language interop vectors for the Python bridge.
///
/// `mas_orchestrator.security.internal_service` implements the same scheme.
/// These constants are the shared test vector: if either side changes its
/// message format, the integration test that compares against them fails.
#[cfg(test)]
mod interop {
    /// A signature produced by the Python implementation with secret
    /// `"s3cr3t"`, service `"mas-api"`, millis `1700000000000`, body
    /// `{"a":1}`.
    pub const PYTHON_VECTOR: &str =
        "v1:mas-api:1700000000000:17985adfe11dbfd97835efc01270debe55c97e0131778e0f0d9099dc5a753b8e";
    /// The body the vector was computed over.
    pub const PYTHON_VECTOR_BODY: &[u8] = br#"{"a":1}"#;
    /// The secret the vector was computed with.
    pub const PYTHON_VECTOR_SECRET: &[u8] = b"s3cr3t";
    /// The service id the vector claims.
    pub const PYTHON_VECTOR_SERVICE: &str = "mas-api";
    /// The timestamp the vector was signed at.
    pub const PYTHON_VECTOR_MILLIS: i64 = 1_700_000_000_000;
}

#[cfg(test)]
mod interop_tests {
    use super::interop::*;
    use super::{sign, verify};
    use mas_common::timestamps::Timestamp;

    /// Rust must produce byte-identical output to the Python implementation
    /// for the same inputs. Divergence here means the bridge is broken in one
    /// direction, which is only discoverable in production otherwise.
    #[test]
    fn rust_signs_the_same_signature_as_python() {
        let at = Timestamp::from_unix_ms(PYTHON_VECTOR_MILLIS).expect("timestamp");
        let produced = sign(
            PYTHON_VECTOR_SECRET,
            PYTHON_VECTOR_SERVICE,
            &at,
            PYTHON_VECTOR_BODY,
        );
        assert_eq!(
            produced, PYTHON_VECTOR,
            "Rust and Python must agree byte-for-byte"
        );
    }

    /// Rust must accept a signature produced by Python.
    #[test]
    fn rust_accepts_a_python_produced_signature() {
        let at = Timestamp::from_unix_ms(PYTHON_VECTOR_MILLIS).expect("timestamp");
        let identity = verify(
            PYTHON_VECTOR_SECRET,
            Some(PYTHON_VECTOR),
            Some(PYTHON_VECTOR_SERVICE),
            &at,
            PYTHON_VECTOR_BODY,
            &["mas-api", "mas-worker"],
        )
        .expect("a Python signature must verify in Rust");
        assert!(identity.is("mas-api"));
        assert_eq!(identity.signed_at.to_unix_ms(), PYTHON_VECTOR_MILLIS);
    }

    /// Rust must reject the Python vector over a different body, which is the
    /// tamper case the bridge relies on.
    #[test]
    fn rust_rejects_the_python_vector_over_a_tampered_body() {
        let at = Timestamp::from_unix_ms(PYTHON_VECTOR_MILLIS).expect("timestamp");
        let error = verify(
            PYTHON_VECTOR_SECRET,
            Some(PYTHON_VECTOR),
            Some(PYTHON_VECTOR_SERVICE),
            &at,
            br#"{"a":2}"#,
            &["mas-api", "mas-worker"],
        )
        .expect_err("tampering must be detected across languages");
        assert_eq!(error.error_code(), "UNAUTHENTICATED");
    }
}
