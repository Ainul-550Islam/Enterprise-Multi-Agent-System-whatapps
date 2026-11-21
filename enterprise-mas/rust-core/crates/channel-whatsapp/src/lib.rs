//! PURPOSE: The WhatsApp channel boundary — everything that speaks the Meta
//! WhatsApp Cloud API wire format lives here and nowhere else.
//!
//! Owning layer: `mas-channel-whatsapp` (provider boundary crate, sibling of
//! `mas-billing-stripe`).
//!
//! Production responsibility:
//!
//! * **Inbound verification** ([`verify_inbound_signature`]): every webhook
//!   delivery is authenticated by `X-Hub-Signature-256` — an HMAC-SHA256 tag
//!   over the *raw* body with the app secret. Comparison is constant-time and
//!   the raw bytes are used exactly as received.
//! * **Inbound parsing** ([`inbound`]): envelope → typed messages and status
//!   updates, tolerant of unknown fields, hostile to unknown shapes.
//! * **Outbound payloads** ([`outbound`]): typed builders for text and
//!   template sends; serialization is the single place the Graph API body
//!   shape is defined.
//! * **Client** ([`client`]): the transport seam. The Graph host is fixed;
//!   secrets arrive by reference and are redacted from every `Debug` output.
//!
//! It must NOT:
//!   • touch the database (persistence owns storage; `mas-application`
//!     orchestrates);
//!   • log token material or raw message bodies at info level;
//!   • accept a delivery whose signature does not verify — verification
//!     failure is a hard error, never a warning.

pub mod client;
pub mod errors;
pub mod inbound;
pub mod outbound;

use hmac::{Hmac, Mac};
use mas_common::error::AppError;
use mas_common::result::Result;
use sha2::Sha256;

/// Prefix Meta uses for the `X-Hub-Signature-256` header value.
pub const SIG_PREFIX: &str = "sha256=";

/// Default replay tolerance for inbound webhook timestamps, in seconds.
pub const DEFAULT_TOLERANCE_SECS: i64 = 300;

/// Upper bound on a webhook POST body. Larger deliveries are refused before
/// HMAC work is spent.
pub const MAX_WEBHOOK_BODY_BYTES: usize = 1024 * 1024;

/// Verifies `X-Hub-Signature-256` over the raw webhook body.
///
/// * `raw_body` — the request body exactly as received;
/// * `header` — the full header value (`sha256=<hex>`);
/// * `app_secret` — the `MAS_WHATSAPP_APP_SECRET` reference value;
/// * `body_unix_ts` — the `entry.changes[].value.messages[].timestamp`
///   (or status timestamp) used for the replay window; pass `None` to skip
///   the window check (single-purpose verification call);
/// * `now_unix` — server clock.
///
/// A mismatched or malformed signature is `AppError::Unauthorized`; a
/// replayed (too old) delivery is `AppError::InvalidRequest`.
pub fn verify_inbound_signature(
    raw_body: &[u8],
    header: &str,
    app_secret: &str,
    body_unix_ts: Option<i64>,
    now_unix: i64,
) -> Result<()> {
    if raw_body.len() > MAX_WEBHOOK_BODY_BYTES {
        return Err(AppError::Validation {
            message: "webhook body exceeds size limit".into(),
            issues: Vec::new(),
        });
    }
    if header.trim().is_empty() {
        return Err(AppError::Unauthorized("missing X-Hub-Signature-256".into()));
    }
    let provided = header
        .strip_prefix(SIG_PREFIX)
        .ok_or_else(|| AppError::Unauthorized("malformed X-Hub-Signature-256".into()))?;
    let provided_bytes = hex::decode(provided.trim())
        .map_err(|_| AppError::Unauthorized("signature is not valid hex".into()))?;

    let mut mac = Hmac::<Sha256>::new_from_slice(app_secret.as_bytes())
        .map_err(|_| AppError::Internal("hmac key init".into()))?;
    mac.update(raw_body);
    mac.verify_slice(&provided_bytes)
        .map_err(|_| AppError::Unauthorized("signature mismatch".into()))?;

    if let Some(ts) = body_unix_ts {
        let age = now_unix.saturating_sub(ts);
        if age > DEFAULT_TOLERANCE_SECS {
            return Err(AppError::Validation {
                message: "webhook delivery outside replay window".into(),
                issues: Vec::new(),
            });
        }
    }
    Ok(())
}

/// GET handshake: `hub.mode=subscribe`, constant-time token compare, echo
/// `hub.challenge`.
pub fn verify_webhook_challenge(
    mode: Option<&str>,
    token: Option<&str>,
    challenge: Option<&str>,
    expected_token: &str,
) -> Result<String> {
    if mode != Some("subscribe") {
        return Err(AppError::forbidden(
            "webhook subscription mode must be subscribe",
        ));
    }
    let token = token.unwrap_or_default();
    if !constant_time_eq(token.as_bytes(), expected_token.as_bytes()) {
        return Err(AppError::forbidden("webhook verify token mismatch"));
    }
    challenge
        .filter(|c| !c.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| AppError::validation("webhook challenge missing"))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut acc = (left.len() as u64) ^ (right.len() as u64);
    let max = left.len().max(right.len());
    for i in 0..max {
        let a = u64::from(*left.get(i).unwrap_or(&0));
        let b = u64::from(*right.get(i).unwrap_or(&0));
        acc |= a ^ b;
    }
    acc == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac");
        mac.update(body);
        format!("{}{}", SIG_PREFIX, hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn accepts_a_valid_signature() {
        let body = br#"{"object":"whatsapp_business_account"}"#;
        let header = sign("app-secret", body);
        assert!(verify_inbound_signature(body, &header, "app-secret", None, 0).is_ok());
    }

    #[test]
    fn rejects_a_foreign_secret() {
        let body = br#"{"object":"whatsapp_business_account"}"#;
        let header = sign("other-secret", body);
        assert!(verify_inbound_signature(body, &header, "app-secret", None, 0).is_err());
    }
}
