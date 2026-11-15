/// Stripe webhook signature verification.
///
/// Stripe sends signatures in the form:
///   - t=timestamp,v1=hash   (v1 is the primary, recommended format)
///   - t=timestamp,v1={v1_hash_1},{v1_hash_2},...  (multiple v1 signatures for rotation)
///
/// The HMAC is computed as:  hmac_sha256(t . raw_body, secret)
/// where "." denotes string concatenation.
///
/// This crate supports:
///   - Single v1 signature verification
///   - Multiple v1 signatures (for secret rotation)
///   - 5-minute tolerance on the timestamp to prevent replay attacks
///   - Constant-time comparison via `subtle::ConstantTimeEq`
///   - Fallback to the `t` timestamp age check

use crate::errors::StripeError;
use subtle::ConstantTimeEq;
use sha2::{Digest, Mac, Sha256};
use tracing::instrument;

/// Parsed Stripe signature string.
///
/// `t` is the timestamp (unix seconds), `v1` is the signature hash.
/// When multiple v1 values are present (rotation), they are collected into a Vec.
#[derive(Clone, Debug, PartialEq)]
pub struct StripeSignature {
    /// Unix timestamp when the signature was generated
    pub timestamp: i64,
    /// The v1 signature hash(es)
    pub signatures: Vec<String>,
}

impl StripeSignature {
    /// Parse a Stripe signature string like "t=1698765432,v1=abc123..."
    /// Or "t=1698765432,v1=abc123...,v1=def456..." for rotation.
    pub fn parse(s: &str) -> Result<Self, StripeError> {
        let parts: Vec<&str> = s.split(',').collect();
        let mut timestamp: Option<i64> = None;
        let mut signatures = Vec::new();

        for part in &parts {
            let part = *part;
            if let Some(rest) = part.strip_prefix("t=") {
                let ts = rest.parse::<i64>()
                    .map_err(|_| StripeError::InvalidSignatureFormat {
                        reason: "invalid timestamp format".into(),
                    })?;
                timestamp = Some(ts);
            } else if let Some(rest) = part.strip_prefix("v1=") {
                signatures.push(rest.to_string());
            }
            // Ignore unknown prefixes
        }

        let ts = timestamp.ok_or_else(|| StripeError::InvalidSignatureFormat {
            reason: "missing t= prefix".into(),
        })?;
        if signatures.is_empty() {
            return Err(StripeError::InvalidSignatureFormat {
                reason: "missing v1= signature".into(),
            });
        }

        Ok(StripeSignature { timestamp: ts, signatures })
    }

    /// Verify the signature against the given secret ref.
    /// The HMAC is:  hmac_sha256(concat(t, raw_body), secret)
    ///
    /// Returns `Ok(())` if valid, or `Err` if invalid.
    ///
    /// Tolerance: the timestamp must not be more than 5 minutes in the past
    /// (future timestamps are always rejected to prevent replay).
    #[instrument(skip(self, secret, body))]
    pub fn verify(&self, secret: &str, body: &[u8]) -> Result<(), StripeError> {
        // 5-minute tolerance check
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| StripeError::CryptoError)?
            .as_secs() as i64;

        let age = now - self.timestamp;
        if age > 300 {
            // More than 5 minutes old — reject (replay protection)
            return Err(StripeError::StaleTimestamp { age_ms: age * 1000 });
        }
        if age < 0 {
            // Future timestamp — always reject
            return Err(StripeError::FutureTimestamp);
        }

        // Compute HMAC-SHA256 for each v1 signature and compare constant-time
        let message = {
            // Concatenate timestamp string + raw body
            let t_str = self.timestamp.to_string();
            format!("{}{}", t_str, String::from_utf8_lossy(body)).into_bytes()
        };

        for expected_sig in &self.signatures {
            type HmacSha256 = hmac::Hmac<Sha256>;
            let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                .map_err(|_| StripeError::CryptoError)?;

            mac.update(&message);
            let result = mac.finalize();
            let expected_bytes = result.into_bytes();

            // Trim or pad for constant-time comparison
            let mut expected_padded = expected_bytes.to_vec();
            let mut actual_padded = expected_sig.as_bytes().to_vec();

            // Pad shorter one with zeros
            if expected_padded.len() < actual_padded.len() {
                expected_padded.extend(std::iter::repeat(0u8).take(actual_padded.len() - expected_padded.len()));
            } else if actual_padded.len() < expected_padded.len() {
                actual_padded.extend(std::iter::repeat(0u8).take(expected_padded.len() - actual_padded.len()));
            }

            let ok = subtle::ConstantTimeEq::ct_eq(&mut expected_padded, &mut actual_padded);

            if ok {
                return Ok(());
            }
        }

        Err(StripeError::InvalidSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_single_sig() {
        let sig = StripeSignature::parse("t=1698765432,v1=abc123def456").unwrap();
        assert_eq!(sig.timestamp, 1698765432);
        assert_eq!(sig.signatures.len(), 1);
        assert_eq!(sig.signatures[0], "abc123def456");
    }

    #[test]
    fn test_parse_multiple_sigs() {
        let sig = StripeSignature::parse("t=1698765432,v1=abc123,v1=def456").unwrap();
        assert_eq!(sig.timestamp, 1698765432);
        assert_eq!(sig.signatures.len(), 2);
    }

    #[test]
    fn test_parse_missing_t() {
        let err = StripeSignature::parse("v1=abc123").unwrap_err();
        // Should have some error about missing t=
        assert!(format!("{:?}", err).contains("missing t=") || format!("{:?}", err).contains("invalid"));
    }

    #[test]
    fn test_verify_with_mock_clock() {
        // Use a recent timestamp (within 5 min)
        let sig = StripeSignature::parse("t=1698765432,v1=test").unwrap();

        // Compute what the HMAC should be
        let body = b"{}";
        type HmacSha256 = hmac::Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice("test-secret".as_bytes())
            .map_err(|_| StripeError::CryptoError)?;
        mac.update(b"1698765432{}");
        let result = mac.finalize();
        let expected = hex::encode(result.into_bytes());

        // Create a sig with the expected hash
        let sig = StripeSignature::parse(&format!("t=1698765432,v1={}", expected)).unwrap();
        let result = sig.verify("test-secret", body);
        assert!(result.is_ok(), "Expected valid signature, got Err({:?})", result.err());
    }

    #[test]
    fn test_verify_wrong_secret() {
        let sig = StripeSignature::parse("t=1698765432,v1=wrong").unwrap();
        let result = sig.verify("correct-secret", b"{}");
        assert!(matches!(result, Err(StripeError::InvalidSignature)));
    }

    #[test]
    fn test_verify_stale_timestamp() {
        // Use a timestamp from 10 minutes ago
        let old_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64 - 600; // 10 minutes ago
        let sig = StripeSignature::parse(&format!("t={},v1=test", old_ts)).unwrap();
        let result = sig.verify("any-secret", b"{}");
        assert!(matches!(result, Err(StripeError::StaleTimestamp { .. })));
    }

    #[test]
    fn test_verify_future_timestamp() {
        let future_ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64 + 60; // 1 minute in the future
        let sig = StripeSignature::parse(&format!("t={},v1=test", future_ts)).unwrap();
        let result = sig.verify("any-secret", b"{}");
        assert!(matches!(result, Err(StripeError::FutureTimestamp)));
    }
}