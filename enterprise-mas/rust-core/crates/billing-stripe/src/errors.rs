/// Stripe-specific errors, mapped to `StableApiError` in the API layer.
///
/// Each variant represents a failure mode that can occur when interacting
/// with the Stripe API or processing Stripe webhooks. The `to_stable_api_error()`
/// method maps each variant to an appropriate `crate::api::response::StableApiError`
/// so that the HTTP response body is consistent across all crates.
use serde_json::Value;
use std::num::ParseIntError;

#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum StripeError {
    #[error("invalid signature: {0}")]
    InvalidSignature(String),

    #[error("stale timestamp: {0}ms ago")]
    StaleTimestamp { age_ms: u64 },

    #[error("future timestamp")]
    FutureTimestamp,

    #[error("invalid signature format: {0}")]
    InvalidSignatureFormat { reason: String },

    #[error("client setup: {0}")]
    ClientSetup { source: anyhow::Error },

    #[error("JSON parse: {0}")]
    JsonParse { source: serde_json::Error },

    #[error("body too large: {received} bytes (max {max})")]
    BodyTooLarge { received: usize, max: usize },

    #[error("no allowed host for egress")]
    NoAllowedHost,

    #[error("API error: status {status}, body: {body}")]
    ApiError { status: u16, body: String },

    #[error("missing field: {field}")]
    MissingField { field: String },

    #[error("payment required (402)")]
    PaymentRequired,

    #[error("not found: {what}")]
    NotFound { what: String },

    #[error("parse int: {0}")]
    ParseInt(ParseIntError),

    #[error("unexpected webhook event: {0}")]
    UnexpectedWebhook(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("rate limited")]
    RateLimited,
}

impl StripeError {
    /// Map this error to a `StableApiError` suitable for the HTTP API response.
    pub fn to_stable_api_error(&self) -> crate::api::response::StableApiError {
        use crate::api::response::StableApiError;
        match self {
            StripeError::InvalidSignature(msg) => {
                StableApiError::Unauthorized(format!("invalid stripe signature: {}", msg))
            }
            StripeError::StaleTimestamp { age_ms } => {
                StableApiError::BadRequest(format!("stale stripe webhook timestamp ({}ms)", age_ms))
            }
            StripeError::FutureTimestamp => {
                StableApiError::BadRequest("stripe webhook timestamp is in the future".into())
            }
            StripeError::InvalidSignatureFormat { reason } => {
                StableApiError::BadRequest(format!("invalid stripe signature format: {}", reason))
            }
            StripeError::ClientSetup { source } => {
                StableApiError::InternalError(format!("stripe client setup failure: {}", source))
            }
            StripeError::JsonParse { source } => {
                StableApiError::BadRequest(format!("failed to parse stripe JSON: {}", source))
            }
            StripeError::BodyTooLarge { received, max } => {
                StableApiError::PayloadTooLarge(format!(
                    "request body too large ({} > {} received)",
                    received, max
                ))
            }
            StripeError::NoAllowedHost => {
                StableApiError::ServiceUnavailable("stripe egress host not allowed".into())
            }
            StripeError::ApiError { status, body } => {
                let code = match status {
                    429 => 429,
                    402 => 402,
                    404 => 404,
                    500 | 502 | 503 => 502,
                    _ => 400,
                };
                StableApiError::ExternalServiceError {
                    service: "api.stripe.com".into(),
                    code,
                    message: body.clone().into(),
                }
            }
            StripeError::MissingField { field } => {
                StableApiError::BadRequest(format!("missing required field: {}", field))
            }
            StripeError::PaymentRequired => {
                StableApiError::PaymentRequired("stripe returned 402 Payment Required".into())
            }
            StripeError::NotFound { what } => {
                StableApiError::NotFound(format!("{} not found", what))
            }
            StripeError::ParseInt(_) => {
                StableApiError::BadRequest("failed to parse stripe integer field".into())
            }
            StripeError::UnexpectedWebhook(msg) => {
                StableApiError::BadRequest(format!("unexpected stripe webhook: {}", msg))
            }
            StripeError::Config(msg) => {
                StableApiError::InternalError(format!("stripe configuration error: {}", msg))
            }
            StripeError::RateLimited => {
                StableApiError::TooManyRequests("stripe rate limit exceeded".into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_signature_to_stable() {
        let err = StripeError::InvalidSignature("bad sig".into());
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::Unauthorized(msg) => {
                assert!(msg.contains("invalid stripe signature"));
            }
            _ => panic!("expected Unauthorized, got {:?}", stable),
        }
    }

    #[test]
    fn test_stale_timestamp_to_stable() {
        let err = StripeError::StaleTimestamp { age_ms: 600_000 };
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::BadRequest(msg) => {
                assert!(msg.contains("600000"));
            }
            _ => panic!("expected BadRequest, got {:?}", stable),
        }
    }

    #[test]
    fn test_api_error_to_stable() {
        let err = StripeError::ApiError { status: 400, body: "{}".into() };
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::ExternalServiceError { .. } => {}
            _ => panic!("expected ExternalServiceError, got {:?}", stable),
        }
    }

    #[test]
    fn test_body_too_large_to_stable() {
        let err = StripeError::BodyTooLarge { received: 2_000_000, max: 1_000_000 };
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::PayloadTooLarge(msg) => {
                assert!(msg.contains("2000000"));
            }
            _ => panic!("expected PayloadTooLarge, got {:?}", stable),
        }
    }
}