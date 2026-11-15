/// Channel-specific errors for the WhatsApp integration.
///
/// These errors are mapped to `StableApiError` in the API layer so that
/// the HTTP response body is consistent across all crates.
#[derive(Clone, Debug, PartialEq, thiserror::Error)]
pub enum ChannelError {
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    #[error("missing or invalid webhook token")]
    InvalidWebhookToken { route_id: String },

    #[error("missing signature header")]
    MissingSignature,

    #[error("invalid signature: expected sha256=<hex>, got malformed header")]
    InvalidSignature,

    #[error("body too large: {received} bytes (max {max})")]
    BodyTooLarge { received: usize, max: usize },

    #[error("unsupported message type: {0}")]
    UnsupportedMessageType(String),

    #[error("malformed payload: {0}")]
    MalformedPayload(String),

    #[error("invalid MIME type: {0}, allowed: {allowed}")]
    InvalidMimeType { mime: String, allowed: String },

    #[error("no allowed host for egress")]
    NoAllowedHost,

    #[error("Graph API request failed: status {status}, body: {body}")]
    GraphResponse { status: u16, body: String },

    #[error("Graph request I/O error: {0}")]
    IoError { source: std::io::Error },

    #[error("JSON parse error: {0}")]
    JsonParse { source: serde_json::Error },

    #[error("client setup error: {0}")]
    ClientSetup { source: reqwest::Error },

    #[error("crypto error")]
    CryptoError,

    #[error("secret ref not resolved: {0}")]
    SecretNotResolved(String),

    #[error("rate limit exceeded, backoff {0}ms")]
    RateLimitExceeded(u64),

    #[error("window closed, only approved templates allowed")]
    WindowClosed,

    #[error("template not approved: {0}")]
    TemplateNotApproved(String),

    #[error("template validation error: {0}")]
    TemplateValidation(String),

    #[error(" unknown error: {0}")]
    Unknown(String),
}

impl ChannelError {
    /// Map this error to a `StableApiError` suitable for the HTTP API response.
    /// The mapping is many-to-one: several channel errors map to the same
    /// HTTP status/code combination.
    pub fn to_stable_api_error(&self) -> crate::api::response::StableApiError {
        use crate::api::response::StableApiError;
        match self {
            ChannelError::InvalidConfig(_) => StableApiError::BadRequest(
                "invalid configuration".into(),
            ),
            ChannelError::InvalidWebhookToken { .. } => {
                StableApiError::Unauthorized("invalid webhook token".into())
            }
            ChannelError::MissingSignature => {
                StableApiError::BadRequest("missing X-Hub-Signature-256".into())
            }
            ChannelError::InvalidSignature => {
                StableApiError::Unauthorized("invalid signature".into())
            }
            ChannelError::BodyTooLarge { .. } => {
                StableApiError::PayloadTooLarge(
                    "message body exceeds size limit".into(),
                )
            }
            ChannelError::UnsupportedMessageType(_) => {
                StableApiError::BadRequest(
                    "unsupported message type".into(),
                )
            }
            ChannelError::MalformedPayload(_) => {
                StableApiError::BadRequest(
                    "malformed webhook payload".into(),
                )
            }
            ChannelError::InvalidMimeType { .. } => {
                StableApiError::BadRequest(
                    "invalid MIME type".into(),
                )
            }
            ChannelError::NoAllowedHost => {
                StableApiError::ServiceUnavailable(
                    "egress host not allowed".into(),
                )
            }
            ChannelError::GraphResponse { status, body } => {
                // Map common Meta statuses to appropriate HTTP codes
                let code = match status {
                    429 => 429, // rate limited
                    401 | 403 => 401,
                    500 | 502 | 503 => 502,
                    _ => 400,
                };
                StableApiError::ExternalServiceError {
                    service: "graph.facebook.com".into(),
                    code: *status,
                    message: body.clone().into(),
                }
            }
            ChannelError::IoError { .. } => {
                StableApiError::ServiceUnavailable(
                    "graph I/O error".into(),
                )
            }
            ChannelError::JsonParse { .. } => {
                StableApiError::BadRequest(
                    "failed to parse Graph response".into(),
                )
            }
            ChannelError::ClientSetup { .. } => {
                StableApiError::InternalError(
                    "HTTP client setup failure".into(),
                )
            }
            ChannelError::CryptoError => {
                StableApiError::InternalError(
                    "cryptographic failure".into(),
                )
            }
            ChannelError::SecretNotResolved(_) => {
                StableApiError::InternalError(
                    "secret not resolved from key store".into(),
                )
            }
            ChannelError::RateLimitExceeded(_) => {
                StableApiError::TooManyRequests(
                    "rate limit exceeded, see 130429".into(),
                )
            }
            ChannelError::WindowClosed => {
                StableApiError::BadRequest(
                    "window closed, use approved template".into(),
                )
            }
            ChannelError::TemplateNotApproved(msg) => {
                StableApiError::BadRequest(
                    format!("template not approved: {}", msg).into(),
                )
            }
            ChannelError::TemplateValidation(msg) => {
                StableApiError::BadRequest(
                    format!("template validation error: {}", msg).into(),
                )
            }
            ChannelError::Unknown(msg) => {
                StableApiError::InternalError(
                    msg.into(),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_config() {
        let err = ChannelError::InvalidConfig("waba_id required".into());
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::BadRequest(msg) => {
                assert!(msg.contains("invalid configuration"));
            }
            _ => panic!("expected BadRequest, got {:?}", stable),
        }
    }

    #[test]
    fn test_rate_limit_exceeded() {
        let err = ChannelError::RateLimitExceeded(30_000);
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::TooManyRequests(msg) => {
                assert!(msg.contains("130429"));
            }
            _ => panic!("expected TooManyRequests, got {:?}", stable),
        }
    }

    #[test]
    fn test_window_closed() {
        let err = ChannelError::WindowClosed;
        let stable = err.to_stable_api_error();
        match stable {
            crate::api::response::StableApiError::BadRequest(msg) => {
                assert!(msg.contains("window closed"));
            }
            _ => panic!("expected BadRequest, got {:?}", stable),
        }
    }
}