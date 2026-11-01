//! PURPOSE: Stable OAuth authorization initiation and public callback DTOs.
//!
//! Owning layer: `mas-contracts` HTTP/gRPC wire boundary.
//!
//! Production role: expose only a browser authorization URL to an authenticated
//! connector manager and safely decode bounded provider callback parameters.
//!
//! Security boundary: no DTO carries a token, PKCE verifier, tenant id,
//! connector id for public callback routing, client secret, or a caller-chosen
//! OAuth endpoint/redirect URI. Provider error text is accepted only as an
//! opaque presence signal and must never be reflected.
//!
//! Data ownership: durable OAuth state/token lifecycle belongs to integrations
//! and persistence; this module owns wire shape and syntactic bounds only.
//!
//! Prohibited responsibilities: token exchange, state validation against SQL,
//! redirect URL generation, credential binding, or a success response that
//! claims an external token was persisted before the callback service returns.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};

/// Authenticated response for a connector-bound OAuth authorization start.
/// The URL contains a one-shot state and must not be logged by callers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthAuthorizationUrlDto {
    pub authorization_url: String,
}

/// Public provider callback query. `error` is deliberately not exposed in any
/// response; it causes a one-shot state-consuming rejection in the service.
#[derive(Debug, Clone, Deserialize)]
pub struct OAuthCallbackQuery {
    pub state: String,
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default, rename = "error")]
    pub provider_error: Option<String>,
}

impl OAuthCallbackQuery {
    pub fn validate_shape(&self) -> Result<()> {
        if self.state.len() != 64 || !self.state.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(AppError::unauthorized("invalid OAuth callback state"));
        }
        if self.code.as_deref().is_some_and(|code| {
            code.is_empty() || code.len() > 8_192 || code.chars().any(char::is_control)
        }) {
            return Err(AppError::unauthorized("invalid OAuth callback code"));
        }
        if self.provider_error.as_deref().is_some_and(|error| {
            error.is_empty() || error.len() > 512 || error.chars().any(char::is_control)
        }) {
            return Err(AppError::unauthorized(
                "invalid OAuth callback provider error",
            ));
        }
        Ok(())
    }
}

/// Deliberately content-free callback acknowledgement. A browser/provider only
/// needs an HTTP completion code; no credential id, tenant, provider response,
/// or token lifecycle detail is returned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthCallbackAcknowledgement {
    pub accepted: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn callback_shape_refuses_unbounded_or_non_hex_state() {
        assert!(OAuthCallbackQuery {
            state: "z".repeat(64),
            code: None,
            provider_error: None,
        }
        .validate_shape()
        .is_err());
        assert!(OAuthCallbackQuery {
            state: "a".repeat(64),
            code: Some("code".to_owned()),
            provider_error: None,
        }
        .validate_shape()
        .is_ok());
    }
}
