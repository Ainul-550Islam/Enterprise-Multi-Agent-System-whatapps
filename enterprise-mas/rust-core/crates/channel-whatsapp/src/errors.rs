//! Meta Graph API error codes that the WhatsApp channel maps onto
//! [`AppError`]. The numeric codes are Meta's; the mapping is ours.
//!
//! StableApiError compatibility: every mapped error uses an `AppError`
//! variant whose `error_code()` is a platform-stable string (`RATE_LIMITED`,
//! `UNAUTHENTICATED`, `FAILED_PRECONDITION`, `UNAVAILABLE`). Handlers convert
//! `AppError` through the existing envelope — this module must not invent a
//! parallel error type.

use mas_common::error::AppError;

/// Meta error 131047 — the 24-hour customer-care window has closed; a
/// free-form reply is refused until the customer messages again or a
/// template is used.
pub const CODE_REENGAGEMENT_WINDOW: i64 = 131047;

/// Meta error 131026 — the message was undeliverable (invalid recipient,
/// user blocked the business, etc.).
pub const CODE_UNDELIVERABLE: i64 = 131026;

/// Meta error 130429 — Graph throughput / pair-rate limit.
pub const CODE_RATE_LIMITED: i64 = 130429;

/// Meta error 190 — access token expired or revoked.
pub const CODE_ACCESS_TOKEN: i64 = 190;

/// One error object from a status update or Graph error envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphError {
    /// Meta numeric code.
    pub code: i64,
    /// Meta title or message, when present.
    pub title: String,
}

impl GraphError {
    /// Builds from a JSON object with `code` and optional `title`/`message`.
    #[must_use]
    pub fn from_value(value: &serde_json::Value) -> Option<Self> {
        let code = value.get("code")?.as_i64()?;
        let title = value
            .get("title")
            .and_then(serde_json::Value::as_str)
            .or_else(|| value.get("message").and_then(serde_json::Value::as_str))
            .unwrap_or("")
            .to_owned();
        Some(Self { code, title })
    }
}

/// Maps a Meta Graph error code onto a platform [`AppError`].
///
/// Unknown codes become [`AppError::ExternalService`] so a new Meta code
/// surfaces as a provider failure, never as success.
#[must_use]
pub fn map_graph_error(code: i64, message: &str) -> AppError {
    match code {
        CODE_REENGAGEMENT_WINDOW => AppError::conflict(format!(
            "whatsapp 24h re-engagement window closed ({CODE_REENGAGEMENT_WINDOW}): {message}"
        )),
        CODE_UNDELIVERABLE => AppError::conflict(format!(
            "whatsapp message undeliverable ({CODE_UNDELIVERABLE}): {message}"
        )),
        CODE_RATE_LIMITED => AppError::rate_limited(format!(
            "whatsapp graph rate limited ({CODE_RATE_LIMITED}): {message}"
        )),
        CODE_ACCESS_TOKEN => AppError::unauthorized(format!(
            "whatsapp access token rejected ({CODE_ACCESS_TOKEN}): {message}"
        )),
        other => AppError::external_service("whatsapp", format!("graph error {other}: {message}")),
    }
}

/// Parses a Graph error envelope (`{"error":{"code":…,"message":…}}`).
#[must_use]
pub fn error_from_graph_body(body: &str) -> Option<AppError> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let error = value.get("error")?;
    let code = error.get("code")?.as_i64()?;
    let message = error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    Some(map_graph_error(code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_the_documented_codes() {
        assert_eq!(
            map_graph_error(CODE_REENGAGEMENT_WINDOW, "window").error_code(),
            "CONFLICT"
        );
        assert_eq!(
            map_graph_error(CODE_UNDELIVERABLE, "gone").error_code(),
            "CONFLICT"
        );
        assert_eq!(
            map_graph_error(CODE_RATE_LIMITED, "slow").error_code(),
            "RATE_LIMITED"
        );
        assert_eq!(
            map_graph_error(CODE_ACCESS_TOKEN, "expired").error_code(),
            "UNAUTHENTICATED"
        );
        assert_eq!(
            map_graph_error(1, "other").error_code(),
            "EXTERNAL_SERVICE_ERROR"
        );
    }

    #[test]
    fn parses_a_graph_envelope() {
        let body = r#"{"error":{"code":130429,"message":"too many"}}"#;
        let err = error_from_graph_body(body).expect("parses");
        assert_eq!(err.error_code(), "RATE_LIMITED");
    }
}
