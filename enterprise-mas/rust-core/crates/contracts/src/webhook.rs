//! Outbound webhook endpoint and delivery DTOs.

use mas_common::ids::{EventId, SecretReferenceId, WebhookId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Register a webhook endpoint using a secret-manager reference only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterWebhookRequest {
    /// Absolute endpoint URL; the application reapplies `SafeUrl` SSRF checks.
    pub url: String,
    /// Event names delivered to this endpoint.
    pub subscribed_events: Vec<String>,
    /// Reference to an externally stored HMAC signing secret.
    pub secret_reference_id: SecretReferenceId,
    /// Optional display description.
    pub description: Option<String>,
}

impl RegisterWebhookRequest {
    /// Validates the URL envelope and event subscription bounds.
    ///
    /// The application service must additionally parse the URL with the
    /// domain `SafeUrl` type, which performs the authoritative SSRF checks.
    pub fn validate(&self) -> Result<()> {
        validation::validate_length("url", &self.url, 1, 2048)?;
        if !(self.url.starts_with("https://") || self.url.starts_with("http://"))
            || self.url.split_once("://").is_none_or(|(_, authority)| {
                authority
                    .split('/')
                    .next()
                    .is_none_or(|host| host.is_empty() || host.contains('@'))
            })
        {
            return Err(mas_common::error::AppError::invalid_field(
                "url",
                "invalid_url",
                "webhook URL must be absolute and must not contain credentials",
            ));
        }
        if self.subscribed_events.is_empty() || self.subscribed_events.len() > 128 {
            return Err(mas_common::error::AppError::invalid_field(
                "subscribed_events",
                "out_of_range",
                "a webhook must subscribe to between 1 and 128 events",
            ));
        }
        let distinct: BTreeSet<&str> = self.subscribed_events.iter().map(String::as_str).collect();
        if distinct.len() != self.subscribed_events.len() {
            return Err(mas_common::error::AppError::invalid_field(
                "subscribed_events",
                "duplicate",
                "event subscriptions must be unique",
            ));
        }
        for event in &self.subscribed_events {
            validation::validate_length("event", event, 1, 128)?;
            if !event
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || ".:_-*".contains(character))
            {
                return Err(mas_common::error::AppError::invalid_field(
                    "subscribed_events",
                    "invalid_format",
                    format!("invalid webhook event name '{event}'"),
                ));
            }
        }
        if let Some(description) = &self.description {
            validation::validate_length("description", description, 0, 512)?;
        }
        Ok(())
    }
}

/// Keyset query for the current tenant's outbound endpoint catalogue.
///
/// The cursor is opaque and comes only from the preceding stable
/// `created_at DESC, id DESC` page. It cannot name a tenant or signing secret.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebhookListQuery {
    /// Maximum number of endpoint metadata records to return.
    pub limit: Option<u16>,
    /// Opaque URL-safe base64 cursor from the preceding page.
    pub cursor: Option<String>,
}

impl WebhookListQuery {
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "webhook page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validation::validate_length("cursor", cursor, 1, 512)?;
            if !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(mas_common::error::AppError::invalid_field(
                    "cursor",
                    "invalid_format",
                    "webhook cursor must be URL-safe base64 without padding",
                ));
            }
        }
        Ok(())
    }

    #[must_use]
    pub const fn page_limit(&self) -> u16 {
        match self.limit {
            Some(limit) => limit,
            None => 50,
        }
    }
}

/// Stable tenant outbound-endpoint page.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookPageDto {
    pub items: Vec<WebhookDto>,
    pub next_cursor: Option<String>,
}

/// Safe endpoint metadata; the signing secret is never represented.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDto {
    /// Endpoint id.
    pub id: WebhookId,
    /// Destination URL.
    pub url: String,
    /// Subscriptions.
    pub subscribed_events: Vec<String>,
    /// Whether delivery is enabled.
    pub enabled: bool,
    /// Secret reference is intentionally not returned.
    pub has_signing_secret: bool,
    /// Consecutive failure count.
    pub consecutive_failures: u32,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// Delivery history row; contains a digest, not the signed payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveryDto {
    /// Delivery identifier.
    pub id: uuid::Uuid,
    /// Destination endpoint.
    pub endpoint_id: WebhookId,
    /// Emitted event.
    pub event_id: EventId,
    /// Delivery status.
    pub state: String,
    /// Attempts so far.
    pub attempt_count: u32,
    /// Last HTTP status, if any.
    pub last_status_code: Option<u16>,
    /// Retry time when scheduled.
    pub next_attempt_at: Option<Timestamp>,
    /// SHA-256 payload digest.
    pub payload_hash: String,
    /// Sanitized error summary.
    pub last_error: Option<String>,
}

/// Bounded delivery-history page (`GET /v1/webhooks/{id}/deliveries`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookDeliveryPageDto {
    pub items: Vec<WebhookDeliveryDto>,
    pub next_cursor: Option<String>,
}

/// Query parameters for a bounded webhook delivery history page.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WebhookDeliveryQuery {
    /// Opaque URL-safe base64 cursor returned by the preceding page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    /// Maximum number of deliveries to return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u16>,
}

impl WebhookDeliveryQuery {
    /// Bounds the page size and cursor shape before the store is touched.
    pub fn validate(&self) -> Result<()> {
        if self.limit.is_some_and(|limit| !(1..=200).contains(&limit)) {
            return Err(mas_common::error::AppError::invalid_field(
                "limit",
                "out_of_range",
                "delivery page limit must be between 1 and 200",
            ));
        }
        if let Some(cursor) = &self.cursor {
            validation::validate_length("cursor", cursor, 1, 512)?;
            if !cursor
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(mas_common::error::AppError::invalid_field(
                    "cursor",
                    "invalid_format",
                    "delivery cursor must be URL-safe base64 without padding",
                ));
            }
        }
        Ok(())
    }

    /// Page size with the default applied.
    #[must_use]
    pub const fn page_limit(&self) -> u16 {
        match self.limit {
            Some(limit) => limit,
            None => 50,
        }
    }
}
