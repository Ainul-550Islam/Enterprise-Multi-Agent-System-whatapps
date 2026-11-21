//! PURPOSE: The WhatsApp application service -- channel registration,
//! outbound sends, and inbound webhook processing, through ports only.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: keep every conversation keyed to a platform
//! truth (channel -> tenant), verify every inbound delivery before it can
//! move data, and record every outbound send durably.
//!
//! The inbound webhook is processed in two phases, mirroring the billing
//! service: [`WhatsAppService::receive_inbound`] verifies the signature over
//! the **raw body** and resolves the owning tenant from the `phone_number_id`
//! through the store's resolver (never from body fields), then the caller
//! opens the authoritative scope and calls
//! [`WhatsAppService::apply_inbound`].
//!
//! It must NOT:
//!   * accept an inbound delivery whose `X-Hub-Signature-256` does not
//!     verify against the app secret (constant-time compare);
//!   * send anything except through the guarded egress the gateway port
//!     stands for;
//!   * hold a raw secret -- secrets arrive as
//!     `mas_security::SecretString` and are never logged.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::{
    OrganizationId, TenantId, WhatsAppChannelId, WhatsAppConversationId, WhatsAppMessageId,
    WhatsAppTemplateId,
};
use mas_common::result::Result;
use mas_domain::whatsapp::{
    WhatsAppChannel, WhatsAppConversation, WhatsAppDirection, WhatsAppMessage,
    WhatsAppMessageStatus, WhatsAppMessageType, WhatsAppStats, WhatsAppTemplate,
};
use mas_security::{constant_time_eq, SecretString};

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

/// Durable WhatsApp storage, scoped where the platform owns the rows.
#[async_trait]
pub trait WhatsAppStorePort: fmt::Debug + Send + Sync {
    /// Scoped: registers the channel (idempotent on `phone_number_id`).
    async fn create_channel(&self, channel: &WhatsAppChannel) -> Result<WhatsAppChannel>;

    /// Scoped: one channel by id.
    async fn get_channel(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
    ) -> Result<Option<WhatsAppChannel>>;

    /// Scoped: the tenant's active channels.
    async fn list_channels(&self, tenant_id: TenantId) -> Result<Vec<WhatsAppChannel>>;

    /// Pre-scope: which tenant and organization own this phone number id.
    /// `None` = unknown channel; refuse the delivery, never guess.
    async fn resolve_owner_by_phone_number(
        &self,
        phone_number_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>>;

    /// Scoped: records one conversation message. A repeat `wamid` is a
    /// no-op that returns the recorded message.
    async fn record_message(&self, message: &WhatsAppMessage) -> Result<WhatsAppMessage>;

    /// Scoped: one channel by its provider phone number handle.
    async fn get_channel_by_phone_number(
        &self,
        tenant_id: TenantId,
        phone_number_id: &str,
    ) -> Result<Option<WhatsAppChannel>>;

    /// Scoped: one channel's recent messages.
    async fn list_messages(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        limit: i64,
    ) -> Result<Vec<WhatsAppMessage>>;

    /// Scoped: the conversation one contact has with one channel.
    async fn get_conversation(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        contact: &str,
    ) -> Result<Vec<WhatsAppMessage>>;

    /// Scoped: records a status update for an outbound message.
    async fn update_message_status(
        &self,
        tenant_id: TenantId,
        wamid: &str,
        status: WhatsAppMessageStatus,
    ) -> Result<Option<WhatsAppMessage>>;

    /// Scoped: upserts the inbox conversation for an inbound contact.
    async fn upsert_conversation(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        contact: &str,
        inbound_at: mas_common::timestamps::Timestamp,
        opt_out: Option<bool>,
    ) -> Result<WhatsAppConversation>;

    /// Scoped: one conversation by id.
    async fn get_conversation_by_id(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
    ) -> Result<Option<WhatsAppConversation>>;

    /// Scoped: the tenant's conversations, optionally filtered by channel.
    async fn list_conversations(
        &self,
        tenant_id: TenantId,
        channel_id: Option<WhatsAppChannelId>,
        limit: i64,
    ) -> Result<Vec<WhatsAppConversation>>;

    /// Scoped: sets the human-handoff flag.
    async fn set_handoff(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
        handoff: bool,
    ) -> Result<Option<WhatsAppConversation>>;

    /// Scoped: templates registered for a channel.
    async fn list_templates(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
    ) -> Result<Vec<WhatsAppTemplate>>;

    /// Scoped: inserts or updates a template row.
    async fn upsert_template(&self, template: &WhatsAppTemplate) -> Result<WhatsAppTemplate>;

    /// Scoped: aggregate inbox counters.
    async fn stats(&self, tenant_id: TenantId) -> Result<WhatsAppStats>;
}

// The gateway boundary is declared in `mas-integrations` and composed here.
pub use mas_integrations::provider_ports::{
    GatewaySendReceipt, GatewayTemplate, WhatsAppGatewayPort,
};

// ---------------------------------------------------------------------------
// Inbound delivery
// ---------------------------------------------------------------------------

/// One message extracted from an inbound webhook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    /// The contact's phone number (E.164 as the provider reports it).
    pub from: String,
    /// The provider message id.
    pub wamid: String,
    /// The message type as the provider reported it (`text`, `image`, ...).
    pub kind: String,
    /// The text body, when the type carries one.
    pub body: Option<String>,
    /// The provider's send timestamp (unix seconds).
    pub timestamp_unix: i64,
    /// The untrusted raw provider payload for this message (evidence only).
    pub raw: serde_json::Value,
}

/// Outcome of the pre-scope inbound phase.
#[derive(Debug, Clone)]
pub enum WhatsAppDelivery {
    /// The delivery is not authentic. No effect.
    Rejected {
        /// Why.
        reason: String,
    },
    /// Verified and resolved; open the scope and call
    /// [`WhatsAppService::apply_inbound`].
    Resolved(ResolvedInbound),
}

/// A verified, tenant-resolved inbound delivery.
#[derive(Debug, Clone)]
pub struct ResolvedInbound {
    /// Owning tenant and organization (from the channel resolver).
    pub owner: (TenantId, OrganizationId),
    /// The channel's provider phone number id.
    pub phone_number_id: String,
    /// The messages the delivery carries.
    pub messages: Vec<InboundMessage>,
    /// The contact profile name, when the provider sent one.
    pub contact_name: Option<String>,
}

/// The WhatsApp application service.
pub struct WhatsAppService {
    store: Arc<dyn WhatsAppStorePort>,
    gateway: Arc<dyn WhatsAppGatewayPort>,
    /// The Meta app secret: signs every inbound webhook.
    app_secret: SecretString,
    /// The configured webhook verify token for subscription checks.
    verify_token: SecretString,
}

impl fmt::Debug for WhatsAppService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WhatsAppService")
            .field("app_secret", &"[redacted]")
            .field("verify_token", &"[redacted]")
            .finish_non_exhaustive()
    }
}

impl WhatsAppService {
    /// Builds the service. Secrets arrive as [`SecretString`] and are held
    /// only for verification.
    #[must_use]
    pub fn new(
        store: Arc<dyn WhatsAppStorePort>,
        gateway: Arc<dyn WhatsAppGatewayPort>,
        app_secret: SecretString,
        verify_token: SecretString,
    ) -> Self {
        Self {
            store,
            gateway,
            app_secret,
            verify_token,
        }
    }

    /// Registers a channel (scoped). The outbound secret is stored as a
    /// *reference* by the store; the plaintext never lands in the row.
    ///
    /// # Errors
    ///
    /// [`AppError`] when the channel conflicts with an existing one or the
    /// store fails.
    pub async fn register_channel(&self, channel: &WhatsAppChannel) -> Result<WhatsAppChannel> {
        self.store.create_channel(channel).await
    }

    /// The tenant's active channels (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn list_channels(&self, tenant_id: TenantId) -> Result<Vec<WhatsAppChannel>> {
        self.store.list_channels(tenant_id).await
    }

    /// Sends one text message through the gateway and records it (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when the tenant has no such channel;
    /// [`AppError`] on gateway or store failure.
    pub async fn send_text(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        to: &str,
        body: &str,
    ) -> Result<WhatsAppMessage> {
        let channel = self
            .store
            .get_channel(tenant_id, channel_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found("whatsapp channel", channel_id.as_uuid().to_string())
            })?;
        let receipt = self.gateway.send_text(&channel, to, body).await?;
        self.store
            .record_message(&WhatsAppMessage {
                id: WhatsAppMessageId::new(),
                tenant_id,
                channel_id,
                direction: WhatsAppDirection::Outbound,
                wamid: Some(receipt.wamid),
                from_addr: Some(channel.phone_number_id.clone()),
                to_addr: Some(to.to_owned()),
                message_type: WhatsAppMessageType::Text,
                body_text: Some(body.to_owned()),
                status: WhatsAppMessageStatus::Sent,
                payload: serde_json::Value::Null,
                occurred_at: mas_common::timestamps::Timestamp::now(),
                created_at: mas_common::timestamps::Timestamp::now(),
                updated_at: mas_common::timestamps::Timestamp::now(),
            })
            .await
    }

    /// One channel's recorded messages (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn list_messages(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        limit: i64,
    ) -> Result<Vec<WhatsAppMessage>> {
        self.store.list_messages(tenant_id, channel_id, limit).await
    }

    /// One conversation (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn get_conversation(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
        contact: &str,
    ) -> Result<Vec<WhatsAppMessage>> {
        self.store
            .get_conversation(tenant_id, channel_id, contact)
            .await
    }

    /// Records a delivery-status update (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn update_message_status(
        &self,
        tenant_id: TenantId,
        wamid: &str,
        status: WhatsAppMessageStatus,
    ) -> Result<Option<WhatsAppMessage>> {
        self.store
            .update_message_status(tenant_id, wamid, status)
            .await
    }

    /// One conversation's messages by conversation id (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when the conversation is missing;
    /// [`AppError`] on store failure.
    pub async fn conversation_messages(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
    ) -> Result<Vec<WhatsAppMessage>> {
        let conversation = self
            .store
            .get_conversation_by_id(tenant_id, conversation_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found(
                    "whatsapp conversation",
                    conversation_id.as_uuid().to_string(),
                )
            })?;
        self.store
            .get_conversation(tenant_id, conversation.channel_id, &conversation.contact)
            .await
    }

    /// A human agent's reply on a conversation (scoped).
    ///
    /// Free-form text is allowed only inside the 24-hour window and only
    /// when the contact has not opted out. After the window, `template_name`
    /// is required.
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when the conversation or channel is missing;
    /// [`AppError::conflict`] when the contact opted out;
    /// [`AppError::validation`] when the window is closed and no template
    /// was named, or the body is empty;
    /// [`AppError`] on gateway or store failure.
    pub async fn human_reply(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
        body: Option<&str>,
        template_name: Option<&str>,
        template_language: Option<&str>,
    ) -> Result<WhatsAppMessage> {
        let conversation = self
            .store
            .get_conversation_by_id(tenant_id, conversation_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found(
                    "whatsapp conversation",
                    conversation_id.as_uuid().to_string(),
                )
            })?;
        if conversation.opt_out {
            return Err(AppError::conflict(
                "the contact opted out; outbound is refused",
            ));
        }
        let channel = self
            .store
            .get_channel(tenant_id, conversation.channel_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found(
                    "whatsapp channel",
                    conversation.channel_id.as_uuid().to_string(),
                )
            })?;
        let receipt = if let Some(name) = template_name {
            let language = template_language.unwrap_or("en");
            self.gateway
                .send_template(&channel, &conversation.contact, name, language)
                .await?
        } else {
            let window_open = conversation
                .window_expires_at
                .is_some_and(|expires| expires > mas_common::timestamps::Timestamp::now());
            if !window_open {
                return Err(AppError::validation(
                    "the 24-hour customer-care window has closed; send a template",
                ));
            }
            let body = body
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    AppError::validation("human reply body is required inside the window")
                })?;
            self.gateway
                .send_text(&channel, &conversation.contact, body)
                .await?
        };
        let body_text = body.map(str::to_owned);
        self.store
            .record_message(&WhatsAppMessage {
                id: WhatsAppMessageId::new(),
                tenant_id,
                channel_id: conversation.channel_id,
                direction: WhatsAppDirection::Outbound,
                wamid: Some(receipt.wamid),
                from_addr: Some(channel.phone_number_id),
                to_addr: Some(conversation.contact),
                message_type: if template_name.is_some() {
                    WhatsAppMessageType::Template
                } else {
                    WhatsAppMessageType::Text
                },
                body_text,
                status: WhatsAppMessageStatus::Sent,
                payload: serde_json::Value::Null,
                occurred_at: mas_common::timestamps::Timestamp::now(),
                created_at: mas_common::timestamps::Timestamp::now(),
                updated_at: mas_common::timestamps::Timestamp::now(),
            })
            .await
    }

    /// The tenant's templates for a channel (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn list_templates(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
    ) -> Result<Vec<WhatsAppTemplate>> {
        self.store.list_templates(tenant_id, channel_id).await
    }

    /// Pulls the WABA's templates through the gateway and upserts them (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when the channel is missing;
    /// [`AppError`] on gateway or store failure.
    pub async fn sync_templates(
        &self,
        tenant_id: TenantId,
        channel_id: WhatsAppChannelId,
    ) -> Result<Vec<WhatsAppTemplate>> {
        let channel = self
            .store
            .get_channel(tenant_id, channel_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found("whatsapp channel", channel_id.as_uuid().to_string())
            })?;
        let listed: Vec<GatewayTemplate> = self.gateway.list_templates(&channel).await?;
        let mut recorded = Vec::with_capacity(listed.len());
        for item in listed {
            recorded.push(
                self.store
                    .upsert_template(&WhatsAppTemplate {
                        id: WhatsAppTemplateId::new(),
                        tenant_id,
                        channel_id,
                        name: item.name,
                        language: item.language,
                        status: item.status,
                        body: item.body,
                        created_at: mas_common::timestamps::Timestamp::now(),
                        updated_at: mas_common::timestamps::Timestamp::now(),
                    })
                    .await?,
            );
        }
        Ok(recorded)
    }

    /// Aggregate inbox counters (scoped).
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn stats(&self, tenant_id: TenantId) -> Result<WhatsAppStats> {
        self.store.stats(tenant_id).await
    }

    /// The webhook subscription check (GET). Echoes the challenge only for
    /// the right mode and a verify token that matches, in constant time.
    ///
    /// # Errors
    ///
    /// [`AppError::forbidden`] when the mode or token is wrong;
    /// [`AppError::validation`] when the challenge is missing.
    pub fn verify_subscription(
        &self,
        mode: Option<&str>,
        verify_token: Option<&str>,
        challenge: Option<&str>,
    ) -> Result<String> {
        if mode != Some("subscribe") {
            return Err(AppError::forbidden(
                "webhook subscription mode must be subscribe",
            ));
        }
        let token = verify_token.unwrap_or_default();
        if !constant_time_eq(token.as_bytes(), self.verify_token.expose().as_bytes()) {
            return Err(AppError::forbidden("webhook verify token mismatch"));
        }
        challenge
            .map(str::to_owned)
            .ok_or_else(|| AppError::validation("webhook challenge missing"))
    }

    /// Phase 1 (pre-scope): verify the `X-Hub-Signature-256` header over the
    /// raw body, parse the messages, resolve the owning tenant from the
    /// channel's `phone_number_id`.
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure. An unauthentic delivery is a
    /// [`WhatsAppDelivery::Rejected`], not an error.
    pub async fn receive_inbound(
        &self,
        raw_body: &[u8],
        signature_header: &str,
    ) -> Result<WhatsAppDelivery> {
        if let Err(reason) = self.verify_signature(raw_body, signature_header) {
            return Ok(WhatsAppDelivery::Rejected { reason });
        }
        let parsed = match parse_inbound(raw_body) {
            Ok(parsed) => parsed,
            Err(reason) => {
                return Ok(WhatsAppDelivery::Rejected { reason });
            },
        };
        let Some(owner) = self
            .store
            .resolve_owner_by_phone_number(&parsed.phone_number_id)
            .await?
        else {
            return Ok(WhatsAppDelivery::Rejected {
                reason: "unknown phone_number_id; refusing delivery".to_owned(),
            });
        };
        Ok(WhatsAppDelivery::Resolved(ResolvedInbound {
            owner,
            phone_number_id: parsed.phone_number_id,
            messages: parsed.messages,
            contact_name: parsed.contact_name,
        }))
    }

    /// Phase 2 (inside the resolved tenant's scope): record each message.
    /// A repeat `wamid` is a no-op.
    ///
    /// # Errors
    ///
    /// [`AppError`] on store failure.
    pub async fn apply_inbound(&self, resolved: &ResolvedInbound) -> Result<Vec<WhatsAppMessage>> {
        let (tenant, _organization) = resolved.owner;
        // The channel row (platform truth) binds the message to the right
        // channel id; the pre-scope phase carried only the provider handle.
        let channel = self
            .store
            .get_channel_by_phone_number(tenant, &resolved.phone_number_id)
            .await?
            .ok_or_else(|| {
                AppError::not_found("whatsapp channel", resolved.phone_number_id.clone())
            })?;
        let mut recorded = Vec::with_capacity(resolved.messages.len());
        for message in &resolved.messages {
            let occurred_at =
                mas_common::timestamps::Timestamp::from_unix_seconds(message.timestamp_unix)
                    .map_err(|error| {
                        AppError::validation(format!("inbound timestamp rejected: {error}"))
                    })?;
            recorded.push(
                self.store
                    .record_message(&WhatsAppMessage {
                        id: WhatsAppMessageId::new(),
                        tenant_id: tenant,
                        channel_id: channel.id,
                        direction: WhatsAppDirection::Inbound,
                        wamid: Some(message.wamid.clone()),
                        from_addr: Some(message.from.clone()),
                        to_addr: Some(resolved.phone_number_id.clone()),
                        message_type: message.kind.parse().unwrap_or(WhatsAppMessageType::Unknown),
                        body_text: message.body.clone(),
                        status: WhatsAppMessageStatus::Received,
                        payload: message.raw.clone(),
                        occurred_at,
                        created_at: mas_common::timestamps::Timestamp::now(),
                        updated_at: mas_common::timestamps::Timestamp::now(),
                    })
                    .await?,
            );
            let opt_out = message.body.as_deref().and_then(|body| {
                match body.trim().to_ascii_uppercase().as_str() {
                    "STOP" => Some(true),
                    "START" => Some(false),
                    _ => None,
                }
            });
            self.store
                .upsert_conversation(tenant, channel.id, &message.from, occurred_at, opt_out)
                .await?;
        }
        Ok(recorded)
    }

    /// The `X-Hub-Signature-256` check: `sha256=<hex hmac-sha256>` over the
    /// raw body, compared in constant time.
    fn verify_signature(
        &self,
        raw_body: &[u8],
        signature_header: &str,
    ) -> std::result::Result<(), String> {
        let hex_digest = signature_header
            .strip_prefix("sha256=")
            .unwrap_or(signature_header)
            .trim()
            .to_owned();
        if hex_digest.len() != 64 || !hex_digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err("signature header malformed".to_owned());
        }
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.app_secret.expose().as_bytes())
            .map_err(|error| format!("hmac setup failed: {error}"))?;
        mac.update(raw_body);
        let produced = hex::encode(mac.finalize().into_bytes());
        if constant_time_eq(produced.as_bytes(), hex_digest.as_bytes()) {
            Ok(())
        } else {
            Err("inbound signature verification failed".to_owned())
        }
    }
}

/// What the tolerant parser pulls from one delivery.
struct ParsedInbound {
    phone_number_id: String,
    messages: Vec<InboundMessage>,
    contact_name: Option<String>,
}

/// Walks the webhook JSON, ignoring anything it does not understand.
fn parse_inbound(raw_body: &[u8]) -> std::result::Result<ParsedInbound, String> {
    let value: serde_json::Value = serde_json::from_slice(raw_body)
        .map_err(|error| format!("inbound body not JSON: {error}"))?;
    let mut phone_number_id = None;
    let mut messages = Vec::new();
    let mut contact_name = None;

    for entry in value
        .get("entry")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        for change in entry
            .get("changes")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let Some(change_value) = change.get("value") else {
                continue;
            };
            if phone_number_id.is_none() {
                phone_number_id = change_value
                    .get("metadata")
                    .and_then(|metadata| metadata.get("phone_number_id"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
            }
            if contact_name.is_none() {
                contact_name = change_value
                    .get("contacts")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|contacts| contacts.first())
                    .and_then(|contact| contact.get("profile"))
                    .and_then(|profile| profile.get("name"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
            }
            for message in change_value
                .get("messages")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
            {
                let from = message
                    .get("from")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let wamid = message
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                if from.is_empty() || wamid.is_empty() {
                    continue;
                }
                let kind = message
                    .get("type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned();
                let body = message
                    .get("text")
                    .and_then(|text| text.get("body"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned);
                let timestamp_unix = message
                    .get("timestamp")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or_default();
                messages.push(InboundMessage {
                    from,
                    wamid,
                    kind,
                    body,
                    timestamp_unix,
                    raw: message.clone(),
                });
            }
        }
    }

    let phone_number_id = phone_number_id.ok_or("inbound body carries no phone_number_id")?;
    Ok(ParsedInbound {
        phone_number_id,
        messages,
        contact_name,
    })
}

#[cfg(test)]
mod tests {
    //! Signature gating + parse tolerance, with no I/O.

    use super::*;

    #[derive(Debug, Default)]
    struct FakeStore {
        conversation: std::sync::Mutex<Option<WhatsAppConversation>>,
        channel: std::sync::Mutex<Option<WhatsAppChannel>>,
    }

    #[async_trait]
    impl WhatsAppStorePort for FakeStore {
        async fn create_channel(&self, channel: &WhatsAppChannel) -> Result<WhatsAppChannel> {
            Ok(channel.clone())
        }

        async fn get_channel(
            &self,
            _tenant_id: TenantId,
            _channel_id: WhatsAppChannelId,
        ) -> Result<Option<WhatsAppChannel>> {
            Ok(self.channel.lock().expect("lock").clone())
        }

        async fn list_channels(&self, _tenant_id: TenantId) -> Result<Vec<WhatsAppChannel>> {
            Ok(vec![])
        }

        async fn resolve_owner_by_phone_number(
            &self,
            _phone_number_id: &str,
        ) -> Result<Option<(TenantId, OrganizationId)>> {
            Ok(None)
        }

        async fn record_message(&self, message: &WhatsAppMessage) -> Result<WhatsAppMessage> {
            Ok(message.clone())
        }

        async fn get_channel_by_phone_number(
            &self,
            _tenant_id: TenantId,
            _phone_number_id: &str,
        ) -> Result<Option<WhatsAppChannel>> {
            Ok(None)
        }

        async fn list_messages(
            &self,
            _tenant_id: TenantId,
            _channel_id: WhatsAppChannelId,
            _limit: i64,
        ) -> Result<Vec<WhatsAppMessage>> {
            Ok(vec![])
        }

        async fn get_conversation(
            &self,
            _tenant_id: TenantId,
            _channel_id: WhatsAppChannelId,
            _contact: &str,
        ) -> Result<Vec<WhatsAppMessage>> {
            Ok(vec![])
        }

        async fn update_message_status(
            &self,
            _tenant_id: TenantId,
            _wamid: &str,
            _status: WhatsAppMessageStatus,
        ) -> Result<Option<WhatsAppMessage>> {
            Ok(None)
        }

        async fn upsert_conversation(
            &self,
            tenant_id: TenantId,
            channel_id: WhatsAppChannelId,
            contact: &str,
            inbound_at: mas_common::timestamps::Timestamp,
            opt_out: Option<bool>,
        ) -> Result<WhatsAppConversation> {
            Ok(WhatsAppConversation {
                id: WhatsAppConversationId::new(),
                tenant_id,
                channel_id,
                contact: contact.to_owned(),
                handoff: false,
                opt_out: opt_out.unwrap_or(false),
                window_expires_at: Some(inbound_at),
                last_inbound_at: Some(inbound_at),
                created_at: mas_common::timestamps::Timestamp::now(),
                updated_at: mas_common::timestamps::Timestamp::now(),
            })
        }

        async fn get_conversation_by_id(
            &self,
            _tenant_id: TenantId,
            _conversation_id: WhatsAppConversationId,
        ) -> Result<Option<WhatsAppConversation>> {
            Ok(self.conversation.lock().expect("lock").clone())
        }

        async fn list_conversations(
            &self,
            _tenant_id: TenantId,
            _channel_id: Option<WhatsAppChannelId>,
            _limit: i64,
        ) -> Result<Vec<WhatsAppConversation>> {
            Ok(vec![])
        }

        async fn set_handoff(
            &self,
            _tenant_id: TenantId,
            _conversation_id: WhatsAppConversationId,
            _handoff: bool,
        ) -> Result<Option<WhatsAppConversation>> {
            Ok(None)
        }

        async fn list_templates(
            &self,
            _tenant_id: TenantId,
            _channel_id: WhatsAppChannelId,
        ) -> Result<Vec<WhatsAppTemplate>> {
            Ok(vec![])
        }

        async fn upsert_template(&self, template: &WhatsAppTemplate) -> Result<WhatsAppTemplate> {
            Ok(template.clone())
        }

        async fn stats(&self, _tenant_id: TenantId) -> Result<WhatsAppStats> {
            Ok(WhatsAppStats::default())
        }
    }

    #[derive(Debug, Default)]
    struct FakeGateway;

    #[async_trait]
    impl WhatsAppGatewayPort for FakeGateway {
        async fn send_text(
            &self,
            _channel: &WhatsAppChannel,
            _to: &str,
            _body: &str,
        ) -> Result<GatewaySendReceipt> {
            Ok(GatewaySendReceipt {
                wamid: "wamid.out".to_owned(),
                timestamp_unix: Some(1_700_000_000),
            })
        }

        async fn send_template(
            &self,
            _channel: &WhatsAppChannel,
            _to: &str,
            _name: &str,
            _language: &str,
        ) -> Result<GatewaySendReceipt> {
            Ok(GatewaySendReceipt {
                wamid: "wamid.tpl".to_owned(),
                timestamp_unix: Some(1_700_000_000),
            })
        }

        async fn list_templates(&self, _channel: &WhatsAppChannel) -> Result<Vec<GatewayTemplate>> {
            Ok(vec![])
        }
    }

    fn service() -> WhatsAppService {
        WhatsAppService::new(
            Arc::new(FakeStore::default()),
            Arc::new(FakeGateway),
            SecretString::new("app-secret"),
            SecretString::new("verify-token"),
        )
    }

    #[tokio::test]
    async fn a_bad_inbound_signature_is_rejected_without_touching_storage() {
        let service = service();
        let outcome = service
            .receive_inbound(b"{}", "sha256=00")
            .await
            .expect("no store failure");
        assert!(matches!(outcome, WhatsAppDelivery::Rejected { .. }));
    }

    #[tokio::test]
    async fn a_signed_inbound_delivery_with_an_unknown_channel_is_rejected() {
        let service = service();
        let body = br#"{"entry":[{"changes":[{"value":{
            "metadata":{"phone_number_id":"12345"},
            "messages":[{"from":"15550001","id":"wamid.1","type":"text",
                         "text":{"body":"hello"},"timestamp":"1700000000"}]
        }}]}]}"#;
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;
        let mut mac = HmacSha256::new_from_slice(b"app-secret").expect("key");
        mac.update(body);
        let digest = hex::encode(mac.finalize().into_bytes());
        let outcome = service
            .receive_inbound(body, &format!("sha256={digest}"))
            .await
            .expect("no store failure");
        match outcome {
            WhatsAppDelivery::Rejected { reason } => {
                assert!(reason.contains("unknown phone_number_id"));
            },
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn the_subscription_check_honours_the_verify_token() {
        let service = service();
        assert!(service
            .verify_subscription(Some("subscribe"), Some("verify-token"), Some("c1"))
            .is_ok());
        assert!(service
            .verify_subscription(Some("subscribe"), Some("wrong"), Some("c1"))
            .is_err());
        assert!(service
            .verify_subscription(Some("unsubscribe"), Some("verify-token"), Some("c1"))
            .is_err());
        assert!(service
            .verify_subscription(Some("subscribe"), Some("verify-token"), None)
            .is_err());
    }

    #[tokio::test]
    async fn the_parser_extracts_messages_and_ignores_unknown_shapes() {
        let body = br#"{"entry":[{"changes":[{"value":{
            "metadata":{"phone_number_id":"12345"},
            "contacts":[{"profile":{"name":"Customer"}}],
            "messages":[
                {"from":"15550001","id":"wamid.1","type":"text",
                 "text":{"body":"hello"},"timestamp":"1700000000"},
                {"from":"15550001","id":"wamid.2","type":"sticker"},
                {"type":"text"}
            ]
        }}]}]}"#;
        let parsed = parse_inbound(body).expect("parses");
        assert_eq!(parsed.phone_number_id, "12345");
        assert_eq!(parsed.contact_name.as_deref(), Some("Customer"));
        assert_eq!(
            parsed.messages.len(),
            2,
            "the entry without from/id is dropped"
        );
        assert_eq!(parsed.messages[0].body.as_deref(), Some("hello"));
        assert_eq!(parsed.messages[1].kind, "sticker");
        assert!(
            parse_inbound(b"{}").is_err(),
            "no phone_number_id, no delivery"
        );
    }

    #[tokio::test]
    async fn human_reply_refuses_an_opted_out_contact() {
        let store = FakeStore::default();
        let now = mas_common::timestamps::Timestamp::now();
        let conversation_id = WhatsAppConversationId::new();
        *store.conversation.lock().expect("lock") = Some(WhatsAppConversation {
            id: conversation_id,
            tenant_id: TenantId::nil(),
            channel_id: WhatsAppChannelId::nil(),
            contact: "15550001".to_owned(),
            handoff: false,
            opt_out: true,
            window_expires_at: Some(now),
            last_inbound_at: Some(now),
            created_at: now,
            updated_at: now,
        });
        let service = WhatsAppService::new(
            Arc::new(store),
            Arc::new(FakeGateway),
            SecretString::new("app-secret"),
            SecretString::new("verify-token"),
        );
        let error = service
            .human_reply(TenantId::nil(), conversation_id, Some("hello"), None, None)
            .await
            .expect_err("opt-out must refuse");
        assert_eq!(error.error_code(), "CONFLICT");
    }
}
