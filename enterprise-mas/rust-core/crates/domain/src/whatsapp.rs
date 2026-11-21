//! PURPOSE: The WhatsApp channel aggregates -- tenant-owned channel
//! configuration and the message log.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: describe what a connected WhatsApp Business
//! channel *is* (its provider handles, Graph API version, status, and the
//! secret *reference* of its webhook verify token) and record every message
//! that entered or left it. The provider wire format lives in the integration
//! layer; these aggregates carry only opaque handles.
//!
//! Data ownership: Rust-owned platform state; PostgreSQL is authoritative.
//!
//! It must NOT:
//!   * hold a WhatsApp access token or verify token -- `verify_token_ref` is a
//!     reference into the secret store, never the secret;
//!   * store media bytes -- media is referenced by provider id inside `payload`;
//!   * treat `payload` as anything but untrusted evidence.

use mas_common::ids::{
    TenantId, WhatsAppChannelId, WhatsAppConversationId, WhatsAppMessageId, WhatsAppTemplateId,
};
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;

string_enum! {
    /// Lifecycle of a connected channel.
    WhatsAppChannelStatus {
        /// Connected and serving.
        Active => "active",
        /// Connected but deliberately not serving.
        Paused => "paused",
        /// Removed at the provider or revoked.
        Disconnected => "disconnected",
    }
}

string_enum! {
    /// Which side of the channel a message is on.
    WhatsAppDirection {
        /// Provider -> platform.
        Inbound => "inbound",
        /// Platform -> provider.
        Outbound => "outbound",
    }
}

string_enum! {
    /// The declared content type of a message.
    WhatsAppMessageType {
        Text => "text",
        Image => "image",
        Audio => "audio",
        Video => "video",
        Document => "document",
        Template => "template",
        Interactive => "interactive",
        Button => "button",
        Reaction => "reaction",
        Location => "location",
        /// A type this build does not model. Preserved, never guessed at.
        Unknown => "unknown",
    }
}

string_enum! {
    /// Delivery lifecycle of a message row.
    WhatsAppMessageStatus {
        /// Inbound message received.
        Received => "received",
        /// Outbound accepted by the provider.
        Sent => "sent",
        /// Delivered to the device.
        Delivered => "delivered",
        /// Read by the recipient.
        Read => "read",
        /// The provider rejected or failed the message.
        Failed => "failed",
    }
}

/// One connected WhatsApp Business channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhatsAppChannel {
    /// Row id.
    pub id: WhatsAppChannelId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Operator-facing name.
    pub display_name: String,
    /// Provider phone-number handle. Globally unique: an inbound webhook
    /// names it, and it must resolve to exactly one owning tenant.
    pub phone_number_id: String,
    /// WhatsApp Business Account handle, when known.
    pub waba_id: Option<String>,
    /// Graph API version to drive this channel with.
    pub api_version: String,
    /// Lifecycle status.
    pub status: WhatsAppChannelStatus,
    /// Secret reference for the webhook verify token.
    pub verify_token_ref: String,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// One message that entered or left a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhatsAppMessage {
    /// Row id.
    pub id: WhatsAppMessageId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// The channel this message moved on.
    pub channel_id: WhatsAppChannelId,
    /// Which side the message is on.
    pub direction: WhatsAppDirection,
    /// The provider message id (`wamid`), when the provider assigned one.
    pub wamid: Option<String>,
    /// Sending address (E.164 or provider id), when known.
    pub from_addr: Option<String>,
    /// Receiving address, when known.
    pub to_addr: Option<String>,
    /// Declared content type.
    pub message_type: WhatsAppMessageType,
    /// Extracted text body, when the message carried one.
    pub body_text: Option<String>,
    /// Delivery lifecycle status.
    pub status: WhatsAppMessageStatus,
    /// The untrusted raw provider payload, preserved for evidence only.
    pub payload: serde_json::Value,
    /// When the provider says the message happened.
    pub occurred_at: Timestamp,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// One inbox conversation: a contact on a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhatsAppConversation {
    /// Row id.
    pub id: WhatsAppConversationId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Channel.
    pub channel_id: WhatsAppChannelId,
    /// Contact phone (E.164 as the provider reports it).
    pub contact: String,
    /// When true, outbound automation must not reply; a human owns the thread.
    pub handoff: bool,
    /// When true, the contact sent STOP; outbound is refused.
    pub opt_out: bool,
    /// End of the 24-hour customer-care window, when known.
    pub window_expires_at: Option<Timestamp>,
    /// Last inbound timestamp, when known.
    pub last_inbound_at: Option<Timestamp>,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// One message template registered for a channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WhatsAppTemplate {
    /// Row id.
    pub id: WhatsAppTemplateId,
    /// Owning tenant.
    pub tenant_id: TenantId,
    /// Channel.
    pub channel_id: WhatsAppChannelId,
    /// Provider template name.
    pub name: String,
    /// BCP-47 language the template is approved for.
    pub language: String,
    /// `draft` | `pending` | `approved` | `rejected`.
    pub status: String,
    /// Optional body preview.
    pub body: Option<String>,
    /// Creation time.
    pub created_at: Timestamp,
    /// Last update time.
    pub updated_at: Timestamp,
}

/// Aggregate inbox counters for one tenant.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WhatsAppStats {
    /// Connected channels.
    pub channels: i64,
    /// Open conversations.
    pub conversations: i64,
    /// Inbound messages recorded.
    pub inbound: i64,
    /// Outbound messages recorded.
    pub outbound: i64,
    /// Failed outbound messages.
    pub failed: i64,
}
