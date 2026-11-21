//! Inbound webhook parsing: the Meta WhatsApp Cloud API envelope → typed
//! messages and status updates.
//!
//! The envelope is hostile input. Parsing is therefore:
//!
//! * **shape-tolerant** — unknown fields are ignored (Meta adds fields
//!   without notice);
//! * **shape-strict on the spine** — an envelope without
//!   `entry[].changes[].value` is a `Serialization` error, not a silent
//!   empty result, so a format change surfaces loudly;
//! * **lossless on identity** — `wamid` ids and raw timestamps survive
//!   verbatim, because dedup and audit keys depend on them.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde_json::Value;

use crate::errors::GraphError;

/// A single inbound message, normalized from the webhook envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboundMessage {
    /// WhatsApp message id (`wamid.…`) — the idempotency key upstream.
    pub wamid: String,
    /// Sender phone number in international format.
    pub from: String,
    /// Provider timestamp (unix seconds, as sent).
    pub timestamp: String,
    /// What the message carries.
    pub kind: InboundKind,
}

/// Message payload kinds the channel handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboundKind {
    /// `type: text` — plain text body.
    Text { body: String },
    /// `type: audio|image|video|document` — media reference (id + mime).
    Media { media_id: String, mime_type: String },
    /// `type: location`.
    Location {
        latitude: String,
        longitude: String,
        name: String,
        address: String,
    },
    /// `type: reaction`.
    Reaction { emoji: String, message_id: String },
    /// `type: button` or `interactive.button_reply`.
    Button { payload: String, text: String },
    /// `interactive.list_reply`.
    ListReply { payload: String, title: String },
    /// Declared `unsupported` type.
    Unsupported { raw_type: String },
    /// Any other type: kept visible instead of silently dropped.
    Other { raw_type: String },
}

/// Delivery status values Meta emits on `statuses[]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// Accepted by Graph.
    Sent,
    /// Delivered to the device.
    Delivered,
    /// Read by the recipient.
    Read,
    /// Failed; see [`StatusUpdate::errors`].
    Failed,
    /// A future value Meta has not documented here.
    Other(String),
}

impl DeliveryStatus {
    /// Parses a raw status string.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw {
            "sent" => Self::Sent,
            "delivered" => Self::Delivered,
            "read" => Self::Read,
            "failed" => Self::Failed,
            other => Self::Other(other.to_owned()),
        }
    }

    /// Wire value.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Sent => "sent",
            Self::Delivered => "delivered",
            Self::Read => "read",
            Self::Failed => "failed",
            Self::Other(raw) => raw,
        }
    }
}

/// A delivery/status update keyed by `wamid`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusUpdate {
    /// The `wamid` this status belongs to.
    pub wamid: String,
    /// `sent` | `delivered` | `read` | `failed` (or future values).
    pub status: DeliveryStatus,
    /// Provider timestamp (unix seconds, as sent).
    pub timestamp: String,
    /// Meta error objects, populated on `failed`.
    pub errors: Vec<GraphError>,
}

/// Everything parsed out of one webhook POST.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InboundBatch {
    /// Messages in envelope order.
    pub messages: Vec<InboundMessage>,
    /// Status updates in envelope order.
    pub statuses: Vec<StatusUpdate>,
}

impl InboundBatch {
    /// True when the envelope carried nothing actionable.
    pub fn is_empty(&self) -> bool {
        self.messages.is_empty() && self.statuses.is_empty()
    }
}

/// Parses one webhook envelope (already signature-verified).
pub fn parse_envelope(body: &[u8]) -> Result<InboundBatch> {
    let root: Value = serde_json::from_slice(body)
        .map_err(|e| AppError::Serialization(format!("whatsapp envelope: {e}")))?;
    let entries = root
        .get("entry")
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::Serialization("whatsapp envelope missing entry[]".into()))?;

    let mut batch = InboundBatch::default();
    for entry in entries {
        let changes = entry
            .get("changes")
            .and_then(Value::as_array)
            .ok_or_else(|| AppError::Serialization("whatsapp entry missing changes[]".into()))?;
        for change in changes {
            let value = change
                .get("value")
                .ok_or_else(|| AppError::Serialization("whatsapp change missing value".into()))?;
            if let Some(msgs) = value.get("messages").and_then(Value::as_array) {
                for m in msgs {
                    batch.messages.push(parse_message(m)?);
                }
            }
            if let Some(statuses) = value.get("statuses").and_then(Value::as_array) {
                for s in statuses {
                    batch.statuses.push(parse_status(s)?);
                }
            }
        }
    }
    Ok(batch)
}

fn req_str(v: &Value, key: &str) -> Result<String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| AppError::Serialization(format!("whatsapp message missing {key}")))
}

fn parse_message(m: &Value) -> Result<InboundMessage> {
    let wamid = req_str(m, "id")?;
    let from = req_str(m, "from")?;
    let timestamp = req_str(m, "timestamp")?;
    let raw_type = m
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let kind = match raw_type.as_str() {
        "text" => InboundKind::Text {
            body: m
                .get("text")
                .and_then(|t| t.get("body"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        },
        "audio" | "image" | "video" | "document" => {
            let media = m.get(&raw_type).cloned().unwrap_or(Value::Null);
            InboundKind::Media {
                media_id: req_str(&media, "id")?,
                mime_type: media
                    .get("mime_type")
                    .and_then(Value::as_str)
                    .unwrap_or("application/octet-stream")
                    .to_owned(),
            }
        },
        "button" => {
            let button = m.get("button").cloned().unwrap_or(Value::Null);
            InboundKind::Button {
                payload: req_str(&button, "payload")?,
                text: button
                    .get("text")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            }
        },
        "interactive" => {
            let interactive = m.get("interactive").cloned().unwrap_or(Value::Null);
            if let Some(reply) = interactive.get("button_reply") {
                InboundKind::Button {
                    payload: req_str(reply, "id")?,
                    text: reply
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                }
            } else if let Some(reply) = interactive.get("list_reply") {
                InboundKind::ListReply {
                    payload: req_str(reply, "id")?,
                    title: reply
                        .get("title")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                }
            } else {
                InboundKind::Other {
                    raw_type: "interactive".to_owned(),
                }
            }
        },
        "location" => {
            let loc = m.get("location").cloned().unwrap_or(Value::Null);
            InboundKind::Location {
                latitude: json_coord(&loc, "latitude"),
                longitude: json_coord(&loc, "longitude"),
                name: loc
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                address: loc
                    .get("address")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            }
        },
        "reaction" => {
            let reaction = m.get("reaction").cloned().unwrap_or(Value::Null);
            InboundKind::Reaction {
                emoji: reaction
                    .get("emoji")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                message_id: reaction
                    .get("message_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            }
        },
        "unsupported" => InboundKind::Unsupported {
            raw_type: "unsupported".to_owned(),
        },
        other => InboundKind::Other {
            raw_type: other.to_owned(),
        },
    };
    Ok(InboundMessage {
        wamid,
        from,
        timestamp,
        kind,
    })
}

fn json_coord(v: &Value, key: &str) -> String {
    match v.get(key) {
        Some(Value::Number(n)) => n.to_string(),
        Some(Value::String(s)) => s.clone(),
        _ => String::new(),
    }
}

fn parse_status(s: &Value) -> Result<StatusUpdate> {
    let errors = s
        .get("errors")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(GraphError::from_value).collect())
        .unwrap_or_default();
    Ok(StatusUpdate {
        wamid: req_str(s, "id")?,
        status: DeliveryStatus::parse(&req_str(s, "status")?),
        timestamp: req_str(s, "timestamp")?,
        errors,
    })
}
