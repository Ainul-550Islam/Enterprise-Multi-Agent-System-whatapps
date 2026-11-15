/// Inbound message models and normaliser for WhatsApp Cloud API payloads.
///
/// WhatsApp sends a variety of message kinds. We normalise them into a small
/// set of `MessageKind` variants. Fields that are not relevant to our use
/// cases are dropped; the callee receives only what it can act on.
///
/// All payloads are received via the signed webhook POST (see `webhook_verify`).
/// The raw Meta JSON is normalised here; no schema validation is performed in
/// this crate — the application layer may add validation if needed.
use serde::Deserialize;
use serde_json::Value;
use std::borrow::Cow;

/// The ultimate normalised inbound message that the control plane processes.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct InboundMessage {
    /// WhatsApp-assigned message ID (unique per account)
    pub wamid: String,
    /// WhatsApp-assigned sender ID (wa_id)
    pub wa_id: String,
    /// Message kind — what sort of message this is
    pub kind: MessageKind,
    /// Timestamp when Meta received the message (epoch-ms, optional)
    #[serde(skip_option)]
    pub timestamp: Option<i64>,
    /// Account route identifier (for multi-tenant routing)
    pub route_id: String,
}

/// Enum of supported WhatsApp message kinds.
///
/// Variants cover the full range of message types Meta can send. Unknown
/// types are mapped to `Unsupported` so the control plane never crashes on
/// a new kind; it is simply logged/replayed.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MessageKind {
    /// Text message (plain or quoted)
    Text { body: String, quote: Option<String> },
    /// Image message
    Image { caption: Option<String> },
    /// Audio message (speech-to-text handled by orchestrator)
    Audio { mime_type: String, sha256: String, duration_ms: Option<u32> },
    /// Video message
    Video { caption: Option<String>, mime_type: String, sha256: String, duration_ms: Option<u32> },
    /// Document message
    Document { filename: Option<String>, mime_type: String, sha256: String, size_bytes: u64 },
    /// Location message
    Location { latitude: f64, longitude: f64, name: Option<String>, address: Option<String> },
    /// Interactive message (button, list)
    Interactive { kind: InteractiveKind, body: String, title: Option<String>, description: Option<String> },
    /// Button interactive component
    Button { payload: String, text: String },
    /// List interactive component
    List { title: String, description: Option<String>, sections: Vec<ListSection> },
    /// Reaction (emoji on an existing message)
    Reaction { emoji: String, message_id: String },
    /// Unsupported / unknown type
    Unsupported { raw: Value },
}

/// Interactive sub-kind.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum InteractiveKind {
    Button,
    List,
}

/// A section in a WhatsApp list message.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ListSection {
    /// Row title
    pub title: String,
    /// Row description (optional)
    pub description: Option<String>,
    /// Rows (up to 10)
    pub rows: Vec<ListRow>,
}

/// A row in a WhatsApp list message.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ListRow {
    /// Row title (truncated by Meta to 24 chars)
    pub title: String,
    /// Row description (optional, truncated by Meta to 72 chars)
    pub description: Option<String>,
    /// Confirmation text (shown when user selects this row)
    pub confirmation: Option<String>,
    /// The row's payload value (the `id` sent back when user selects)
    pub id: String,
}

/// Normalise a raw WhatsApp JSON payload into `InboundMessage`.
///
/// The raw payload is the JSON body that Meta POSTs to the webhook URL.
/// This function:
///   - Parses the outer structure
///   - Maps the `entry[changes[value.messages[...]]]` path into our types
///   - Maps unknown message kinds to `MessageKind::Unsupported`
///   - Extracts `wamid`, `wa_id`, `kind`, and `route_id` (from webhook path)
///
/// Returns `Err` if the payload structure is unrecognised.
#[instrument(skip(raw))]
pub fn normalise(raw: &Value, route_id: &str) -> Result<InboundMessage, ChannelError> {
    let entry = raw.get("entry")
        .and_then(|e| e.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| ChannelError::MalformedPayload("missing 'entry'".into()))?;

    let changes = entry.get("changes")
        .and_then(|c| c.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| ChannelError::MalformedPayload("missing 'changes'".into()))?;

    let value = changes.get("value")
        .and_then(|v| v.as_object())
        .ok_or_else(|| ChannelError::MalformedPayload("missing 'value'".into()))?;

    let messages = value.get("messages")
        .and_then(|m| m.as_array())
        .and_then(|a| a.first());

    let message = messages.ok_or_else(|| {
        // It could be a status update, not a message
        ChannelError::UnsupportedMessageType("no message object".into())
    })?;

    let wamid = message.get("id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| ChannelError::MalformedPayload("missing wamid".into()))?;

    let wa_id_obj = message.get("from");
    let wa_id = wa_id_obj
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_default();

    let kind = match message.get("type").and_then(|v| v.as_str()) {
        Some("text") => {
            let body = message.get("text").and_then(|t| t.get("body")).and_then(|b| b.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let quote = message.get("text")
                .and_then(|t| t.get("quote"))
                .and_then(|q| q.as_str())
                .map(|s| s.to_string());
            MessageKind::Text { body, quote }
        }
        Some("image") => {
            let caption = message.get("image").and_then(|i| i.get("caption")).and_then(|c| c.as_str())
                .map(|s| s.to_string());
            let mime_type = message.get("image").and_then(|i| i.get("mime_type")).and_then(|m| m.as_str())
                .map(|s| s.to_string()).unwrap_or_else(|| "image/jpeg".into());
            let sha256 = message.get("image").and_then(|i| i.get("sha256")).and_then(|s| s.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            let duration_ms = None; // images don't have duration
            MessageKind::Image { caption }
        }
        Some("audio") => {
            let mime_type = message.get("audio").and_then(|a| a.get("mime_type")).and_then(|m| m.as_str())
                .map(|s| s.to_string()).unwrap_or_else(|| "audio/ogg".into());
            let sha256 = message.get("audio").and_then(|a| a.get("sha256")).and_then(|s| s.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            let duration_ms = message.get("audio").and_then(|a| a.get("duration_ms")).and_then(|v| v.as_u64())
                .map(|v| v as u32);
            MessageKind::Audio { mime_type, sha256, duration_ms }
        }
        Some("video") => {
            let caption = message.get("video").and_then(|v| v.get("caption")).and_then(|c| c.as_str())
                .map(|s| s.to_string());
            let mime_type = message.get("video").and_then(|v| v.get("mime_type")).and_then(|m| m.as_str())
                .map(|s| s.to_string()).unwrap_or_else(|| "video/mp4".into());
            let sha256 = message.get("video").and_then(|v| v.get("sha256")).and_then(|s| s.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            let duration_ms = message.get("video").and_then(|v| v.get("duration_ms")).and_then(|v| v.as_u64())
                .map(|v| v as u32);
            MessageKind::Video { caption, mime_type, sha256, duration_ms }
        }
        Some("document") => {
            let filename = message.get("document").and_then(|d| d.get("filename")).and_then(|f| f.as_str())
                .map(|s| s.to_string());
            let mime_type = message.get("document").and_then(|d| d.get("mime_type")).and_then(|m| m.as_str())
                .map(|s| s.to_string()).unwrap_or_else(|| "application/octet-stream".into());
            let sha256 = message.get("document").and_then(|d| d.get("sha256")).and_then(|s| s.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            let size_bytes = message.get("document").and_then(|d| d.get("size")).and_then(|v| v.as_u64())
                .map(|v| v as u64).unwrap_or(0);
            MessageKind::Document { filename, mime_type, sha256, size_bytes }
        }
        Some("location") => {
            let latitude = message.get("location").and_then(|l| l.get("latitude")).and_then(|v| v.as_f64())
                .ok_or_else(|| ChannelError::MalformedPayload("missing latitude".into()))?;
            let longitude = message.get("location").and_then(|l| l.get("longitude")).and_then(|v| v.as_f64())
                .ok_or_else(|| ChannelError::MalformedPayload("missing longitude".into()))?;
            let name = message.get("location").and_then(|l| l.get("name")).and_then(|n| n.as_str())
                .map(|s| s.to_string());
            let address = message.get("location").and_then(|l| l.get("address")).and_then(|a| a.as_str())
                .map(|s| s.to_string());
            MessageKind::Location { latitude, longitude, name, address }
        }
        Some("interactive") => {
            let interactive = message.get("interactive").and_then(|i| i.as_object())
                .ok_or_else(|| ChannelError::MalformedPayload("missing interactive object".into()))?;
            let kind = interactive.get("type").and_then(|t| t.as_str())
                .and_then(|t| match t.as_str() {
                    "button" => InteractiveKind::Button,
                    "list" => InteractiveKind::List,
                    other => InteractiveKind::Button, // fallback, will be mapped
                })
                .unwrap_or(InteractiveKind::Button);
            let body = interactive.get("body")
                .and_then(|b| b.get("text"))
                .and_then(|t| t.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default();
            let title = interactive.get("header")
                .and_then(|h| h.get("text"))
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());
            let description = interactive.get("footer")
                .and_then(|f| f.as_str())
                .map(|s| s.to_string());
            let (button, list) = match kind {
                InteractiveKind::Button => {
                    let button_obj = interactive.get("button").and_then(|b| b.as_object())
                        .ok_or_else(|| ChannelError::MalformedPayload("missing button object".into()))?;
                    let payload = button_obj.get("payload").and_then(|p| p.as_str()).map(|s| s.to_string()).unwrap_or_default();
                    let btn_text = button_obj.get("text").and_then(|t| t.as_str()).map(|s| s.to_string()).unwrap_or_default();
                    (MessageKind::Button { payload, text: btn_text }, None)
                }
                InteractiveKind::List => {
                    let list_obj = interactive.get("list").and_then(|l| l.as_object())
                        .ok_or_else(|| ChannelError::MalformedPayload("missing list object".into()))?;
                    let title = list_obj.get("title").and_then(|t| t.as_str()).map(|s| s.to_string()).unwrap_or_default();
                    let description = list_obj.get("description").and_then(|d| d.as_str()).map(|s| s.to_string());
                    let sections = list_obj.get("sections").and_then(|s| s.as_array())
                        .map(|arr| arr.iter().filter_map(|r| r.as_object()).collect())
                        .unwrap_or_default();
                    let rows = sections.iter().map(|s| ListRow {
                        title: s.get("title").and_then(|t| t.as_str()).map(|t| t.to_string()).unwrap_or_default(),
                        description: s.get("description").and_then(|d| d.as_str()).map(|d| d.to_string()),
                        confirmation: s.get("confirmation").and_then(|c| c.as_str()).map(|c| c.to_string()),
                        id: s.get("id").and_then(|i| i.as_str()).map(|i| i.to_string()).unwrap_or_default(),
                    }).collect();
                    (None, MessageKind::List { title, description, sections, rows })
                }
            };
            // We need to combine; for now emit Interactive as a wrapper.
            // The caller can pattern-match on the sub-kind.
            MessageKind::Interactive { kind, body, title, description }
        }
        Some("reaction") => {
            let emoji = message.get("reaction").and_then(|r| r.get("emoji")).and_then(|e| e.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            let msg_id = message.get("reaction").and_then(|r| r.get("message_id")).and_then(|m| m.as_str())
                .map(|s| s.to_string()).unwrap_or_default();
            MessageKind::Reaction { emoji, message_id }
        }
        Some(other) => MessageKind::Unsupported { raw: message.clone() },
        None => MessageKind::Unsupported { raw: message.clone() },
    };

    Ok(InboundMessage {
        wamid,
        wa_id,
        kind,
        timestamp: None, // we'll extract if needed from entry timing
        route_id: route_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_normalisation() {
        let raw = serde_json::json!({
            "entry": [{"changes": [{"value": {"messages": [{"id": "wamid-123", "from": "15551234567", "type": "text", "text": {"body": "Hello", "quote": "Hi"}}]}]}]}
        });
        let msg = normalise(&raw, "route-whatsapp-1").unwrap();
        assert_eq!(msg.wamid, "wamid-123");
        assert_eq!(msg.wa_id, "15551234567");
        assert!(matches!(msg.kind, MessageKind::Text { body: "Hello", quote: Some(q) if q == "Hi" }));
    }

    #[test]
    fn test_unsupported_fallback() {
        let raw = serde_json::json!({
            "entry": [{"changes": [{"value": {"messages": [{"id": "wamid-999", "from": "15551234567", "type": "unknown_type"}]]}]}]
        });
        let msg = normalise(&raw, "route-whatsapp-1").unwrap();
        assert!(matches!(msg.kind, MessageKind::Unsupported { .. }));
    }
}