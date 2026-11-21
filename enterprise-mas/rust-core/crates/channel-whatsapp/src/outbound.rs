//! Outbound payload construction — the single place the Graph API send body
//! shape is defined.
//!
//! Building the JSON here (instead of at call sites) keeps three invariants
//! enforceable in one place:
//!
//! * the `messaging_product: "whatsapp"` + recipient shape is always correct;
//! * a template send always carries name + language + components together;
//! * payloads serialize through `serde`, so a shape change breaks compile
//!   time tests rather than production sends.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde_json::{json, Value};

/// An outbound send, before serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutboundPayload {
    /// Free-form text send.
    Text {
        /// Recipient phone number (international format).
        to: String,
        /// Message body (1..=4096 chars, WhatsApp limit).
        body: String,
    },
    /// Approved-template send.
    Template {
        /// Recipient phone number (international format).
        to: String,
        /// Template name registered with WhatsApp.
        name: String,
        /// BCP-47 language code the template is approved for.
        language: String,
        /// Rendered body parameters, in template placeholder order.
        parameters: Vec<String>,
    },
    /// Interactive reply buttons.
    InteractiveButtons {
        /// Recipient phone number.
        to: String,
        /// Body text above the buttons.
        body: String,
        /// Up to three button titles.
        buttons: Vec<(String, String)>,
    },
    /// Interactive list.
    InteractiveList {
        /// Recipient phone number.
        to: String,
        /// Body text.
        body: String,
        /// Button label that opens the list.
        button: String,
        /// Section title.
        section_title: String,
        /// Row id + title pairs.
        rows: Vec<(String, String)>,
    },
    /// Media send (`image`/`audio`/`document`/`video`) by Graph media id.
    Media {
        /// Recipient phone number.
        to: String,
        /// Graph media type.
        kind: String,
        /// Graph media id.
        media_id: String,
        /// Optional caption.
        caption: Option<String>,
    },
    /// Mark an inbound message as read.
    MarkAsRead {
        /// The inbound `wamid`.
        message_id: String,
    },
}

impl OutboundPayload {
    /// Serializes to the Graph API `messages` body.
    pub fn to_graph_json(&self) -> Result<Value> {
        match self {
            OutboundPayload::Text { to, body } => {
                if body.is_empty() || body.len() > 4096 {
                    return Err(AppError::Validation {
                        message: "text body must be 1..=4096 chars".into(),
                        issues: Vec::new(),
                    });
                }
                Ok(json!({
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to,
                    "type": "text",
                    "text": { "preview_url": false, "body": body },
                }))
            },
            OutboundPayload::Template {
                to,
                name,
                language,
                parameters,
            } => {
                if name.is_empty() || language.is_empty() {
                    return Err(AppError::Validation {
                        message: "template name and language are required".into(),
                        issues: Vec::new(),
                    });
                }
                let mut components = Vec::new();
                if !parameters.is_empty() {
                    components.push(json!({
                        "type": "body",
                        "parameters": parameters
                            .iter()
                            .map(|p| json!({ "type": "text", "text": p }))
                            .collect::<Vec<_>>(),
                    }));
                }
                Ok(json!({
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to,
                    "type": "template",
                    "template": {
                        "name": name,
                        "language": { "code": language },
                        "components": components,
                    },
                }))
            },
            OutboundPayload::InteractiveButtons { to, body, buttons } => {
                if buttons.is_empty() || buttons.len() > 3 {
                    return Err(AppError::Validation {
                        message: "interactive buttons must be 1..=3".into(),
                        issues: Vec::new(),
                    });
                }
                let button_rows: Vec<Value> = buttons
                    .iter()
                    .map(|(id, title)| {
                        json!({
                            "type": "reply",
                            "reply": { "id": id, "title": title },
                        })
                    })
                    .collect();
                Ok(json!({
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to,
                    "type": "interactive",
                    "interactive": {
                        "type": "button",
                        "body": { "text": body },
                        "action": { "buttons": button_rows },
                    },
                }))
            },
            OutboundPayload::InteractiveList {
                to,
                body,
                button,
                section_title,
                rows,
            } => {
                if rows.is_empty() || rows.len() > 10 {
                    return Err(AppError::Validation {
                        message: "interactive list rows must be 1..=10".into(),
                        issues: Vec::new(),
                    });
                }
                let list_rows: Vec<Value> = rows
                    .iter()
                    .map(|(id, title)| json!({ "id": id, "title": title }))
                    .collect();
                Ok(json!({
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to,
                    "type": "interactive",
                    "interactive": {
                        "type": "list",
                        "body": { "text": body },
                        "action": {
                            "button": button,
                            "sections": [{
                                "title": section_title,
                                "rows": list_rows,
                            }],
                        },
                    },
                }))
            },
            OutboundPayload::Media {
                to,
                kind,
                media_id,
                caption,
            } => {
                if !matches!(kind.as_str(), "image" | "audio" | "video" | "document") {
                    return Err(AppError::Validation {
                        message: "media kind must be image|audio|video|document".into(),
                        issues: Vec::new(),
                    });
                }
                let mut media = json!({ "id": media_id });
                if let Some(caption) = caption {
                    if kind == "image" || kind == "document" {
                        media["caption"] = json!(caption);
                    }
                }
                let mut body = json!({
                    "messaging_product": "whatsapp",
                    "recipient_type": "individual",
                    "to": to,
                    "type": kind,
                });
                body[kind] = media;
                Ok(body)
            },
            OutboundPayload::MarkAsRead { message_id } => {
                if message_id.is_empty() {
                    return Err(AppError::Validation {
                        message: "mark-as-read requires a message id".into(),
                        issues: Vec::new(),
                    });
                }
                Ok(json!({
                    "messaging_product": "whatsapp",
                    "status": "read",
                    "message_id": message_id,
                }))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_text() {
        let p = OutboundPayload::Text {
            to: "15551234567".into(),
            body: String::new(),
        };
        assert!(p.to_graph_json().is_err());
    }
}
