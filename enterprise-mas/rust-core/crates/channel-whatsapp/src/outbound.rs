/// Outbound message builders for WhatsApp Cloud API.
///
/// Each builder produces a JSON payload ready for the Graph API `messages`
/// endpoint.  The caller is responsible for routing through the guarded
/// `GraphClient` (see `client.rs`) which applies mime/size allowlists and
/// SSRF guards.
use serde::Serialize;
use crate::config::WhatsAppAccountConfig;

/// Builder for outbound WhatsApp messages.
/// All methods return a `serde_json::Value` that can be serialised and
/// sent via the Graph API.
#[derive(Clone, Debug)]
pub struct OutboundBuilder {
    /// The WABA/phone-number this message is sent from
    account: WhatsAppAccountConfig,
}

impl OutboundBuilder {
    /// Create a new builder bound to the given account config.
    pub fn new(account: WhatsAppAccountConfig) -> Self {
        OutboundBuilder { account }
    }

    /// Send a simple text message to a WhatsApp number.
    /// The `to` number must be in international format without the leading
    /// `+` (e.g. "15551234567").
    pub fn text(mut self, to: &str, body: &str) -> serde_json::Value {
        serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": body}
        })
    }

    /// Send a template message.
    /// The template must already be approved by Meta. `template_name` and
    /// `language_code` must match the approved template exactly.
    pub fn template(mut self, to: &str, template_name: &str, language_code: &str, components: &[(&str, &str)]) -> serde_json::Value {
        let comps: Vec<serde_json::Value> = components.iter().map(|(key, val)| {
            serde_json::json!({"@type": key, "@value": val})
        }).collect();
        serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
                "components": comps
            }
        })
    }

    /// Send an interactive button message.
    pub fn button(mut self, to: &str, text: &str, button_text: &str, payload: &str) -> serde_json::Value {
        serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "interactive",
            "interactive": {
                "type": "button",
                "button": {"type": "reply", "text": button_text, "payload": payload},
                "action": {"name": "reply", "button_reply": {"text": button_text, "payload": payload}}
            }
        })
    }

    /// Send an interactive list message.
    pub fn list(mut self, to: &str, title: &str, description: Option<&str>, sections: &[(&str, &str, &str)]) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = sections.iter().map(|(t, d, c)| {
            serde_json::json!({
                "title": t,
                "description": d,
                "confirmation": c
            })
        }).collect();
        serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "interactive",
            "interactive": {
                "type": "list",
                "template": {
                    "headers": [{"type": "text", "text": title}],
                    "body": {"text": description.unwrap_or_default()},
                    "sections": {"rows": rows}
                }
            }
        })
    }

    /// Send an image message.
    pub fn image(mut self, to: &str, caption: Option<&str>) -> serde_json::Value {
        let obj: serde_json::Value = serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "image",
            "image": {"caption": caption.unwrap_or_default()}
        });
        obj
    }

    /// Send a document message.
    pub fn document(mut self, to: &str, filename: &str, mime_type: &str) -> serde_json::Value {
        serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "document",
            "document": {"filename": filename, "mime_type": mime_type}
        })
    }

    /// Send a status update (mark-as-read).
    pub fn mark_as_read(mut self, message_id: &str) -> serde_json::Value {
        serde_json::json!({
            "messaging_product": "whatsapp",
            "status": "read",
            "message_id": message_id
        })
    }
}

/// Convenience constructors
/// ------------

/// Create a text message builder.
pub fn text_builder(account: WhatsAppAccountConfig, to: &str, body: &str) -> serde_json::Value {
    OutboundBuilder::new(account).text(to, body)
}

/// Create a template message builder.
pub fn template_builder(account: WhatsAppAccountConfig, to: &str, template_name: &str, language_code: &str, components: &[(&str, &str)]) -> serde_json::Value {
    OutboundBuilder::new(account).template(to, template_name, language_code, components)
}

/// Create a button builder.
pub fn button_builder(account: WhatsAppAccountConfig, to: &str, text: &str, button_text: &str, payload: &str) -> serde_json::Value {
    OutboundBuilder::new(account).button(to, text, button_text, payload)
}

/// Create a list builder.
pub fn list_builder(account: WhatsAppAccountConfig, to: &str, title: &str, description: Option<&str>, sections: &[(&str, &str, &str)]) -> serde_json::Value {
    OutboundBuilder::new(account).list(to, title, description, sections)
}

/// Create an image message builder.
pub fn image_builder(account: WhatsAppAccountConfig, to: &str, caption: Option<&str>) -> serde_json::Value {
    OutboundBuilder::new(account).image(to, caption)
}

/// Create a document message builder.
pub fn document_builder(account: WhatsAppAccountConfig, to: &str, filename: &str, mime_type: &str) -> serde_json::Value {
    OutboundBuilder::new(account).document(to, filename, mime_type)
}

/// Create a mark-as-read builder.
pub fn mark_as_read_builder(account: WhatsAppAccountConfig, message_id: &str) -> serde_json::Value {
    OutboundBuilder::new(account).mark_as_read(message_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WhatsAppAccountConfig;
    use uuid::Uuid;

    fn make_account() -> WhatsAppAccountConfig {
        WhatsAppAccountConfig {
            waba_id: Uuid::new_v4(),
            phone_number_id: Uuid::new_v4(),
            graph_version: "v20.0".into(),
            app_secret_ref: "ref-app-secret".into(),
            access_token_ref: "ref-access-token".into(),
            verify_token_ref: "ref-verify-token".into(),
        }
    }

    #[test]
    fn test_text_builder() {
        let account = make_account();
        let payload = text_builder(account, "15551234567", "Hello world");
        assert!(payload["to"].as_str() == Some("15551234567"));
        assert!(payload["text"]["body"].as_str() == Some("Hello world"));
    }

    #[test]
    fn test_template_builder() {
        let account = make_account();
        let components &[("body", "Hello")]; // simplified
        let payload = template_builder(account, "15551234567", "template-name", "en", &[("body", "Hello")]);
        assert!(payload["to"].as_str() == Some("15551234567"));
        assert!(payload["template"]["name"].as_str() == Some("template-name"));
    }

    #[test]
    fn test_button_builder() {
        let account = make_account();
        let payload = button_builder(account, "15551234567", "Choose option", "Option A", "payload-a");
        assert!(payload["to"].as_str() == Some("15551234567"));
        assert!(payload["interactive"]["button"]["text"].as_str() == Some("Option A"));
    }

    #[test]
    fn test_list_builder() {
        let account = make_account();
        let payload = list_builder(account, "15551234567", "Select item", Some("Pick one"), &[("One", "Description one", "1")]);
        assert!(payload["to"].as_str() == Some("15551234567"));
    }

    #[test]
    fn test_mark_as_read_builder() {
        let account = make_account();
        let payload = mark_as_read_builder(account, "wamid-abc");
        assert!(payload["message_id"].as_str() == Some("wamid-abc"));
        assert!(payload["status"].as_str() == Some("read"));
    }
}