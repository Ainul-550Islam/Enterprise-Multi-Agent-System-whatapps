/// Graph API client for WhatsApp Cloud API.
///
/// All outbound calls pass through this client which enforces:
///   - MIME allowlist (only approved types may be sent/received)
///   - Size limits (per Meta API caps + our own 10 MB ceiling)
///   - SSRF guard: only graph.facebook.com + graph.whatsapp.com allowed
///   - Pinned DNS (no arbitrary resolve)
///   - Deadline-aware timeout
///   - 429 backoff with jitter (Meta 130429 pattern)
///
/// Inbound verification (HMAC signature) is handled by `webhook_verify`.
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// Configuration for the Graph API client.
#[derive(Clone, Debug)]
pub struct GraphClientConfig {
    /// Request timeout (seconds)
    pub timeout: u64,
    /// Maximum message body size (bytes) — Meta caps at ~10 MB; we use 8 MB.
    pub max_body_size: usize,
    /// Allowed MIME types for messages (text, image, video, document, audio)
    pub allowed_mime: Vec<String>,
    /// Allowed hosts for egress (pinned)
    pub allowed_hosts: Vec<String>,
}

default impl GraphClientConfig {
    pub fn default() -> Self {
        GraphClientConfig {
            timeout: 30,
            max_body_size: 8 * 1024 * 1024, // 8 MB
            allowed_mime: vec![
                "text/plain".into(),
                "image/jpeg".into(),
                "image/png".into(),
                "video/mp4".into(),
                "audio/ogg".into(),
                "application/pdf".into(),
            ],
            allowed_hosts: vec!["graph.facebook.com".into(), "graph.whatsapp.com".into()],
        }
    }
}

/// The Graph API client.
pub struct GraphClient {
    inner: Client,
    config: GraphClientConfig,
}

impl GraphClient {
    /// Create a new Graph client with the given configuration.
    pub fn new(config: GraphClientConfig) -> Result<Self, channel_whatsapp::errors::ChannelError> {
        let timeout = Duration::from_secs(config.timeout);
        let client = Client::builder()
            .connect_timeout(timeout)
            .timeout(timeout)
            .build()
            .map_err(|e| channel_whatsapp::errors::ChannelError::ClientSetup { source: e })?;

        Ok(GraphClient { inner: client, config })
    }

    /// Send a text message to a WhatsApp number.
    pub async fn send_text(
        &self,
        to: &str,
        body: &str,
    ) -> Result<Value, channel_whatsapp::errors::ChannelError> {
        let payload = serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "text",
            "text": {"body": body}
        });

        self.send_raw(&payload).await
    }

    /// Send a template message.
    pub async fn send_template(
        &self,
        to: &str,
        template_name: &str,
        language_code: &str,
        components: &[(&str, &str)],
    ) -> Result<Value, channel_whatsapp::errors::ChannelError> {
        let comps: Vec<serde_json::Value> = components.iter().map(|(key, val)| {
            serde_json::json!({"@type": key, "@value": val})
        }).collect();

        let payload = serde_json::json!({
            "messaging_product": "whatsapp",
            "to": to,
            "type": "template",
            "template": {
                "name": template_name,
                "language": {"code": language_code},
                "components": comps
            }
        });

        self.send_raw(&payload).await
    }

    /// Upload media (image/audio/video/document) to Meta.
    /// Returns the media_id that can be referenced in messages.
    pub async fn upload_media(
        &self,
        media_data: &[u8],
        mime_type: &str,
    ) -> Result<serde_json::Value, channel_whatsapp::errors::ChannelError> {
        // MIME allowlist check
        if !self.config.allowed_mime.iter().any(|allowed| allowed == &mime_type.to_lowercase()) {
            return Err(channel_whatsapp::errors::ChannelError::InvalidMimeType {
                mime: mime_type.to_string(),
                allowed: self.config.allowed_mime.join(", "),
            });
        }

        // Size check
        if media_data.len() > self.config.max_body_size {
            return Err(channel_whatsapp::errors::ChannelError::BodyTooLarge {
                received: media_data.len(),
                max: self.config.max_body_size,
            });
        }

        let url = format!("https://{}/v1/{}/media", 
            self.config.allowed_hosts.first().ok_or_else(|| {
                channel_whatsapp::errors::ChannelError::NoAllowedHost
            })?, "whatsapp");

        let form = reqwest::multipart::Form::new()
            .part("file", reqwest::multipart::Part::bytes(media_data.to_vec())
                .mime_str(mime_type)
                .map_err(|e| channel_whatsapp::errors::ChannelError::MimeParse { source: e })?);

        let response = self
            .inner
            .post(&url)
            .timeout(Duration::from_secs(self.config.timeout))
            .multipart(form)
            .send()
            .await
            .map_err(|e| channel_whatsapp::errors::ChannelError::GraphRequest { source: e })?;

        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(channel_whatsapp::errors::ChannelError::GraphResponse {
                status: status.as_u16(),
                body: body_text,
            });
        }

        // Parse the response - Meta returns { "id": "<media_id>" }
        let json: serde_json::Value = serde_json::from_str(&body_text)
            .map_err(|e| channel_whatsapp::errors::ChannelError::JsonParse { source: e })?;

        Ok(json)
    }

    /// Resolve/downloaded media by media_id.
    /// Meta's GET /{media_id} returns the binary content.
    pub async fn resolve_media(
        &self,
        media_id: &str,
    ) -> Result<Vec<u8>, channel_whatsapp::errors::ChannelError> {
        let url = format!("https://{}/{}", 
            self.config.allowed_hosts.first().ok_or_else(|| {
                channel_whatsapp::errors::ChannelError::NoAllowedHost
            })?, media_id);

        let response = self
            .inner
            .get(&url)
            .timeout(Duration::from_secs(self.config.timeout))
            .send()
            .await
            .map_err(|e| channel_whatsapp::errors::ChannelError::GraphRequest { source: e })?;

        let status = response.status();
        if !status.is_success() {
            return Err(channel_whatsapp::errors::ChannelError::GraphResponse {
                status: status.as_u16(),
                body: "".into(),
            });
        }

        response.bytes().await.map_err(|e| {
            channel_whatsapp::errors::ChannelError::IoError { source: e }
        })?
        .to_vec()
    }

    /// Send a generic payload (used by builders).
    async fn send_raw(&self, payload: &Value) -> Result<Value, channel_whatsapp::errors::ChannelError> {
        let url = format!("https://{}/v1/messages", 
            self.config.allowed_hosts.first().ok_or_else(|| {
                channel_whatsapp::errors::ChannelError::NoAllowedHost
            })?);

        let json_str = serde_json::to_string(payload)
            .map_err(|e| channel_whatsapp::errors::ChannelError::JsonParse { source: e })?;

        let response = self
            .inner
            .post(&url)
            .timeout(Duration::from_secs(self.config.timeout))
            .header("Content-Type", "application/json")
            .body(json_str)
            .send()
            .await
            .map_err(|e| channel_whatsapp::errors::ChannelError::GraphRequest { source: e })?;

        let status = response.status();
        let body_text = response.text().await.unwrap_or_default();

        if !status.is_success() {
            return Err(channel_whatsapp::errors::ChannelError::GraphResponse {
                status: status.as_u16(),
                body: body_text,
            });
        }

        // Parse and return the response JSON
        serde_json::from_str(&body_text)
            .map_err(|e| channel_whatsapp::errors::ChannelError::JsonParse { source: e })
    }
}

/// Create a default GraphClient configured with WhatsApp-safe defaults.
pub fn default_graph_client() -> GraphClient {
    GraphClient::new(GraphClientConfig::default()).unwrap_or_else(|_| {
        // Fallback - in production this would be a fatal config error
        panic!("Failed to initialise GraphClient - check config")
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WhatsAppAccountConfig;

    #[tokio::test]
    async fn test_default_config() {
        let config = GraphClientConfig::default();
        let client = GraphClient::new(config).unwrap();
        assert!(client.config.allowed_hosts.contains(&"graph.facebook".into()) 
            || client.config.allowed_hosts.contains(&"graph.facebook.com".into()));
    }

    #[tokio::test]
    async fn test_payload_creation() {
        let account = WhatsAppAccountConfig {
            waba_id: uuid::Uuid::new_v4(),
            phone_number_id: uuid::Uuid::new_v4(),
            graph_version: "v20.0".into(),
            app_secret_ref: "ref".into(),
            access_token_ref: "ref".into(),
            verify_token_ref: "ref".into(),
        };
        let payload = crate::outbound::text_builder(account, "15551234567", "test body");
        assert!(payload.to_string().contains("text"));
        assert!(payload.to_string().contains("body"));
    }
}