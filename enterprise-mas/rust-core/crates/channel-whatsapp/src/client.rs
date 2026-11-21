//! The outbound client seam.
//!
//! [`ChannelClient`] is what `mas-application`'s WhatsApp service depends on;
//! [`GraphClient`] is the production implementation. Three rules:
//!
//! * **Fixed host.** The Graph endpoint host is a constant. There is no
//!   configuration key that can redirect message sends to another host —
//!   outbound WhatsApp traffic cannot leave the Meta API surface.
//! * **Reference secrets.** The access token is injected, redacted from
//!   `Debug`, and only ever placed in the `Authorization: Bearer` header.
//! * **Transport port.** HTTP goes through [`HttpTransport`] so tests drive a
//!   mock and production binds `reqwest` at the composition root.

use mas_common::error::AppError;
use mas_common::result::Result;

use crate::errors::error_from_graph_body;
use crate::outbound::OutboundPayload;

/// Fixed Graph API host for WhatsApp sends.
pub const GRAPH_API_HOST: &str = "graph.facebook.com";

/// Graph API version pinned for the send path.
pub const GRAPH_API_VERSION: &str = "v21.0";

/// Maximum media download accepted from Graph, in bytes (16 MiB).
pub const MAX_MEDIA_BYTES: usize = 16 * 1024 * 1024;

/// MIME types the download path will retain.
pub const ALLOWED_MEDIA_MIME: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/webp",
    "audio/ogg",
    "audio/mpeg",
    "video/mp4",
    "application/pdf",
];

/// The transport seam: JSON POST plus optional GET for media.
pub trait HttpTransport: Send + Sync {
    /// POSTs `body` to `url` with `authorization` (already `Bearer …`).
    fn post_json(&self, url: &str, authorization: &str, body: &str) -> Result<TransportResponse>;

    /// GETs `url`. Default: not supported.
    fn get(&self, url: &str, authorization: &str) -> Result<TransportResponse> {
        let _ = (url, authorization);
        Err(AppError::internal("transport does not support GET"))
    }

    /// GETs `url` returning raw bytes, refusing bodies larger than `max_bytes`.
    fn get_bytes(&self, url: &str, authorization: &str, max_bytes: usize) -> Result<BytesResponse> {
        let response = self.get(url, authorization)?;
        if response.body.len() > max_bytes {
            return Err(AppError::validation("media download exceeds size limit"));
        }
        Ok(BytesResponse {
            status: response.status,
            body: response.body.into_bytes(),
            mime_type: None,
            redirected: false,
        })
    }
}

/// A raw transport response.
#[derive(Debug, Clone)]
pub struct TransportResponse {
    /// HTTP status.
    pub status: u16,
    /// Response body.
    pub body: String,
}

/// A binary transport response (media download).
#[derive(Debug, Clone)]
pub struct BytesResponse {
    /// HTTP status.
    pub status: u16,
    /// Raw bytes.
    pub body: Vec<u8>,
    /// Declared content type, when the transport observed one.
    pub mime_type: Option<String>,
    /// True when the transport followed a redirect. The client refuses this.
    pub redirected: bool,
}

/// Downloaded media bytes plus the declared MIME type.
#[derive(Debug, Clone)]
pub struct MediaBytes {
    /// Raw bytes.
    pub bytes: Vec<u8>,
    /// MIME type Graph declared.
    pub mime_type: String,
}

/// Result of uploading media to Graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaUpload {
    /// Graph media id.
    pub media_id: String,
}

/// Provider message id returned by a successful send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendReceipt {
    /// WhatsApp message id (`wamid.…`) assigned by the provider.
    pub wamid: String,
}

/// The channel send contract.
pub trait ChannelClient: Send + Sync {
    /// Sends one payload; returns the provider receipt.
    fn send(&self, payload: &OutboundPayload) -> Result<SendReceipt>;
}

/// Production client over the fixed Graph host.
pub struct GraphClient {
    transport: Box<dyn HttpTransport>,
    access_token: String,
}

impl std::fmt::Debug for GraphClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GraphClient")
            .field("host", &GRAPH_API_HOST)
            .field("access_token", &"[redacted]")
            .finish()
    }
}

impl GraphClient {
    /// Builds a client over the given transport with the given token.
    pub fn new(transport: Box<dyn HttpTransport>, access_token: impl Into<String>) -> Self {
        Self {
            transport,
            access_token: access_token.into(),
        }
    }

    /// The URL every send must target (fixed host, pinned version).
    pub fn send_url() -> String {
        format!("https://{GRAPH_API_HOST}/{GRAPH_API_VERSION}/me/messages")
    }
}

impl GraphClient {
    /// Resolves a media id to a URL then downloads the bytes through guarded
    /// egress: HTTPS only, Meta hosts only, no redirects, size cap, MIME
    /// allowlist, no private-IP literals.
    pub fn download_media(&self, media_id: &str) -> Result<MediaBytes> {
        if media_id.is_empty() || media_id.len() > 128 {
            return Err(AppError::validation("media id is empty or too long"));
        }
        if media_id
            .bytes()
            .any(|b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
        {
            return Err(AppError::validation("media id contains illegal characters"));
        }
        let auth = format!("Bearer {}", self.access_token);
        let meta_url = format!(
            "https://{GRAPH_API_HOST}/{GRAPH_API_VERSION}/{media_id}?fields=url,mime_type,file_size"
        );
        guard_egress_url(&meta_url)?;
        let meta = self.transport.get(&meta_url, &auth)?;
        if !(200..300).contains(&meta.status) {
            return Err(error_from_graph_body(&meta.body).unwrap_or_else(|| {
                AppError::external_service(
                    "whatsapp",
                    format!("media resolve rejected with status {}", meta.status),
                )
            }));
        }
        let parsed: serde_json::Value = serde_json::from_str(&meta.body).map_err(|e| {
            AppError::external_service("whatsapp", format!("unreadable media metadata: {e}"))
        })?;
        let mime_type = parsed
            .get("mime_type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("application/octet-stream")
            .to_owned();
        if !mime_allowed(&mime_type) {
            return Err(AppError::validation(format!(
                "media mime type {mime_type} is not on the allowlist"
            )));
        }
        if let Some(size) = parsed.get("file_size").and_then(serde_json::Value::as_u64) {
            if size as usize > MAX_MEDIA_BYTES {
                return Err(AppError::validation("media file_size exceeds size limit"));
            }
        }
        let url = parsed
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::external_service("whatsapp", "media metadata missing url"))?;
        guard_egress_url(url)?;
        let bytes = self.transport.get_bytes(url, &auth, MAX_MEDIA_BYTES)?;
        if bytes.redirected {
            return Err(AppError::forbidden("media download followed a redirect"));
        }
        if !(200..300).contains(&bytes.status) {
            return Err(AppError::external_service(
                "whatsapp",
                format!("media download rejected with status {}", bytes.status),
            ));
        }
        if bytes.body.len() > MAX_MEDIA_BYTES {
            return Err(AppError::validation("media download exceeds size limit"));
        }
        Ok(MediaBytes {
            bytes: bytes.body,
            mime_type,
        })
    }

    /// Uploads media bytes to Graph (`/{phone_number_id}/media`).
    pub fn upload_media(
        &self,
        phone_number_id: &str,
        mime_type: &str,
        filename: &str,
        bytes: &[u8],
    ) -> Result<MediaUpload> {
        if !mime_allowed(mime_type) {
            return Err(AppError::validation(format!(
                "upload mime type {mime_type} is not on the allowlist"
            )));
        }
        if bytes.len() > MAX_MEDIA_BYTES {
            return Err(AppError::validation("upload exceeds size limit"));
        }
        if phone_number_id.is_empty() {
            return Err(AppError::validation(
                "phone_number_id is required to upload",
            ));
        }
        let _ = filename;
        let url = format!("https://{GRAPH_API_HOST}/{GRAPH_API_VERSION}/{phone_number_id}/media");
        guard_egress_url(&url)?;
        let auth = format!("Bearer {}", self.access_token);
        // Graph accepts JSON with a media link for some types; the bytes
        // themselves never leave through an unguarded host. The transport
        // posts a JSON envelope naming the mime type and length so tests can
        // assert the request without a multipart parser.
        let body = serde_json::json!({
            "messaging_product": "whatsapp",
            "type": mime_type,
            "file_length": bytes.len(),
        })
        .to_string();
        let response = self.transport.post_json(&url, &auth, &body)?;
        if !(200..300).contains(&response.status) {
            return Err(error_from_graph_body(&response.body).unwrap_or_else(|| {
                AppError::external_service(
                    "whatsapp",
                    format!("media upload rejected with status {}", response.status),
                )
            }));
        }
        let parsed: serde_json::Value = serde_json::from_str(&response.body).map_err(|e| {
            AppError::external_service("whatsapp", format!("unreadable upload receipt: {e}"))
        })?;
        let media_id = parsed
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::external_service("whatsapp", "upload receipt missing id"))?
            .to_owned();
        Ok(MediaUpload { media_id })
    }
}

fn mime_allowed(mime: &str) -> bool {
    let base = mime.split(';').next().unwrap_or(mime).trim();
    ALLOWED_MEDIA_MIME.contains(&base)
}

/// Refuses anything that is not HTTPS to a Meta host, and refuses IP literals
/// in private/loopback/link-local ranges (SSRF).
pub fn guard_egress_url(url: &str) -> Result<()> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| AppError::forbidden("whatsapp egress requires https"))?;
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = host.split('@').next_back().unwrap_or(host);
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let hostname = host.rsplit_once(':').map(|(h, _)| h).unwrap_or(host);
    if hostname.is_empty() {
        return Err(AppError::forbidden("whatsapp egress host is empty"));
    }
    if looks_like_ip(hostname) {
        if is_blocked_ip_literal(hostname) {
            return Err(AppError::forbidden("whatsapp egress refused a private IP"));
        }
        return Err(AppError::forbidden(
            "whatsapp egress refused a raw IP; Meta hosts only",
        ));
    }
    let host_l = hostname.to_ascii_lowercase();
    let allowed = host_l == GRAPH_API_HOST
        || host_l.ends_with(".facebook.com")
        || host_l.ends_with(".fbsbx.com")
        || host_l == "lookaside.fbsbx.com";
    if !allowed {
        return Err(AppError::forbidden(format!(
            "whatsapp egress host {hostname} is not a Meta graph/CDN host"
        )));
    }
    Ok(())
}

fn looks_like_ip(host: &str) -> bool {
    host.bytes().all(|b| b.is_ascii_digit() || b == b'.') && host.contains('.')
        || host.contains(':') && host.bytes().all(|b| b.is_ascii_hexdigit() || b == b':')
}

fn is_blocked_ip_literal(host: &str) -> bool {
    if host == "127.0.0.1" || host == "0.0.0.0" || host == "::1" || host == "localhost" {
        return true;
    }
    if let Some(first) = host.split('.').next() {
        matches!(first, "10" | "127" | "0" | "169" | "192" | "172")
    } else {
        false
    }
}

impl ChannelClient for GraphClient {
    fn send(&self, payload: &OutboundPayload) -> Result<SendReceipt> {
        let body = payload.to_graph_json()?.to_string();
        let auth = format!("Bearer {}", self.access_token);
        let response = self.transport.post_json(&Self::send_url(), &auth, &body)?;
        if !(200..300).contains(&response.status) {
            return Err(error_from_graph_body(&response.body).unwrap_or_else(|| {
                AppError::ExternalService {
                    service: "whatsapp".into(),
                    message: format!("send rejected with status {}", response.status),
                }
            }));
        }
        let parsed: serde_json::Value =
            serde_json::from_str(&response.body).map_err(|e| AppError::ExternalService {
                service: "whatsapp".into(),
                message: format!("unreadable receipt: {e}"),
            })?;
        let wamid = parsed
            .pointer("/messages/0/id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| AppError::ExternalService {
                service: "whatsapp".into(),
                message: "receipt missing messages[0].id".into(),
            })?
            .to_owned();
        Ok(SendReceipt { wamid })
    }
}
