/// mas-channel-whatsapp — protocol-only crate
///
/// No database dependencies. All types, models, and pure functions live here.
/// Integration into the control plane happens via:
///   • `crates/persistence` — RLS-armed repository implementations
///   • `crates/application` — service layer that owns execution-state + audit
///   • `crates/api` — HTTP handlers + webhook ingress
///
/// WhatsApp Cloud API contract:
///   - GET/POST /v1/public/whatsapp/webhook/{route_id}   (hub challenge + HMAC-signed POST)
///   - POST /v1/whatsapp/*   (protected, authZ'd per-tenant)
///   - Graph API egress is guarded by `crates/integrations::http_guard` + mime/size allowlists
///   - All mutations are audit-keyed (wamid, account_id, tenant_id) and idempotent
///   - Secret values (app_secret, access_token) are referenced via `secret_ref`, never inline
///
/// Crate policy: zero DB, zero runtime deps beyond workspace deps + standard libs.
/// Any DB/state interaction lives in the application crate; this crate purely
/// defines the protocol shapes, builders, verifiers, and client adapters.
mod config;
mod webhook_verify;
mod inbound;
mod outbound;
mod client;
mod session_window;
mod rate_limit;
mod template;
mod notification_adapter;
mod errors;

pub use config::WhatsAppAccountConfig;
pub use webhook_verify::{verify_hub_challenge, verify_post_signature};
pub use inbound::{InboundMessage, StatusUpdate, MessageKind};
pub use outbound::{OutboundBuilder, text, template, interactive, media, mark_as_read};
pub use client::{GraphClient, send_message, upload_media, resolve_media};
pub use session_window::CustomerWindow;
pub use rate_limit::{RateLimiter, token_bucket, meta_130429_backoff};
pub use template::{TemplateModel, TemplateCategory, TemplateStatus};
pub use notification_adapter::WhatsAppAdapter;
pub use errors::ChannelError;