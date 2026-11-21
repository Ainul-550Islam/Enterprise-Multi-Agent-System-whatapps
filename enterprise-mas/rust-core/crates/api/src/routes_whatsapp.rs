//! PURPOSE: The WhatsApp + billing route tables, split by authentication
//! envelope.
//!
//! Owning layer: `mas-api`.
//!
//! Security boundary: `public_routes` carries only provider-facing webhook
//! paths (authenticated by body signature); `protected_routes` carries only
//! tenant paths (authenticated by the bearer layer). A route registered on
//! the wrong table is a hole, not a bug.

use axum::routing::{get, post, put};
use axum::Router;

use crate::handlers::{billing_checkout, billing_public, whatsapp, whatsapp_public};
use crate::state::AppState;

/// Provider-facing webhook paths. Public by necessity; authenticated by the
/// signature over the raw body.
pub fn public_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/webhooks/whatsapp",
            get(whatsapp_public::verify).post(whatsapp_public::inbound),
        )
        .route(
            "/v1/public/whatsapp/webhook/{route_id}",
            get(whatsapp_public::verify).post(whatsapp_public::inbound),
        )
        .route(
            "/webhooks/billing/stripe",
            post(billing_public::stripe_inbound),
        )
}

/// Tenant-facing WhatsApp + billing paths. Behind the authenticated layer.
pub fn protected_routes() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/whatsapp/channels",
            post(whatsapp::register_channel).get(whatsapp::list_channels),
        )
        .route(
            "/v1/whatsapp/channels/{channel_id}/messages",
            post(whatsapp::send_text).get(whatsapp::list_messages),
        )
        .route("/v1/whatsapp/conversations", get(whatsapp::conversation))
        .route(
            "/v1/whatsapp/conversations/{conversation_id}/messages",
            get(whatsapp::conversation_messages).post(whatsapp::human_reply),
        )
        .route(
            "/v1/whatsapp/messages/{wamid}/status",
            put(whatsapp::update_status),
        )
        .route(
            "/v1/whatsapp/templates",
            get(whatsapp::list_templates).post(whatsapp::sync_templates),
        )
        .route("/v1/whatsapp/stats", get(whatsapp::stats))
        .route("/v1/billing/checkout", post(billing_checkout::create))
        .route(
            "/v1/billing/checkout-session",
            post(billing_checkout::create),
        )
        .route(
            "/v1/billing/portal-session",
            post(billing_checkout::create_portal),
        )
}
