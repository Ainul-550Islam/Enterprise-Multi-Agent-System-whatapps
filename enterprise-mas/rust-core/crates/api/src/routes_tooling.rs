//! PURPOSE: Registers the additive tooling, connector administration, webhook
//! operations, SSE, and assistant routes as one authenticated route group.
//!
//! Owning layer: `mas-api` route composition.
//!
//! Production responsibility: define twelve BATCH 02 method/path endpoints
//! (ten Axum registrations) inside the existing protected `/v1` route group.
//!
//! Data ownership: route handlers delegate all state to application services.
//!
//! MUST NOT: re-register any existing connector, webhook, insight, notification,
//! rollback, or analytics endpoint.

use axum::routing::{get, post, put};
use axum::Router;

use crate::handlers::{assistant, connectors, stream, tools, webhooks};
use crate::state::AppState;

/// Builds exactly the additive routes required by BATCH 02.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/tools", post(tools::register).get(tools::list))
        .route("/v1/tools/{id}", get(tools::get).patch(tools::update))
        .route(
            "/v1/agents/{agent_id}/tool-permissions",
            put(tools::replace_agent_permissions),
        )
        .route("/v1/connector-manifests", get(connectors::manifests))
        .route(
            "/v1/connectors/{connector_id}/probe",
            post(connectors::probe),
        )
        .route(
            "/v1/connectors/{connector_id}/disable",
            post(connectors::disable),
        )
        .route("/v1/webhooks/{id}/deliveries", get(webhooks::deliveries))
        .route(
            "/v1/webhooks/{id}/deliveries/{delivery_id}/replay",
            post(webhooks::replay),
        )
        .route("/v1/stream", get(stream::connect))
        .route("/v1/assistant/messages", post(assistant::submit))
}

#[cfg(test)]
mod tests {
    #[test]
    fn route_group_has_only_additive_route_registrations() {
        let source = include_str!("routes_tooling.rs");
        let registrations = source.split("#[cfg(test)]").next().unwrap_or(source);
        assert_eq!(registrations.matches("\n        .route(").count(), 10);
        assert!(!registrations.contains("/v1/assistant/insights"));
        assert!(!registrations.contains("/v1/connectors/test"));
    }
}
