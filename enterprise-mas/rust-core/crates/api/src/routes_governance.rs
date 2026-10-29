//! PURPOSE: Registers the governance, identity, audit, policy, quota and usage routes beneath the authenticated `/v1` group.
//!
//! Owning layer: `mas-api` process edge.
//! Production responsibility: expose exactly the governance HTTP methods without applying a second or weaker middleware envelope.
//! Data ownership: routes only dispatch to handlers; state and persistence remain owned by application/persistence layers.
//! MUST NOT: register legacy usage/billing routes, duplicate routes, or apply a public middleware stack.

use axum::{
    routing::{get, post},
    Router,
};

use crate::{
    handlers::{audit, auth, policies, usage},
    state::AppState,
};

/// Builds the authenticated governance route table. The caller merges this router inside the protected `/v1` router.
pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/v1/auth/me", get(auth::me))
        .route("/v1/audit-events", get(audit::list))
        .route("/v1/policies", get(policies::list).post(policies::create))
        .route(
            "/v1/policies/{id}",
            get(policies::get).patch(policies::update),
        )
        .route("/v1/policies/{id}/publish", post(policies::publish))
        .route("/v1/policies/{id}/rollback", post(policies::rollback))
        .route("/v1/policies/{id}/evaluate", post(policies::evaluate))
        .route("/v1/quotas", get(usage::quotas))
        .route("/v1/usage/summary", get(usage::summary))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governance_router_is_stateful_and_constructible() {
        let _routes: Router<AppState> = routes();
    }
}
