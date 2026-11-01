//! PURPOSE: Authenticated OAuth-start and public OAuth-callback HTTP handlers.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: authorize an OAuth2 connector manager before issuing a
//! one-shot browser URL, and route a provider callback through durable PKCE,
//! token exchange, external-secret materialization, and canonical credential
//! binding without exposing token or tenant data.
//!
//! Security boundary: the start path is bearer-authenticated/authorized and
//! derives provider identity from a stored connector. The callback path has no
//! tenant input, consumes opaque state exactly once, and reflects no provider
//! error text, token value, credential id, or secret-reference metadata.
//!
//! Data ownership: application services own start authorization; integrations
//! own OAuth protocol lifecycle; persistence owns state/reference/binding
//! metadata; external secret management owns verifier/token bytes.
//!
//! Prohibited responsibilities: direct SQL, callback tenant headers, raw-token
//! logging, client secrets, arbitrary redirect targets, user-chosen providers,
//! or a success acknowledgement before credential persistence succeeds.

use axum::extract::{Path, Query, State};
use mas_common::ids::ConnectorId;
use mas_common::timestamps::Timestamp;
use mas_contracts::oauth::{
    OAuthAuthorizationUrlDto, OAuthCallbackAcknowledgement, OAuthCallbackQuery,
};

use crate::handlers::Ctx;
use crate::response::{accepted, ok, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/connectors/{connector_id}/oauth/authorize`.
pub async fn begin(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(connector_id): Path<ConnectorId>,
) -> ApiResult<OAuthAuthorizationUrlDto> {
    let service_ctx = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Connector,
        mas_security::Action::Update,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    let authorization_url = state
        .oauth_authorization_service(&ctx)?
        .begin(&service_ctx, connector_id)
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, OAuthAuthorizationUrlDto { authorization_url }))
}

/// `GET /v1/public/oauth/{provider}/callback`.
///
/// Provider is a route selector for an operator-enabled static profile only;
/// durable state independently proves that this callback belongs to the same
/// provider configuration before a code can be exchanged.
pub async fn callback(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(provider): Path<String>,
    Query(query): Query<OAuthCallbackQuery>,
) -> ApiResult<OAuthCallbackAcknowledgement> {
    query
        .validate_shape()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .oauth_callback_service(&ctx)?
        .complete(
            &provider,
            &query.state,
            query.code.as_deref(),
            query.provider_error.as_deref(),
            &Timestamp::now(),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(accepted(
        &ctx,
        OAuthCallbackAcknowledgement { accepted: true },
    ))
}
