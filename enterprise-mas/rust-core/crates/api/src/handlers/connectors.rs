//! PURPOSE: Thin tenant-authorized connector manifest, guarded-probe, and
//! disable HTTP handlers.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production responsibility: authorize the connector operation, delegate one
//! use case, and project only non-secret connector metadata.
//!
//! Data ownership: provider manifests, probe results, connector lifecycle, and
//! credential revocation remain owned by the integration application service.
//!
//! MUST NOT: open HTTP clients, resolve credentials, accept egress policy,
//! return secret references, or duplicate canonical connector routes.

use axum::extract::{Path, State};
use mas_common::ids::ConnectorId;
use mas_contracts::connector::{ConnectorManifestDto, ConnectorResponse, ConnectorStatusResponse};

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// `GET /v1/connector-manifests`.
pub async fn manifests(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ConnectorManifestDto>> {
    let _service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Connector,
        mas_security::Action::Read,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .connector_admin_service(&ctx)?
        .manifests()
        .await
        .map(|manifests| {
            ok(
                &ctx,
                manifests
                    .into_iter()
                    .map(|manifest| ConnectorManifestDto {
                        provider: manifest.provider,
                        name: manifest.name,
                        category: manifest.category.to_string(),
                        supported_auth: manifest
                            .supported_auth
                            .into_iter()
                            .map(|auth| auth.to_string())
                            .collect(),
                        required_config: manifest.required_config,
                        optional_config: manifest.optional_config,
                        description: manifest.description,
                    })
                    .collect(),
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/connectors/{id}/probe` — the application service reaches only the
/// already-configured SSRF-guarded egress client.
pub async fn probe(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<ConnectorId>,
) -> ApiResult<ConnectorStatusResponse> {
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Connector,
        mas_security::Action::Update,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .connector_admin_service(&ctx)?
        .probe(&service_context, id)
        .await
        .map(|connector| {
            ok(
                &ctx,
                ConnectorStatusResponse {
                    connector_id: connector.id,
                    status: connector.status,
                    ok: connector.status == mas_common::enums::ConnectorStatus::Active,
                    message: connector.last_error,
                    latency_ms: None,
                    tested_at: connector.last_tested_at.unwrap_or(connector.updated_at),
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/connectors/{id}/disable` — disable and revoke in-flight OAuth
/// credential/materialization work atomically under tenant RLS.
pub async fn disable(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(id): Path<ConnectorId>,
) -> ApiResult<ConnectorResponse> {
    let service_context = crate::handlers::authorized_service_context(
        &ctx,
        mas_security::ResourceKind::Connector,
        mas_security::Action::Update,
        None,
    )
    .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .connector_admin_service(&ctx)?
        .disable(&service_context, id)
        .await
        .map(|connector| {
            ok(
                &ctx,
                ConnectorResponse {
                    id: connector.id,
                    tenant_id: connector.tenant_id,
                    provider: connector.provider,
                    name: connector.name,
                    display_name: connector.display_name,
                    auth_mode: connector.auth_mode.to_string(),
                    status: connector.status,
                    has_credential: connector.credential_id.is_some(),
                    last_tested_at: connector.last_tested_at,
                    last_error: connector.last_error,
                    created_at: connector.created_at,
                    updated_at: connector.updated_at,
                },
            )
        })
        .map_err(|error| ApiError::new(error, &ctx))
}
