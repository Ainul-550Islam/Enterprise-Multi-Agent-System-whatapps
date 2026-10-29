//! PURPOSE: Read-only tenant audit event endpoint with canonical ViewAudit authorization and a stable response envelope.
//!
//! Owning layer: `mas-api` transport edge.
//! Production responsibility: validate/forward bounded filters and return the application's redacted keyset page.
//! Data ownership: events are persistence-owned; the handler uses only resolved middleware scope.
//! MUST NOT: perform SQL, reveal foreign tenant rows, mutate audit state, or implement a second authorization matrix.

use axum::extract::{Query, State};
use mas_contracts::audit::{AuditPageDto, AuditQuery};
use mas_security::{Action, ResourceKind};

use crate::{
    handlers::{authorized_service_context, Ctx},
    response::{ApiError, ApiResult},
    state::AppState,
};

/// `GET /v1/audit-events` — filtered, keyset-paginated, redacted audit events.
pub async fn list(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<AuditQuery>,
) -> ApiResult<AuditPageDto> {
    let _service_context =
        authorized_service_context(&ctx, ResourceKind::Audit, Action::ViewAudit, None)
            .map_err(|error| ApiError::new(error, &ctx))?;
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let scope = ctx
        .require_scope()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let principal = ctx.principal.as_ref().ok_or_else(|| {
        ApiError::new(
            mas_common::error::AppError::unauthorized("authentication required"),
            &ctx,
        )
    })?;
    state
        .audit_query_service(&ctx)?
        .query(
            principal,
            scope.tenant_id(),
            scope.organization_id(),
            &query,
        )
        .await
        .map(|data| crate::response::ok(&ctx, data))
        .map_err(|error| ApiError::new(error, &ctx))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_audit_filter_contract_accepts_an_empty_first_page() {
        assert!(AuditQuery::default().validate().is_ok());
    }
}
