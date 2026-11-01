//! PURPOSE: Authenticated recipient-notification HTTP handlers.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: expose only stable keyset inbox listing and idempotent
//! read/archive lifecycle routes over the canonical application service.
//!
//! Security boundary: every route is behind resolved-scope authentication,
//! canonical authorization, and the service's recipient-or-admin check. Path
//! recipient IDs are never trusted as authority, and no delivery destination,
//! retry diagnostic, deduplication key, or secret metadata crosses the wire.
//!
//! Data ownership: the application service/PostgreSQL own notification rows,
//! cursors, transitions, and audit records. Handlers own only DTO conversion.
//!
//! Prohibited responsibilities: notification emission, template rendering,
//! delivery dispatch, arbitrary-channel sends, direct SQL, offset pagination,
//! or ad-hoc role checks.

use axum::extract::{Path, Query, State};
use mas_common::ids::UserId;
use mas_contracts::notification::{NotificationDto, NotificationPageDto, NotificationQuery};

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// `GET /v1/notifications/recipients/{recipient_id}`.
pub async fn list(
    State(state): State<AppState>,
    Path(recipient_id): Path<UserId>,
    Query(query): Query<NotificationQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<NotificationPageDto> {
    query
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let service_ctx = authorized_notification(&ctx, mas_security::Action::Read)?;
    let page = state
        .notification_service(&ctx)?
        .list_for_recipient(
            &service_ctx,
            recipient_id,
            query.include_archived,
            query.unread_only,
            query.cursor.as_deref(),
            query.page_limit(),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(
        &ctx,
        NotificationPageDto {
            items: page.items.iter().map(notification_dto).collect(),
            next_cursor: page.next_cursor,
        },
    ))
}

/// `POST /v1/notifications/recipients/{recipient_id}/{notification_id}/read`.
pub async fn mark_read(
    State(state): State<AppState>,
    Path((recipient_id, notification_id)): Path<(UserId, uuid::Uuid)>,
    Ctx(ctx): Ctx,
) -> ApiResult<NotificationDto> {
    let service_ctx = authorized_notification(&ctx, mas_security::Action::Update)?;
    state
        .notification_service(&ctx)?
        .mark_read(&service_ctx, recipient_id, notification_id)
        .await
        .map(|notification| ok(&ctx, notification_dto(&notification)))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/notifications/recipients/{recipient_id}/{notification_id}/archive`.
pub async fn archive(
    State(state): State<AppState>,
    Path((recipient_id, notification_id)): Path<(UserId, uuid::Uuid)>,
    Ctx(ctx): Ctx,
) -> ApiResult<NotificationDto> {
    let service_ctx = authorized_notification(&ctx, mas_security::Action::Update)?;
    state
        .notification_service(&ctx)?
        .archive(&service_ctx, recipient_id, notification_id)
        .await
        .map(|notification| ok(&ctx, notification_dto(&notification)))
        .map_err(|error| ApiError::new(error, &ctx))
}

fn notification_dto(notification: &mas_domain::Notification) -> NotificationDto {
    NotificationDto {
        id: notification.id,
        recipient_id: notification.recipient_id,
        channel: notification.channel.to_string(),
        subject: notification.subject.clone(),
        body: notification.body.clone(),
        delivery_state: notification.state.to_string(),
        read_at: notification.read_at,
        archived_at: notification.archived_at,
        created_at: notification.created_at,
    }
}

fn authorized_notification(
    ctx: &crate::context::RequestContext,
    action: mas_security::Action,
) -> std::result::Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(
        ctx,
        mas_security::ResourceKind::Notification,
        action,
        None,
    )
    .map_err(|error| ApiError::new(error, ctx))
}
