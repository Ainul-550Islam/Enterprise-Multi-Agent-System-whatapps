//! PURPOSE: Authenticated tenant-facing WhatsApp channel handlers.
//!
//! Owning layer: `mas-api` process edge.
//!
//! Production role: register channels, send text, and read the tenant's
//! conversation log over the canonical WhatsApp service.
//!
//! Security boundary: behind resolved-scope authentication; the tenant
//! comes from the resolved scope, never from the body. Access tokens are
//! referenced, never returned.
//!
//! Data ownership: the WhatsApp service/PostgreSQL own channel rows and the
//! message log. Handlers own only DTO conversion.
//!
//! Prohibited responsibilities: provider calls, SQL, secret handling.

use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};

use crate::handlers::Ctx;
use crate::response::{ok, ApiError, ApiResult};
use crate::state::AppState;

/// `POST /v1/whatsapp/channels` body.
#[derive(Debug, Deserialize)]
pub struct RegisterChannelRequest {
    /// Operator-facing name.
    pub display_name: String,
    /// Provider phone-number handle.
    pub phone_number_id: String,
    /// WhatsApp Business Account handle, when known.
    #[serde(default)]
    pub waba_id: Option<String>,
    /// Graph API version, e.g. `v21.0`.
    pub api_version: String,
    /// Secret *reference* for the webhook verify token.
    pub verify_token_ref: String,
}

/// One channel over the wire.
#[derive(Debug, Serialize)]
pub struct ChannelDto {
    /// Row id.
    pub id: uuid::Uuid,
    /// Operator-facing name.
    pub display_name: String,
    /// Provider phone-number handle.
    pub phone_number_id: String,
    /// WhatsApp Business Account handle.
    pub waba_id: Option<String>,
    /// Graph API version.
    pub api_version: String,
    /// Lifecycle status.
    pub status: String,
}

fn channel_dto(channel: &mas_domain::whatsapp::WhatsAppChannel) -> ChannelDto {
    ChannelDto {
        id: channel.id.into_uuid(),
        display_name: channel.display_name.clone(),
        phone_number_id: channel.phone_number_id.clone(),
        waba_id: channel.waba_id.clone(),
        api_version: channel.api_version.clone(),
        status: channel.status.as_str().to_owned(),
    }
}

/// One message over the wire.
#[derive(Debug, Serialize)]
pub struct MessageDto {
    /// Row id.
    pub id: uuid::Uuid,
    /// Provider message id.
    pub wamid: Option<String>,
    /// Which side the message is on.
    pub direction: String,
    /// Sender address.
    pub from: Option<String>,
    /// Receiver address.
    pub to: Option<String>,
    /// Declared content type.
    pub message_type: String,
    /// Text body, when present.
    pub body: Option<String>,
    /// Delivery lifecycle.
    pub status: String,
    /// When the provider says it happened.
    pub occurred_at: String,
}

fn message_dto(message: &mas_domain::whatsapp::WhatsAppMessage) -> MessageDto {
    MessageDto {
        id: message.id.into_uuid(),
        wamid: message.wamid.clone(),
        direction: message.direction.as_str().to_owned(),
        from: message.from_addr.clone(),
        to: message.to_addr.clone(),
        message_type: message.message_type.as_str().to_owned(),
        body: message.body_text.clone(),
        status: message.status.as_str().to_owned(),
        occurred_at: message.occurred_at.to_rfc3339_millis(),
    }
}

/// `POST /v1/whatsapp/channels`.
pub async fn register_channel(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    axum::Json(request): axum::Json<RegisterChannelRequest>,
) -> ApiResult<ChannelDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let channel = mas_domain::whatsapp::WhatsAppChannel {
        id: mas_common::ids::WhatsAppChannelId::new(),
        tenant_id: scope.tenant_id(),
        display_name: request.display_name,
        phone_number_id: request.phone_number_id,
        waba_id: request.waba_id,
        api_version: request.api_version,
        status: mas_domain::whatsapp::WhatsAppChannelStatus::Active,
        verify_token_ref: request.verify_token_ref,
        created_at: mas_common::timestamps::Timestamp::now(),
        updated_at: mas_common::timestamps::Timestamp::now(),
    };
    let registered = state
        .whatsapp_service(&ctx)?
        .register_channel(&channel)
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, channel_dto(&registered)))
}

/// `GET /v1/whatsapp/channels`.
pub async fn list_channels(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ChannelDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let channels = state
        .whatsapp_service(&ctx)?
        .list_channels(scope.tenant_id())
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, channels.iter().map(channel_dto).collect()))
}

/// `POST /v1/whatsapp/channels/{channel_id}/messages` body.
#[derive(Debug, Deserialize)]
pub struct SendTextRequest {
    /// Recipient phone number (E.164).
    pub to: String,
    /// The text to send.
    pub body: String,
}

/// `POST /v1/whatsapp/channels/{channel_id}/messages`.
pub async fn send_text(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(channel_id): Path<uuid::Uuid>,
    axum::Json(request): axum::Json<SendTextRequest>,
) -> ApiResult<MessageDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let message = state
        .whatsapp_service(&ctx)?
        .send_text(
            scope.tenant_id(),
            mas_common::ids::WhatsAppChannelId::from_uuid(channel_id),
            &request.to,
            &request.body,
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, message_dto(&message)))
}

/// `GET /v1/whatsapp/channels/{channel_id}/messages` query.
#[derive(Debug, Deserialize)]
pub struct ListMessagesQuery {
    /// Page size (1..=200).
    #[serde(default = "default_limit")]
    pub limit: i64,
}

const fn default_limit() -> i64 {
    50
}

/// `GET /v1/whatsapp/channels/{channel_id}/messages`.
pub async fn list_messages(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(channel_id): Path<uuid::Uuid>,
    Query(query): Query<ListMessagesQuery>,
) -> ApiResult<Vec<MessageDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let limit = query.limit.clamp(1, 200);
    let messages = state
        .whatsapp_service(&ctx)?
        .list_messages(
            scope.tenant_id(),
            mas_common::ids::WhatsAppChannelId::from_uuid(channel_id),
            limit,
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, messages.iter().map(message_dto).collect()))
}

/// `GET /v1/whatsapp/conversations` query.
#[derive(Debug, Deserialize)]
pub struct ConversationQuery {
    /// The channel.
    pub channel_id: uuid::Uuid,
    /// The contact's phone number.
    pub contact: String,
}

/// `GET /v1/whatsapp/conversations`.
pub async fn conversation(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<ConversationQuery>,
) -> ApiResult<Vec<MessageDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let messages = state
        .whatsapp_service(&ctx)?
        .get_conversation(
            scope.tenant_id(),
            mas_common::ids::WhatsAppChannelId::from_uuid(query.channel_id),
            &query.contact,
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, messages.iter().map(message_dto).collect()))
}

/// `PUT /v1/whatsapp/messages/{wamid}/status` body.
#[derive(Debug, Deserialize)]
pub struct UpdateStatusRequest {
    /// The new delivery status.
    pub status: String,
}

/// `PUT /v1/whatsapp/messages/{wamid}/status`.
pub async fn update_status(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(wamid): Path<String>,
    axum::Json(request): axum::Json<UpdateStatusRequest>,
) -> ApiResult<MessageDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let status: mas_domain::whatsapp::WhatsAppMessageStatus = request
        .status
        .parse()
        .map_err(|error| ApiError::new(error, &ctx))?;
    let message = state
        .whatsapp_service(&ctx)?
        .update_message_status(scope.tenant_id(), &wamid, status)
        .await
        .map_err(|error| ApiError::new(error, &ctx))?
        .ok_or_else(|| {
            ApiError::new(
                mas_common::error::AppError::not_found("whatsapp message", wamid),
                &ctx,
            )
        })?;
    Ok(ok(&ctx, message_dto(&message)))
}

/// One template over the wire.
#[derive(Debug, Serialize)]
pub struct TemplateDto {
    /// Row id.
    pub id: uuid::Uuid,
    /// Channel id.
    pub channel_id: uuid::Uuid,
    /// Provider template name.
    pub name: String,
    /// Language.
    pub language: String,
    /// Lifecycle status.
    pub status: String,
    /// Body preview, when known.
    pub body: Option<String>,
}

fn template_dto(template: &mas_domain::whatsapp::WhatsAppTemplate) -> TemplateDto {
    TemplateDto {
        id: template.id.into_uuid(),
        channel_id: template.channel_id.into_uuid(),
        name: template.name.clone(),
        language: template.language.clone(),
        status: template.status.clone(),
        body: template.body.clone(),
    }
}

/// `GET /v1/whatsapp/templates` query.
#[derive(Debug, Deserialize)]
pub struct ListTemplatesQuery {
    /// The channel whose templates to list.
    pub channel_id: uuid::Uuid,
}

/// `GET /v1/whatsapp/templates`.
pub async fn list_templates(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Query(query): Query<ListTemplatesQuery>,
) -> ApiResult<Vec<TemplateDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let templates = state
        .whatsapp_service(&ctx)?
        .list_templates(
            scope.tenant_id(),
            mas_common::ids::WhatsAppChannelId::from_uuid(query.channel_id),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, templates.iter().map(template_dto).collect()))
}

/// `POST /v1/whatsapp/templates` body (sync).
#[derive(Debug, Deserialize)]
pub struct SyncTemplatesRequest {
    /// The channel whose WABA to pull from.
    pub channel_id: uuid::Uuid,
}

/// `POST /v1/whatsapp/templates`.
pub async fn sync_templates(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    axum::Json(request): axum::Json<SyncTemplatesRequest>,
) -> ApiResult<Vec<TemplateDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let templates = state
        .whatsapp_service(&ctx)?
        .sync_templates(
            scope.tenant_id(),
            mas_common::ids::WhatsAppChannelId::from_uuid(request.channel_id),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, templates.iter().map(template_dto).collect()))
}

/// `GET /v1/whatsapp/conversations/{conversation_id}/messages`.
pub async fn conversation_messages(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(conversation_id): Path<uuid::Uuid>,
) -> ApiResult<Vec<MessageDto>> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let messages = state
        .whatsapp_service(&ctx)?
        .conversation_messages(
            scope.tenant_id(),
            mas_common::ids::WhatsAppConversationId::from_uuid(conversation_id),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, messages.iter().map(message_dto).collect()))
}

/// `POST /v1/whatsapp/conversations/{conversation_id}/messages` body.
#[derive(Debug, Deserialize)]
pub struct HumanReplyRequest {
    /// Free-form text, required inside the 24h window.
    #[serde(default)]
    pub body: Option<String>,
    /// Template name, required after the window.
    #[serde(default)]
    pub template_name: Option<String>,
    /// Template language, default `en`.
    #[serde(default)]
    pub template_language: Option<String>,
}

/// `POST /v1/whatsapp/conversations/{conversation_id}/messages`.
pub async fn human_reply(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Path(conversation_id): Path<uuid::Uuid>,
    axum::Json(request): axum::Json<HumanReplyRequest>,
) -> ApiResult<MessageDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let message = state
        .whatsapp_service(&ctx)?
        .human_reply(
            scope.tenant_id(),
            mas_common::ids::WhatsAppConversationId::from_uuid(conversation_id),
            request.body.as_deref(),
            request.template_name.as_deref(),
            request.template_language.as_deref(),
        )
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(&ctx, message_dto(&message)))
}

/// Inbox counters over the wire.
#[derive(Debug, Serialize)]
pub struct StatsDto {
    /// Connected channels.
    pub channels: i64,
    /// Open conversations.
    pub conversations: i64,
    /// Inbound messages.
    pub inbound: i64,
    /// Outbound messages.
    pub outbound: i64,
    /// Failed outbound messages.
    pub failed: i64,
}

/// `GET /v1/whatsapp/stats`.
pub async fn stats(State(state): State<AppState>, Ctx(ctx): Ctx) -> ApiResult<StatsDto> {
    let scope = crate::handlers::resolved_scope(&ctx)?;
    let stats = state
        .whatsapp_service(&ctx)?
        .stats(scope.tenant_id())
        .await
        .map_err(|error| ApiError::new(error, &ctx))?;
    Ok(ok(
        &ctx,
        StatsDto {
            channels: stats.channels,
            conversations: stats.conversations,
            inbound: stats.inbound,
            outbound: stats.outbound,
            failed: stats.failed,
        },
    ))
}
