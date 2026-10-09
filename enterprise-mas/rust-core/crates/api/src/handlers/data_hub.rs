//! Data Hub HTTP adapters for Rust-owned catalogue metadata.
//!
//! Source/document authorization, tenant ownership and audit stay in Rust.
//! Raw document bytes are accepted only as a bounded multipart field and are
//! handed to the authenticated Python bridge by `DataHubApplicationService`;
//! they are never persisted in an API DTO or returned by these endpoints.

use axum::extract::multipart::{Multipart, MultipartRejection};
use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::{DataSourceId, DocumentId, ProjectId};
use mas_contracts::data_hub::{
    CreateDataSourceRequest, CreateDocumentRequest, DataSourceDto, DataSourceSyncDto, DocumentDto,
};
use serde::Deserialize;

use crate::handlers::Ctx;
use crate::response::{created, ok, ApiError, ApiResult};
use crate::state::AppState;

/// Upper hard ceiling for one multipart request. Deployment configuration may
/// set a smaller `DataHubLimits::max_upload_bytes`; it is checked again while
/// parsing the content field and by the application service before RPC.
pub const MAX_DATA_HUB_MULTIPART_BYTES: usize = 64 * 1024 * 1024;
const MAX_UPLOAD_METADATA_BYTES: usize = 256 * 1024;

/// Project selector for the source catalogue list.
#[derive(Debug, Deserialize)]
pub struct SourceListQuery {
    /// Only sources belonging to this project are listed.
    pub project_id: ProjectId,
}

/// `POST /v1/data-sources`.
pub async fn create_source(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreateDataSourceRequest>,
) -> ApiResult<DataSourceDto> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::DataSource,
        mas_security::Action::Create,
    )?;
    body.validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    state
        .data_hub_service(&ctx)?
        .create_source(&service_ctx, &body)
        .await
        .map(|payload| created(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/data-sources?project_id=`.
pub async fn list_sources(
    State(state): State<AppState>,
    Query(query): Query<SourceListQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<DataSourceDto>> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::DataSource,
        mas_security::Action::Read,
    )?;
    state
        .data_hub_service(&ctx)?
        .list_sources(&service_ctx, query.project_id)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/data-sources/{id}`.
pub async fn get_source(
    State(state): State<AppState>,
    Path(id): Path<DataSourceId>,
    Ctx(ctx): Ctx,
) -> ApiResult<DataSourceDto> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::DataSource,
        mas_security::Action::Read,
    )?;
    state
        .data_hub_service(&ctx)?
        .get_source(&service_ctx, id)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/data-sources/{id}/sync`.
pub async fn sync_source(
    State(state): State<AppState>,
    Path(id): Path<DataSourceId>,
    Ctx(ctx): Ctx,
) -> ApiResult<DataSourceSyncDto> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::DataSource,
        mas_security::Action::Execute,
    )?;
    state
        .data_hub_service(&ctx)?
        .sync_source(&service_ctx, id)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/data-sources/{id}/documents`.
pub async fn list_documents(
    State(state): State<AppState>,
    Path(source_id): Path<DataSourceId>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<DocumentDto>> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::Document,
        mas_security::Action::Read,
    )?;
    state
        .data_hub_service(&ctx)?
        .list_documents(&service_ctx, source_id)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `POST /v1/documents/upload` accepts exactly two multipart fields:
/// `request` (the JSON `CreateDocumentRequest`) and `content` (the raw bytes).
pub async fn upload_document(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    multipart: Result<Multipart, MultipartRejection>,
) -> ApiResult<DocumentDto> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::Document,
        mas_security::Action::Create,
    )?;
    let service = state.data_hub_service(&ctx)?;
    let (request, content) = parse_upload(multipart, service.max_upload_bytes(), &ctx).await?;
    request
        .validate()
        .map_err(|error| ApiError::new(error, &ctx))?;
    service
        .ingest_document(&service_ctx, &request, content)
        .await
        .map(|payload| created(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `GET /v1/documents/{id}`.
pub async fn get_document(
    State(state): State<AppState>,
    Path(id): Path<DocumentId>,
    Ctx(ctx): Ctx,
) -> ApiResult<DocumentDto> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::Document,
        mas_security::Action::Read,
    )?;
    state
        .data_hub_service(&ctx)?
        .get_document(&service_ctx, id)
        .await
        .map(|payload| ok(&ctx, payload))
        .map_err(|error| ApiError::new(error, &ctx))
}

/// `DELETE /v1/documents/{id}` tombstones Rust metadata and asks Python to
/// purge only derived chunks/vectors for the same trusted tenant scope.
pub async fn delete_document(
    State(state): State<AppState>,
    Path(id): Path<DocumentId>,
    Ctx(ctx): Ctx,
) -> ApiResult<serde_json::Value> {
    let service_ctx = authorized(
        &ctx,
        mas_security::ResourceKind::Document,
        mas_security::Action::Delete,
    )?;
    state
        .data_hub_service(&ctx)?
        .delete_document(&service_ctx, id)
        .await
        .map(|_| ok(&ctx, serde_json::json!({"id": id, "deleted": true})))
        .map_err(|error| ApiError::new(error, &ctx))
}

async fn parse_upload(
    multipart: Result<Multipart, MultipartRejection>,
    max_upload_bytes: u64,
    ctx: &crate::context::RequestContext,
) -> Result<(CreateDocumentRequest, Vec<u8>), ApiError> {
    let mut multipart = multipart.map_err(|error| {
        ApiError::new(
            mas_common::error::AppError::invalid_field(
                "multipart",
                "invalid_format",
                format!("invalid multipart upload: {error}"),
            ),
            ctx,
        )
    })?;
    let mut request = None;
    let mut content = None;
    let mut field_count = 0_usize;
    while let Some(field) = multipart.next_field().await.map_err(|error| {
        ApiError::new(
            mas_common::error::AppError::invalid_field(
                "multipart",
                "invalid_format",
                format!("cannot read multipart upload: {error}"),
            ),
            ctx,
        )
    })? {
        field_count = field_count.saturating_add(1);
        if field_count > 2 {
            return Err(ApiError::new(
                mas_common::error::AppError::invalid_field(
                    "multipart",
                    "too_many_fields",
                    "upload requires exactly request and content fields",
                ),
                ctx,
            ));
        }
        let name = field.name().map(str::to_owned).ok_or_else(|| {
            ApiError::new(
                mas_common::error::AppError::invalid_field(
                    "multipart",
                    "missing_field_name",
                    "every upload field must have a name",
                ),
                ctx,
            )
        })?;
        match name.as_str() {
            "request" if request.is_none() => {
                let text = field.text().await.map_err(|error| {
                    ApiError::new(
                        mas_common::error::AppError::invalid_field(
                            "request",
                            "invalid_format",
                            format!("cannot read upload metadata: {error}"),
                        ),
                        ctx,
                    )
                })?;
                if text.len() > MAX_UPLOAD_METADATA_BYTES {
                    return Err(ApiError::new(
                        mas_common::error::AppError::invalid_field(
                            "request",
                            "too_large",
                            "upload metadata exceeds 256 KiB",
                        ),
                        ctx,
                    ));
                }
                request = Some(serde_json::from_str(&text).map_err(|error| {
                    ApiError::new(
                        mas_common::error::AppError::invalid_field(
                            "request",
                            "invalid_json",
                            format!("upload metadata must be a valid JSON request: {error}"),
                        ),
                        ctx,
                    )
                })?);
            },
            "content" if content.is_none() => {
                let bytes = field.bytes().await.map_err(|error| {
                    ApiError::new(
                        mas_common::error::AppError::invalid_field(
                            "content",
                            "invalid_format",
                            format!("cannot read upload content: {error}"),
                        ),
                        ctx,
                    )
                })?;
                let size = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                if size > max_upload_bytes {
                    return Err(ApiError::new(
                        mas_common::error::AppError::invalid_field(
                            "content",
                            "too_large",
                            "document exceeds the configured upload limit",
                        ),
                        ctx,
                    ));
                }
                content = Some(bytes.to_vec());
            },
            "request" | "content" => {
                return Err(ApiError::new(
                    mas_common::error::AppError::invalid_field(
                        "multipart",
                        "duplicate_field",
                        "upload request and content fields may appear only once",
                    ),
                    ctx,
                ));
            },
            _ => {
                return Err(ApiError::new(
                    mas_common::error::AppError::invalid_field(
                        "multipart",
                        "unknown_field",
                        "upload accepts only request and content fields",
                    ),
                    ctx,
                ));
            },
        }
    }
    let request = request.ok_or_else(|| {
        ApiError::new(
            mas_common::error::AppError::invalid_field(
                "request",
                "required",
                "multipart upload requires a request field",
            ),
            ctx,
        )
    })?;
    let content = content.ok_or_else(|| {
        ApiError::new(
            mas_common::error::AppError::invalid_field(
                "content",
                "required",
                "multipart upload requires a content field",
            ),
            ctx,
        )
    })?;
    Ok((request, content))
}

fn authorized(
    ctx: &crate::context::RequestContext,
    resource: mas_security::ResourceKind,
    action: mas_security::Action,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(ctx, resource, action, None)
        .map_err(|error| ApiError::new(error, ctx))
}
