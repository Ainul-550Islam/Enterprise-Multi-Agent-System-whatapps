//! Data Hub HTTP adapters for Rust-owned catalogue metadata.
//!
//! Source/document authorization, tenant ownership and audit stay in Rust.
//! Raw document bytes are accepted only as a bounded multipart field and are
//! handed to the authenticated Python bridge by `DataHubApplicationService`;
//! they are never persisted in an API DTO or returned by these endpoints.

use axum::extract::multipart::{Field, Multipart, MultipartRejection};
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

const MAX_UPLOAD_METADATA_BYTES: usize = 256 * 1024;
// Multipart boundaries and field headers need room in addition to both fields.
const MULTIPART_HEADER_ALLOWANCE: usize = 128 * 1024;
/// Absolute envelope cap, including the maximum configured content and overhead.
pub const MAX_DATA_HUB_MULTIPART_BYTES: usize = mas_application::data_hub_service::MAX_UPLOAD_BYTES
    as usize
    + MAX_UPLOAD_METADATA_BYTES
    + MULTIPART_HEADER_ALLOWANCE;

/// Bound the complete HTTP body to the configured content limit plus metadata
/// and multipart framing. This also bounds buffering inside the multipart parser.
#[must_use]
pub fn multipart_body_limit(max_upload_bytes: u64) -> usize {
    usize::try_from(max_upload_bytes)
        .unwrap_or(usize::MAX)
        .saturating_add(MAX_UPLOAD_METADATA_BYTES)
        .saturating_add(MULTIPART_HEADER_ALLOWANCE)
        .min(MAX_DATA_HUB_MULTIPART_BYTES)
}

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
                let bytes = read_upload_field(
                    field,
                    MAX_UPLOAD_METADATA_BYTES,
                    "request",
                    "upload metadata exceeds 256 KiB",
                    ctx,
                )
                .await?;
                request = Some(serde_json::from_slice(&bytes).map_err(|error| {
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
                let limit = usize::try_from(max_upload_bytes).unwrap_or(usize::MAX);
                content = Some(
                    read_upload_field(
                        field,
                        limit,
                        "content",
                        "document exceeds the configured upload limit",
                        ctx,
                    )
                    .await?,
                );
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

/// Read each field incrementally, rejecting as soon as its own limit is
/// crossed rather than buffering up to the larger multipart envelope limit.
async fn read_upload_field(
    mut field: Field<'_>,
    limit: usize,
    name: &'static str,
    limit_message: &'static str,
    ctx: &crate::context::RequestContext,
) -> Result<Vec<u8>, ApiError> {
    let mut buffer = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(|error| {
        ApiError::new(
            mas_common::error::AppError::invalid_field(
                name,
                "invalid_format",
                format!("cannot read upload field: {error}"),
            ),
            ctx,
        )
    })? {
        if chunk.len() > limit.saturating_sub(buffer.len()) {
            return Err(ApiError::new(
                mas_common::error::AppError::invalid_field(name, "too_large", limit_message),
                ctx,
            ));
        }
        buffer.extend_from_slice(&chunk);
    }
    Ok(buffer)
}

fn authorized(
    ctx: &crate::context::RequestContext,
    resource: mas_security::ResourceKind,
    action: mas_security::Action,
) -> Result<mas_application::context::ServiceContext, ApiError> {
    crate::handlers::authorized_service_context(ctx, resource, action, None)
        .map_err(|error| ApiError::new(error, ctx))
}

#[cfg(test)]
mod tests {
    use axum::body::{Body, Bytes};
    use axum::extract::multipart::{Multipart, MultipartRejection};
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use axum::Router;
    use futures_util::{stream, StreamExt};
    use std::convert::Infallible;
    use tower::ServiceExt;

    use super::*;
    use crate::context::RequestContext;
    use crate::scope_resolution::ScopeClaims;

    async fn parse_for_test(
        multipart: Result<Multipart, MultipartRejection>,
    ) -> Result<String, ApiError> {
        let ctx = RequestContext::with_claims(ScopeClaims::default(), "test".to_owned());
        let (_, content) = parse_upload(multipart, 4, &ctx).await?;
        Ok(format!("{}", content.len()))
    }

    fn router() -> Router {
        Router::new().route("/upload", post(parse_for_test))
    }

    fn request(body: Body) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri("/upload")
            .header(
                "content-type",
                "multipart/form-data; boundary=test-boundary",
            )
            .body(body)
            .expect("request")
    }

    #[test]
    fn multipart_envelope_limit_tracks_content_limit_and_has_a_hard_ceiling() {
        assert_eq!(multipart_body_limit(4), 4 + 256 * 1024 + 128 * 1024);
        assert_eq!(
            multipart_body_limit(mas_application::data_hub_service::MAX_UPLOAD_BYTES),
            MAX_DATA_HUB_MULTIPART_BYTES
        );
        assert_eq!(multipart_body_limit(u64::MAX), MAX_DATA_HUB_MULTIPART_BYTES);
    }

    #[tokio::test]
    async fn multipart_fields_at_limits_are_accepted() {
        let mut metadata = format!(
            "{{\"source_id\":\"{}\",\"title\":\"Test\",\"mime_type\":\"text/plain\",\"size_bytes\":4,\"content_sha256\":\"{}\",\"access_policy\":{{}},\"metadata\":{{}}}}",
            uuid::Uuid::new_v4(), "a".repeat(64)
        );
        metadata.push_str(&" ".repeat(MAX_UPLOAD_METADATA_BYTES - metadata.len()));
        let body = format!("--test-boundary\r\nContent-Disposition: form-data; name=\"request\"\r\n\r\n{metadata}\r\n--test-boundary\r\nContent-Disposition: form-data; name=\"content\"\r\n\r\ndata\r\n--test-boundary--\r\n");
        let response = router()
            .oneshot(request(Body::from(body)))
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn oversized_fields_are_rejected_before_remaining_body_is_read() {
        for (field, size) in [("content", 128), ("request", MAX_UPLOAD_METADATA_BYTES + 1)] {
            let header = format!(
                "--test-boundary\r\nContent-Disposition: form-data; name=\"{field}\"\r\n\r\n"
            );
            let frames = vec![
                Ok::<_, Infallible>(Bytes::from(header)),
                Ok(Bytes::from(vec![b'x'; size])),
            ];
            // The remaining network body never arrives. A parser that buffers
            // the whole field before checking its size will hang here.
            let body = Body::from_stream(stream::iter(frames).chain(stream::pending()));
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                router().oneshot(request(body)),
            )
            .await
            .expect("reject without waiting for the rest of the body")
            .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "field {field}");
        }
    }
}
