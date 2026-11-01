//! API envelope, versions and request metadata shared by all endpoints.

use mas_common::error::AppError;
use mas_common::ids::{OrganizationId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::errors::StableApiError;

/// API version of an envelope/endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApiVersion {
    #[default]
    V1,
}

impl ApiVersion {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::V1 => "v1",
        }
    }

    /// Parses a URL path segment like `v1`.
    pub fn from_path_segment(segment: &str) -> Result<Self> {
        match segment {
            "v1" => Ok(Self::V1),
            other => Err(AppError::invalid_field(
                "api_version",
                "unsupported_version",
                format!("unsupported API version '{other}'"),
            )),
        }
    }
}

impl std::fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Correlation metadata stamped onto every request/response pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestMetadata {
    /// Unique per request (generated at the edge when absent).
    pub request_id: uuid::Uuid,
    /// End-to-end correlation key (client-supplied or generated at intake).
    pub correlation_id: String,
    /// Upstream cause (event id / execution id), when this request is derived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causation_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor_id: Option<UserId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<TenantId>,
    pub api_version: ApiVersion,
    pub received_at: Timestamp,
}

impl RequestMetadata {
    /// Creates metadata for a fresh inbound request.
    pub fn inbound(correlation_id: Option<String>) -> Self {
        Self {
            request_id: uuid::Uuid::now_v7(),
            correlation_id: correlation_id.unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
            causation_id: None,
            actor_id: None,
            tenant_id: None,
            api_version: ApiVersion::V1,
            received_at: Timestamp::now(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validation::validate_non_empty("correlation_id", &self.correlation_id)?;
        validation::validate_length("correlation_id", &self.correlation_id, 1, 256)?;
        Ok(())
    }

    #[must_use]
    pub fn with_actor(mut self, actor_id: UserId, tenant_id: TenantId) -> Self {
        self.actor_id = Some(actor_id);
        self.tenant_id = Some(tenant_id);
        self
    }
}

/// Tenant scoping resolved at the boundary and passed to every use case.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantRequestContext {
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub environment: mas_common::enums::Environment,
}

impl TenantRequestContext {
    pub fn new(
        tenant_id: TenantId,
        organization_id: OrganizationId,
        environment: mas_common::enums::Environment,
    ) -> Result<Self> {
        if tenant_id.is_nil() || organization_id.is_nil() {
            return Err(AppError::invalid_field(
                "tenant_context",
                "required",
                "tenant and organization must be concrete",
            ));
        }
        Ok(Self {
            tenant_id,
            organization_id,
            environment,
        })
    }
}

/// Generic response envelope: exactly one of `data` / `error` is present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiEnvelope<T> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<StableApiError>,
    pub meta: EnvelopeMeta,
}

/// Slim meta block (safe echo of request correlation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeMeta {
    pub request_id: uuid::Uuid,
    pub correlation_id: String,
    pub api_version: ApiVersion,
    pub responded_at: Timestamp,
}

impl EnvelopeMeta {
    #[must_use]
    pub fn for_request(request: &RequestMetadata) -> Self {
        Self {
            request_id: request.request_id,
            correlation_id: request.correlation_id.clone(),
            api_version: request.api_version,
            responded_at: Timestamp::now(),
        }
    }
}

impl<T> ApiEnvelope<T> {
    #[must_use]
    pub fn ok(data: T, request: &RequestMetadata) -> Self {
        Self {
            data: Some(data),
            error: None,
            meta: EnvelopeMeta::for_request(request),
        }
    }

    #[must_use]
    pub fn err(error: StableApiError, request: &RequestMetadata) -> Self {
        Self {
            data: None,
            error: Some(error),
            meta: EnvelopeMeta::for_request(request),
        }
    }

    /// Converts into a `Result` for internal chaining.
    pub fn into_result(self) -> std::result::Result<T, StableApiError>
    where
        T: Serialize + DeserializeOwned,
    {
        match (self.data, self.error) {
            (Some(data), None) => Ok(data),
            (_, Some(error)) => Err(error),
            (None, None) => Err(StableApiError::internal("malformed envelope: no content")),
        }
    }
}

/// Convenience alias kept for handler ergonomics.
pub type ApiResponse<T> = ApiEnvelope<ApiListOrSingle<T>>;

/// Wrapper distinguishing list payloads from single-resource payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ApiListOrSingle<T> {
    Single(T),
    List(Vec<T>),
}

/// List response with cursor pagination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiListResponse<T> {
    pub items: Vec<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

impl<T> ApiListResponse<T> {
    #[must_use]
    pub fn new(items: Vec<T>, next_cursor: Option<String>) -> Self {
        Self { items, next_cursor }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next_cursor: None,
        }
    }

    #[must_use]
    pub fn from_page(page: mas_common::pagination::PageResponse<T>) -> Self {
        Self {
            items: page.items,
            next_cursor: page.next_cursor,
        }
    }
}
