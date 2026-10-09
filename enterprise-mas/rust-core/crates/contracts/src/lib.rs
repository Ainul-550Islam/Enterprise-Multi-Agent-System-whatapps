//! # mas-contracts
//!
//! Stable external contracts of the platform: the versioned request/response
//! DTOs consumed by API clients and by the Python orchestrator, plus the
//! stable error contract ([`StableApiError`]).
//!
//! Compatibility rules:
//! * additive changes only (new optional fields), never renames/removals,
//! * every DTO exposes `validate()` returning `mas_common::Result<()>`,
//! * all timestamps are [`mas_common::Timestamp`] (RFC 3339 UTC),
//! * all IDs are the typed IDs from `mas-common`.

pub mod agent;
pub mod analytics;
pub mod api;
pub mod api_key;
pub mod approval;
pub mod assistant;
pub mod audit;
pub mod auth;
pub mod connector;
pub mod data_hub;
pub mod errors;
pub mod execution;
pub mod health;
pub mod inbound_webhook;
pub mod notification;
/// OAuth authorization initiation and callback wire DTOs.
pub mod oauth;
pub mod policy;
pub mod rollback;
pub mod search;
pub mod task;
pub mod tool;
pub mod usage;
pub mod webhook;
pub mod workflow;

pub use agent::{
    AgentResponse, CreateAgentRequest, DeployAgentRequest, PublishAgentRequest,
    RollbackAgentRequest, UpdateAgentRequest,
};
pub use analytics::{
    AnalyticsGranularity, AnalyticsOverviewDto, AnalyticsQuery, InsightDto, KpiCardDto,
    PerformancePointDto, PerformanceSeriesDto, TasksByAgentDto, TasksByAgentSliceDto, UsageCostDto,
};
pub use api::{
    ApiEnvelope, ApiListResponse, ApiResponse, ApiVersion, RequestMetadata, TenantRequestContext,
};
pub use api_key::{
    ApiKeyMetadataDto, CreateApiKeyRequest, CreatedApiKeyDto, RevokeApiKeyRequest,
    RotateApiKeyRequest,
};
pub use approval::{ApprovalDecisionRequest, ApprovalQuery, ApprovalResponse};
pub use assistant::{AssistantExecutionDto, AssistantInsightDto, AssistantMessageRequest};
pub use audit::{AuditEventDto, AuditPageDto, AuditQuery};
pub use auth::{
    AuthMeResponse, AuthenticateRequest, AuthenticatedActor, AuthenticationClaims, AuthnMethod,
    AuthorizationContext, PermissionMatrixResponse, ServiceIdentityClaims,
};
pub use connector::{
    ConnectorListQuery, ConnectorPageResponse, ConnectorResponse, ConnectorStatusResponse,
    RegisterConnectorRequest, TestConnectorRequest,
};
pub use data_hub::{
    CreateDataSourceRequest, CreateDocumentRequest, DataSourceDto, DataSourceSyncDto, DocumentDto,
};
pub use errors::StableApiError;
pub use execution::{
    CancelExecutionRequest, ExecutionEventResponse, ExecutionResponse, PauseExecutionRequest,
    ResumeExecutionRequest, StartExecutionRequest,
};
pub use health::{DependencyHealth, LivenessResponse, ReadinessResponse, SystemHealthResponse};
pub use inbound_webhook::{
    InboundWebhookDto, InboundWebhookIngestResponse, InboundWebhookPageDto, InboundWebhookQuery,
    RegisterInboundWebhookRequest,
};
pub use notification::{
    MarkNotificationReadRequest, NotificationDto, NotificationPageDto, NotificationPreferencesDto,
    NotificationQuery,
};
pub use oauth::{OAuthAuthorizationUrlDto, OAuthCallbackAcknowledgement, OAuthCallbackQuery};
pub use policy::{
    CreatePolicyRequest, EvaluatePolicyRequest, PolicyDecisionResponse, PolicyResponse,
    UpdatePolicyRequest,
};
pub use rollback::{RollbackRequest, RollbackResponseDto, WorkflowRollbackTargetDto};
pub use search::{SearchQuery, SearchResponseDto, SearchResultDto};
pub use task::{CancelTaskRequest, RetryTaskRequest, SubmitTaskRequest, TaskQuery, TaskResponse};
pub use tool::{
    InvokeToolRequest, RegisterToolRequest, ToolResponse, ToolResultResponse, ToolSchemaDto,
};
pub use usage::{
    Granularity, QuotaUtilizationDto, UsageQuery, UsageRecordResponse, UsageSummaryResponse,
};
pub use webhook::{
    RegisterWebhookRequest, WebhookDeliveryDto, WebhookDto, WebhookListQuery, WebhookPageDto,
};
pub use workflow::{
    CreateWorkflowRequest, PublishWorkflowRequest, UpdateWorkflowRequest, WorkflowEdgeDto,
    WorkflowGraphDto, WorkflowNodeDto, WorkflowResponse,
};
