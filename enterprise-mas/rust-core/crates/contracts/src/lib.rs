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
pub mod api;
pub mod auth;
pub mod connector;
pub mod errors;
pub mod execution;
pub mod health;
pub mod policy;
pub mod task;
pub mod tool;
pub mod usage;
pub mod workflow;

pub use agent::{
    AgentResponse, CreateAgentRequest, DeployAgentRequest, PublishAgentRequest,
    RollbackAgentRequest, UpdateAgentRequest,
};
pub use api::{
    ApiEnvelope, ApiListResponse, ApiResponse, ApiVersion, RequestMetadata, TenantRequestContext,
};
pub use auth::{
    AuthenticateRequest, AuthenticatedActor, AuthenticationClaims, AuthnMethod,
    AuthorizationContext, ServiceIdentityClaims,
};
pub use connector::{
    ConnectorResponse, ConnectorStatusResponse, RegisterConnectorRequest, TestConnectorRequest,
};
pub use errors::StableApiError;
pub use execution::{
    CancelExecutionRequest, ExecutionEventResponse, ExecutionResponse, PauseExecutionRequest,
    ResumeExecutionRequest, StartExecutionRequest,
};
pub use health::{DependencyHealth, LivenessResponse, ReadinessResponse, SystemHealthResponse};
pub use policy::{
    CreatePolicyRequest, EvaluatePolicyRequest, PolicyDecisionResponse, PolicyResponse,
    UpdatePolicyRequest,
};
pub use task::{CancelTaskRequest, RetryTaskRequest, SubmitTaskRequest, TaskQuery, TaskResponse};
pub use tool::{
    InvokeToolRequest, RegisterToolRequest, ToolResponse, ToolResultResponse, ToolSchemaDto,
};
pub use usage::{
    Granularity, QuotaUtilizationDto, UsageQuery, UsageRecordResponse, UsageSummaryResponse,
};
pub use workflow::{
    CreateWorkflowRequest, PublishWorkflowRequest, UpdateWorkflowRequest, WorkflowEdgeDto,
    WorkflowGraphDto, WorkflowNodeDto, WorkflowResponse,
};
