//! PURPOSE: The pure domain model: aggregates, their lifecycle invariants and
//! shared value objects.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: make illegal states unrepresentable — every
//! transition is a method that validates before it applies, records who did it
//! and when, and bumps the aggregate's version so a stale writer can be
//! detected downstream.
//!
//! It must NOT:
//!   • depend on HTTP, SQL, messaging or any infrastructure crate — this crate
//!     depends only on `mas-common` and pure data libraries;
//!   • perform I/O of any kind;
//!   • expose a setter that changes state without going through a transition.

//! # mas-domain
//!
//! Pure domain model of the platform: aggregates, their lifecycle invariants
//! and shared value objects.
//!
//! Dependency rule (enforced by manifest): this crate depends only on
//! `mas-common` and pure data libraries. It contains **no** HTTP/database/
//! messaging/LLM logic; behavior beyond validation/state transitions lives in
//! the application and orchestration layers.

pub mod agent;
pub mod agent_capability;
pub mod agent_version;
pub mod api_key;
pub mod audit_event;
pub mod connector;
pub mod credential;
pub mod entitlement;
pub mod execution;
pub mod execution_step;
pub mod idempotency;
pub mod membership;
pub mod notification;
pub mod organization;
pub mod overrides;
pub mod plan;
pub mod policy;
pub mod project;
pub mod quota;
pub mod reservation;
pub mod schedule;
pub mod secret_reference;
pub mod session;
pub mod state_machine;
pub mod subscription;
pub mod task;
pub mod task_attempt;
pub mod tenant;
pub mod tool;
pub mod tool_permission;
pub mod usage;
pub mod user;
pub mod value_objects;
pub mod webhook;
pub mod workflow;
pub mod workflow_edge;
pub mod workflow_node;

// Aggregate-level re-exports for ergonomic imports at application boundaries.
pub use agent::{Agent, AgentConfig, AgentKind};
pub use agent_capability::{AgentCapability, CapabilityType};
pub use agent_version::AgentVersion;
pub use api_key::{ApiKey, ApiKeyMetadataView};
pub use audit_event::{AuditActor, AuditActorKind, AuditComplianceClass, AuditEvent, AuditOutcome};
pub use connector::{Connector, ConnectorAuthMode};
pub use credential::{CredentialMetadata, CredentialType, RotationMetadata};
pub use entitlement::{Entitlement, EntitlementSource, FeatureKey};
pub use execution::{Execution, ResourceUsageSummary};
pub use execution_step::{ExecutionStep, StepError};
pub use idempotency::{IdempotencyDecision, IdempotencyRecord};
pub use membership::{Membership, MembershipRole, MembershipStatus};
pub use notification::{DeliveryState, Notification, NotificationChannel};
pub use organization::{Organization, OrganizationStatus};
pub use overrides::{
    ActorKind as OverrideActorKind, EntitlementOverride, OverrideActor, QuotaOverride,
};
pub use plan::{Plan, PlanEntitlement, PlanQuota, PlanStatus, ResolvedPlan};
pub use policy::{Policy, PolicyScope, PolicyType};
pub use project::{Project, ProjectConfig, ProjectStatus};
pub use quota::{EnforcementMode, Quota, QuotaDimension, QuotaPeriod};
pub use reservation::{QuotaReservation, ReservationState};
pub use schedule::{Schedule, ScheduleKind};
pub use secret_reference::{
    SecretAvailability, SecretProviderKind, SecretReference, SecretRotationState,
};
pub use session::{AuthMethod, Session};
pub use subscription::{Subscription, SubscriptionState};
// `ActorKind` is deliberately NOT re-exported at the crate root: `usage`
// already owns that name (a usage actor is a different concept from a
// transition actor). Reach it as `mas_domain::state_machine::ActorKind`.
pub use state_machine::{
    AggregateKind, CancellationOutcome, CancellationSignal, ExecutionStateMachine, StateTransition,
    TaskStateMachine, TransitionActor, TransitionFailure, TransitionLog,
};
pub use task::Task;
pub use task_attempt::{AttemptStatus, TaskAttempt};
pub use tenant::{IsolationMode, Tenant};
pub use tool::{SafetyClassification, Tool, ToolKind, ToolRuntime};
pub use tool_permission::{ToolInvocationContext, ToolPermission};
pub use usage::{ActorKind, ActorRef, UsageAggregationKey, UsageRecord};
pub use user::User;
pub use value_objects::{Email, ResourceLimits, SafeUrl, SemanticVersion, Slug, TokenBudget};
pub use webhook::{SignatureConfig, WebhookDelivery, WebhookEndpoint, WebhookRetryPolicy};
pub use workflow::{Workflow, WorkflowGraph};
pub use workflow_edge::WorkflowEdge;
pub use workflow_node::{NodeCapability, WorkflowNode, WorkflowNodeType};
