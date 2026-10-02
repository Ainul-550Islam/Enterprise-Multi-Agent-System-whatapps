//! Protocol versions, limits and queue names shared across the workspace.
//!
//! Values here are *hard* platform guarantees. Configurable behavior lives in
//! `config/*.toml`; anything defined here requires a deliberate code change.

// ---------------------------------------------------------------------------
// Protocol & versioning
// ---------------------------------------------------------------------------

/// Rust↔Python gRPC protocol version. Both sides must agree on the major digit.
pub const PROTOCOL_VERSION: &str = "1";
/// HTTP API version prefix for the first public API.
pub const API_VERSION_V1: &str = "v1";
/// Version stamped into every emitted `EventEnvelope`.
pub const EVENT_ENVELOPE_VERSION: u16 = 1;
/// Version for cursor payloads (page tokens).
pub const CURSOR_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// Field/payload limits
// ---------------------------------------------------------------------------

pub const MAX_NAME_LENGTH: usize = 128;
pub const MAX_SLUG_LENGTH: usize = 64;
pub const MAX_DESCRIPTION_LENGTH: usize = 4 * 1024;
pub const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 128;
pub const MAX_TAG_COUNT: usize = 32;
pub const MAX_TAG_LENGTH: usize = 64;

/// Max JSON body accepted by any API endpoint.
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024; // 1 MiB
/// Max size of a single event payload.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 256 * 1024;
/// Max size of tool invocation arguments.
pub const MAX_TOOL_ARGUMENT_BYTES: usize = 256 * 1024;
/// Max size of a tool result before it must be off-loaded to object storage.
pub const MAX_TOOL_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_AUDIT_METADATA_BYTES: usize = 16 * 1024;

// ---------------------------------------------------------------------------
// Timeouts & retries (defaults; per-request overrides must stay below caps)
// ---------------------------------------------------------------------------

pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_TOOL_TIMEOUT_MS: u64 = 60_000;
pub const DEFAULT_GRPC_TIMEOUT_MS: u64 = 30_000;
pub const DEFAULT_HEARTBEAT_INTERVAL_MS: u64 = 5_000;

pub const DEFAULT_MAX_RETRIES: u32 = 3;
pub const MAX_ALLOWED_RETRIES: u32 = 10;
pub const DEFAULT_RETRY_BACKOFF_MS: u64 = 500;
pub const MAX_RETRY_BACKOFF_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Messaging subjects / queues (NATS JetStream)
// ---------------------------------------------------------------------------

pub const TASKS_QUEUE_SUBJECT: &str = "mas.tasks";
pub const TASKS_DLQ_SUBJECT: &str = "mas.tasks.dlq";
pub const EVENTS_STREAM_NAME: &str = "MAS_EVENTS";
pub const EVENTS_SUBJECT: &str = "mas.events";
pub const WEBHOOKS_SUBJECT: &str = "mas.webhooks";
pub const SCHEDULES_SUBJECT: &str = "mas.schedules";
pub const NOTIFICATIONS_SUBJECT: &str = "mas.notifications";

// ---------------------------------------------------------------------------
// Hard runtime safety limits (can be configured *down*, never up)
// ---------------------------------------------------------------------------

pub const MAX_NODES_PER_WORKFLOW: usize = 500;
pub const MAX_STEPS_PER_EXECUTION: u32 = 1_000;
pub const MAX_TOOL_CALLS_PER_EXECUTION: u32 = 500;
pub const MAX_TOKENS_PER_EXECUTION: u64 = 1_000_000;
pub const MAX_PARALLEL_TASKS_PER_EXECUTION: u32 = 64;
pub const MAX_EXECUTION_DURATION_MS: u64 = 86_400_000; // 24h
pub const HARD_MAX_CONCURRENT_EXECUTIONS_PER_TENANT: u32 = 100;
pub const HARD_MAX_REQUESTS_PER_SECOND_PER_TENANT: u32 = 1_000;
pub const MAX_CHILD_EXECUTIONS: u32 = 256;

// ---------------------------------------------------------------------------
// Security
// ---------------------------------------------------------------------------

/// Prefix of every platform-issued API key (visible, non-secret part).
pub const API_KEY_PREFIX: &str = "mas_";
/// Number of key characters safe to display/persist as lookup prefix.
pub const API_KEY_VISIBLE_PREFIX_LEN: usize = 8;
/// Minimum length of the secret part of an API key.
pub const API_KEY_SECRET_BYTES: usize = 32;
pub const MAX_JWT_SIZE_BYTES: usize = 16 * 1024;
pub const WEBHOOK_SIGNATURE_TOLERANCE_SECONDS: i64 = 300;
