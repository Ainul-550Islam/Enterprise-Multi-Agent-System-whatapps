//! `clap`-parsed argument surface (parse-only; execution lives in
//! [`crate::commands`]). Every global flag has an env fallback so CI and
//! `.env`-driven devs need no shell ceremony.

use clap::{Args, Parser, Subcommand, ValueEnum};
use uuid::Uuid;

/// Global connection + scope flags (apply to every subcommand).
#[derive(Debug, Clone, Args)]
pub struct GlobalOpts {
    /// API base URL (scheme + host, optional :port; no path).
    #[arg(long, env = "MAS_API_URL", default_value = "http://127.0.0.1:8080")]
    pub base_url: String,

    /// Bearer token for the protected routes (dev: `dev-token`).
    #[arg(long, env = "MAS_API_TOKEN", default_value = "dev-token")]
    pub token: String,

    /// Tenant scope header (`x-tenant-id`). Required for scoped routes.
    #[arg(long, env = "MAS_TENANT_ID")]
    pub tenant: Option<Uuid>,

    /// Organization scope header (`x-organization-id`). Scoped routes
    /// require tenant+organization together.
    #[arg(long, env = "MAS_ORGANIZATION_ID")]
    pub organization: Option<Uuid>,

    /// Correlation id echoed to the server and every envelope (`x-correlation-id`).
    /// Defaults to a fresh UUID per invocation.
    #[arg(long, env = "MAS_CORRELATION_ID")]
    pub correlation_id: Option<String>,

    /// Output format.
    #[arg(long, value_enum, default_value = "brief")]
    pub format: Format,
}

/// Output styles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Format {
    /// Key fields, one per line (humans).
    Brief,
    /// Pretty-printed full `data` payload (scripts, `jq`).
    Json,
}

/// `execution submit` payload + wait knobs.
#[derive(Debug, Clone, Args)]
pub struct SubmitArgs {
    /// Project that scopes the execution (path segment).
    #[arg(long)]
    pub project: Uuid,

    /// Workflow to execute (either this or --agent or both, per platform rules).
    #[arg(long)]
    pub workflow: Option<Uuid>,

    /// Agent to execute.
    #[arg(long)]
    pub agent: Option<Uuid>,

    /// Input JSON: inline (`'{"k": 1}'`) or `@path/to/file.json`.
    #[arg(long)]
    pub input: Option<String>,

    /// Idempotency key. Default: `cli-{uuid}` (safe replays on shell retry).
    #[arg(long)]
    pub idempotency_key: Option<String>,

    /// Poll the execution every 500ms until its status is terminal.
    #[arg(long)]
    pub wait: bool,

    /// Max seconds to wait before giving up (still exits 0 on submit).
    #[arg(long, default_value = "300")]
    pub wait_timeout: u64,
}

/// The single id-addressed transition verbs (`POST /v1/{resource}/{id}/{verb}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Verb {
    /// Begin a queued execution.
    Start,
    /// Pause a running execution.
    Pause,
    /// Resume a paused execution (or a paused schedule).
    Resume,
    /// Cancel an execution.
    Cancel,
    /// Pause a schedule.
    PauseSchedule,
    /// Disable a schedule permanently.
    Disable,
}

/// All subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    /// API health probes (live + ready).
    Status,
    /// Manage executions.
    #[command(subcommand)]
    Execution(ExecutionCmd),
    /// Inspect schedules and flip their lifecycle flags.
    #[command(subcommand)]
    Schedule(ScheduleCmd),
    /// Admin operations (org→tenant→project→owner→API key→default plan).
    #[command(subcommand)]
    Admin(AdminCmd),
    /// Inspect agents.
    #[command(subcommand)]
    Agent(ListGet),
    /// Inspect workflows.
    #[command(subcommand)]
    Workflow(ListGet),
    /// Manage API keys (create, rotate, revoke).
    #[command(subcommand)]
    ApiKey(ApiKeyCmd),
}

/// Admin verbs (org→tenant→project→owner→API key→default plan).
#[derive(Debug, Clone, Subcommand)]
pub enum AdminCmd {
    /// Bootstrap a new organization (idempotent: re-runs are no-ops).
    Bootstrap(BootstrapArgs),
    /// Create a new agent under an organization.
    CreateAgent(CreateAgentArgs),
    /// Publish an agent (make it available in the org).
    PublishAgent(PublishAgentArgs),
    /// Create a new workflow under an organization.
    CreateWorkflow(CreateWorkflowArgs),
    /// Create a new API key.
    CreateApiKey(CreateApiKeyArgs),
    /// Rotate an API key.
    RotateApiKey(RotateApiKeyArgs),
    /// Revoke an API key.
    RevokeApiKey(RevokeApiKeyArgs),
}

/// Bootstrap arguments (org→tenant→project→owner→API key→default plan).
#[derive(Debug, Clone, Args)]
pub struct BootstrapArgs {
    /// Organization UUID.
    #[arg(long)]
    pub organization: Uuid,
    /// Tenant UUID under the organization.
    #[arg(long)]
    pub tenant: Uuid,
    /// Project UUID under the tenant.
    #[arg(long)]
    pub project: Uuid,
    /// Owner UUID under the project.
    #[arg(long)]
    pub owner: Uuid,
    /// Default plan ID for the project.
    #[arg(long)]
    pub default_plan: Option<String>,
    /// Idempotency key. Default: `cli-{uuid}` (safe replays on shell retry).
    #[arg(long)]
    pub idempotency_key: Option<String>,
]

/// Create agent arguments.
#[derive(Debug, Clone, Args)]
pub struct CreateAgentArgs {
    /// Agent name.
    #[arg(long)]
    pub name: String,
    /// Agent description.
    #[arg(long)]
    pub description: Option<String>,
    /// Agent scope (tenant or organization).
    #[arg(long)]
    pub scope: Option<String>,
    /// Agent capabilities.
    #[arg(long)]
    pub capabilities: Option<String>,
    /// Idempotency key.
    #[arg(long)]
    pub idempotency_key: Option<String>,
]

/// Publish agent arguments.
#[derive(Debug, Clone, Args)]
pub struct PublishAgentArgs {
    /// Agent ID to publish.
    #[arg(long)]
    pub agent_id: Uuid,
    /// New version to publish.
    #[arg(long)]
    pub version: Option<String>,
    /// Target organization.
    #[arg(long)]
    pub organization: Option<Uuid>,
]

/// Create workflow arguments.
#[derive(Debug, Clone, Args)]
pub struct CreateWorkflowArgs {
    /// Workflow name.
    #[arg(long)]
    pub name: String,
    /// Workflow definition (JSON or @path).
    #[arg(long)]
    pub definition: Option<String>,
    /// Scope (tenant or organization).
    #[arg(long)]
    pub scope: Option<String>,
    /// Idempotency key.
    #[arg(long)]
    pub idempotency_key: Option<String>,
]

/// Create API key arguments.
#[derive(Debug, Clone, Args)]
pub struct CreateApiKeyArgs {
    /// Key name/description.
    #[arg(long)]
    pub name: String,
    /// Key permissions/scopes.
    #[arg(long)]
    pub permissions: Option<String>,
    /// Key expires in seconds (0 = never expires).
    #[arg(long, default_value = "0")]
    pub expires_in: u64,
    /// Idempotency key.
    #[arg(long)]
    pub idempotency_key: Option<String>,
]

/// Rotate API key arguments.
#[derive(Debug, Clone, Args)]
pub struct RotateApiKeyArgs {
    /// Key to rotate.
    #[arg(long)]
    pub key_id: Uuid,
    /// New key name.
    #[arg(long)]
    pub new_name: Option<String>,
    /// Idempotency key.
    #[arg(long)]
    pub idempotency_key: Option<String>,
]

/// Revoke API key arguments.
#[derive(Debug, Clone, Args)]
pub struct RevokeApiKeyArgs {
    /// Key to revoke.
    #[arg(long)]
    pub key_id: Uuid,
    /// Reason for revocation.
    #[arg(long)]
    pub reason: Option<String>,
]

/// API key verbs.
#[derive(Debug, Clone, Subcommand)]
pub enum ApiKeyCmd {
    /// Create a new API key.
    Create(CreateApiKeyArgs),
    /// Rotate an existing API key.
    Rotate(RotateApiKeyArgs),
    /// Revoke an API key.
    Revoke(RevokeApiKeyArgs),
}

/// Shared `list|get <id>` shape for read-only resources.
#[derive(Debug, Clone, Subcommand)]
pub enum ListGet {
    /// List.
    List,
    /// Show one.
    Get {
        /// Resource id.
        id: Uuid,
    },
}

/// Top-level invocation.
#[derive(Debug, Clone, Parser)]
#[command(
    name = "mas",
    version,
    about = "Operator CLI for the multi-agent system (API client)",
    long_about = "Talks to mas-api's public HTTP surface. Global flags may come from \
                  MAS_API_URL / MAS_API_TOKEN / MAS_TENANT_ID / MAS_ORGANIZATION_ID / \
                  MAS_CORRELATION_ID."
)]
pub struct Cli {
    /// Global flags (position-independent).
    #[command(flatten)]
    pub global: GlobalOpts,
    /// The subcommand.
    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    /// Parse helper for tests and embedding.
    #[must_use]
    pub fn parse_from_args<I, T>(args: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        <Self as Parser>::parse_from(args)
    }
}

#[cfg(test)]
mod tests {
    use super::Cli;
    use super::{Command, ExecutionCmd, AdminCmd, ApiKeyCmd};

    #[test]
    fn parses_submit_with_input_file_and_wait() {
        let cli = Cli::parse_from_args([
            "mas",
            "--tenant",
            "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
            "--organization",
            "b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2",
            "execution",
            "submit",
            "--project",
            "c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3",
            "--workflow",
            "d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4",
            "--input",
            "@examples/run.json",
            "--wait",
        ]);
        let Command::Execution(ExecutionCmd::Submit(submit)) = &cli.command else {
            panic!("expected submit, got {:?}", cli.command);
        };
        assert!(submit.wait);
        assert_eq!(submit.input.as_deref(), Some("@examples/run.json"));
        assert!(cli.global.tenant.is_some());
    }

    #[test]
    fn parses_simple_verbs() {
        let cli = Cli::parse_from_args([
            "mas",
            "execution",
            "cancel",
            "9e9e9e9e-9e9e-9e9e-9e9e-9e9e9e9e9e9e",
        ]);
        assert!(matches!(
            cli.command,
            Command::Execution(ExecutionCmd::Cancel { .. })
        ));
        let cli = Cli::parse_from_args(["mas", "schedule", "list"]);
        assert!(matches!(
            cli.command,
            Command::Schedule(super::ScheduleCmd::List)
        ));
    }

    #[test]
    fn parses_admin_bootstrap() {
        let cli = Cli::parse_from_args([
            "mas",
            "admin",
            "bootstrap",
            "--organization",
            "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
            "--tenant",
            "b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2",
            "--project",
            "c3c3c3c3-c3c3-c3c3-c3c3-c3c3c3c3c3c3",
            "--owner",
            "d4d4d4d4-d4d4-d4d4-d4d4-d4d4d4d4d4d4",
        ]);
        matches!(
            cli.command,
            Command::Admin(AdminCmd::Bootstrap(_))
        );
    }

    #[test]
    fn parses_admin_create_agent() {
        let cli = Cli::parse_from_args([
            "mas",
            "admin",
            "create-agent",
            "--name",
            "support-bot",
            "--organization",
            "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
            "--tenant",
            "b2b2b2b2-b2b2-b2b2-b2b2-b2b2b2b2b2b2",
        ]);
        matches!(
            cli.command,
            Command::Admin(AdminCmd::CreateAgent(_))
        );
    }

    #[test]
    fn parses_api_key_create() {
        let cli = Cli::parse_from_args([
            "mas",
            "api-key",
            "create",
            "--name",
            "operator-key",
            "--permissions",
            "exec:read;exec:write",
            "--organization",
            "a1a1a1a1-a1a1-a1a1-a1a1-a1a1a1a1a1a1",
        ]);
        matches!(
            cli.command,
            Command::ApiKey(ApiKeyCmd::Create(_))
        );
    }
}

