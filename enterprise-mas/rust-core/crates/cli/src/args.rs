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
    /// Inspect agents.
    #[command(subcommand)]
    Agent(ListGet),
    /// Inspect workflows.
    #[command(subcommand)]
    Workflow(ListGet),
}

/// Execution verbs.
#[derive(Debug, Clone, Subcommand)]
pub enum ExecutionCmd {
    /// Submit a new execution (project-scoped, idempotent).
    Submit(SubmitArgs),
    /// List executions (scope-filtered by headers).
    List,
    /// Show one execution.
    Get {
        /// Execution id (UUID).
        id: Uuid,
    },
    /// Transition: `mas execution <start|pause|resume|cancel> <id>`.
    Start {
        /// Execution id.
        id: Uuid,
    },
    /// Pause.
    Pause {
        /// Execution id.
        id: Uuid,
    },
    /// Resume.
    Resume {
        /// Execution id.
        id: Uuid,
    },
    /// Cancel.
    Cancel {
        /// Execution id.
        id: Uuid,
    },
}

/// Schedule verbs (register/update stay in provisioning tooling).
#[derive(Debug, Clone, Subcommand)]
pub enum ScheduleCmd {
    /// List schedules.
    List,
    /// Show one schedule.
    Get {
        /// Schedule id.
        id: Uuid,
    },
    /// Pause.
    Pause {
        /// Schedule id.
        id: Uuid,
    },
    /// Resume.
    Resume {
        /// Schedule id.
        id: Uuid,
    },
    /// Disable (terminal).
    Disable {
        /// Schedule id.
        id: Uuid,
    },
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
    use super::{Command, ExecutionCmd};

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
}
