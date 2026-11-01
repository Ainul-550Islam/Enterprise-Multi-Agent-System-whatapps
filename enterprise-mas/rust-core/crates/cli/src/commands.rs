//! Command execution: maps parsed subcommands onto the client and the
//! renderer, and owns the process exit-code contract.
//!
//! Exit codes:
//! * `0` — success (or a successful submit even when `--wait` timed out)
//! * `1` — the API answered with a stable error envelope
//! * `2` — transport failure, bad local input (unreadable file, malformed
//!   JSON), or missing scope headers on a scoped route

use mas_common::enums::ExecutionStatus;
use mas_common::ids::{AgentId, WorkflowId};
use mas_contracts::execution::{ExecutionResponse, StartExecutionRequest};

use crate::args::{Command, ExecutionCmd, ListGet, ScheduleCmd, SubmitArgs};
use crate::client::{ClientConfig, MasApiClient, Outcome, Response};
use crate::render::{render_data, render_envelope_error};
use crate::GlobalOpts;

/// Process exit codes (documented in the module header).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// Success.
    Ok = 0,
    /// API-side failure (stable envelope).
    ApiError = 1,
    /// Local/usage/transport failure.
    UsageOrTransport = 2,
}

/// Runs the full command. Prints results to stdout and errors to stderr;
/// returns the intended exit code.
pub async fn run(global: &GlobalOpts, command: &Command) -> ExitCode {
    let client = match client_from(global) {
        Ok(client) => client,
        Err(err) => {
            eprintln!("error: {err}");
            return ExitCode::UsageOrTransport;
        },
    };
    match command {
        Command::Status => status(&client, global).await,
        Command::Execution(cmd) => execution(&client, global, cmd).await,
        Command::Schedule(cmd) => schedule(&client, global, cmd).await,
        Command::Agent(verbs) => scoped_read(&client, global, "agents", verbs).await,
        Command::Workflow(verbs) => scoped_read(&client, global, "workflows", verbs).await,
    }
}

fn client_from(global: &GlobalOpts) -> mas_common::result::Result<MasApiClient> {
    MasApiClient::new(ClientConfig {
        base_url: global.base_url.clone(),
        token: global.token.clone(),
        tenant: global.tenant,
        organization: global.organization,
        correlation_id: global.correlation_id.clone().unwrap_or_default(),
        request_timeout: std::time::Duration::from_secs(30),
    })
}

/// Scoped routes require tenant + organization headers or the API answers
/// 401/400 with VALIDATION — fail locally with the actionable message.
fn scoped_ok(global: &GlobalOpts) -> Result<(), String> {
    if global.tenant.is_none() || global.organization.is_none() {
        return Err("missing scope: pass --tenant and --organization \
             (or MAS_TENANT_ID / MAS_ORGANIZATION_ID)"
            .to_owned());
    }
    Ok(())
}

async fn status(client: &MasApiClient, global: &GlobalOpts) -> ExitCode {
    let live: Outcome<serde_json::Value> = client.get("health/live").await;
    match live {
        Outcome::Ok(_) => println!("live: ok"),
        Outcome::Err(err) => {
            eprintln!("live: DOWN ({})", err.message);
            return ExitCode::UsageOrTransport;
        },
    }
    let ready: Outcome<serde_json::Value> = client.get("health/ready").await;
    match ready {
        Outcome::Ok(body) => {
            let _ = global;
            println!(
                "ready: {}",
                serde_json::to_string(&body.data).unwrap_or_default()
            );
            ExitCode::Ok
        },
        Outcome::Err(_) => {
            println!("ready: not-ready");
            ExitCode::Ok
        },
    }
}

async fn execution(client: &MasApiClient, global: &GlobalOpts, cmd: &ExecutionCmd) -> ExitCode {
    match cmd {
        ExecutionCmd::Submit(args) => {
            if let Err(msg) = scoped_ok(global) {
                eprintln!("error: {msg}");
                return ExitCode::UsageOrTransport;
            }
            submit(client, global, args).await
        },
        ExecutionCmd::List => {
            if let Err(msg) = scoped_ok(global) {
                eprintln!("error: {msg}");
                return ExitCode::UsageOrTransport;
            }
            let outcome: Outcome<Vec<ExecutionResponse>> = client.get("executions").await;
            outcome_print_ref(&outcome, global)
        },
        ExecutionCmd::Get { id } => {
            if let Err(msg) = scoped_ok(global) {
                eprintln!("error: {msg}");
                return ExitCode::UsageOrTransport;
            }
            let outcome: Outcome<ExecutionResponse> = client.get(&format!("executions/{id}")).await;
            outcome_print_ref(&outcome, global)
        },
        ExecutionCmd::Start { id } => transition(client, global, "executions", *id, "start").await,
        ExecutionCmd::Pause { id } => transition(client, global, "executions", *id, "pause").await,
        ExecutionCmd::Resume { id } => {
            transition(client, global, "executions", *id, "resume").await
        },
        ExecutionCmd::Cancel { id } => {
            transition(client, global, "executions", *id, "cancel").await
        },
    }
}

async fn submit(client: &MasApiClient, global: &GlobalOpts, args: &SubmitArgs) -> ExitCode {
    let input = match &args.input {
        Some(raw) => match read_input(raw) {
            Ok(value) => value,
            Err(msg) => {
                eprintln!("error: {msg}");
                return ExitCode::UsageOrTransport;
            },
        },
        None => serde_json::json!({}),
    };
    let idempotency_key = args
        .idempotency_key
        .clone()
        .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::now_v7()));
    let body = StartExecutionRequest {
        workflow_id: args.workflow.map(WorkflowId::from),
        agent_id: args.agent.map(AgentId::from),
        input,
        correlation_id: Some(client.config().correlation_id.clone()),
        idempotency_key: Some(idempotency_key.clone()),
    };
    let outcome: Outcome<ExecutionResponse> = client
        .post(
            &format!("projects/{}/executions", args.project),
            &body,
            Some(&idempotency_key),
        )
        .await;
    let exit = outcome_print_ref(&outcome, global);
    if exit != ExitCode::Ok {
        return exit;
    }
    let Outcome::Ok(Response { data, .. }) = outcome else {
        return exit;
    };
    if args.wait {
        wait_terminal(
            client,
            global,
            &data,
            std::time::Duration::from_secs(args.wait_timeout),
        )
        .await
    } else {
        ExitCode::Ok
    }
}

async fn wait_terminal(
    client: &MasApiClient,
    global: &GlobalOpts,
    submitted: &ExecutionResponse,
    wait_timeout: std::time::Duration,
) -> ExitCode {
    let deadline = tokio::time::Instant::now() + wait_timeout;
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        if tokio::time::Instant::now() >= deadline {
            eprintln!(
                "warning: execution {} still {} after {}s (submitted ok; watch gave up)",
                submitted.id,
                submitted_status(submitted),
                wait_timeout.as_secs(),
            );
            return ExitCode::Ok;
        }
        let outcome: Outcome<ExecutionResponse> =
            client.get(&format!("executions/{}", submitted.id)).await;
        match outcome {
            Outcome::Ok(Response { data, .. }) => {
                if data.status.is_terminal() {
                    println!("{}", render_data(&data, global.format));
                    // Failed executions are still terminal — surface as exit 1
                    // so scripts branch on the shell status, not JSON parsing.
                    return match data.status {
                        ExecutionStatus::Completed => ExitCode::Ok,
                        _ => ExitCode::ApiError,
                    };
                }
                eprintln!("… {}", data.status.as_str());
            },
            Outcome::Err(err) => {
                eprintln!("{}", render_envelope_error(&err));
                if err.code == "TRANSPORT" {
                    return ExitCode::UsageOrTransport;
                }
                return ExitCode::ApiError;
            },
        }
    }
}

fn submitted_status(execution: &ExecutionResponse) -> &'static str {
    execution.status.as_str()
}

async fn transition(
    client: &MasApiClient,
    global: &GlobalOpts,
    resource: &str,
    id: uuid::Uuid,
    verb: &str,
) -> ExitCode {
    if let Err(msg) = scoped_ok(global) {
        eprintln!("error: {msg}");
        return ExitCode::UsageOrTransport;
    }
    let outcome: Outcome<serde_json::Value> =
        client.post_empty(&format!("{resource}/{id}/{verb}")).await;
    outcome_print_ref(&outcome, global)
}

async fn schedule(client: &MasApiClient, global: &GlobalOpts, cmd: &ScheduleCmd) -> ExitCode {
    match cmd {
        ScheduleCmd::List => scoped_read(client, global, "schedules", &ListGet::List).await,
        ScheduleCmd::Get { id } => {
            scoped_read(client, global, "schedules", &ListGet::Get { id: *id }).await
        },
        ScheduleCmd::Pause { id } => transition(client, global, "schedules", *id, "pause").await,
        ScheduleCmd::Resume { id } => transition(client, global, "schedules", *id, "resume").await,
        ScheduleCmd::Disable { id } => {
            transition(client, global, "schedules", *id, "disable").await
        },
    }
}

async fn scoped_read(
    client: &MasApiClient,
    global: &GlobalOpts,
    resource: &str,
    verbs: &ListGet,
) -> ExitCode {
    if let Err(msg) = scoped_ok(global) {
        eprintln!("error: {msg}");
        return ExitCode::UsageOrTransport;
    }
    match verbs {
        ListGet::List => {
            let outcome: Outcome<serde_json::Value> = client.get(resource).await;
            outcome_print_ref(&outcome, global)
        },
        ListGet::Get { id } => {
            let outcome: Outcome<serde_json::Value> = client.get(&format!("{resource}/{id}")).await;
            outcome_print_ref(&outcome, global)
        },
    }
}

fn read_input(raw: &str) -> Result<serde_json::Value, String> {
    let text = if let Some(path) = raw.strip_prefix('@') {
        std::fs::read_to_string(path).map_err(|e| format!("cannot read {path}: {e}"))?
    } else {
        raw.to_owned()
    };
    serde_json::from_str(&text).map_err(|e| format!("input is not valid JSON: {e}"))
}

fn outcome_print_ref<T: serde::Serialize>(outcome: &Outcome<T>, global: &GlobalOpts) -> ExitCode {
    match outcome {
        Outcome::Ok(resp) => {
            println!("{}", render_data(&resp.data, global.format));
            if resp.idempotent_replay {
                eprintln!("(idempotent replay: original submission returned)");
            }
            ExitCode::Ok
        },
        Outcome::Err(err) => {
            eprintln!("{}", render_envelope_error(err));
            if err.code == "TRANSPORT" {
                ExitCode::UsageOrTransport
            } else {
                ExitCode::ApiError
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_reads_inline_and_file() {
        let value = read_input(r#"{"k":1}"#).expect("inline");
        assert_eq!(value["k"], 1);
        let path = std::env::temp_dir().join("mas-cli-input-test.json");
        std::fs::write(&path, r#"{"file": true}"#).expect("write");
        let spec = format!("@{}", path.display());
        let value = read_input(&spec).expect("file");
        assert_eq!(value["file"], true);
        assert!(read_input("{not json").is_err());
    }

    #[test]
    fn scope_guard_blocks_before_network() {
        let global = GlobalOpts {
            base_url: "http://x".to_owned(),
            token: "t".to_owned(),
            tenant: None,
            organization: None,
            correlation_id: None,
            format: crate::Format::Brief,
        };
        assert!(scoped_ok(&global).is_err());
        let scoped = GlobalOpts {
            tenant: Some(uuid::Uuid::nil()),
            organization: Some(uuid::Uuid::nil()),
            ..global
        };
        assert!(scoped_ok(&scoped).is_ok());
    }
}
