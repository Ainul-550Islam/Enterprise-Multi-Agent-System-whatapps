// PURPOSE:
// End-to-end test: the real Rust bridge against a real Python server.
//
// Every other test in this workspace is one-sided. The bridge's own tests prove
// it encodes a request correctly; the Python suite proves the servicer reads
// one correctly. Neither proves the two implementations actually agree on the
// wire -- and a disagreement there is silent in production, because each side
// looks correct in isolation while the call simply fails.
//
// So this test spawns a real Python server and drives it with a real
// `GrpcAgentBridge` over a real TCP socket.
//
// These integration tests are ignored by default so a Rust-only unit-test run
// remains usable. Run them explicitly with `cargo test -p mas-ai-bridge
// --test e2e_python -- --ignored`. Once selected, missing Python dependencies,
// a missing checkout path, or a server startup failure is a test failure—not a
// green early return.

//! Cross-language end-to-end test.
//!
//! Drives [`GrpcAgentBridge`] against a real Python orchestrator process.
//! Nothing is mocked on the wire: the request is really encoded by Rust, really
//! sent over TCP, really authorized and executed by Python, and the reply is
//! really decoded back into an [`AgentRunVerdict`].
//!
//! Set `MAS_E2E_STUB` to override the server script's location.

use std::ffi::OsString;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use mas_ai_bridge::{GrpcAgentBridge, GrpcAgentBridgeConfig};
use mas_common::enums::Environment;
use mas_common::ids::{
    AgentId, AgentVersionId, OrganizationId, ProjectId, TaskAttemptId, TaskId, TenantId,
};
use mas_domain::{ResourceLimits, TokenBudget};
use mas_execution::agent_bridge::{
    AgentBridge, AgentRunRequest, AgentRunVerdict, AgentRuntimeConfiguration, AgentRuntimeLimits,
    AgentRuntimeSnapshot,
};
use mas_orchestration::runtime_context::RuntimeContext;
use mas_security::secret_string::SecretString;
use uuid::Uuid;

/// Credential the Python stub accepts. Mirrors its `DEFAULT_TOKEN`.
const TOKEN: &str = "bridge-service-token";
/// Subject the Python stub expects. Mirrors its `DEFAULT_SUBJECT`.
const SUBJECT: &str = "rust-core";
/// Shared secret the Python stub verifies against. Mirrors its
/// `DEFAULT_INTERNAL_SECRET`.
///
/// Both sides hold it, which is the point: the Rust worker signs the exact
/// bytes it sends and Python recomputes the MAC over the exact bytes it
/// received. The two are different protobuf implementations, so this constant
/// is what makes the cross-language test a test of the *signature* and not
/// just of the transport.
const INTERNAL_SECRET: &str = "internal-shared-secret";
/// The service id this process signs as. Mirrors the stub's
/// `DEFAULT_INTERNAL_SERVICE`, and is on the servicer's allowlist.
const INTERNAL_SERVICE: &str = "mas-worker";

/// The sibling Python package root, with an explicit override for split checkouts.
fn python_root() -> Result<PathBuf, String> {
    if let Some(root) = std::env::var_os("MAS_E2E_PYTHON_ROOT") {
        let root = PathBuf::from(root)
            .canonicalize()
            .map_err(|error| format!("MAS_E2E_PYTHON_ROOT cannot be resolved: {error}"))?;
        if root.is_dir() {
            return Ok(root);
        }
        return Err(format!(
            "MAS_E2E_PYTHON_ROOT is not a directory: {}",
            root.display()
        ));
    }

    // CARGO_MANIFEST_DIR is rust-core/crates/ai-bridge; the Python package is
    // the sibling enterprise-mas/python-orchestrator, three parents upward.
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../python-orchestrator")
        .canonicalize()
        .map_err(|error| {
            format!("cannot locate sibling python-orchestrator; set MAS_E2E_PYTHON_ROOT: {error}")
        })?;
    if !root.is_dir() {
        return Err(format!(
            "Python orchestrator root is not a directory: {}",
            root.display()
        ));
    }
    Ok(root)
}

/// The Python test server, resolved from the environment or package root.
fn stub_script(root: &Path) -> Result<PathBuf, String> {
    let script = match std::env::var_os("MAS_E2E_STUB") {
        Some(path) => {
            let path = PathBuf::from(path);
            if path.is_absolute() {
                path
            } else {
                std::env::current_dir()
                    .map_err(|error| format!("cannot read current directory: {error}"))?
                    .join(path)
            }
        },
        None => root.join("tests/bridge/stub_server.py"),
    };
    if script.is_file() {
        Ok(script)
    } else {
        Err(format!(
            "Python AgentBridge test server does not exist: {}",
            script.display()
        ))
    }
}

/// Builds a Python import path that resolves this checkout without requiring
/// an editable install. Existing paths are preserved for caller dependencies.
fn python_path(root: &Path) -> Result<OsString, String> {
    let mut paths = vec![root.join("src")];
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        paths.extend(std::env::split_paths(&existing));
    }
    std::env::join_paths(paths).map_err(|error| format!("invalid PYTHONPATH: {error}"))
}

/// Fails with dependency diagnostics instead of turning an unavailable Python
/// bridge into a passing integration test.
fn python_available(root: &Path) -> Result<(), String> {
    let import_path = python_path(root)?;
    let python = std::env::var_os("MAS_E2E_PYTHON").unwrap_or_else(|| "python3".into());
    let output = Command::new(python)
        .arg("-c")
        .arg(
            "import grpc; import mas_orchestrator; \
             from mas_orchestrator.grpc.generated import mas_api_pb2, mas_api_pb2_grpc",
        )
        .env("PYTHONPATH", import_path)
        .current_dir(root)
        .output()
        .map_err(|error| format!("cannot run Python import check: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "Python bridge imports failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// A running Python server, terminated on drop.
///
/// Holding the child in a guard matters: if an assertion panics mid-test the
/// process still has to go, or a stray server keeps a port bound and makes
/// later runs fail for reasons that have nothing to do with the failure.
struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts the Python server and returns once it reports its port.
fn spawn(behaviour: &str, delay: f64) -> Result<Server, String> {
    let root = python_root()?;
    let script = stub_script(&root)?;
    python_available(&root)?;

    let python = std::env::var_os("MAS_E2E_PYTHON").unwrap_or_else(|| "python3".into());
    let mut child = Command::new(python)
        .arg(&script)
        .arg("--behaviour")
        .arg(behaviour)
        .arg("--delay")
        .arg(delay.to_string())
        .env("PYTHONPATH", python_path(&root)?)
        .current_dir(&root)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|error| format!("cannot start Python AgentBridge server: {error}"))?;

    // The server prints its port once it is listening. A reader thread allows
    // a real bounded startup wait; a blocking read_line on this thread would
    // make a broken server hang the integration test forever.
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return Err("Python AgentBridge server stdout was not captured".to_owned());
        },
    };
    let (sender, receiver) = mpsc::sync_channel::<std::result::Result<String, String>>(1);
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = sender.send(Err(
                        "Python AgentBridge server exited before announcing its port".to_owned(),
                    ));
                    return;
                },
                Ok(_) if line.trim().starts_with("PORT=") => {
                    let _ = sender.send(Ok(line));
                    return;
                },
                Ok(_) => {},
                Err(error) => {
                    let _ = sender.send(Err(format!(
                        "cannot read Python AgentBridge server readiness: {error}"
                    )));
                    return;
                },
            }
        }
    });

    let announcement = match receiver.recv_timeout(Duration::from_secs(30)) {
        Ok(announcement) => announcement,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Python AgentBridge server did not become ready within 30 seconds: {error}"
            ));
        },
    };
    let line = match announcement {
        Ok(line) => line,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        },
    };
    let port = match line
        .trim()
        .strip_prefix("PORT=")
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
    {
        Some(port) => port,
        None => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "Python AgentBridge server returned an invalid port: {line:?}"
            ));
        },
    };

    Ok(Server { child, port })
}

/// Builds a bridge pointed at `port`, with the credential the stub accepts.
fn bridge(port: u16) -> GrpcAgentBridgeConfig {
    GrpcAgentBridgeConfig {
        endpoint: format!("http://127.0.0.1:{port}"),
        default_timeout: Duration::from_secs(20),
        max_response_bytes: 8 * 1024 * 1024,
        bearer_token: Some(SecretString::new(TOKEN.to_owned())),
        service_subject: SUBJECT.to_owned(),
        service_issuer: "platform-mtls".to_owned(),
        service_scopes: vec!["execute_agent".to_owned()],
        // The stub is configured with the matching secret, so every call in
        // this suite is really signed and really verified. A bridge that
        // stopped signing, or signed the wrong bytes, would fail here.
        internal_signing_secret: Some(SecretString::new(INTERNAL_SECRET.to_owned())),
        internal_service_id: INTERNAL_SERVICE.to_owned(),
        ..Default::default()
    }
}

/// A bridge that does **not** sign: the negative case.
///
/// The stub is started with the secret configured, so an unsigned call must be
/// refused. That is what proves the server is actually verifying rather than
/// merely checking that a header exists.
fn unsigned_bridge(port: u16) -> GrpcAgentBridgeConfig {
    GrpcAgentBridgeConfig {
        internal_signing_secret: None,
        ..bridge(port)
    }
}

/// A bridge that signs with the wrong secret: the forged case.
fn miskeyed_bridge(port: u16) -> GrpcAgentBridgeConfig {
    GrpcAgentBridgeConfig {
        internal_signing_secret: Some(SecretString::new("not-the-shared-secret".to_owned())),
        ..bridge(port)
    }
}

/// Builds the complete immutable configuration attached to the Rust-owned
/// version selected by each cross-language test request.
fn runtime_snapshot(
    agent_id: AgentId,
    tenant_id: TenantId,
    organization_id: OrganizationId,
    project_id: ProjectId,
    agent_version_id: AgentVersionId,
) -> AgentRuntimeSnapshot {
    AgentRuntimeSnapshot {
        agent_id,
        tenant_id,
        organization_id,
        project_id,
        agent_version_id,
        version_number: 1,
        configuration_checksum: "a".repeat(64),
        name: "E2E Agent".to_owned(),
        slug: "e2e-agent".to_owned(),
        kind: "standard".to_owned(),
        status: "active".to_owned(),
        origin: "user".to_owned(),
        read_only: false,
        configuration: AgentRuntimeConfiguration {
            description: Some("cross-language pinned-version fixture".to_owned()),
            model_hint: Some("test/snapshot-model".to_owned()),
            system_prompt: "Immutable E2E version prompt".to_owned(),
            limits: AgentRuntimeLimits::default(),
            tags: vec!["e2e".to_owned()],
            allowed_tools: vec!["search".to_owned()],
        },
    }
}

/// Builds a properly scoped run request with a pinned Rust snapshot.
fn request() -> AgentRunRequest {
    let tenant_id = TenantId::new();
    let organization_id = OrganizationId::new();
    let project_id = ProjectId::new();
    let agent_id = AgentId::new();
    let agent_version_id = AgentVersionId::new();
    let context = RuntimeContext::new(
        tenant_id,
        organization_id,
        project_id,
        "e2e",
        Environment::Development,
    )
    .expect("context");

    let mut request = AgentRunRequest::from_context(
        &context,
        agent_id,
        serde_json::json!({"prompt": "hello from rust"}),
        ResourceLimits::default(),
        &TokenBudget::unlimited(),
    )
    .expect("request");
    request.agent_version = Some(agent_version_id.to_string());
    request.agent_snapshot = Some(runtime_snapshot(
        agent_id,
        tenant_id,
        organization_id,
        project_id,
        agent_version_id,
    ));
    request
}

/// Converts an unavailable bridge prerequisite into a visible test failure.
macro_rules! require_python {
    ($server:expr) => {{
        $server.unwrap_or_else(|error| {
            panic!(
                "cross-language AgentBridge setup failed: {error}; install \
                 python-orchestrator dependencies and generate the protobuf \
                 bindings, or set MAS_E2E_PYTHON_ROOT / MAS_E2E_STUB"
            )
        })
    }};
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_successful_run_crosses_the_language_boundary() {
    let server = require_python!(spawn("success", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let verdict = bridge.run(request()).await.expect("run");

    match verdict {
        AgentRunVerdict::Succeeded {
            output,
            usage: _,
            model_key,
            finish_reason,
        } => {
            assert_eq!(
                output,
                serde_json::json!("the answer"),
                "output round-tripped"
            );
            assert_eq!(model_key, None, "the runtime did not report a model");
            assert_eq!(finish_reason.as_deref(), Some("completed"));
        },
        AgentRunVerdict::Failed { code, message, .. } => {
            panic!("expected success, got failure {code}: {message}");
        },
    }
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_python_failure_arrives_with_its_machine_readable_code() {
    let server = require_python!(spawn("failure", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let verdict = bridge.run(request()).await.expect("run");

    match verdict {
        AgentRunVerdict::Failed {
            code,
            message,
            retryable,
            ..
        } => {
            assert_eq!(
                code, "model_timeout",
                "the domain code must survive the hop"
            );
            assert!(retryable, "a provider timeout is transient");
            assert!(!message.is_empty(), "the failure carries a message");
        },
        AgentRunVerdict::Succeeded { .. } => panic!("expected failure, got success"),
    }
}

/// Builds a run request under an explicitly chosen scope.
///
/// The default `request()` mints random ids, which is right for the tests above
/// but useless for isolation: to prove a run executed under *this* tenant, the
/// caller has to know which tenant it asked for.
fn scoped_request(
    tenant: Uuid,
    organization: Uuid,
    project: Uuid,
    request_id: Uuid,
    task_id: Uuid,
    task_attempt_id: Uuid,
) -> AgentRunRequest {
    let context = RuntimeContext::new(
        TenantId::from(tenant),
        OrganizationId::from(organization),
        ProjectId::from(project),
        "e2e",
        Environment::Development,
    )
    .expect("context")
    .with_request_id(request_id);

    let agent_id = AgentId::new();
    let agent_version_id = AgentVersionId::new();
    let mut request = AgentRunRequest::from_context(
        &context,
        agent_id,
        serde_json::json!({"prompt": "hello from rust"}),
        ResourceLimits::default(),
        &TokenBudget::unlimited(),
    )
    .expect("request");
    request.agent_version = Some(agent_version_id.to_string());
    request.agent_snapshot = Some(runtime_snapshot(
        agent_id,
        context.tenant_id(),
        context.organization_id(),
        context.project_id(),
        agent_version_id,
    ));
    request.task_id = Some(TaskId::from_uuid(task_id));
    request.task_attempt_id = Some(TaskAttemptId::from_uuid(task_attempt_id));
    request
}

/// Reads the scope the Python side reported back.
fn reported_scope(output: &serde_json::Value) -> serde_json::Value {
    // The stub answers with a JSON document rather than prose, so the caller
    // can compare fields instead of substring-matching a sentence.
    serde_json::from_str(output.as_str().expect("a JSON string")).expect("scope document")
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn each_run_executes_under_the_scope_rust_sent() {
    // Tenant isolation is a property no single side can prove. Rust can assert
    // it puts a tenant on the wire; Python can assert it reads one. Neither can
    // show that the run actually executed under the tenant the caller asked
    // for -- and a mismatch is silent, because both sides behave correctly
    // while the work happens in the wrong tenancy.
    let server = require_python!(spawn("echo_scope", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let tenants = [Uuid::from_u128(1), Uuid::from_u128(2)];
    let organizations = [Uuid::from_u128(11), Uuid::from_u128(12)];
    let projects = [Uuid::from_u128(21), Uuid::from_u128(22)];
    let request_ids = [Uuid::from_u128(31), Uuid::from_u128(32)];
    let task_ids = [Uuid::from_u128(41), Uuid::from_u128(42)];
    let task_attempt_ids = [Uuid::from_u128(51), Uuid::from_u128(52)];

    let mut reported = Vec::new();
    for index in 0..2 {
        let verdict = bridge
            .run(scoped_request(
                tenants[index],
                organizations[index],
                projects[index],
                request_ids[index],
                task_ids[index],
                task_attempt_ids[index],
            ))
            .await
            .expect("run");

        let output = match verdict {
            AgentRunVerdict::Succeeded { output, .. } => output,
            AgentRunVerdict::Failed { code, message, .. } => {
                panic!("expected success, got {code}: {message}")
            },
        };
        reported.push(reported_scope(&output));
    }

    for index in 0..2 {
        let scope = &reported[index];
        assert_eq!(
            scope["tenant_id"].as_str().expect("tenant_id"),
            tenants[index].to_string(),
            "run {index} executed under the caller's tenant"
        );
        assert_eq!(
            scope["organization_id"].as_str().expect("organization_id"),
            organizations[index].to_string(),
            "run {index} executed under the caller's organization"
        );
        assert_eq!(
            scope["project_id"].as_str().expect("project_id"),
            projects[index].to_string(),
            "run {index} executed under the caller's project"
        );
        assert_eq!(
            scope["request_id"].as_str().expect("request_id"),
            request_ids[index].to_string(),
            "run {index} retains the originating request id"
        );
        assert_eq!(
            scope["task_id"].as_str().expect("task_id"),
            task_ids[index].to_string(),
            "run {index} retains the durable task id"
        );
        assert_eq!(
            scope["task_attempt_id"].as_str().expect("task_attempt_id"),
            task_attempt_ids[index].to_string(),
            "run {index} retains the durable worker claim attempt id"
        );
    }

    // The decisive assertion: two runs on one connection must not share scope.
    // A server that cached or leaked context between calls would answer one of
    // these with the other's tenant, and every per-field check above would
    // still have passed for whichever call came first.
    assert_ne!(
        reported[0]["tenant_id"], reported[1]["tenant_id"],
        "concurrent runs on one connection must not share a tenant"
    );
    assert_ne!(
        reported[0]["organization_id"], reported[1]["organization_id"],
        "concurrent runs on one connection must not share an organization"
    );
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_failure_never_carries_a_credential_across_the_wire() {
    // The stub raises an error whose own text embeds an API-key-shaped value
    // and a bearer token. That is not contrived: a provider adapter raises
    // while a credential is in scope, so the key can end up inside the error's
    // own message.
    //
    // This is the one property here that cannot be checked by reading either
    // side alone. Python can prove it redacts; Rust can prove it does not log
    // what it receives. Only crossing the boundary shows whether the redacted
    // text is what actually arrives -- and a leak is silent, because nothing
    // fails, the secret simply travels.
    let server = require_python!(spawn("leak", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let message = match bridge.run(request()).await.expect("run") {
        AgentRunVerdict::Failed { message, .. } => message,
        AgentRunVerdict::Succeeded { .. } => panic!("the leak stub must fail"),
    };

    const LEAKED_KEY: &str = "sk-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const LEAKED_BEARER: &str = "Bearer eyJhbGciOiJIUzI1NiJ9.ZmFrZS1wYXlsb2Fk.c2lnbmF0dXJl";

    assert!(
        !message.contains(LEAKED_KEY),
        "an API key reached the caller: {message}"
    );
    assert!(
        !message.contains(LEAKED_BEARER),
        "a bearer token reached the caller: {message}"
    );
    // The call's own credential must not be echoed back either.
    assert!(
        !message.contains(TOKEN),
        "the caller's own credential was echoed back: {message}"
    );
    assert!(
        !message.is_empty(),
        "redaction must not silence the failure entirely"
    );
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_wrong_credential_is_refused() {
    let server = require_python!(spawn("success", 0.0));
    let mut config = bridge(server.port);
    config.bearer_token = Some(SecretString::new("not-the-token".to_owned()));
    let bridge = GrpcAgentBridge::connect(config).await.expect("connect");

    let error = bridge
        .run(request())
        .await
        .expect_err("a forged credential must not be honoured");

    // The important property is that this is an *authorization* failure and
    // not a success. It must also not be retryable: replaying a rejected
    // credential just burns the caller's retry budget.
    //
    // And it must not be an ExternalService error. That variant is what a
    // transport failure maps to, and it is retryable -- so asserting only on
    // retryability would let "the server was unreachable" pass as "the server
    // rejected us". The test has to tell those apart or it proves nothing.
    assert!(
        !matches!(error, mas_common::AppError::ExternalService { .. }),
        "a rejection must not be reported as a transport failure: {error}"
    );
    assert!(
        !error.is_retryable(),
        "a rejected credential is terminal: {error}"
    );
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_missing_credential_is_refused() {
    let server = require_python!(spawn("success", 0.0));
    let mut config = bridge(server.port);
    config.bearer_token = None;
    let bridge = GrpcAgentBridge::connect(config).await.expect("connect");

    let error = bridge
        .run(request())
        .await
        .expect_err("an unauthenticated call must not be honoured");

    assert!(
        !matches!(error, mas_common::AppError::ExternalService { .. }),
        "a rejection must not be reported as a transport failure: {error}"
    );
    assert!(
        !error.is_retryable(),
        "a missing credential is not transient: {error}"
    );
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_subject_that_disagrees_with_the_credential_is_refused() {
    // The token authenticates the subject "rust-core". Declaring a different
    // subject in the body is exactly the forgery the orchestrator exists to
    // catch: the body is attacker-influenced in a way the bearer credential is
    // not, so the two must be reconciled rather than the body believed.
    let server = require_python!(spawn("success", 0.0));
    let mut config = bridge(server.port);
    config.service_subject = "someone-else".to_owned();
    let bridge = GrpcAgentBridge::connect(config).await.expect("connect");

    let error = bridge
        .run(request())
        .await
        .expect_err("a declared subject must not override the authenticated one");

    assert!(
        !matches!(error, mas_common::AppError::ExternalService { .. }),
        "a rejection must not be reported as a transport failure: {error}"
    );
    assert!(
        !error.is_retryable(),
        "a rejected identity is terminal: {error}"
    );
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_deadline_the_python_side_cannot_meet_is_not_a_success() {
    // A slow server plus a tight Rust deadline. The run may fail with a timeout
    // or be cancelled; what it must never do is return Succeeded, because the
    // platform would then build on output from a run that was cut short.
    let server = require_python!(spawn("success", 5.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let mut request = request();
    request.deadline =
        mas_orchestration::timeout::Deadline::after(Duration::from_millis(400)).expires_at();

    match bridge.run(request).await {
        Ok(AgentRunVerdict::Succeeded { .. }) => {
            panic!("a run stopped by its deadline must not be reported as success")
        },
        Ok(AgentRunVerdict::Failed { code, .. }) => {
            assert!(
                !code.is_empty(),
                "the failure carries a machine-readable code"
            );
        },
        Err(error) => assert!(
            matches!(error, mas_common::AppError::Timeout(_))
                || matches!(error, mas_common::AppError::Cancelled(_)),
            "expected a timeout or cancellation, got {error:?}"
        ),
    }
}

#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_cancelled_run_is_never_reported_as_success() {
    let server = require_python!(spawn("hang", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let mut request = request();
    request.cancellation.cancel();
    request.deadline =
        mas_orchestration::timeout::Deadline::after(Duration::from_millis(500)).expires_at();

    // Whether the run is stopped by the deadline or by the cancellation, the
    // one unacceptable answer is "Succeeded". A platform that records a
    // cancelled run as completed will build on output that does not exist.
    let outcome = bridge.run(request).await;

    match outcome {
        Ok(AgentRunVerdict::Succeeded { .. }) => {
            panic!("a cancelled run must never be reported as success")
        },
        Ok(AgentRunVerdict::Failed { code, .. }) => {
            assert_eq!(
                code, "cancellation_requested",
                "cancellations carry one code"
            );
        },
        Err(error) => assert!(
            matches!(error, mas_common::AppError::Cancelled(_))
                || matches!(error, mas_common::AppError::Timeout(_)),
            "expected a cancellation or timeout, got {error:?}"
        ),
    }
}

/// A correctly signed call crosses the boundary and is accepted.
///
/// This is the load-bearing one. The signature is computed by prost's encoding
/// on this side and verified against protobuf's encoding on the other; if the
/// two serializations disagreed by a single byte, this test would fail with
/// `Unauthenticated` rather than passing silently.
#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_signed_call_is_accepted_across_the_language_boundary() {
    let server = require_python!(spawn("success", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let verdict = bridge
        .run(request())
        .await
        .expect("a signed call must verify");
    assert!(
        matches!(verdict, AgentRunVerdict::Succeeded { .. }),
        "expected the signed call to run, got: {verdict:?}"
    );
}

/// An unsigned call is refused even though it carries a valid bearer token.
///
/// The token proves the caller holds a credential; it says nothing about
/// *this* message. With a secret configured on the receiver, the absence of a
/// signature is a refusal, not a fallback.
#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn an_unsigned_call_is_refused() {
    let server = require_python!(spawn("success", 0.0));
    let bridge = GrpcAgentBridge::connect(unsigned_bridge(server.port))
        .await
        .expect("connect");

    let error = bridge
        .run(request())
        .await
        .expect_err("an unsigned call must be refused");
    let text = error.to_string().to_lowercase();
    assert!(
        text.contains("unauthenticated") || text.contains("forbidden"),
        "expected an authentication refusal, got: {error}"
    );
}

/// A signature computed under the wrong secret is refused.
///
/// The MAC is keyed. A structurally perfect signature produced without the
/// shared secret proves nothing, and must not be accepted.
#[tokio::test]
#[ignore = "requires Python dependencies; run this target with --ignored"]
async fn a_signature_under_the_wrong_secret_is_refused() {
    let server = require_python!(spawn("success", 0.0));
    let bridge = GrpcAgentBridge::connect(miskeyed_bridge(server.port))
        .await
        .expect("connect");

    let error = bridge
        .run(request())
        .await
        .expect_err("a signature under the wrong secret must be refused");
    let text = error.to_string().to_lowercase();
    assert!(
        text.contains("unauthenticated") || text.contains("forbidden"),
        "expected an authentication refusal, got: {error}"
    );
}
