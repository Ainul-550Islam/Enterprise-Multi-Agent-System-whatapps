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
// It skips, rather than fails, when Python or the orchestrator package is not
// installed, so `cargo test` stays usable in a Rust-only environment. A skipped
// end-to-end test is visible in the run summary; a hard failure would not be.

//! Cross-language end-to-end test.
//!
//! Drives [`GrpcAgentBridge`] against a real Python orchestrator process.
//! Nothing is mocked on the wire: the request is really encoded by Rust, really
//! sent over TCP, really authorized and executed by Python, and the reply is
//! really decoded back into an [`AgentRunVerdict`].
//!
//! Set `MAS_E2E_STUB` to override the server script's location.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use mas_ai_bridge::{GrpcAgentBridge, GrpcAgentBridgeConfig};
use mas_common::enums::Environment;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId};
use mas_domain::{ResourceLimits, TokenBudget};
use mas_execution::agent_bridge::{AgentBridge, AgentRunRequest, AgentRunVerdict};
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

/// The Python harness, resolved from the environment or by convention.
fn stub_script() -> Option<std::path::PathBuf> {
    if let Ok(path) = std::env::var("MAS_E2E_STUB") {
        return Some(std::path::PathBuf::from(path));
    }
    // This file lives at <workspace>/crates/ai-bridge/tests/. `..` walks back
    // to <workspace>, so five levels reach the checkout root that holds
    // `python-orchestrator/` beside the Rust tree. Probed rather than assumed:
    // a wrong guess here once made every test below report "passed" while
    // silently doing nothing.
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../../..");
    let relative = "python-orchestrator/tests/bridge/stub_server.py";
    [
        root.join(relative),
        std::path::PathBuf::from("/home/user").join(relative),
    ]
    .into_iter()
    .find(|base| base.is_file())
}

/// Whether the Python side is usable at all.
fn python_available(script: &std::path::Path) -> bool {
    Command::new("python3")
        .arg("-c")
        .arg("import mas_orchestrator")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
        && script.is_file()
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
fn spawn(behaviour: &str, delay: f64) -> Option<Server> {
    let script = stub_script()?;
    if !python_available(&script) {
        return None;
    }

    let mut child = Command::new("python3")
        .arg(&script)
        .arg("--behaviour")
        .arg(behaviour)
        .arg("--delay")
        .arg(delay.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    // The server prints its port once it is listening. Reading that line is
    // the readiness signal: connecting on a guess would race the bind.
    let stdout = child.stdout.take()?;
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    let deadline = Instant::now() + Duration::from_secs(30);

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) => break,
            Ok(_) => {
                if let Some(value) = line.trim().strip_prefix("PORT=") {
                    if let Ok(port) = value.parse::<u16>() {
                        return Some(Server { child, port });
                    }
                }
            },
            Err(_) => break,
        }
        if Instant::now() > deadline {
            break;
        }
    }

    let _ = child.kill();
    None
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

/// Builds a properly scoped run request.
fn request() -> AgentRunRequest {
    let context = RuntimeContext::new(
        TenantId::new(),
        OrganizationId::new(),
        ProjectId::new(),
        "e2e",
        Environment::Development,
    )
    .expect("context");

    AgentRunRequest::from_context(
        &context,
        AgentId::new(),
        serde_json::json!({"prompt": "hello from rust"}),
        ResourceLimits::default(),
        &TokenBudget::unlimited(),
    )
    .expect("request")
}

/// Skips the test with a visible reason when the Python side is missing.
macro_rules! require_python {
    ($server:expr) => {{
        match $server {
            Some(server) => server,
            None => {
                eprintln!(
                    "SKIPPED: the Python orchestrator is not available; \
                     install it (`pip install -e .`) or set MAS_E2E_STUB"
                );
                return;
            },
        }
    }};
}

#[tokio::test]
async fn a_successful_run_crosses_the_language_boundary() {
    let server = require_python!(spawn("success", 0.0));
    let bridge = GrpcAgentBridge::connect(bridge(server.port))
        .await
        .expect("connect");

    let verdict = bridge.run(request()).await.expect("run");

    match verdict {
        AgentRunVerdict::Succeeded { output, usage: _ } => {
            assert_eq!(
                output,
                serde_json::json!("the answer"),
                "output round-tripped"
            );
        },
        AgentRunVerdict::Failed { code, message, .. } => {
            panic!("expected success, got failure {code}: {message}");
        },
    }
}

#[tokio::test]
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
            usage: _,
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
fn scoped_request(tenant: Uuid, organization: Uuid, project: Uuid) -> AgentRunRequest {
    let context = RuntimeContext::new(
        TenantId::from(tenant),
        OrganizationId::from(organization),
        ProjectId::from(project),
        "e2e",
        Environment::Development,
    )
    .expect("context");

    AgentRunRequest::from_context(
        &context,
        AgentId::new(),
        serde_json::json!({"prompt": "hello from rust"}),
        ResourceLimits::default(),
        &TokenBudget::unlimited(),
    )
    .expect("request")
}

/// Reads the scope the Python side reported back.
fn reported_scope(output: &serde_json::Value) -> serde_json::Value {
    // The stub answers with a JSON document rather than prose, so the caller
    // can compare fields instead of substring-matching a sentence.
    serde_json::from_str(output.as_str().expect("a JSON string")).expect("scope document")
}

#[tokio::test]
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

    let mut reported = Vec::new();
    for index in 0..2 {
        let verdict = bridge
            .run(scoped_request(
                tenants[index],
                organizations[index],
                projects[index],
            ))
            .await
            .expect("run");

        let output = match verdict {
            AgentRunVerdict::Succeeded { output, usage: _ } => output,
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
