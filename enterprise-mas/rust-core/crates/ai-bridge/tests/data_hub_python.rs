//! Real Rust Data Hub bridge through Python authentication and live pgvector.
//! The Python test listener uses a hashing embedder, not an external provider.
//! Requires MAS_E2E_PYTHON, MAS_TEST_RAG_DATABASE_URL and
//! MAS_TEST_REPLAY_DATABASE_URL. Missing prerequisites are failures.

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use mas_ai_bridge::{GrpcAgentBridgeConfig, GrpcDataHubBridge, GrpcDataHubBridgeConfig};
use mas_common::ids::{OrganizationId, ProjectId, TenantId};
use mas_common::timestamps::Timestamp;
use mas_execution::data_hub_bridge::{
    DataHubBridgePort, DataHubCallContext, DataHubIngestDocumentRequest,
    DataHubPurgeDocumentRequest, DataHubSourceKind,
};
use mas_security::SecretString;
use uuid::Uuid;

struct Server(Child, u16);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn server() -> Server {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../python-orchestrator")
        .canonicalize()
        .expect("python-orchestrator checkout");
    let python = std::env::var_os("MAS_E2E_PYTHON").expect("MAS_E2E_PYTHON is required");
    std::env::var("MAS_TEST_RAG_DATABASE_URL").expect("MAS_TEST_RAG_DATABASE_URL is required");
    std::env::var("MAS_TEST_REPLAY_DATABASE_URL")
        .expect("MAS_TEST_REPLAY_DATABASE_URL is required");
    let mut child = Command::new(python)
        .arg("tests/integration/data_hub_bridge_server.py")
        .current_dir(&root)
        .env("PYTHONPATH", root.join("src"))
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("start Python Data Hub server");
    let stdout = child.stdout.take().expect("server stdout");
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = sender.send(result);
    });
    let mut guard = Server(child, 0);
    let line = receiver
        .recv_timeout(Duration::from_secs(30))
        .expect("Python listener readiness timeout")
        .expect("read listener port");
    guard.1 = line
        .trim()
        .strip_prefix("PORT=")
        .and_then(|value| value.parse().ok())
        .filter(|port| *port != 0)
        .expect("valid listener port");
    guard
}

async fn bridge(port: u16, subject: &str) -> GrpcDataHubBridge {
    GrpcDataHubBridge::connect(GrpcDataHubBridgeConfig {
        agent_bridge: GrpcAgentBridgeConfig {
            endpoint: format!("http://127.0.0.1:{port}"),
            default_timeout: Duration::from_secs(60),
            bearer_token: Some(SecretString::new("test-data-hub-token".to_owned())),
            service_subject: subject.to_owned(),
            service_scopes: vec!["data_hub_ingest".to_owned(), "data_hub_purge".to_owned()],
            internal_signing_secret: Some(SecretString::new(
                "test-data-hub-signing-secret".to_owned(),
            )),
            internal_service_id: "mas-api".to_owned(),
            ..Default::default()
        },
        max_request_bytes: 2 * 1024 * 1024 + 128 * 1024,
    })
    .await
    .expect("connect Data Hub bridge")
}

#[tokio::test]
async fn signed_rust_ingest_retry_and_purge_cross_python_and_live_postgres() {
    let prerequisites = [
        "MAS_E2E_PYTHON",
        "MAS_TEST_RAG_DATABASE_URL",
        "MAS_TEST_REPLAY_DATABASE_URL",
    ];
    let missing: Vec<_> = prerequisites
        .iter()
        .filter(|name| std::env::var_os(name).is_none())
        .copied()
        .collect();
    if !missing.is_empty() {
        eprintln!(
            "skipping live Rust/Python/PostgreSQL Data Hub test; missing {}",
            missing.join(", ")
        );
        return;
    }
    let server = server();
    let bad_subject = bridge(server.1, "mas-api-data-hub").await;
    let bridge = bridge(server.1, "rust-core").await;
    let context = DataHubCallContext::new(
        TenantId::new(),
        OrganizationId::new(),
        ProjectId::new(),
        "cross-language-data-hub",
        Some(Uuid::new_v4().to_string()),
        Timestamp::now()
            .checked_add(Duration::from_secs(120))
            .expect("deadline"),
    )
    .expect("scope");
    let source_id = Uuid::new_v4();
    let document_id = Uuid::new_v4();
    let mut content = b"A signed Rust document crosses into Python.".to_vec();
    content.resize(2 * 1024 * 1024, b' ');
    let request = DataHubIngestDocumentRequest {
        context: context.clone(),
        source_id,
        document_id,
        source_kind: DataHubSourceKind::Upload,
        title: "Cross-language test".to_owned(),
        mime_type: "text/plain".to_owned(),
        content,
        source_uri: None,
        content_sha256: None,
        required_roles: vec!["reader".to_owned()],
        denied_principals: vec![],
        metadata: serde_json::json!({}),
    };
    let error = bad_subject
        .ingest_document(request.clone())
        .await
        .expect_err("mismatched subject must fail");
    assert!(
        !error.is_retryable(),
        "subject rejection must be terminal: {error}"
    );
    let first = bridge
        .ingest_document(request.clone())
        .await
        .expect("ingest at raw byte limit");
    assert_eq!(first.byte_count, 2 * 1024 * 1024);
    assert_eq!(first.chunk_count, 1);
    assert_eq!(first.embedded_count, 1);
    let retry = bridge
        .ingest_document(request)
        .await
        .expect("fresh-nonce retry");
    assert_eq!(retry.chunk_count, 1);
    let purge = DataHubPurgeDocumentRequest {
        context,
        source_id,
        document_id,
    };
    assert_eq!(
        bridge
            .purge_document(purge.clone())
            .await
            .expect("purge")
            .removed_chunks,
        1
    );
    assert_eq!(
        bridge
            .purge_document(purge)
            .await
            .expect("purge retry")
            .removed_chunks,
        0
    );
}
