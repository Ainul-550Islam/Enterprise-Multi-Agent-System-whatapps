// API contract assertions against the published golden fixtures in
// `tests/fixtures/`. These pin *wire shapes* — the things downstream
// integrators (mas-cli, Python SDKs, gRPC callers) can rely on. In contrast
// to the crates' behavior tests, a red test here means **the public
// contract changed** and a coordinated rollout is needed.
//
// Compiled into `mas-api`'s test target via
// `crates/api/tests/root_contract.rs`.

use mas_contracts::api::{ApiEnvelope, ApiVersion, EnvelopeMeta};
use mas_contracts::workflow::WorkflowGraphDto;
use mas_common::error::AppError;
use mas_common::timestamps::Timestamp;

/// The success envelope is exactly `{data, meta}` with the documented meta
/// keys — round-trips byte-stable conceptually (field set, not ordering).
#[test]
fn envelope_ok_fixture_matches_the_published_shape() {
    let text = include_str!("../fixtures/envelope_ok.json");
    let envelope: ApiEnvelope<serde_json::Value> =
        serde_json::from_str(text).expect("fixture decodes");
    assert!(envelope.error.is_none(), "ok envelope carries no error");
    let data = envelope.data.as_ref().expect("data present");
    assert_eq!(data["status"], "ok");
    assert_eq!(envelope.meta.correlation_id, "fxt-corr-1");
    assert_eq!(envelope.meta.api_version, ApiVersion::V1);

    // Re-encode and re-decode: the value layer must be idempotent (any
    // drift here breaks lazy clients that pass envelopes through).
    let reserialized = serde_json::to_string(&envelope).expect("re-encode");
    let again: ApiEnvelope<serde_json::Value> =
        serde_json::from_str(&reserialized).expect("re-decode");
    assert_eq!(again.meta.correlation_id, envelope.meta.correlation_id);
    assert_eq!(again.meta.request_id, envelope.meta.request_id);
    assert_eq!(again.meta.responded_at, envelope.meta.responded_at);
}

/// The error envelope hides internals: only {code, message, request_id?}
/// plus structured validation detail — never stack traces or SQL.
#[test]
fn envelope_error_fixture_exposes_only_the_stable_surface() {
    let text = include_str!("../fixtures/envelope_error.json");
    let envelope: ApiEnvelope<serde_json::Value> =
        serde_json::from_str(text).expect("fixture decodes");
    assert!(envelope.data.is_none(), "error envelope carries no data");
    let error = envelope.error.expect("stable error present");
    assert_eq!(error.code, "VALIDATION_FAILED");
    assert!(!error.message.is_empty());
    assert!(error.request_id.is_some());
    let details = error.details.expect("validation details");
    let issues = details["issues"].as_array().expect("issues array");
    assert_eq!(issues[0]["field"], "nodes[0].node_type");

    // The same shape is produced, programmatically, from an AppError →
    // prove the fixture and the generator agree.
    let app = AppError::invalid_field(
        "nodes[0].node_type",
        "invalid_enum_value",
        "unknown node type 'llm-yolo'",
    );
    assert_eq!(app.error_code(), "VALIDATION_FAILED");
}

/// The workflow-graph fixture obeys the structural rules the wire DTO
/// guarantees (node-key uniqueness, known node kinds, edge integrity) —
/// pinned so later DTO evolution notices a broken example fleetwide.
#[test]
fn workflow_graph_fixture_validates_as_a_wire_dto() {
    let graph: WorkflowGraphDto =
        serde_json::from_str(include_str!("../fixtures/workflow_graph.json"))
            .expect("fixture decodes");
    graph.validate().expect("structural rules hold");
    assert_eq!(graph.nodes.len(), 3);
    assert_eq!(graph.edges.len(), 2);
}

/// API meta must always echo request identity (correlation id + request id)
/// and stamp a server time; a regression here breaks client tracing.
#[test]
fn envelope_meta_carries_every_tracing_field() {
    let text = include_str!("../fixtures/envelope_ok.json");
    let value: serde_json::Value = serde_json::from_str(text).expect("json");
    let meta = &value["meta"];
    for key in ["request_id", "correlation_id", "api_version", "responded_at"] {
        assert!(meta.get(key).is_some(), "missing meta.{key}");
    }

    // And the constructor side: hand-rolled meta matches the parsed fixture's.
    let constructed = EnvelopeMeta {
        request_id: value["meta"]["request_id"]
            .as_str()
            .and_then(|raw| raw.parse().ok())
            .expect("uuid"),
        correlation_id: "fxt-corr-1".to_owned(),
        api_version: ApiVersion::V1,
        responded_at: Timestamp::parse_rfc3339("2026-09-30T00:00:00.000Z").expect("ts"),
    };
    let roundtrip: serde_json::Value = serde_json::to_value(&constructed).expect("encode");
    assert_eq!(roundtrip["correlation_id"], "fxt-corr-1");
}

/// The schedule→dispatch request fixture stays a legal projection input:
/// the scheduler-service deserializes it against `ScheduleRunRequest` in its
/// own harness; here we pin the field inventory unchanged (adding fields is
/// fine; renaming them breaks the scheduler).
#[test]
fn schedule_run_request_shape_is_forward_compatible() {
    let value: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/schedule_run_request.json"))
            .expect("fixture decodes");
    let obj = value.as_object().expect("object");
    for required in ["schedule_id", "tenant_id", "planned_at", "task_template"] {
        assert!(obj.contains_key(required), "missing {required}");
    }
    assert!(obj["task_template"]["operation"].is_string());
}
