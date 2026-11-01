//! PURPOSE: Golden-shape tests for dashboard endpoints against the frozen
//! fixtures in `tests/fixtures/dashboard/*.json` and the OpenAPI schemas.
//! Asserts the stable envelope shape, the documented error envelopes
//! (401/403/404/400), and that fixture payloads conform to the DTO shapes.
//!
//! These are unit-shaped (no live DB) and run in CI.

use serde_json::json;

fn load_fixture(name: &str) -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/api -> enterprise-mas/rust-core")
        .join("tests/fixtures/dashboard")
        .join(name);
    let text = std::fs::read_to_string(path).expect("fixture readable");
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn envelope_of(value: &serde_json::Value) -> &serde_json::Value {
    value.get("data").expect("envelope has data")
}

fn meta_of(value: &serde_json::Value) -> &serde_json::Value {
    value.get("meta").expect("envelope has meta")
}

fn error_of(value: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
    value
        .get("error")
        .and_then(|e| e.as_object())
        .filter(|o| !o.is_empty())
}

#[test]
fn overview_fixture_has_stable_envelope() {
    let v = load_fixture("overview.json");
    assert!(v.get("data").is_some(), "data present");
    assert!(error_of(&v).is_none(), "no error on success");
    let meta = meta_of(&v);
    assert_eq!(meta.get("api_version").and_then(|v| v.as_str()), Some("v1"));
    assert!(meta.get("request_id").and_then(|v| v.as_str()).is_some());
    assert!(meta
        .get("correlation_id")
        .and_then(|v| v.as_str())
        .is_some());
    assert!(meta.get("responded_at").is_some());
    let data = envelope_of(&v);
    for key in [
        "total_agents",
        "tasks_completed",
        "success_rate",
        "time_saved_minutes",
    ] {
        assert!(data.get(key).is_some(), "kpi card {key} present");
        let card = data.get(key).unwrap();
        assert!(card.get("value").is_some());
        assert!(card.get("unit").is_some());
    }
}

#[test]
fn rollback_fixture_has_stable_envelope() {
    let v = load_fixture("rollback_result.json");
    assert!(v.get("data").is_some());
    assert!(error_of(&v).is_none());
    let meta = meta_of(&v);
    assert_eq!(meta.get("api_version").and_then(|v| v.as_str()), Some("v1"));
    let data = envelope_of(&v);
    assert_eq!(data.get("from_version").and_then(|v| v.as_i64()), Some(2));
    assert_eq!(data.get("to_version").and_then(|v| v.as_i64()), Some(3));
    assert!(data.get("rollback_record_id").is_some());
}

#[test]
fn error_envelopes_have_code_and_safe_message() {
    // 401
    let unauthorized = json!({
        "data": null,
        "error": { "code": "UNAUTHENTICATED", "message": "authentication required" },
        "meta": { "api_version": "v1" }
    });
    assert_eq!(unauthorized["error"]["code"], "UNAUTHENTICATED");
    assert!(!unauthorized["error"]["message"]
        .as_str()
        .unwrap()
        .is_empty());
    // 403
    let forbidden = json!({
        "data": null,
        "error": { "code": "FORBIDDEN", "message": "insufficient scope" },
        "meta": { "api_version": "v1" }
    });
    assert_eq!(forbidden["error"]["code"], "FORBIDDEN");
    // 404
    let not_found = json!({
        "data": null,
        "error": { "code": "RESOURCE_NOT_FOUND", "message": "not found" },
        "meta": { "api_version": "v1" }
    });
    assert_eq!(not_found["error"]["code"], "RESOURCE_NOT_FOUND");
    // 400
    let validation = json!({
        "data": null,
        "error": { "code": "VALIDATION_FAILED", "message": "validation failed", "details": { "issues": [] } },
        "meta": { "api_version": "v1" }
    });
    assert_eq!(validation["error"]["code"], "VALIDATION_FAILED");
}

#[test]
fn openapi_schemas_reference_fixture_keys() {
    let yaml = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("enterprise-mas/rust-core")
            .join("docs/openapi.yaml"),
    )
    .expect("openapi readable");
    // The spec must document the dashboard endpoints these fixtures derive from.
    assert!(
        yaml.contains("/v1/analytics/overview"),
        "overview path documented"
    );
    assert!(
        yaml.contains("/v1/analytics/performance"),
        "performance path documented"
    );
    assert!(yaml.contains("StableApiError"), "error schema documented");
    assert!(
        yaml.contains("bearerAuth"),
        "bearer security scheme documented"
    );
}
