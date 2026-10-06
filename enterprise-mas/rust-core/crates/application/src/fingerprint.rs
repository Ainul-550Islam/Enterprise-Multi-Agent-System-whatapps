//! PURPOSE:
//! Canonical request fingerprints for durable submission.
//!
//! Idempotency is only safe if "the same request" means the same thing every
//! time. Two submissions of the same logical request can differ in irrelevant
//! ways (key order in a JSON object, a fresh correlation id), so the ledger
//! cannot compare raw request bodies — it compares a **canonical** digest.
//!
//! Layer / ownership
//! -----------------
//! `mas-application`. This is the only place in the platform allowed to
//! compute a submission fingerprint, so the Postgres implementation and the
//! in-memory reference implementation cannot drift apart.
//!
//! Production responsibility
//! -------------------------
//! * Produce a **stable** digest: byte-identical for logically identical
//!   requests regardless of JSON object key order, and stable across releases
//!   (the canonical form is part of the contract).
//! * Produce a **secret-free** digest: only the SHA-256 hex is ever stored or
//!   logged. The request body is hashed, never copied into the ledger.
//! * Exclude volatile identity from the digest (correlation id, actor, clock),
//!   so a client retry with a fresh correlation id still replays.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT include the idempotency key inside its own digest — the key
//!   is the lookup term, not part of the payload.
//! * It must NOT log or return request bodies.
//! * It must NOT treat array order as insignificant: `["a","b"]` and
//!   `["b","a"]` are genuinely different requests.

use mas_common::enums::TaskPriority;
use mas_common::ids::{AgentId, OrganizationId, ProjectId, TenantId, WorkflowId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Byte prefix mixed into every digest so a submission fingerprint can never
/// collide with a response digest or a future digest kind.
const DOMAIN_SUBMISSION: &[u8] = b"mas/submission/v1";
/// Domain separator for response digests.
const DOMAIN_RESPONSE: &[u8] = b"mas/response/v1";

/// Recursively canonicalises a JSON value: object keys sorted lexicographically,
/// array order preserved, numbers and strings left in their serde form.
///
/// Sorting only objects is deliberate. JSON object key order is not
/// meaningful, so it must not affect the digest; array order is meaningful, so
/// it must.
#[must_use]
pub fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut canonical = serde_json::Map::with_capacity(keys.len());
            for key in keys {
                if let Some(nested) = map.get(key) {
                    canonical.insert(key.clone(), canonical_json(nested));
                }
            }
            Value::Object(canonical)
        },
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        other => other.clone(),
    }
}

/// Renders a value to its canonical JSON text form.
pub fn canonical_json_string(value: &Value) -> Result<String> {
    serde_json::to_string(&canonical_json(value)).map_err(|error| {
        mas_common::error::AppError::serialization(format!("canonical json: {error}"))
    })
}

/// SHA-256 over `domain || 0x00 || canonical_json(bytes)`, hex encoded.
fn digest(domain: &[u8], value: &Value) -> Result<String> {
    let canonical = canonical_json_string(value)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update([0u8]);
    hasher.update(canonical.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

/// Everything that makes a submission *the same request*.
///
/// Volatile identity (correlation id, actor, clock) is deliberately absent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FingerprintInput<'a> {
    /// Owning tenant — the same body under a different tenant is a different
    /// request.
    pub tenant_id: TenantId,
    pub organization_id: OrganizationId,
    pub project_id: ProjectId,
    pub workflow_id: Option<WorkflowId>,
    pub agent_id: Option<AgentId>,
    /// The operation the task will carry.
    pub operation: &'a str,
    /// Request body. Hashed, never stored.
    pub input: &'a Value,
    /// Scheduling priority: two otherwise-identical submissions at different
    /// priorities are different instructions.
    pub priority: TaskPriority,
    /// Deadline, when the caller set one.
    pub deadline: Option<Timestamp>,
}

/// Computes request and response digests.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestFingerprint;

impl RequestFingerprint {
    /// The canonical submission fingerprint (64 hex chars).
    pub fn submission(input: &FingerprintInput<'_>) -> Result<String> {
        let canonical = serde_json::json!({
            "tenant_id": input.tenant_id.to_string(),
            "organization_id": input.organization_id.to_string(),
            "project_id": input.project_id.to_string(),
            "workflow_id": input.workflow_id.map(|id| id.to_string()),
            "agent_id": input.agent_id.map(|id| id.to_string()),
            "operation": input.operation,
            "input": input.input,
            "priority": input.priority.as_str(),
            "deadline": input.deadline.map(|ts| ts.to_rfc3339_millis()),
        });
        digest(DOMAIN_SUBMISSION, &canonical)
    }

    /// Digest of a returned response, so a replay can be proven to return the
    /// *same* answer rather than merely an answer.
    pub fn response(value: &Value) -> Result<String> {
        digest(DOMAIN_RESPONSE, value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One fixed identity shared by every variant in a test: a helper that
    /// minted fresh ids per call would make two "identical" requests differ by
    /// scope, which is itself part of the fingerprint.
    fn identity() -> (TenantId, OrganizationId, ProjectId, Option<WorkflowId>) {
        (
            TenantId::from_uuid(uuid::Uuid::from_u128(1)),
            OrganizationId::from_uuid(uuid::Uuid::from_u128(2)),
            ProjectId::from_uuid(uuid::Uuid::from_u128(3)),
            Some(WorkflowId::from_uuid(uuid::Uuid::from_u128(4))),
        )
    }

    fn base<'a>(input: &'a Value) -> FingerprintInput<'a> {
        let (tenant_id, organization_id, project_id, workflow_id) = identity();
        FingerprintInput {
            tenant_id,
            organization_id,
            project_id,
            workflow_id,
            agent_id: None,
            operation: "execution.run",
            input,
            priority: TaskPriority::Normal,
            deadline: None,
        }
    }

    fn parse(text: &str) -> Value {
        serde_json::from_str(text).expect("json")
    }

    #[test]
    fn object_key_order_does_not_change_the_digest() {
        let a = parse(r#"{"b": 1, "a": {"d": 4, "c": [1, 2]}}"#);
        let b = parse(r#"{"a": {"c": [1, 2], "d": 4}, "b": 1}"#);
        assert_eq!(
            RequestFingerprint::submission(&base(&a)).expect("a"),
            RequestFingerprint::submission(&base(&b)).expect("b"),
            "logically identical requests must fingerprint identically"
        );
    }

    #[test]
    fn array_order_does_change_the_digest() {
        let a = parse(r#"[1, 2, 3]"#);
        let b = parse(r#"[3, 2, 1]"#);
        assert_ne!(
            RequestFingerprint::submission(&base(&a)).expect("a"),
            RequestFingerprint::submission(&base(&b)).expect("b"),
            "array order is meaningful and must be preserved"
        );
    }

    #[test]
    fn scope_and_target_change_the_digest() {
        let body = parse(r#"{"prompt": "hi"}"#);
        let base_fp = RequestFingerprint::submission(&base(&body)).expect("base");

        let mut other_tenant = base(&body);
        other_tenant.tenant_id = TenantId::new();
        assert_ne!(
            base_fp,
            RequestFingerprint::submission(&other_tenant).expect("other tenant"),
            "the same body under another tenant is a different request"
        );

        let mut other_priority = base(&body);
        other_priority.priority = TaskPriority::Critical;
        assert_ne!(
            base_fp,
            RequestFingerprint::submission(&other_priority).expect("other priority")
        );

        let mut other_operation = base(&body);
        other_operation.operation = "tool.invoke";
        assert_ne!(
            base_fp,
            RequestFingerprint::submission(&other_operation).expect("other operation")
        );
    }

    #[test]
    fn digests_are_hex_and_do_not_contain_request_text() {
        let body = parse(r#"{"secret": "sk-live-abcdef"}"#);
        let fingerprint = RequestFingerprint::submission(&base(&body)).expect("fp");
        assert_eq!(fingerprint.len(), 64);
        assert!(fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!fingerprint.contains("sk-live"), "no plaintext ever");
        assert!(!fingerprint.contains("secret"));
    }

    #[test]
    fn response_digest_is_separated_from_submission_digest() {
        let body = parse(r#"{"a": 1}"#);
        let submission = RequestFingerprint::submission(&base(&body)).expect("s");
        let response = RequestFingerprint::response(&body).expect("r");
        assert_ne!(
            submission, response,
            "domain separation prevents collisions"
        );
        assert_eq!(
            response,
            RequestFingerprint::response(&parse(r#"{"a":1}"#)).expect("r2")
        );
    }

    #[test]
    fn canonical_json_sorts_objects_and_preserves_arrays() {
        let value = parse(r#"{"z": 1, "a": {"y": [3, 1], "b": true}}"#);
        let canonical = canonical_json(&value);
        let rendered = canonical_json_string(&value).expect("render");
        assert!(
            rendered.starts_with(r#"{"a":{"b":true,"y":[3,1]},"z":1}"#),
            "{rendered}"
        );
        assert_eq!(canonical, parse(&rendered));
    }
}
