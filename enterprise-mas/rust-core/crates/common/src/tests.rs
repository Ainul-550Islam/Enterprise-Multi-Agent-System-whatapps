//! Unit tests for the foundation primitives: IDs, timestamps, enums,
//! validation, pagination, redaction and stable error-code behavior.

use crate::constants;
use crate::enums::{
    AgentStatus, AuditSeverity, ConnectorStatus, DeploymentStatus, Environment, EventStatus,
    ExecutionStatus, ExecutionStepStatus, PolicyDecision, ScheduleStatus, TaskPriority, TaskStatus,
    TenantStatus, ToolStatus, UserStatus, WorkflowStatus,
};
use crate::error::AppError;
use crate::ids::*;
use crate::pagination::{Cursor, PageRequest, PageResponse};
use crate::redaction::{self, SecretRedactor};
use crate::result::ExecutionOutcome;
use crate::timestamps::Timestamp;
use crate::validation;
use http::StatusCode;
use serde_json::json;
use std::str::FromStr;

// ---------------------------------------------------------------------------
// IDs
// ---------------------------------------------------------------------------

#[test]
fn ids_roundtrip_display_fromstr_serde() {
    macro_rules! check {
        ($ty:ty) => {{
            let id = <$ty>::new();
            let text = id.to_string();
            let parsed: $ty = text.parse().expect("parse must succeed");
            assert_eq!(id, parsed);
            let json = serde_json::to_string(&id).expect("serialize");
            let back: $ty = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(id, back);
            assert_eq!(json, format!("\"{text}\""));
            assert!(!id.is_nil());
            assert_eq!(<$ty>::from_uuid(id.into_uuid()), id);
            assert_eq!(id.as_bytes().len(), 16);
            assert_eq!(id.to_database_string(), text);
        }};
    }
    check!(OrganizationId);
    check!(TenantId);
    check!(UserId);
    check!(MembershipId);
    check!(ProjectId);
    check!(AgentId);
    check!(AgentVersionId);
    check!(WorkflowId);
    check!(WorkflowVersionId);
    check!(WorkflowNodeId);
    check!(TaskId);
    check!(TaskAttemptId);
    check!(ExecutionId);
    check!(ExecutionStepId);
    check!(ToolId);
    check!(ConnectorId);
    check!(CredentialId);
    check!(SecretReferenceId);
    check!(PolicyId);
    check!(QuotaId);
    check!(UsageRecordId);
    check!(AuditEventId);
    check!(ScheduleId);
    check!(WebhookId);
    check!(ApiKeyId);
    check!(SessionId);
    check!(EventId);
}

#[test]
fn ids_are_time_ordered_uuid_v7() {
    let first = ExecutionId::new();
    let second = ExecutionId::new();
    assert!(
        first.as_uuid() <= second.as_uuid(),
        "UUIDv7 ids must be k-sortable"
    );
    assert_eq!(first.as_uuid().get_version(), Some(uuid::Version::SortRand));
}

#[test]
fn ids_reject_garbage() {
    let err = TenantId::from_str("not-a-uuid").expect_err("must reject");
    assert!(matches!(err, AppError::Validation { .. }));
    assert_eq!(err.error_code(), "VALIDATION_FAILED");
}

#[test]
fn ids_nil_sentinel() {
    assert!(TaskId::nil().is_nil());
    assert_eq!(
        TaskId::nil().to_string(),
        "00000000-0000-0000-0000-000000000000"
    );
}

// ---------------------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------------------

#[test]
fn timestamp_unix_ms_roundtrip() {
    let now = Timestamp::now();
    let ms = now.to_unix_ms();
    let back = Timestamp::from_unix_ms(ms).expect("roundtrip");
    assert_eq!(back.to_unix_ms(), ms);
}

#[test]
fn timestamp_rfc3339_roundtrip_and_utc_normalization() {
    let ts = Timestamp::parse_rfc3339("2026-09-29T12:30:00.500+06:00").expect("parse");
    assert_eq!(ts.to_rfc3339_millis(), "2026-09-29T06:30:00.500Z");
    let reparsed: Timestamp = ts.to_rfc3339_millis().parse().expect("reparse");
    assert_eq!(ts, reparsed);
}

#[test]
fn timestamp_ordering_and_arithmetic() {
    let a = Timestamp::from_unix_ms(1_000).expect("a");
    let b = Timestamp::from_unix_ms(2_500).expect("b");
    assert!(b.is_after(&a));
    assert!(a.is_before(&b));
    assert_eq!(b.duration_since(&a).expect("diff").as_millis(), 1500);
    let shifted = a
        .checked_add(std::time::Duration::from_millis(500))
        .expect("add");
    assert_eq!(shifted.to_unix_ms(), 1500);
    let back = shifted
        .checked_sub(std::time::Duration::from_millis(500))
        .expect("sub");
    assert_eq!(back, a);
}

#[test]
fn timestamp_rejects_out_of_range() {
    assert!(Timestamp::from_unix_ms(i64::MAX).is_err());
}

#[test]
fn timestamp_serde_forms() {
    let ts = Timestamp::from_unix_ms(1_758_874_353_123).expect("ts");
    let json = serde_json::to_value(ts).expect("value");
    assert_eq!(json.as_str().expect("string"), ts.to_rfc3339_millis());

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Holder {
        #[serde(with = "crate::timestamps::unix_ms_serde")]
        at: Timestamp,
    }
    let holder = Holder { at: ts };
    let json = serde_json::to_value(&holder).expect("value");
    assert_eq!(json["at"], json!(1_758_874_353_123_i64));
    let back: Holder = serde_json::from_value(json).expect("back");
    assert_eq!(back.at, ts);
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

macro_rules! check_enum {
    ($ty:ty, $first:expr) => {{
        for variant in <$ty>::ALL {
            let text = variant.as_str();
            let parsed: $ty = text.parse().expect("parse must succeed");
            assert_eq!(*variant, parsed);
            let json = serde_json::to_string(variant).expect("serialize");
            assert_eq!(json, format!("\"{text}\""));
            let back: $ty = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, *variant);
        }
        let _: &[$ty] = <$ty>::ALL;
        let first: $ty = $first;
        assert!(<$ty>::variant_names().contains(&first.as_str()));
        assert!(format!("{first}").len() > 1);
    }};
}

#[test]
fn enums_roundtrip_all_variants() {
    check_enum!(Environment, Environment::Production);
    check_enum!(UserStatus, UserStatus::Active);
    check_enum!(TenantStatus, TenantStatus::Active);
    check_enum!(AgentStatus, AgentStatus::Draft);
    check_enum!(DeploymentStatus, DeploymentStatus::Deployed);
    check_enum!(WorkflowStatus, WorkflowStatus::Published);
    check_enum!(TaskStatus, TaskStatus::Running);
    check_enum!(TaskPriority, TaskPriority::High);
    check_enum!(ExecutionStatus, ExecutionStatus::Running);
    check_enum!(ExecutionStepStatus, ExecutionStepStatus::Completed);
    check_enum!(ToolStatus, ToolStatus::Published);
    check_enum!(ConnectorStatus, ConnectorStatus::Active);
    check_enum!(PolicyDecision, PolicyDecision::Allow);
    check_enum!(AuditSeverity, AuditSeverity::Critical);
    check_enum!(EventStatus, EventStatus::Delivered);
    check_enum!(ScheduleStatus, ScheduleStatus::Active);
}

#[test]
fn enums_reject_unknown_values() {
    let err = TaskStatus::from_str("runnning").expect_err("typo must not parse");
    assert_eq!(err.error_code(), "VALIDATION_FAILED");
    assert!(err.to_string().contains("running"));
}

#[test]
fn enum_behavior_helpers() {
    assert!(TaskStatus::Completed.is_terminal());
    assert!(TaskStatus::Cancelled.is_terminal());
    assert!(TaskStatus::DeadLettered.is_terminal());
    assert!(!TaskStatus::Queued.is_terminal());
    assert!(TaskPriority::Critical.rank() > TaskPriority::Low.rank());
    assert!(ExecutionStatus::Failed.is_terminal());
    assert!(TenantStatus::Active.is_operational());
    assert!(!TenantStatus::Suspended.is_operational());
    assert!(PolicyDecision::Allow.permits_execution());
    assert!(PolicyDecision::Transform.permits_execution());
    assert!(!PolicyDecision::Deny.permits_execution());
    assert!(Environment::Production.is_production());
    assert!(!Environment::Staging.is_production());
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[test]
fn validation_slug_rules() {
    validation::validate_slug("slug", "my-agent-01").expect("valid");
    validation::validate_slug("slug", "a").expect("short ok");
    for bad in [
        "My-Agent",
        "-lead",
        "trail-",
        "a--b",
        "with space",
        "with_underscore",
        "",
    ] {
        assert!(
            validation::validate_slug("slug", bad).is_err(),
            "must reject: {bad}"
        );
    }
}

#[test]
fn validation_url_rules() {
    validation::validate_url("url", "https://example.com/hook?x=1").expect("https ok");
    validation::validate_url("url", "http://service.internal:8080/path").expect("http ok");
    for bad in [
        "ftp://example.com",
        "https://user:pw@example.com/",
        "notaurl",
        "https://",
    ] {
        assert!(
            validation::validate_url("url", bad).is_err(),
            "must reject: {bad}"
        );
    }
}

#[test]
fn validation_length_and_required() {
    validation::validate_length("name", "abc", 1, 3).expect("bounds ok");
    assert!(validation::validate_length("name", "abcd", 1, 3).is_err());
    assert!(validation::validate_length("name", "", 1, 3).is_err());
    assert!(validation::validate_non_empty("name", "   ").is_err());
    validation::validate_resource_name("name", "My Agent Server").expect("name ok");
    assert!(validation::validate_resource_name("name", "padded ").is_err());
    assert!(validation::validate_resource_name("name", "bad\u{0007}control").is_err());
}

#[test]
fn validation_builder_accumulates_issues() {
    let mut builder = validation::ValidationBuilder::new();
    builder.check(|| validation::validate_non_empty("name", ""));
    builder.check(|| validation::validate_length("slug", "x".repeat(200).as_str(), 1, 64));
    assert!(builder.has_issues());
    let err = builder.finish().expect_err("must fail");
    match err {
        AppError::Validation { issues, .. } => assert_eq!(issues.len(), 2),
        other => panic!("expected validation error, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[test]
fn cursor_roundtrip() {
    let cursor = Cursor::new(1_758_874_353_000, TaskId::new().to_string());
    let encoded = cursor.encode().expect("encode");
    let decoded = Cursor::decode(&encoded).expect("decode");
    assert_eq!(cursor, decoded);
}

#[test]
fn cursor_rejects_invalid_tokens() {
    for bad in ["", "!!!not-base64!!!", "aGVsbG8"] {
        assert!(Cursor::decode(bad).is_err(), "must reject: {bad}");
    }
    // Wrong version must fail loudly.
    let rogue = base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        serde_json::to_vec(&json!({"v": 99, "ts_ms": 1, "id": "x"})).expect("json"),
    );
    assert!(Cursor::decode(&rogue).is_err());
}

#[test]
fn page_request_enforces_bounds() {
    PageRequest::new().validate().expect("empty ok");
    assert!(PageRequest::new().with_limit(0).validate().is_err());
    assert!(PageRequest::new()
        .with_limit(crate::pagination::MAX_PAGE_SIZE + 1)
        .validate()
        .is_err());
    assert_eq!(
        PageRequest::new().effective_limit(),
        crate::pagination::DEFAULT_PAGE_SIZE
    );
}

#[test]
fn page_response_semantics() {
    let page: PageResponse<u32> = PageResponse::with_items(vec![1, 2, 3], Some("next".into()));
    assert!(!page.is_empty());
    assert_eq!(page.len(), 3);
    assert!(page.has_more());
    let mapped = page.map(|value| value * 2);
    assert_eq!(mapped.items, vec![2, 4, 6]);
    let empty: PageResponse<u32> = PageResponse::empty();
    assert!(!empty.has_more());
}

// ---------------------------------------------------------------------------
// Redaction
// ---------------------------------------------------------------------------

#[test]
fn redact_email_keeps_domain_only() {
    assert_eq!(
        redaction::redact_email("alice.smith@example.com"),
        "a***@example.com"
    );
    assert_eq!(redaction::redact_email("not-an-email"), redaction::REDACTED);
}

#[test]
fn redact_token_and_api_key() {
    assert_eq!(
        redaction::redact_token("supersecret"),
        "[REDACTED token, 11 chars]"
    );
    let redacted = redaction::redact_api_key("mas_a1b2c3d4e5f60718293a4b5c6d7e8f90");
    assert!(redacted.starts_with(constants::API_KEY_PREFIX));
    assert!(redacted.contains(redaction::REDACTED));
    assert!(!redacted.contains("d4e5f6"));
    assert_eq!(redaction::redact_api_key("opaque"), redaction::REDACTED);
}

#[test]
fn redact_json_walks_nested_structures() {
    let mut doc = json!({
        "username": "worker-01",
        "password": "hunter2",
        "nested": {
            "client_secret": "abcd",
            "Authorization": "Bearer eyJhbGciOi"
        },
        "items": [
            {"api_key": "mas_a1b2c3d4e5f6"},
            {"note": "reachable at bob@corp.example"}
        ]
    });
    redaction::redact_json_value(&mut doc);
    assert_eq!(doc["username"], "worker-01");
    assert_eq!(doc["password"], redaction::REDACTED);
    assert_eq!(doc["nested"]["client_secret"], redaction::REDACTED);
    assert_eq!(doc["nested"]["Authorization"], redaction::REDACTED);
    assert_eq!(doc["items"][0]["api_key"], redaction::REDACTED);
    // Value-level email redaction (whole-string values) requires the redactor:
    let mut via_redactor = json!({
        "contact": "bob@corp.example",
        "prose": "reachable at bob@corp.example anytime"
    });
    SecretRedactor::new().redact_value(&mut via_redactor);
    assert_eq!(via_redactor["contact"], json!("b***@corp.example"));
    // Prose merely containing an email is intentionally left alone.
    assert_eq!(
        via_redactor["prose"],
        json!("reachable at bob@corp.example anytime")
    );
}

#[test]
fn redactor_honors_extra_keys() {
    let redactor = SecretRedactor::new().with_extra_keys(["tenant-salt"]);
    assert!(redactor.is_sensitive("tenant-salt"));
    assert!(redactor.is_sensitive("Tenant-Salt"));
    assert!(!redactor.is_sensitive("tenant_name"));
    let mut doc = json!({"tenant-salt": "pepper", "tenant_name": "acme"});
    redactor.redact_value(&mut doc);
    assert_eq!(doc["tenant-salt"], redaction::REDACTED);
    assert_eq!(doc["tenant_name"], "acme");
}

#[test]
fn sensitive_key_classification() {
    use crate::redaction::{classify_key, SensitiveField};
    assert_eq!(classify_key("db_password"), Some(SensitiveField::Password));
    assert_eq!(classify_key("X-API-Key"), Some(SensitiveField::ApiKey));
    assert_eq!(classify_key("refresh_token"), Some(SensitiveField::Token));
    assert_eq!(
        classify_key("tls_private_key"),
        Some(SensitiveField::PrivateKey)
    );
    assert_eq!(classify_key("display_name"), None);
}

// ---------------------------------------------------------------------------
// Result helpers
// ---------------------------------------------------------------------------

#[test]
fn execution_outcome_conversion() {
    let ok: crate::Result<i32> = ExecutionOutcome::completed(7).into_result();
    assert_eq!(ok.expect("ok"), 7);
    let deferred: crate::Result<i32> =
        ExecutionOutcome::<i32>::deferred("quota exhausted", Some(500)).into_result();
    assert!(deferred.expect_err("err").is_retryable());
    let cancelled: crate::Result<i32> =
        ExecutionOutcome::<i32>::cancelled("user abort").into_result();
    assert!(!cancelled.expect_err("err").is_retryable());
}

// ---------------------------------------------------------------------------
// Stable error-code behavior (API contract — do not break silently)
// ---------------------------------------------------------------------------

#[test]
fn error_codes_are_stable() {
    let cases: Vec<(AppError, &str, StatusCode, bool)> = vec![
        (
            AppError::validation("bad input"),
            "VALIDATION_FAILED",
            StatusCode::BAD_REQUEST,
            false,
        ),
        (
            AppError::not_found("agent", "123"),
            "RESOURCE_NOT_FOUND",
            StatusCode::NOT_FOUND,
            false,
        ),
        (
            AppError::unauthorized("expired"),
            "UNAUTHENTICATED",
            StatusCode::UNAUTHORIZED,
            false,
        ),
        (
            AppError::forbidden("nope"),
            "FORBIDDEN",
            StatusCode::FORBIDDEN,
            false,
        ),
        (
            AppError::conflict("duplicate"),
            "CONFLICT",
            StatusCode::CONFLICT,
            false,
        ),
        (
            AppError::rate_limited("slow down"),
            "RATE_LIMITED",
            StatusCode::TOO_MANY_REQUESTS,
            true,
        ),
        (
            AppError::timeout("deadline"),
            "TIMEOUT",
            StatusCode::GATEWAY_TIMEOUT,
            true,
        ),
        (
            AppError::cancelled("aborted"),
            "CANCELLED",
            http::StatusCode::from_u16(499).expect("499"),
            false,
        ),
        (
            AppError::database("connection reset"),
            "DATABASE_ERROR",
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
        ),
        (
            AppError::messaging("nats down"),
            "MESSAGING_ERROR",
            StatusCode::INTERNAL_SERVER_ERROR,
            true,
        ),
        (
            AppError::serialization("bad json"),
            "SERIALIZATION_ERROR",
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ),
        (
            AppError::external_service("llm", "500"),
            "EXTERNAL_SERVICE_ERROR",
            StatusCode::BAD_GATEWAY,
            true,
        ),
        (
            AppError::internal("bug #42"),
            "INTERNAL_ERROR",
            StatusCode::INTERNAL_SERVER_ERROR,
            false,
        ),
    ];
    for (err, code, status, retryable) in cases {
        assert_eq!(err.error_code(), code, "code mismatch for {err:?}");
        assert_eq!(err.http_status(), status, "status mismatch for {err:?}");
        assert_eq!(
            err.is_retryable(),
            retryable,
            "retryability mismatch for {err:?}"
        );
    }
}

#[test]
fn public_message_never_leaks_internals() {
    for err in [
        AppError::internal("sqlx pool exhausted at shard-7.internal:5432"),
        AppError::database("deadlock in table tenants_pkey"),
        AppError::messaging("nats://broker.internal:4222 auth failed"),
        AppError::serialization("unknown variant `secret_v1`"),
    ] {
        let message = err.public_message();
        assert!(
            !message.contains("internal:")
                && !message.contains("broker")
                && !message.contains("secret"),
            "leaked internals: {message}"
        );
        assert_eq!(message, "an internal error occurred");
    }
    // Client-actionable errors keep their message.
    assert!(AppError::validation("name is required")
        .public_message()
        .contains("name is required"));
}

#[test]
fn retry_hints() {
    assert!(AppError::rate_limited("x").retry_after_hint().is_some());
    assert!(AppError::timeout("x").retry_after_hint().is_some());
    assert!(AppError::database("x").retry_after_hint().is_none());
}

#[test]
fn with_context_preserves_retryability() {
    let err = AppError::messaging("broker unreachable").with_context("publishing task event");
    assert!(err.to_string().contains("publishing task event"));
    assert!(err.is_retryable());
    let err = AppError::validation("x").with_context("ignored");
    assert_eq!(err.error_code(), "VALIDATION_FAILED");
}
