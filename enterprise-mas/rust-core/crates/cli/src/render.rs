//! Presentation: `data` payload to stdout; a `StableApiError`-shaped
//! failure → stderr. Both formats are pure functions — zero I/O, fully
//! unit-testable.

use crate::client::ApiFailure;
use crate::Format;

/// Renders a success payload for the operator.
#[must_use]
pub fn render_data<T: serde::Serialize>(data: &T, format: Format) -> String {
    let value = serde_json::to_value(data).unwrap_or(serde_json::Value::Null);
    match format {
        Format::Json => serde_json::to_string_pretty(&value).unwrap_or_else(|_| format!("{value}")),
        Format::Brief => brief(&value),
    }
}

/// Renders an API failure for stderr (`stable code + message + details`).
#[must_use]
pub fn render_envelope_error(failure: &ApiFailure) -> String {
    let mut out = format!("error: {} ({})", failure.message, failure.code);
    if let Some(request_id) = failure.request_id {
        out.push_str(&format!("\nrequest-id: {request_id}"));
    }
    if let Some(details) = &failure.details {
        out.push_str(&format!("\ndetails: {details}"));
    }
    out
}

/// Brief form: top-level scalar/object leaves flattened to `key: value`
/// lines; nested objects inline-compact; arrays shown compactly on one line
/// each (bounded) — an operator should never need `jq` for the happy path.
fn brief(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut lines = Vec::with_capacity(map.len());
            for (key, val) in map {
                lines.push(format!("{key}: {}", compact(val)));
            }
            lines.join("\n")
        },
        serde_json::Value::Array(items) => {
            if items.is_empty() {
                return "(empty list)".to_owned();
            }
            let mut lines = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    serde_json::Value::Object(map) => {
                        let first = map
                            .get("id")
                            .or_else(|| map.get("name"))
                            .map(compact)
                            .unwrap_or_default();
                        let second = map
                            .get("status")
                            .or_else(|| map.get("created_at"))
                            .map(compact)
                            .unwrap_or_default();
                        lines.push(format!("{first}  {second}"));
                    },
                    other => lines.push(compact(other)),
                }
            }
            lines.join("\n")
        },
        other => compact(other),
    }
}

/// Single-line rendering: strings unquoted, objects/arrays compact JSON.
fn compact(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "-".to_owned(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        other => serde_json::to_string(other).unwrap_or_else(|_| "?".to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::ApiFailure;

    #[test]
    fn brief_flattens_top_level_and_lists() {
        let obj = serde_json::json!({"id": "abc", "status": "completed", "usage": {"steps": 3}});
        let out = render_data(&obj, Format::Brief);
        assert!(out.contains("id: abc"), "{out}");
        assert!(out.contains("status: completed"), "{out}");
        assert!(out.contains("usage: {\"steps\":3}"), "{out}");

        let list = serde_json::json!([
            {"id": "a", "status": "running"},
            {"id": "b", "status": "queued"},
        ]);
        let out = render_data(&list, Format::Brief);
        assert!(out.contains("a  running"));
        assert!(out.contains("b  queued"));
        assert_eq!(
            render_data(&serde_json::json!([]), Format::Brief),
            "(empty list)"
        );
    }

    #[test]
    fn json_format_is_pretty() {
        let out = render_data(&serde_json::json!({"a": 1}), Format::Json);
        assert!(out.contains("\n"), "pretty JSON has newlines");
    }

    #[test]
    fn error_render_includes_request_id() {
        let failure = ApiFailure {
            status: 409,
            code: "CONFLICT".to_owned(),
            message: "already running".to_owned(),
            request_id: Some(uuid::Uuid::nil()),
            details: None,
        };
        let out = render_envelope_error(&failure);
        assert!(out.contains("CONFLICT"));
        assert!(out.contains("request-id"));
    }
}
