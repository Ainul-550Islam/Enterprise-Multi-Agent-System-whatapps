//! Node output validation: contracts that outputs must satisfy before they
//! flow downstream or into execution results.
//!
//! Deliberately NOT a JSON-Schema engine — workflow configs can declare a
//! minimal, deterministic contract (required fields, simple type
//! expectations) that is cheap to check and stable across platform versions.
//! Size/shape sanity applies to *every* output via [`PortabilityLimits`].

use mas_common::constants;
use mas_common::result::Result;
use mas_common::validation::ValidationBuilder;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Cross-cutting limits every node output must satisfy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortabilityLimits {
    /// Serialized size ceiling for a node output.
    pub max_output_bytes: usize,
    /// Maximum nesting depth (0 = unbounded within size limits).
    pub max_depth: u32,
    /// Maximum object keys / array items per container.
    pub max_container_items: usize,
}

impl Default for PortabilityLimits {
    fn default() -> Self {
        Self {
            max_output_bytes: constants::MAX_TOOL_OUTPUT_BYTES.min(constants::MAX_PAYLOAD_BYTES),
            max_depth: 32,
            max_container_items: 10_000,
        }
    }
}

impl PortabilityLimits {
    /// The declared output contract of a node, read from `node.config`
    /// (`output_contract` object) when present.
    pub fn from_node_config(config: &serde_json::Map<String, Value>) -> Self {
        let mut limits = Self::default();
        if let Some(contract) = config.get("output_contract") {
            if let Some(bytes) = contract.get("max_bytes").and_then(Value::as_u64) {
                limits.max_output_bytes = (bytes as usize).min(constants::MAX_PAYLOAD_BYTES);
            }
            if let Some(depth) = contract.get("max_depth").and_then(Value::as_u64) {
                limits.max_depth = (depth as u32).min(128);
            }
        }
        limits
    }
}

/// Validates node outputs.
#[derive(Debug, Clone, Default)]
pub struct OutputValidator;

impl OutputValidator {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Full check: portability limits + declared contract.
    ///
    /// Returns the list of issue messages (empty = valid); callers needing
    /// `Result` semantics use [`Self::validate_or_error`].
    pub fn validate(
        &self,
        node_key: &str,
        output: &Value,
        limits: PortabilityLimits,
    ) -> Vec<String> {
        let mut issues = Vec::new();
        // 1. Serialized size.
        match serde_json::to_vec(output) {
            Ok(bytes) => {
                if bytes.len() > limits.max_output_bytes {
                    issues.push(format!(
                        "output is {} serialized bytes; {} allowed",
                        bytes.len(),
                        limits.max_output_bytes
                    ));
                }
            },
            Err(e) => issues.push(format!("output is not JSON-serializable: {e}")),
        }
        // 2. Structural scan (depth + container sizes), iterative to stay
        //    recursion-safe on hostile inputs.
        let mut stack: Vec<(&Value, u32)> = vec![(output, 0)];
        while let Some((value, depth)) = stack.pop() {
            if limits.max_depth > 0 && depth > limits.max_depth {
                issues.push(format!(
                    "output nests deeper than {} levels",
                    limits.max_depth
                ));
                break;
            }
            match value {
                Value::Array(items) => {
                    if items.len() > limits.max_container_items {
                        issues.push(format!(
                            "array at depth {depth} has {} items; {} allowed",
                            items.len(),
                            limits.max_container_items
                        ));
                        continue;
                    }
                    stack.extend(items.iter().map(|item| (item, depth + 1)));
                },
                Value::Object(map) => {
                    if map.len() > limits.max_container_items {
                        issues.push(format!(
                            "object at depth {depth} has {} keys; {} allowed",
                            map.len(),
                            limits.max_container_items
                        ));
                        continue;
                    }
                    stack.extend(map.values().map(|item| (item, depth + 1)));
                },
                // Reject NUL characters — they corrupt downstream stores.
                Value::String(s) if s.contains('\u{0000}') => {
                    issues.push("output contains NUL characters".to_owned());
                },
                _ => {},
            }
        }
        if !issues.is_empty() {
            tracing::debug!(node_key, issues = issues.len(), "output validation issues");
        }
        issues
    }

    /// Required-field + type check for node configs declaring
    /// `output_contract.fields`, e.g.
    /// `{ "fields": { "summary": "string", "items": "array" } }`.
    /// Only makes sense when the output is an object.
    pub fn contract_check(
        &self,
        config: &serde_json::Map<String, Value>,
        output: &Value,
    ) -> Vec<String> {
        let mut issues = Vec::new();
        let Some(contract) = config.get("output_contract") else {
            return issues;
        };
        let Some(fields) = contract.get("fields").and_then(Value::as_object) else {
            return issues;
        };
        let Value::Object(object) = output else {
            return vec!["contract requires an object output".to_owned()];
        };
        for (field, expected) in fields {
            let Some(name) = expected.as_str() else {
                issues.push(format!("field '{field}' has an invalid type declaration"));
                continue;
            };
            match object.get(field) {
                None => issues.push(format!("required field '{field}' is missing")),
                Some(value) => {
                    let matches = match name {
                        "string" => value.is_string(),
                        "number" => value.is_number(),
                        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
                        "boolean" => value.is_boolean(),
                        "array" => value.is_array(),
                        "object" => value.is_object(),
                        "null" => value.is_null(),
                        "any" => true,
                        other => {
                            issues.push(format!("field '{field}' declares unknown type '{other}'"));
                            true
                        },
                    };
                    if !matches {
                        issues.push(format!("field '{field}' must be of type '{name}'"));
                    }
                },
            }
        }
        issues
    }

    /// Combined convenience: `false`-on-fail variant for hot paths; returns
    /// `Ok(())` when valid, aggregated validation error otherwise.
    pub fn validate_or_error(
        &self,
        node_key: &str,
        config: &serde_json::Map<String, Value>,
        output: &Value,
    ) -> Result<()> {
        let limits = PortabilityLimits::from_node_config(config);
        let mut issues = self.validate(node_key, output, limits);
        issues.extend(self.contract_check(config, output));
        if issues.is_empty() {
            return Ok(());
        }
        let mut builder = ValidationBuilder::new();
        for issue in &issues {
            builder.add("output", "invalid_output", issue.clone());
        }
        builder
            .finish()
            .map_err(|e| e.with_context(format!("node '{node_key}' produced an invalid output")))
    }
}

/// A rejected output annotated for rejection telemetry/audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRejection {
    pub node_key: String,
    pub issues: Vec<String>,
}

impl From<(&str, Vec<String>)> for OutputRejection {
    fn from((node_key, issues): (&str, Vec<String>)) -> Self {
        Self {
            node_key: node_key.to_owned(),
            issues,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn healthy_outputs_pass() {
        let validator = OutputValidator::new();
        let output = serde_json::json!({
            "summary": "ok",
            "items": [1, 2, 3],
            "meta": {"nested": {"deep": true}},
        });
        let issues = validator.validate("n1", &output, PortabilityLimits::default());
        assert!(issues.is_empty(), "unexpected issues: {issues:?}");
    }

    #[test]
    fn depth_and_size_breaches_are_detected() {
        let validator = OutputValidator::new();
        let mut deep = Value::from(1);
        for _ in 0..40 {
            deep = serde_json::json!([deep]);
        }
        let issues = validator.validate(
            "n1",
            &deep,
            PortabilityLimits {
                max_depth: 8,
                ..PortabilityLimits::default()
            },
        );
        assert!(issues.iter().any(|i| i.contains("deeper than")));

        let huge = Value::from("x".repeat(constants::MAX_PAYLOAD_BYTES + 10));
        let issues = validator.validate("n1", &huge, PortabilityLimits::default());
        assert!(issues.iter().any(|i| i.contains("serialized bytes")));
    }

    #[test]
    fn contracts_enforce_required_fields_and_types() {
        let validator = OutputValidator::new();
        let config = serde_json::Map::from_iter([(
            "output_contract".to_owned(),
            serde_json::json!({
                "fields": { "summary": "string", "count": "integer" }
            }),
        )]);
        assert!(validator
            .contract_check(&config, &serde_json::json!({"summary": "s", "count": 3}))
            .is_empty());
        let issues = validator.contract_check(&config, &serde_json::json!({"summary": 5}));
        assert_eq!(issues.len(), 2);
        assert!(issues.iter().any(|i| i.contains("missing")));
        assert!(issues.iter().any(|i| i.contains("type 'string'")));
        assert!(validator
            .validate_or_error(
                "n1",
                &config,
                &serde_json::json!({"summary": "s", "count": 1})
            )
            .is_ok());
        assert!(validator
            .validate_or_error("n1", &config, &serde_json::json!({"summary": "s"}))
            .is_err());
    }

    #[test]
    fn malicious_content_is_flagged() {
        let validator = OutputValidator::new();
        let bad = Value::from("a\u{0000}b");
        let issues = validator.validate("n1", &bad, PortabilityLimits::default());
        assert!(issues.iter().any(|i| i.contains("NUL")));
    }
}
