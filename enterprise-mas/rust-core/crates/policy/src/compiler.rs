//! Compiles a domain [`Policy`] definition (JSON DSL) into a validated,
//! canonical, checksummed [`CompiledPolicy`].
//!
//! Strictness is the contract: unknown keys anywhere, unknown effects,
//! missing `rate_limit`/`transform` payloads for their effects, duplicate
//! rule ids, empty glob lists — all are compile errors surfaced at publish
//! time. A published policy can therefore never fail at request time.

use crate::expression::Expr;
use crate::rate_limit::RateLimitSpec;
use mas_common::enums::PolicyDecision;
use mas_common::error::AppError;
use mas_common::ids::PolicyId;
use mas_common::result::Result;
use mas_common::validation;
use mas_domain::{Policy, PolicyScope};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;

/// Rule identifier within a policy (unique per policy, ≤ 64 chars).
pub type RuleId = String;

/// Rule outcome; mirrors [`PolicyDecision`] plus compile-time knowledge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effect {
    Allow,
    Deny,
    RequireApproval,
    RateLimit,
    Transform,
}

impl Effect {
    pub const NAMES: [(&'static str, Effect); 5] = [
        ("allow", Effect::Allow),
        ("deny", Effect::Deny),
        ("require_approval", Effect::RequireApproval),
        ("rate_limit", Effect::RateLimit),
        ("transform", Effect::Transform),
    ];

    pub fn parse(name: &str) -> Result<Self> {
        Self::NAMES
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .map(|(_, effect)| *effect)
            .ok_or_else(|| {
                AppError::invalid_field(
                    "rule.effect",
                    "unknown_effect",
                    format!("'{name}' is not a rule effect"),
                )
            })
    }

    #[must_use]
    pub const fn decision(self) -> PolicyDecision {
        match self {
            Effect::Allow => PolicyDecision::Allow,
            Effect::Deny => PolicyDecision::Deny,
            Effect::RequireApproval => PolicyDecision::RequireApproval,
            Effect::RateLimit => PolicyDecision::RateLimit,
            Effect::Transform => PolicyDecision::Transform,
        }
    }
}

impl fmt::Display for Effect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(self.decision().as_str())
    }
}

/// Within-policy combination of matching rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum Combining {
    /// Any matching deny rule wins (secure default).
    #[default]
    DenyOverrides,
    /// Within this policy: first matching rule (array order) decides.
    FirstApplicable,
}

/// A validated, immutable rule.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompiledRule {
    pub id: RuleId,
    pub effect: Effect,
    /// Action globs (`tool.invoke`, `agent.*`, `*`); at least one.
    pub actions: Vec<String>,
    /// Resource globs; at least one.
    pub resources: Vec<String>,
    pub when: Expr,
    /// Present iff `effect == Effect::RateLimit`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimitSpec>,
    /// Attribute merge patch; present iff `effect == Effect::Transform`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CompiledRule {
    /// Whether this rule applies to the given action/resource strings, using
    /// the same linear glob matcher as conditions (`*` and `?` wildcards;
    /// every other character is literal).
    #[must_use]
    pub fn applies_to(&self, action: &str, resource: &str) -> bool {
        self.actions
            .iter()
            .any(|pattern| crate::expression::glob_match(action, pattern))
            && self
                .resources
                .iter()
                .any(|pattern| crate::expression::glob_match(resource, pattern))
    }
}

/// Published-form policy: validated + canonical + fingerprinted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledPolicy {
    pub policy_id: PolicyId,
    pub version: u64,
    pub priority: i32,
    pub scope: PolicyScope,
    pub combining: Combining,
    /// Vote when no rule matches inside this policy (allow or deny only).
    pub default_effect: Effect,
    pub rules: Vec<CompiledRule>,
    /// sha256 hex of the canonical definition — set into
    /// [`Policy::compiled_checksum`] by [`compile`].
    pub checksum: String,
}

/// sha256 hex of the canonical definition of `policy` (stable across peers).
#[must_use]
pub fn definition_checksum(definition: &Value) -> String {
    let canonical = canonicalize(definition);
    let bytes = serde_json::to_vec(&canonical).unwrap_or_default();
    hex::encode(Sha256::digest(bytes))
}

/// Recursively sorts object keys; arrays give each element a canonical slot,
/// and `rules` arrays are additionally ordered by rule `id` so rule order in
/// a definition cannot move the checksum.
fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut sorted = Map::new();
            for (key, item) in map {
                sorted.insert(key.clone(), canonicalize(item));
            }
            if let Some(Value::Array(rules)) = map.get("rules") {
                let mut canonical_rules: Vec<Value> = rules.iter().map(canonicalize).collect();
                canonical_rules.sort_by_key(|rule| {
                    rule.get("id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned()
                });
                sorted.insert("rules".to_owned(), Value::Array(canonical_rules));
            }
            // serde_json Maps preserve insertion order for canonical dumps.
            Value::Object(sorted.into_iter().collect::<Map<_, _>>())
        },
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        other => other.clone(),
    }
}

/// Compiles (validates + fingerprints) a policy definition for publication.
///
/// On success the caller can `policy.publish(compiled.checksum)`.
pub fn compile(policy: &Policy) -> Result<CompiledPolicy> {
    let object = policy.definition.as_object().ok_or_else(|| {
        AppError::invalid_field(
            "definition",
            "invalid_format",
            "definition must be an object",
        )
    })?;

    // -- strict key envelope ------------------------------------------------
    const TOP_KEYS: [&str; 4] = ["version", "combining", "default_effect", "rules"];
    for key in object.keys() {
        if !TOP_KEYS.contains(&key.as_str()) {
            return Err(AppError::invalid_field(
                format!("definition.{key}"),
                "unknown_key",
                "unknown top-level key in policy definition",
            ));
        }
    }
    if let Some(version) = object.get("version") {
        if version.as_u64() != Some(1) {
            return Err(AppError::invalid_field(
                "definition.version",
                "unsupported_version",
                "only DSL version 1 is supported",
            ));
        }
    }
    let combining = match object.get("combining").and_then(Value::as_str) {
        None => Combining::DenyOverrides,
        Some("deny_overrides") => Combining::DenyOverrides,
        Some("first_applicable") => Combining::FirstApplicable,
        Some(other) => {
            return Err(AppError::invalid_field(
                "definition.combining",
                "unknown_combining",
                format!("'{other}' is not a combining strategy"),
            ));
        },
    };
    let default_effect = match object.get("default_effect").and_then(Value::as_str) {
        None => Effect::Deny, // secure default
        Some(raw) => {
            let effect = Effect::parse(raw)?;
            if matches!(effect, Effect::Allow | Effect::Deny) {
                effect
            } else {
                return Err(AppError::invalid_field(
                    "definition.default_effect",
                    "invalid_default",
                    "default_effect must be 'allow' or 'deny'",
                ));
            }
        },
    };

    let rules_raw = object
        .get("rules")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::invalid_field(
                "definition.rules",
                "required",
                "definition needs a rules array",
            )
        })?;
    if rules_raw.is_empty() {
        return Err(AppError::invalid_field(
            "definition.rules",
            "empty",
            "a policy needs at least one rule",
        ));
    }

    let mut seen_ids = BTreeSet::new();
    let mut rules = Vec::with_capacity(rules_raw.len());
    for raw in rules_raw {
        let rule = compile_rule(raw)?;
        if !seen_ids.insert(rule.id.clone()) {
            return Err(AppError::invalid_field(
                "definition.rules",
                "duplicate_rule_id",
                format!("rule id '{}' is used twice", rule.id),
            ));
        }
        rules.push(rule);
    }

    Ok(CompiledPolicy {
        policy_id: policy.id,
        version: policy.version,
        priority: policy.priority,
        scope: policy.scope,
        combining,
        default_effect,
        rules,
        checksum: definition_checksum(&policy.definition),
    })
}

const RULE_KEYS: [&str; 8] = [
    "id",
    "effect",
    "actions",
    "resources",
    "when",
    "rate_limit",
    "transform",
    "reason",
];

fn compile_rule(raw: &Value) -> Result<CompiledRule> {
    let object = raw.as_object().ok_or_else(|| {
        AppError::invalid_field("rules", "invalid_format", "each rule must be an object")
    })?;
    for key in object.keys() {
        if !RULE_KEYS.contains(&key.as_str()) {
            return Err(AppError::invalid_field(
                format!("rules.{key}"),
                "unknown_key",
                format!("'{key}' is not a rule key"),
            ));
        }
    }
    let id = object
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    validation::validate_non_empty("rule.id", &id)?;
    if id.len() > 64 {
        return Err(AppError::invalid_field(
            "rule.id",
            "too_long",
            "rule ids are at most 64 characters",
        ));
    }
    let effect = Effect::parse(
        object
            .get("effect")
            .and_then(Value::as_str)
            .unwrap_or_default(),
    )?;

    let globs = |key: &str, default: &[&str]| -> Result<Vec<String>> {
        match object.get(key) {
            None => Ok(default.iter().map(|s| (*s).to_owned()).collect()),
            Some(Value::Array(items)) => {
                let mut out = Vec::with_capacity(items.len());
                for item in items {
                    let pattern = item.as_str().ok_or_else(|| {
                        AppError::invalid_field(
                            format!("rules.{key}"),
                            "invalid_type",
                            "glob patterns must be strings",
                        )
                    })?;
                    if pattern.trim().is_empty() {
                        return Err(AppError::invalid_field(
                            format!("rules.{key}"),
                            "empty",
                            "glob patterns must not be empty",
                        ));
                    }
                    out.push(pattern.to_owned());
                }
                if out.is_empty() {
                    return Err(AppError::invalid_field(
                        format!("rules.{key}"),
                        "empty",
                        "glob lists must not be empty",
                    ));
                }
                Ok(out)
            },
            Some(_) => Err(AppError::invalid_field(
                format!("rules.{key}"),
                "invalid_type",
                "expects an array of glob strings",
            )),
        }
    };
    let actions = globs("actions", &["*"])?;
    let resources = globs("resources", &["*"])?;

    let rate_limit = match object.get("rate_limit") {
        Some(raw) => Some(RateLimitSpec::parse(raw)?),
        None => None,
    };
    let transform = match object.get("transform") {
        Some(Value::Object(map)) if !map.is_empty() => Some(map.clone()),
        Some(_) => {
            return Err(AppError::invalid_field(
                "rules.transform",
                "invalid_format",
                "transform must be a non-empty object",
            ));
        },
        None => None,
    };

    match effect {
        Effect::RateLimit if rate_limit.is_none() => {
            return Err(AppError::invalid_field(
                "rules.rate_limit",
                "required",
                "rate_limit rules need a rate_limit block",
            ));
        },
        Effect::Transform if transform.is_none() => {
            return Err(AppError::invalid_field(
                "rules.transform",
                "required",
                "transform rules need a transform patch",
            ));
        },
        Effect::RateLimit | Effect::Transform => {},
        _ if rate_limit.is_some() || transform.is_some() => {
            return Err(AppError::invalid_field(
                "rules",
                "incoherent_effect",
                "rate_limit/transform payloads belong to their effects only",
            ));
        },
        _ => {},
    }

    let reason = object
        .get("reason")
        .and_then(Value::as_str)
        .map(str::to_owned);

    Ok(CompiledRule {
        id,
        effect,
        actions,
        resources,
        when: Expr::compile(object.get("when"))?,
        rate_limit,
        transform,
        reason,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};
    use mas_domain::{PolicyScope as Scope, PolicyType};
    use serde_json::json;

    fn policy(definition: Value) -> Policy {
        Policy::new(
            Some(TenantId::new()),
            Some(OrganizationId::new()),
            Some(ProjectId::new()),
            "tool-guard",
            PolicyType::Authorization,
            Scope::Project,
            10,
            definition,
        )
        .expect("policy")
    }

    fn valid_definition() -> Value {
        json!({
            "version": 1,
            "default_effect": "deny",
            "rules": [
                {"id": "allow-http", "effect": "allow",
                 "actions": ["tool.invoke"],
                 "resources": ["tool:*"],
                 "when": {"eq": ["environment", "development"]}},
                {"id": "rate-tools", "effect": "rate_limit",
                 "actions": ["tool.invoke"],
                 "rate_limit": {"max_tokens": 10, "refill_per_minute": 60, "key": "subject.actor"}},
                {"id": "patch-cost", "effect": "transform",
                 "transform": {"cost_class": "standard"}},
                {"id": "deny-prod", "effect": "deny",
                 "when": {"eq": ["environment", "production"]},
                 "reason": "blocked in prod"}
            ]
        })
    }

    #[test]
    fn compiles_a_happy_path_policy() {
        let policy = policy(valid_definition());
        let compiled = compile(&policy).expect("compiles");
        assert_eq!(compiled.rules.len(), 4);
        assert_eq!(compiled.default_effect, Effect::Deny);
        assert_eq!(compiled.checksum, definition_checksum(&policy.definition));
        let checksum_a = definition_checksum(&policy.definition);
        let mut reordered = valid_definition();
        reordered
            .get_mut("rules")
            .and_then(Value::as_array_mut)
            .expect("rules")
            .reverse();
        assert_eq!(
            checksum_a,
            definition_checksum(&reordered),
            "checksum insensitive to rule order"
        );
    }

    #[test]
    fn strictness_rejects_bad_definitions() {
        let bad = [
            json!({"rules": [{"id": "a", "effect": "deny"}], "typo_key": true}), // unknown top key
            json!({"version": 2, "rules": [{"id": "a", "effect": "deny"}]}),     // bad version
            json!({"combining": "magic", "rules": [{"id": "a", "effect": "deny"}]}),
            json!({"default_effect": "transform", "rules": [{"id": "a", "effect": "deny"}]}),
            json!({"rules": [{"id": "a", "effect": "deny"}, {"id": "a", "effect": "deny"}]}), // dup id
            json!({"rules": [{"id": "a", "effect": "rate_limit"}]}), // missing bucket
            json!({"rules": [{"id": "a", "effect": "transform"}]}),  // missing patch
            json!({"rules": [{"id": "a", "effect": "deny", "transform": {"x": 1}}]}), // incoherent
            json!({"rules": [{"id": "a", "effect": "deny", "actions": []}]}), // empty globs
            json!({"rules": [{"effect": "deny"}]}),                  // missing id
            json!({"rules": [{"id": "a", "effect": "deny", "typo": 1}]}), // unknown rule key
        ];
        for definition in bad {
            let policy = policy(definition.clone());
            assert!(
                compile(&policy).is_err(),
                "must reject: {}",
                serde_json::to_string(&definition).unwrap_or_default()
            );
        }
    }

    #[test]
    fn applies_to_matches_globs() {
        let mut rules_vec = vec![];
        let policy = policy(json!({
            "rules": [{"id": "r", "effect": "allow",
                       "actions": ["tool.invoke", "agent.*"],
                       "resources": ["tool:*"]}]
        }));
        let compiled = compile(&policy).expect("c");
        rules_vec.push(compiled.rules[0].clone());
        let rule = &rules_vec[0];
        assert!(rule.applies_to("tool.invoke", "tool:xyz"));
        assert!(rule.applies_to("agent.start", "tool:x"));
        assert!(!rule.applies_to("task.submit", "tool:x"));
        assert!(!rule.applies_to("tool.invoke", "workflow:x"));
    }
}
