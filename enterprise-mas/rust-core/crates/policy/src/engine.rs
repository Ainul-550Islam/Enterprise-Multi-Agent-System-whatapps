//! The evaluation engine (PDP): policy set → single decision.
//!
//! # Semantics (normative)
//!
//! 1. **Applicable set**: every active policy matching the request scope
//!    (global → organization → tenant → project), sorted by
//!    `(priority desc, policy id asc)` — deterministic.
//! 2. **Matching**: a rule matches when its action glob, resource glob and
//!    `when` condition all hold. `first_applicable` policies contribute
//!    their first matching rule; the others contribute all their matches.
//! 3. **Votes**: across all matched rules, the decision is:
//!    * any `deny` → **Deny** (reason from the highest-priority deny),
//!    * else any `require_approval` → **RequireApproval**,
//!    * else any `rate_limit` → each spec consumes a token; first exhausted
//!      bucket yields **RateLimit** (all granted → the rule votes `allow`),
//!      and a missing rate limiter is a **secure Deny**,
//!    * else any `transform` → **Transform**, with patches merged in
//!      ascending-priority order (highest priority wins conflicts),
//!    * else any `allow` → **Allow**,
//!    * else **no rule matched anywhere** → the worst-case of the
//!      participating policies' `default_effect` (any `deny` default ⇒ Deny).
//! 4. **Caching**: decisions keyed by `(request, active-policy fingerprint)`
//!    are cached for `ttl`. Any decision that touched a rate-limit bucket
//!    skips the cache — throttling must see every call.

use crate::cache::DecisionCache;
use crate::compiler::{
    compile, definition_checksum, Combining, CompiledPolicy, CompiledRule, Effect,
};
use crate::rate_limit::RateLimiterPort;
use crate::request::EvaluationRequest;
use crate::store::{PolicyQuery, PolicyStorePort};
use mas_common::enums::PolicyDecision;
use mas_common::ids::PolicyId;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Which rule a decision traces back to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleRef {
    pub policy_id: PolicyId,
    pub rule_id: String,
}

/// One decision + full provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evaluation {
    pub decision: PolicyDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Every rule that matched (also non-deciding ones, for audit).
    pub matched: Vec<RuleRef>,
    /// Merged transform patch (only meaningful when `decision` permits).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transform: Option<Map<String, Value>>,
    /// Every policy consulted (for cache invalidation traces).
    pub policy_set_fingerprint: String,
    #[serde(skip)]
    pub from_cache: bool,
    pub evaluated_at: Timestamp,
}

/// Builder for [`Evaluation`] instances.
#[derive(Debug)]
pub struct EvaluationBuilder {
    evaluation: Evaluation,
}

impl EvaluationBuilder {
    #[must_use]
    pub fn decided(decision: PolicyDecision, fingerprint: String) -> Self {
        Self {
            evaluation: Evaluation {
                decision,
                reason: None,
                matched: Vec::new(),
                transform: None,
                policy_set_fingerprint: fingerprint,
                from_cache: false,
                evaluated_at: Timestamp::now(),
            },
        }
    }

    #[must_use]
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.evaluation.reason = Some(reason.into());
        self
    }

    #[must_use]
    pub fn matched(mut self, matched: Vec<RuleRef>) -> Self {
        self.evaluation.matched = matched;
        self
    }

    #[must_use]
    pub fn transform(mut self, transform: Map<String, Value>) -> Self {
        if !transform.is_empty() {
            self.evaluation.transform = Some(transform);
        }
        self
    }

    #[must_use]
    pub const fn from_cache(mut self, from_cache: bool) -> Self {
        self.evaluation.from_cache = from_cache;
        self
    }

    #[must_use]
    pub fn build(self) -> Evaluation {
        self.evaluation
    }
}

impl Evaluation {
    /// Whether the caller may proceed (parseable for downstream mapping).
    #[must_use]
    pub const fn permits(&self) -> bool {
        matches!(
            self.decision,
            PolicyDecision::Allow | PolicyDecision::Transform
        )
    }
}

/// The policy decision point.
pub struct PolicyEngine {
    store: Arc<dyn PolicyStorePort>,
    limiter: Option<Arc<dyn RateLimiterPort>>,
    decisions: DecisionCache<Evaluation>,
    compiled: Mutex<HashMap<(PolicyId, u64), CompiledPolicy>>,
}

impl fmt::Debug for PolicyEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PolicyEngine")
            .field("store", &self.store)
            .field("has_rate_limiter", &self.limiter.is_some())
            .field("decisions", &self.decisions)
            .finish_non_exhaustive()
    }
}

impl PolicyEngine {
    pub fn new(store: Arc<dyn PolicyStorePort>) -> Self {
        Self {
            store,
            limiter: None,
            decisions: DecisionCache::default(),
            compiled: Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub fn with_rate_limiter(mut self, limiter: Arc<dyn RateLimiterPort>) -> Self {
        self.limiter = Some(limiter);
        self
    }

    #[must_use]
    pub fn with_decision_cache(mut self, cache: DecisionCache<Evaluation>) -> Self {
        self.decisions = cache;
        self
    }

    #[must_use]
    pub fn store(&self) -> &Arc<dyn PolicyStorePort> {
        &self.store
    }

    /// Evaluates a request against the whole applicable policy set.
    pub async fn evaluate(&self, request: &EvaluationRequest) -> Result<Evaluation> {
        // 1. Applicable set, sorted deterministically.
        let policies = self
            .store
            .list_applicable(&PolicyQuery::for_context(
                request.context.tenant_id,
                request.context.organization_id,
                request.context.project_id,
            ))
            .await?;

        // 2. Compiled view (cache by (id, version); a version bump forces
        //    recompilation; the definition checksum guards stale versions).
        let mut compiled_policies = Vec::with_capacity(policies.len());
        for policy in &policies {
            compiled_policies.push(self.compiled_for(policy)?);
        }
        let fingerprint = fingerprint_of(&compiled_policies);

        // 3. Decision cache (only bucket-free evaluations are cached).
        let cache_key = cache_key(request, &fingerprint)?;
        if let Some(mut cached) = self.decisions.get(&cache_key) {
            cached.from_cache = true;
            cached.evaluated_at = Timestamp::now();
            return Ok(cached);
        }

        // 4. Match + vote.
        let vote = self.compute(request, &compiled_policies).await?;
        let evaluation = Evaluation {
            decision: vote.decision,
            reason: vote.reason,
            matched: vote.matched,
            transform: (!vote.transform.is_empty()).then_some(vote.transform),
            policy_set_fingerprint: fingerprint,
            from_cache: false,
            evaluated_at: Timestamp::now(),
        };

        // 5. Cache unless buckets were consulted.
        if !vote.bucket_touched {
            self.decisions.put(cache_key, evaluation.clone());
        }
        Ok(evaluation)
    }

    /// Drops compiled + decision caches (admin forced refresh).
    pub fn force_refresh(&self) {
        self.compiled
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.decisions.clear();
    }

    #[must_use]
    pub fn cache_stats(&self) -> crate::cache::CacheStats {
        self.decisions.stats()
    }

    fn compiled_for(&self, policy: &mas_domain::Policy) -> Result<CompiledPolicy> {
        let mut cache = self.compiled.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(compiled) = cache.get(&(policy.id, policy.version)) {
            if compiled.checksum == definition_checksum(&policy.definition) {
                return Ok(compiled.clone());
            }
        }
        let compiled = compile(policy)?;
        cache.insert((policy.id, policy.version), compiled.clone());
        Ok(compiled)
    }

    async fn compute(
        &self,
        request: &EvaluationRequest,
        policies: &[CompiledPolicy],
    ) -> Result<Vote> {
        let mut deny: Option<(i32, RuleRef, Option<String>)> = None;
        let mut approval: Option<(i32, RuleRef, Option<String>)> = None;
        let mut allow_seen = false;
        let mut any_default_deny = false;
        let mut any_default_allow = false;
        let mut matched: Vec<RuleRef> = Vec::new();
        // Transforms merge in ascending priority (reversed iteration) so the
        // highest priority writes last.
        let mut transform_layers: Vec<(i32, Map<String, Value>)> = Vec::new();
        let mut rate_specs: Vec<(i32, RuleRef, CompiledRule)> = Vec::new();

        for policy in policies {
            let matching: Vec<&CompiledRule> = match policy.combining {
                Combining::FirstApplicable => policy
                    .rules
                    .iter()
                    .find(|rule| {
                        rule.applies_to(&request.action, &request.resource)
                            && rule.when.evaluate(request)
                    })
                    .into_iter()
                    .collect(),
                Combining::DenyOverrides => policy
                    .rules
                    .iter()
                    .filter(|rule| {
                        rule.applies_to(&request.action, &request.resource)
                            && rule.when.evaluate(request)
                    })
                    .collect(),
            };

            if matching.is_empty() {
                match policy.default_effect {
                    Effect::Deny => any_default_deny = true,
                    Effect::Allow => any_default_allow = true,
                    _ => {},
                }
                continue;
            }

            for rule in matching {
                let rule_ref = RuleRef {
                    policy_id: policy.policy_id,
                    rule_id: rule.id.clone(),
                };
                matched.push(rule_ref.clone());
                match rule.effect {
                    Effect::Deny => {
                        if deny.as_ref().is_none_or(|(p, _, _)| policy.priority > *p) {
                            deny = Some((policy.priority, rule_ref, rule.reason.clone()));
                        }
                    },
                    Effect::RequireApproval => {
                        if approval
                            .as_ref()
                            .is_none_or(|(p, _, _)| policy.priority > *p)
                        {
                            approval = Some((policy.priority, rule_ref, rule.reason.clone()));
                        }
                    },
                    Effect::RateLimit => rate_specs.push((policy.priority, rule_ref, rule.clone())),
                    Effect::Transform => {
                        if let Some(patch) = &rule.transform {
                            transform_layers.push((policy.priority, patch.clone()));
                        }
                    },
                    Effect::Allow => allow_seen = true,
                }
            }
        }

        // 1. Deny overrides everything.
        if let Some((_, rule, reason)) = deny {
            return Ok(Vote {
                decision: PolicyDecision::Deny,
                reason: Some(reason.unwrap_or_else(|| {
                    format!("denied by rule '{}' ({})", rule.rule_id, rule.policy_id)
                })),
                matched,
                transform: Map::new(),
                bucket_touched: false,
            });
        }
        // 2. Approval gates.
        if let Some((_, rule, reason)) = approval {
            return Ok(Vote {
                decision: PolicyDecision::RequireApproval,
                reason: Some(reason.unwrap_or_else(|| {
                    format!("action requires approval (rule '{}')", rule.rule_id)
                })),
                matched,
                transform: Map::new(),
                bucket_touched: false,
            });
        }
        // 3. Rate limits: consume; first exhaustion throttles.
        if !rate_specs.is_empty() {
            // Highest priority first — it decides the throttle reason.
            rate_specs.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.rule_id.cmp(&b.1.rule_id)));
            let Some(limiter) = &self.limiter else {
                tracing::error!("rate_limit rules active but no RateLimiterPort configured");
                return Ok(Vote {
                    decision: PolicyDecision::Deny,
                    reason: Some("rate limiting backend is unavailable".to_owned()),
                    matched,
                    transform: Map::new(),
                    bucket_touched: true,
                });
            };
            for (_, _rule_ref, rule) in &rate_specs {
                let spec = rule
                    .rate_limit
                    .as_ref()
                    .expect("validated at compile: rate_limit present");
                let key = spec.key_for(request);
                if !limiter.try_consume(&key, spec, 1).await? {
                    let reason = rule.reason.clone().unwrap_or_else(|| {
                        format!("rate limit exceeded by rule '{}'", spec_line(spec))
                    });
                    return Ok(Vote {
                        decision: PolicyDecision::RateLimit,
                        reason: Some(reason),
                        matched,
                        transform: Map::new(),
                        bucket_touched: true,
                    });
                }
            }
            allow_seen = true; // granted limits vote allow
        }
        // 4. Transforms merge (ascending priority).
        transform_layers.sort_by_key(|(priority, _)| *priority);
        let mut transform = Map::new();
        for (_, layer) in transform_layers {
            for (key, value) in layer {
                transform.insert(key, value);
            }
        }
        if !transform.is_empty() {
            return Ok(Vote {
                decision: PolicyDecision::Transform,
                reason: None,
                matched,
                transform,
                bucket_touched: !rate_specs.is_empty(),
            });
        }
        // 5. Permits.
        if allow_seen || (any_default_allow && !any_default_deny) {
            return Ok(Vote {
                decision: PolicyDecision::Allow,
                reason: None,
                matched,
                transform: Map::new(),
                bucket_touched: !rate_specs.is_empty(),
            });
        }
        // 6. Nothing permitted: deny (secure default). If some policy
        //    defaulted to deny, name that; else the empty match denies.
        if any_default_deny || !any_default_allow {
            return Ok(Vote {
                decision: PolicyDecision::Deny,
                reason: Some("no matching allow rule; default is deny".to_owned()),
                matched,
                transform: Map::new(),
                bucket_touched: false,
            });
        }
        Ok(Vote {
            decision: PolicyDecision::Deny,
            reason: Some("policy set is indeterminate".to_owned()),
            matched,
            transform: Map::new(),
            bucket_touched: false,
        })
    }
}

fn spec_line(spec: &crate::rate_limit::RateLimitSpec) -> String {
    format!(
        "{}/{} per minute (key {})",
        spec.max_tokens, spec.refill_per_minute, spec.key
    )
}

struct Vote {
    decision: PolicyDecision,
    reason: Option<String>,
    matched: Vec<RuleRef>,
    transform: Map<String, Value>,
    bucket_touched: bool,
}

/// Fingerprint of the applicable compiled set — any change (content,
/// order, membership) moves it, invalidating cached decisions.
fn fingerprint_of(policies: &[CompiledPolicy]) -> String {
    let mut hasher = Sha256::new();
    for policy in policies {
        hasher.update(policy.policy_id.to_string().as_bytes());
        hasher.update(policy.version.to_le_bytes());
        hasher.update(priority_bytes(policy.priority));
        hasher.update(policy.checksum.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn priority_bytes(priority: i32) -> [u8; 4] {
    priority.to_le_bytes()
}

fn cache_key(request: &EvaluationRequest, fingerprint: &str) -> Result<String> {
    let canonical = request.canonical()?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    hasher.update(b"|");
    hasher.update(fingerprint.as_bytes());
    Ok(hex::encode(hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::TokenBucketRateLimiter;
    use crate::request::{EvaluationContext, EvaluationSubject};
    use crate::store::InMemoryPolicyStore;
    use mas_common::enums::Environment;
    use mas_common::ids::{OrganizationId, ProjectId, TenantId};
    use mas_domain::{Policy, PolicyScope, PolicyType};
    use serde_json::json;

    fn world() -> (
        Arc<InMemoryPolicyStore>,
        TenantId,
        OrganizationId,
        ProjectId,
    ) {
        (
            Arc::new(InMemoryPolicyStore::new()),
            TenantId::new(),
            OrganizationId::new(),
            ProjectId::new(),
        )
    }

    fn scope_policy(
        tenant: TenantId,
        org: OrganizationId,
        project: ProjectId,
        name: &str,
        priority: i32,
        definition: Value,
    ) -> Policy {
        Policy::new(
            Some(tenant),
            Some(org),
            Some(project),
            name,
            PolicyType::Execution,
            PolicyScope::Project,
            priority,
            definition,
        )
        .expect("policy")
    }

    fn request(
        tenant: TenantId,
        org: OrganizationId,
        project: ProjectId,
        action: &str,
        resource: &str,
        actor: &str,
    ) -> EvaluationRequest {
        EvaluationRequest::new(
            EvaluationSubject::new(actor).expect("subject"),
            action,
            resource,
            json!({}),
            EvaluationContext::new(tenant, Environment::Development)
                .with_organization(org)
                .with_project(project),
        )
        .expect("request")
    }

    #[tokio::test]
    async fn deny_overrides_allow_and_reason_names_the_rule() {
        let (store, tenant, org, project) = world();
        store
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "base",
                1,
                json!({"rules": [{"id": "allow-all", "effect": "allow", "actions": ["tool.*"]}]}),
            ))
            .await
            .expect("seed");
        store
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "strict",
                10,
                json!({"rules": [{"id": "deny-destructive", "effect": "deny",
                                   "when": {"contains": ["attributes.labels", "destructive"]},
                                   "reason": "destructive actions are blocked"}]}),
            ))
            .await
            .expect("seed");

        let engine = PolicyEngine::new(store);
        let mut req = request(tenant, org, project, "tool.invoke", "tool:hammer", "alice");
        req.attributes = json!({"labels": ["destructive"]});
        let evaluation = engine.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::Deny);
        assert_eq!(
            evaluation.reason.as_deref(),
            Some("destructive actions are blocked")
        );
        assert!(evaluation
            .matched
            .iter()
            .any(|m| m.rule_id == "deny-destructive"));
        // Non-matching request → allow from base policy.
        req.attributes = json!({"labels": []});
        let evaluation = engine.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::Allow);
    }

    #[tokio::test]
    async fn precedence_is_deny_approval_ratelimit_transform_allow() {
        let (store, tenant, org, project) = world();
        store
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "gate",
                5,
                json!({
                    "rules": [
                        {"id": "approve", "effect": "require_approval", "actions": ["tool.invoke"]},
                        {"id": "patch", "effect": "transform", "transform": {"class": "safe"}},
                        {"id": "allow", "effect": "allow"}
                    ]
                }),
            ))
            .await
            .expect("seed");
        let engine = PolicyEngine::new(store);
        let req = request(tenant, org, project, "tool.invoke", "tool:x", "alice");
        let evaluation = engine.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::RequireApproval);

        // Drop the approval rule: transform merges.
        let store2 = Arc::new(InMemoryPolicyStore::new());
        store2
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "gate",
                5,
                json!({
                    "rules": [
                        {"id": "patch", "effect": "transform", "transform": {"class": "safe"}},
                        {"id": "allow", "effect": "allow"}
                    ]
                }),
            ))
            .await
            .expect("seed");
        let engine = PolicyEngine::new(store2);
        let evaluation = engine.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::Transform);
        assert_eq!(
            evaluation.transform.as_ref().and_then(|m| m.get("class")),
            Some(&json!("safe"))
        );
    }

    #[tokio::test]
    async fn rate_limit_rule_throttles_after_bucket_drains() {
        let (store, tenant, org, project) = world();
        store
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "throttle",
                5,
                json!({"rules": [{"id": "rl", "effect": "rate_limit",
                                   "rate_limit": {"max_tokens": 2, "refill_per_minute": 60,
                                                  "key": "subject.actor"}}]}),
            ))
            .await
            .expect("seed");
        let engine =
            PolicyEngine::new(store).with_rate_limiter(Arc::new(TokenBucketRateLimiter::new()));
        let req = request(tenant, org, project, "tool.invoke", "tool:x", "alice");
        for _ in 0..2 {
            let evaluation = engine.evaluate(&req).await.expect("evaluate");
            assert_eq!(evaluation.decision, PolicyDecision::Allow);
        }
        let evaluation = engine.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::RateLimit);
        // Every bucket-touching decision skips the cache entirely.
        assert_eq!(engine.cache_stats().puts, 0);

        // Missing limiter denies securely.
        let (store2, _, _, _) = world();
        store2
            .seed_published(&scope_policy(
                tenant,
                org,
                project,
                "throttle",
                5,
                json!({"rules": [{"id": "rl", "effect": "rate_limit",
                                   "rate_limit": {"max_tokens": 2, "refill_per_minute": 60,
                                                  "key": "subject.actor"}}]}),
            ))
            .await
            .expect("seed");
        let no_limiter = PolicyEngine::new(store2);
        let evaluation = no_limiter.evaluate(&req).await.expect("evaluate");
        assert_eq!(evaluation.decision, PolicyDecision::Deny);
        assert_eq!(
            evaluation.reason.as_deref(),
            Some("rate limiting backend is unavailable")
        );
    }

    #[tokio::test]
    async fn cache_hits_and_invalidates_on_policy_change() {
        let (store, tenant, org, project) = world();
        let policy = scope_policy(
            tenant,
            org,
            project,
            "flip",
            1,
            json!({"rules": [{"id": "a", "effect": "allow"}]}),
        );
        store.seed_published(&policy).await.expect("seed");
        let engine = PolicyEngine::new(store);
        let req = request(tenant, org, project, "task.submit", "task", "alice");
        let first = engine.evaluate(&req).await.expect("first");
        assert!(!first.from_cache);
        let second = engine.evaluate(&req).await.expect("second");
        assert!(second.from_cache, "repeat request should cache");
        assert_eq!(engine.cache_stats().hits, 1);
        // Change policy → new fingerprint → cache miss.
        let mut some_policy = engine
            .store()
            .get(policy.id)
            .await
            .expect("get")
            .expect("policy");
        some_policy
            .update_definition(json!({"rules": [{"id": "a", "effect": "deny"}]}))
            .expect("update");
        engine.store().upsert(&some_policy).await.expect("upsert");
        engine
            .store()
            .publish(policy.id, "tester")
            .await
            .expect("publish");
        let third = engine.evaluate(&req).await.expect("third");
        assert!(!third.from_cache);
        assert_eq!(third.decision, PolicyDecision::Deny);
    }
}
