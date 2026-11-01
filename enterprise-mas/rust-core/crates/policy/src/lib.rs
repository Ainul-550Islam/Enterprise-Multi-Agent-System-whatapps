//! Policy crate: the platform's authorization, execution and governance
//! decision point (PDP).
//!
//! # Pieces
//!
//! * [`EvaluationRequest`] — the canonical evaluation input (subject, action,
//!   resource, attributes, scope + time). Everything the engine decides from.
//! * [`Expr`] — the condition language: comparisons over attribute paths
//!   (`subject.roles`, `attributes.<path>`, `action`, …) combined with
//!   `all`/`any`/`not`. Compiles from the JSON DSL, strict validation at
//!   compile time, total evaluation at runtime (no panics, no partiality).
//! * [`compile`] — turns a domain [`Policy`](mas_domain::Policy) definition
//!   (`{"combining", "default_effect", "rules"}`) into a validated,
//!   checksummed [`CompiledPolicy`]. Compilation is strict: unknown keys,
//!   bad ops and incoherent effects are rejected at publish time.
//! * [`PolicyEngine`] — evaluation over all *active* policies from a
//!   [`PolicyStorePort`]: priority-ordered, deny-overrides combining,
//!   rate-limit buckets, transform merging, and a TTL
//!   [`DecisionCache`]
//!   (skipped whenever a rule's outcome depends on a mutating bucket).
//! * [`ApprovalRequest`] + [`ApprovalService`] — the require-approval flow:
//!   quorum-based, role-gated, expirable approvals raised by policy hits.
//! * [`RateLimiterPort`] + [`TokenBucketRateLimiter`] — the enforcement of
//!   `rate_limit` rules.
//! * [`EnginePolicyPort`] — adapter implementing the orchestration
//!   `PolicyPort` so the scheduler, tool runner and node adapters all speak
//!   to this engine unchanged.
//!
//! Determinism guarantees: evaluation depends only on the request and the
//! active policy set; ties break on `(priority desc, policy id, rule id)`.

pub mod adapter;
pub mod approval;
pub mod cache;
pub mod compiler;
pub mod engine;
pub mod expression;
pub mod rate_limit;
pub mod request;
pub mod store;

pub use adapter::EnginePolicyPort;
pub use approval::{
    ApprovalRequest, ApprovalService, ApprovalStatus, ApprovalStorePort, ApprovalVote,
    InMemoryApprovalStore,
};
pub use cache::{CacheStats, DecisionCache};
pub use compiler::{compile, Combining, CompiledPolicy, CompiledRule, Effect, RuleId};
pub use engine::{Evaluation, EvaluationBuilder, PolicyEngine, RuleRef};
pub use expression::{CmpOp, Expr, Path};
pub use rate_limit::{RateLimitKey, RateLimitSpec, RateLimiterPort, TokenBucketRateLimiter};
pub use request::{EvaluationContext, EvaluationRequest, EvaluationSubject};
pub use store::{InMemoryPolicyStore, PolicyQuery, PolicyStorePort};
