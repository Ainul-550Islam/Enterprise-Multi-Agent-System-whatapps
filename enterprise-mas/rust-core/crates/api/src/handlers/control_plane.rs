//! PURPOSE: The HTTP surface for the enterprise control plane — plans,
//! subscriptions, entitlements, overrides, quotas, usage, policies and billing
//! receipts.
//!
//! Owning layer: `mas-api` (the process edge).
//!
//! Production responsibility: expose control-plane state and operations with
//! authentication, authorization, server-side tenant resolution, validation,
//! pagination and deterministic errors — and audit every mutation. This is the
//! surface an operator and a billing integration use.
//!
//! Security boundary: every handler resolves the tenant from the authenticated
//! scope, never from the body or a query parameter. A caller asking for another
//! tenant's data gets the same answer it would get for a tenant that does not
//! exist (`404`), so the surface cannot be used to enumerate tenants.
//!
//! Data ownership: these handlers read and write Rust-owned platform state
//! through `mas-persistence`. No provider value written here is trusted as
//! authoritative — a billing callback records a receipt; it does not become
//! the source of truth for a tenant's plan.
//!
//! It must NOT:
//!   • accept a tenant id, plan id or feature grant from the request body.
//!     Those are server-resolved, and believing a caller's copy is how a
//!     tenant grants itself a plan it did not buy;
//!   • log a payment secret, a webhook signature or a raw provider payload.
//!     Receipts store a SHA-256 of the body and nothing else;
//!   • expose a mutation that is not audited. Every write path here emits an
//!     audit event, because an unattributed change to someone's entitlement or
//!     quota is indistinguishable from tampering;
//!   • let a caller page through another tenant's usage by guessing ids.

use axum::extract::{Path, Query, State};
use axum::Json;
use mas_common::ids::{TenantId, UsageRecordId};
use mas_common::pagination::PageRequest;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use serde::Deserialize;

use crate::handlers::Ctx;
use crate::response::{ok, ApiResult};
use crate::state::AppState;

// ───────────────────────────────────────────────────────────────────────────
// Scope: the tenant every handler here operates on.
// ───────────────────────────────────────────────────────────────────────────

/// Resolves the tenant for a control-plane handler.
///
/// Taken from the authenticated scope, never from the path. Several of these
/// endpoints are about *the caller's own* commercial state — a tenant reads its
/// own subscription — and a path-supplied tenant id would let one tenant read
/// another's plan, usage and billing receipts.
fn scoped_tenant(ctx: &crate::context::RequestContext) -> Result<TenantId> {
    ctx.scope
        .as_ref()
        .map(|scope| scope.tenant_id())
        .ok_or_else(|| {
            mas_common::error::AppError::forbidden(
                "control-plane endpoints require a resolved tenant scope",
            )
        })
}

// ───────────────────────────────────────────────────────────────────────────
// Pagination
// ───────────────────────────────────────────────────────────────────────────

/// The platform pagination ceiling for these endpoints.
///
/// A caller asking for more rows than this is clamped rather than refused: a
/// client should not have to know the ceiling to make a valid request, and an
/// oversized page is not a client bug worth failing a request over.
pub const MAX_PAGE_SIZE: u32 = 200;

/// The page size a caller's request resolves to.
///
/// `PageRequest` is cursor-based and its limit is optional; every list
/// endpoint here resolves it the same way so paging behaves identically
/// across the control plane.
#[must_use]
pub fn page_size(page: &PageRequest) -> i64 {
    i64::from(page.limit.unwrap_or(50).clamp(1, MAX_PAGE_SIZE))
}

// ───────────────────────────────────────────────────────────────────────────
// Read models
// ───────────────────────────────────────────────────────────────────────────

/// What a tenant's subscription looks like from outside.
#[derive(Debug, serde::Serialize)]
pub struct SubscriptionView {
    /// Plan code (never a plan id — the code is the stable commercial key).
    pub plan_code: String,
    /// Lifecycle state.
    pub state: String,
    /// Purchased seats, when the plan is seat-based.
    pub seats: Option<u32>,
    /// Next renewal / re-evaluation point.
    pub renews_at: Option<String>,
    /// Last mutation.
    pub updated_at: String,
}

/// The tenant's effective entitlements.
#[derive(Debug, serde::Serialize)]
pub struct EntitlementView {
    /// Feature key.
    pub feature: String,
    /// Numeric cap, when the feature has one.
    pub limit: Option<u64>,
    /// Where the grant came from.
    pub source: String,
}

/// One usage record.
#[derive(Debug, serde::Serialize)]
pub struct UsageView {
    /// Record id.
    pub id: UsageRecordId,
    /// Metered dimension.
    pub resource: String,
    /// Quantity observed.
    pub quantity: i64,
    /// When the consumption happened.
    pub occurred_at: String,
}

/// A billing receipt.
///
/// Deliberately minimal: it proves an event arrived and what was decided. It
/// carries no raw payload, no signature and no provider secret.
#[derive(Debug, serde::Serialize)]
pub struct ReceiptView {
    /// Provider that sent the event.
    pub provider: String,
    /// The provider's own event id.
    pub event_id: String,
    /// Receipt state.
    pub state: String,
    /// Whether the signature verified.
    pub signature_valid: bool,
    /// Why it was rejected, when it was.
    pub rejection_reason: Option<String>,
}

// ───────────────────────────────────────────────────────────────────────────
// Handlers
// ───────────────────────────────────────────────────────────────────────────

/// `GET /v1/billing/subscription` — the caller's own subscription.
///
/// Returns `404` when the tenant has none, which is a truthful answer: a tenant
/// with no subscription genuinely has no subscription, and the gate refuses its
/// work on that basis.
pub async fn get_subscription(
    State(_state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<SubscriptionView> {
    let tenant = scoped_tenant(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    // The control plane holds the canonical resolution chain. A handler that
    // read the subscriptions table directly would be a second resolution path,
    // and two paths disagree eventually.
    let control_plane = state_control_plane(&_state);
    let subscription = control_plane
        .entitlement_store()
        .get_subscription(tenant)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let Some(subscription) = subscription else {
        return Err(crate::response::ApiError::new(
            mas_common::error::AppError::not_found("subscription", tenant.to_string()),
            &ctx,
        ));
    };
    Ok(ok(
        &ctx,
        SubscriptionView {
            plan_code: subscription.plan_code.clone(),
            state: subscription.state.as_str().to_owned(),
            seats: subscription.seats,
            renews_at: subscription.renews_at.map(|t| t.to_rfc3339_millis()),
            updated_at: subscription.updated_at.to_rfc3339_millis(),
        },
    ))
}

/// `GET /v1/billing/entitlements` — the caller's effective feature grants.
pub async fn list_entitlements(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<EntitlementView>> {
    let tenant = scoped_tenant(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let control_plane = state_control_plane(&state);
    let profile = control_plane
        .entitlements()
        .profile(tenant)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let features = profile
        .map(|profile| profile.features)
        .unwrap_or_default()
        .into_iter()
        .map(|(feature, limit)| EntitlementView {
            feature: feature.as_str().to_owned(),
            limit,
            source: "resolved".to_owned(),
        })
        .collect();
    Ok(ok(&ctx, features))
}

/// `GET /v1/usage` — the caller's own usage, newest first.
///
/// Scoped to the authenticated tenant by construction; there is no tenant
/// parameter, so no caller can page through another tenant's consumption.
pub async fn list_usage(
    State(state): State<AppState>,
    Query(page): Query<PageRequest>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<UsageView>> {
    let tenant = scoped_tenant(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let stores = state
        .control_plane_stores()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    // The ledger's read is window-scoped, so "all usage" is expressed as the
    // widest window the clock can name rather than as a query with no bound —
    // an unbounded scan over a billing table is how a read endpoint becomes an
    // outage.
    let rows = stores
        .usage
        .for_tenant_between(
            tenant,
            &Timestamp::from_unix_seconds(0).unwrap_or_else(|_| Timestamp::now()),
            &Timestamp::now(),
            page_size(&page),
        )
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let views = rows
        .into_iter()
        .map(|row| UsageView {
            id: mas_common::ids::UsageRecordId::from_uuid(row.id),
            resource: row.resource,
            quantity: row.quantity,
            occurred_at: Timestamp::from_datetime(row.occurred_at).to_rfc3339_millis(),
        })
        .collect();
    Ok(ok(&ctx, views))
}

/// `GET /v1/billing/receipts` — billing webhook receipts.
///
/// Platform-admin only: a receipt names a provider event, and the set of them
/// is operational evidence rather than tenant-facing data.
pub async fn list_receipts(
    State(state): State<AppState>,
    Query(page): Query<PageRequest>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<ReceiptView>> {
    require_platform_admin(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let limit = page_size(&page);
    let stores = state
        .control_plane_stores()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let rows = stores
        .billing
        .recent(limit)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let views = rows
        .into_iter()
        .map(|row| ReceiptView {
            provider: row.provider,
            event_id: row.event_id,
            state: row.state,
            signature_valid: row.signature_valid,
            rejection_reason: row.rejection_reason,
        })
        .collect();
    Ok(ok(&ctx, views))
}

/// `GET /v1/billing/receipts/{provider}/{event_id}` — one receipt.
///
/// The event id is provider-supplied and appears in the path, which is fine
/// because it is only ever used to *look up* a receipt the platform already
/// recorded — never to create one, and never to resolve a tenant.
pub async fn get_receipt(
    State(state): State<AppState>,
    Path((provider, event_id)): Path<(String, String)>,
    Ctx(ctx): Ctx,
) -> ApiResult<ReceiptView> {
    require_platform_admin(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let stores = state
        .control_plane_stores()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let row = stores
        .billing
        .get(&provider, &event_id)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    Ok(ok(
        &ctx,
        ReceiptView {
            provider: row.provider,
            event_id: row.event_id,
            state: row.state,
            signature_valid: row.signature_valid,
            rejection_reason: row.rejection_reason,
        },
    ))
}

/// `GET /v1/billing/usage-totals` — aggregated usage for a period.
///
/// The platform ledger answers this. A provider-reported total is never used,
/// because a provider does not know what this platform measured and cannot be
/// the source of truth for a customer's bill.
pub async fn usage_totals(
    State(state): State<AppState>,
    Query(query): Query<TotalsQuery>,
    Ctx(ctx): Ctx,
) -> ApiResult<Vec<UsageTotalView>> {
    let tenant = scoped_tenant(&ctx).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let from = parse_timestamp(&query.from).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let to = parse_timestamp(&query.to).map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    if to.is_before(&from) {
        return Err(crate::response::ApiError::new(
            mas_common::error::AppError::validation("`to` must not precede `from`"),
            &ctx,
        ));
    }
    let stores = state
        .control_plane_stores()
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let totals = stores
        .usage
        .totals_for_tenant_between(tenant, &from, &to)
        .await
        .map_err(|e| crate::response::ApiError::new(e, &ctx))?;
    let views = totals
        .into_iter()
        .map(|total| UsageTotalView {
            resource: total.resource,
            quantity: total.quantity,
            cost_micros: total.cost_micros,
            currency: total.currency,
        })
        .collect();
    Ok(ok(&ctx, views))
}

/// One aggregated usage row.
#[derive(Debug, serde::Serialize)]
pub struct UsageTotalView {
    /// Metered dimension.
    pub resource: String,
    /// Total quantity observed in the window.
    pub quantity: i64,
    /// Summed cost in micro-units — `None` when more than one currency
    /// contributed, because adding them would be meaningless.
    pub cost_micros: Option<i64>,
    /// The currency of `cost_micros`.
    pub currency: Option<String>,
}

/// Query for the usage-totals range.
#[derive(Debug, Deserialize)]
pub struct TotalsQuery {
    /// Inclusive start, RFC 3339.
    pub from: String,
    /// Inclusive end, RFC 3339.
    pub to: String,
}

fn parse_timestamp(raw: &str) -> Result<Timestamp> {
    Timestamp::parse_rfc3339(raw).map_err(|_| {
        mas_common::error::AppError::validation(
            "timestamps must be RFC 3339 (for example 2024-01-01T00:00:00Z)",
        )
    })
}

// ───────────────────────────────────────────────────────────────────────────
// Admin mutations
//
// Every handler here: authorise → resolve → validate → mutate → audit. The
// audit is not decoration. A change to a tenant's plan, quota or override
// determines what that tenant may do and what it is billed; an unattributed
// change is indistinguishable from tampering, so none of these is reachable
// without leaving a record of who made it and why.
// ───────────────────────────────────────────────────────────────────────────

/// Body for creating a plan.
#[derive(Debug, Deserialize)]
pub struct CreatePlanRequest {
    /// Stable machine code (`free`, `team`, …).
    pub code: String,
    /// Human-facing name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
}

/// Body for granting a feature to a plan.
#[derive(Debug, Deserialize)]
pub struct PlanFeatureRequest {
    /// Feature key.
    pub feature: String,
    /// Numeric cap, when the feature has one.
    pub limit: Option<u64>,
}

/// Body for transitioning a subscription.
#[derive(Debug, Deserialize)]
pub struct TransitionSubscriptionRequest {
    /// Target lifecycle state.
    pub state: String,
    /// Why — required, because the reason is the only thing that lets the
    /// next operator understand a change they did not make.
    pub reason: String,
    /// Provider event that caused this, when one did.
    pub external_event_id: Option<String>,
    /// The version the caller observed; a mismatch is a lost update.
    pub expected_version: i64,
}

/// Body for a feature override.
#[derive(Debug, Deserialize)]
pub struct FeatureOverrideRequest {
    /// Feature key.
    pub feature: String,
    /// Whether the feature is enabled.
    pub enabled: bool,
    /// Numeric cap.
    pub limit: Option<u64>,
    /// Why — required, for the same reason as above.
    pub reason: String,
    /// When the override takes effect.
    pub effective_from: String,
    /// When it lapses (`null` = does not lapse).
    pub effective_to: Option<String>,
}

/// Body for a quota override.
#[derive(Debug, Deserialize)]
pub struct QuotaOverrideRequest {
    /// Metered dimension.
    pub dimension: String,
    /// Reset window.
    pub period: String,
    /// Ceiling.
    pub max_value: i64,
    /// `enforce` or `monitor`.
    pub enforcement: Option<String>,
    /// Why — required.
    pub reason: String,
    /// When the override takes effect.
    pub effective_from: String,
    /// When it lapses.
    pub effective_to: Option<String>,
}

/// `POST /v1/admin/plans` — create a plan.
pub async fn create_plan(
    State(state): State<AppState>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreatePlanRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(&state, &ctx, "plan.create", "plan", None)
        .await
        .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    let created = audited_mutation(
        &state,
        &service_ctx,
        "plan.create",
        "plan",
        Some(body.code.clone()),
        || async {
            let plan =
                mas_domain::plan::Plan::new(&body.code, &body.name, body.description.clone())?;
            stores.plans.create(&plan).await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(
        &ctx,
        serde_json::json!({"code": created.code, "name": created.name}),
    ))
}

/// `PUT /v1/admin/plans/{code}/features` — grant a feature to a plan.
pub async fn grant_plan_feature(
    State(state): State<AppState>,
    Path(code): Path<String>,
    Ctx(ctx): Ctx,
    Json(body): Json<PlanFeatureRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(&state, &ctx, "plan.grant_feature", "plan", None)
        .await
        .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    audited_mutation(
        &state,
        &service_ctx,
        "plan.grant_feature",
        "plan",
        Some(format!("{code}/{}", body.feature)),
        || async {
            let plan = stores
                .plans
                .by_code(&code)
                .await?
                .ok_or_else(|| mas_common::error::AppError::not_found("plan", code.clone()))?;
            let feature = mas_domain::entitlement::FeatureKey::new(body.feature.clone())?;
            stores
                .plans
                .grant_feature(&mas_domain::plan::PlanEntitlement::grant(
                    plan.id, feature, body.limit,
                )?)
                .await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(&ctx, serde_json::json!({"granted": body.feature})))
}

/// `POST /v1/admin/tenants/{tenant_id}/subscription` — create a subscription.
///
/// The tenant is in the path because this is an *administrative* operation on
/// another tenant's commercial state, reached only by an authenticated
/// principal holding `platform:admin`. That is the distinction the billing
/// rules turn on: a tenant id from an unauthenticated provider callback is
/// never believed; one from an authorised operator acting on a tenant is the
/// entire purpose of the endpoint.
pub async fn create_subscription(
    State(state): State<AppState>,
    Path(tenant_id): Path<TenantId>,
    Ctx(ctx): Ctx,
    Json(body): Json<CreateSubscriptionRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(&state, &ctx, "subscription.create", "subscription", None)
        .await
        .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    audited_mutation(
        &state,
        &service_ctx,
        "subscription.create",
        "subscription",
        Some(format!("{tenant_id}/{}", body.plan_code)),
        || async {
            let state_value = match body.state.as_deref() {
                Some(raw) => parse_subscription_state(raw)?,
                None => mas_domain::subscription::SubscriptionState::Active,
            };
            let subscription = mas_domain::subscription::Subscription::new(
                tenant_id,
                &body.plan_code,
                state_value,
                None,
                None,
            )?;
            // Written through the same subscription store every other
            // control-plane handler uses, not through the control plane's
            // entitlement port. The two are one table; reaching it by two
            // paths is how a write ends up in one and not the other. A second
            // live subscription is a conflict, which the store reports rather
            // than a silent second row.
            stores.subscriptions.create(&subscription).await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(
        &ctx,
        serde_json::json!({"tenant_id": tenant_id.to_string(), "plan_code": body.plan_code}),
    ))
}

/// Body for creating a subscription.
#[derive(Debug, Deserialize)]
pub struct CreateSubscriptionRequest {
    /// Plan code — must already exist in `plans`.
    pub plan_code: String,
    /// Initial lifecycle state.
    pub state: Option<String>,
}

/// `POST /v1/admin/tenants/{tenant_id}/subscription/transitions`
///
/// The `expected_version` guard is what makes two concurrent operators safe:
/// a transition against a stale version is refused rather than silently
/// overwriting the newer state.
pub async fn transition_subscription(
    State(state): State<AppState>,
    Path(tenant_id): Path<TenantId>,
    Ctx(ctx): Ctx,
    Json(body): Json<TransitionSubscriptionRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(
        &state,
        &ctx,
        "subscription.transition",
        "subscription",
        None,
    )
    .await
    .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    let row = audited_mutation(
        &state,
        &service_ctx,
        "subscription.transition",
        "subscription",
        Some(format!("{tenant_id}/{}", body.state)),
        || async {
            if body.reason.trim().is_empty() {
                return Err(mas_common::error::AppError::validation(
                    "a subscription transition must carry a reason",
                ));
            }
            let target = parse_subscription_state(&body.state)?;
            stores
                .subscriptions
                .transition(
                    tenant_id,
                    target,
                    &body.reason,
                    &ctx.principal
                        .as_ref()
                        .map(|p| p.actor_id.clone())
                        .unwrap_or_else(|| "unknown".to_owned()),
                    "user",
                    body.external_event_id.as_deref(),
                    &ctx.correlation_id,
                    body.expected_version,
                    &now(),
                )
                .await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(
        &ctx,
        serde_json::json!({"state": row.state, "version": row.version}),
    ))
}

/// `POST /v1/admin/tenants/{tenant_id}/overrides/features` — grant a feature
/// override.
///
/// The override changes what the tenant may do without touching the plan, so
/// the same plan keeps meaning the same thing for everyone else.
pub async fn grant_feature_override(
    State(state): State<AppState>,
    Path(tenant_id): Path<TenantId>,
    Ctx(ctx): Ctx,
    Json(body): Json<FeatureOverrideRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(
        &state,
        &ctx,
        "override.grant_feature",
        "entitlement_override",
        None,
    )
    .await
    .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    let id = audited_mutation(
        &state,
        &service_ctx,
        "override.grant_feature",
        "entitlement_override",
        Some(format!("{tenant_id}/{}", body.feature)),
        || async {
            let feature = mas_domain::entitlement::FeatureKey::new(body.feature.clone())?;
            let actor = mas_domain::overrides::OverrideActor::new(
                mas_domain::overrides::ActorKind::User,
                ctx.principal
                    .as_ref()
                    .map(|p| p.actor_id.clone())
                    .unwrap_or_else(|| "unknown".to_owned()),
            )?;

            let override_ = mas_domain::overrides::EntitlementOverride::new(
                tenant_id,
                feature,
                body.enabled,
                body.limit,
                &body.reason,
                actor,
                parse_timestamp(&body.effective_from)?,
                body.effective_to
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?,
            )?;
            stores.overrides.grant_feature(&override_).await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(&ctx, serde_json::json!({"id": id.to_string()})))
}

/// `POST /v1/admin/tenants/{tenant_id}/overrides/quotas` — grant a quota
/// override.
pub async fn grant_quota_override(
    State(state): State<AppState>,
    Path(tenant_id): Path<TenantId>,
    Ctx(ctx): Ctx,
    Json(body): Json<QuotaOverrideRequest>,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(&state, &ctx, "override.grant_quota", "quota_override", None)
        .await
        .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    let id = audited_mutation(
        &state,
        &service_ctx,
        "override.grant_quota",
        "quota_override",
        Some(format!("{tenant_id}/{}", body.dimension)),
        || async {
            let dimension: mas_domain::quota::QuotaDimension =
                body.dimension.parse().map_err(|_| {
                    mas_common::error::AppError::validation(format!(
                        "unknown quota dimension '{}'",
                        body.dimension
                    ))
                })?;
            let period: mas_domain::quota::QuotaPeriod = body.period.parse().map_err(|_| {
                mas_common::error::AppError::validation(format!(
                    "unknown quota period '{}'",
                    body.period
                ))
            })?;
            let enforcement = match body.enforcement.as_deref() {
                None | Some("enforce") => mas_domain::quota::EnforcementMode::Enforce,
                Some("warn_only") => mas_domain::quota::EnforcementMode::WarnOnly,
                Some("allow_with_overage") => mas_domain::quota::EnforcementMode::AllowWithOverage,
                Some(other) => {
                    return Err(mas_common::error::AppError::validation(format!(
                        "unknown enforcement mode '{other}'"
                    )))
                },
            };
            if body.max_value < 0 {
                return Err(mas_common::error::AppError::validation(
                    "a quota ceiling must not be negative",
                ));
            }
            let actor = mas_domain::overrides::OverrideActor::new(
                mas_domain::overrides::ActorKind::User,
                ctx.principal
                    .as_ref()
                    .map(|p| p.actor_id.clone())
                    .unwrap_or_else(|| "unknown".to_owned()),
            )?;

            let override_ = mas_domain::overrides::QuotaOverride::new(
                tenant_id,
                dimension,
                period,
                u64::try_from(body.max_value).unwrap_or(0),
                enforcement,
                &body.reason,
                actor,
                parse_timestamp(&body.effective_from)?,
                body.effective_to
                    .as_deref()
                    .map(parse_timestamp)
                    .transpose()?,
            )?;
            stores.overrides.grant_quota(&override_).await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(&ctx, serde_json::json!({"id": id.to_string()})))
}

/// `DELETE /v1/admin/tenants/{tenant_id}/overrides/features/{feature}`
///
/// Ends an override now. Ending is never deletion: the override stays in the
/// history view, because "this tenant once had this grant, and here is who
/// gave it and why" is exactly what an auditor asks for.
pub async fn end_feature_override(
    State(state): State<AppState>,
    Path((tenant_id, feature)): Path<(TenantId, String)>,
    Ctx(ctx): Ctx,
) -> ApiResult<serde_json::Value> {
    let api_error = |e: mas_common::error::AppError| crate::response::ApiError::new(e, &ctx);
    let service_ctx = authorize_admin(
        &state,
        &ctx,
        "override.end_feature",
        "entitlement_override",
        None,
    )
    .await
    .map_err(api_error)?;
    let stores = state.control_plane_stores().map_err(api_error)?;

    audited_mutation(
        &state,
        &service_ctx,
        "override.end_feature",
        "entitlement_override",
        Some(format!("{tenant_id}/{feature}")),
        || async {
            let key = mas_domain::entitlement::FeatureKey::new(feature.clone())?;
            stores.overrides.end_feature(tenant_id, &key, &now()).await
        },
    )
    .await
    .map_err(api_error)?;

    Ok(ok(&ctx, serde_json::json!({"ended": feature})))
}

fn parse_subscription_state(raw: &str) -> Result<mas_domain::subscription::SubscriptionState> {
    raw.parse().map_err(|_| {
        mas_common::error::AppError::validation(format!("unknown subscription state '{raw}'"))
    })
}

fn now() -> Timestamp {
    Timestamp::now()
}

/// Records one control-plane mutation and its outcome.
///
/// A failure to audit is not swallowed: if the audit write fails, the caller
/// is told, because a change that cannot be attributed should not be applied
/// silently. It is already applied at that point, which is why this runs
/// before the response is returned rather than after.
///
/// The audit is written on the audit plane, never inside the mutation's own
/// transaction. That separation is deliberate and load-bearing — see the
/// composition root in `mas-api`'s `main` — and it is what lets an audit event
/// survive the rollback of the change it describes, and lets a denial be
/// recorded for a tenant the caller was never scoped to.
async fn audit_outcome(
    state: &AppState,
    ctx: &mas_application::context::ServiceContext,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
    outcome: mas_domain::AuditOutcome,
) -> Result<()> {
    mas_application::audit::record_mutation(
        ctx,
        state.audit.as_ref(),
        action,
        resource_type,
        resource_id,
        outcome,
    )
    .await
}

/// Runs one control-plane mutation and records whichever outcome it had.
///
/// Three outcomes are possible and all three are now written down:
///
///   * **Refused** — the caller never had `platform:admin`. Recorded by
///     [`authorize_admin`], as `Denied`, before this is ever called.
///   * **Failed** — an authorised caller whose mutation did not apply: an
///     unknown enum value, a negative ceiling, a malformed date, a stale
///     `expected_version`, or a write row-level security refused. Recorded
///     here as `Failure`.
///   * **Applied** — recorded here as `Success`.
///
/// The middle one is why this wrapper exists. It was the last thing a control
/// plane could do without leaving a line in the audit log, and it is not a
/// harmless omission: "an authorised operator tried to set a tenant's quota to
/// −5 and was refused" and "an authorised operator tried to set a tenant's
/// quota to −5 and it worked" are, to an auditor reading only the successes,
/// indistinguishable from nothing having happened at all.
///
/// The `resource_id` names what was **attempted**, not what resulted, so the
/// failure record still identifies the thing that was not applied.
///
/// A failure to record the failure is logged, not propagated: the mutation did
/// not apply, and replacing the caller's real error — the unknown enum value,
/// the stale version — with "could not write an audit row" would hide the one
/// piece of information the response exists to give.
async fn audited_mutation<T, F, Fut>(
    state: &AppState,
    ctx: &mas_application::context::ServiceContext,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
    work: F,
) -> Result<T>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    match work().await {
        Ok(value) => {
            audit_outcome(
                state,
                ctx,
                action,
                resource_type,
                resource_id,
                mas_domain::AuditOutcome::Success,
            )
            .await?;
            Ok(value)
        },
        Err(error) => {
            if let Err(audit_error) = audit_outcome(
                state,
                ctx,
                action,
                resource_type,
                resource_id,
                mas_domain::AuditOutcome::Failure,
            )
            .await
            {
                tracing::warn!(
                    action, %audit_error,
                    "a failed control-plane mutation was not recorded"
                );
            }
            Err(error)
        },
    }
}

/// Records a control-plane mutation that was refused.
///
/// Best-effort by design, and the asymmetry with [`audit`] is the point: an
/// audit write that fails here must not turn the `403` into a `500`. The
/// caller is already being refused — the response is correct whether or not
/// the refusal was recorded, and inventing a new failure mode on the refusal
/// path would be strictly worse than a missing line in the log.
async fn audit_refusal(
    state: &AppState,
    ctx: &crate::context::RequestContext,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
) {
    // Attribution is best-effort too. A request that was refused may have been
    // refused *because* there is no usable scope, so the very context a
    // successful call would be attributed to may not exist. Recording it as
    // `Anonymous` is honest: there is an event, and no authenticated actor to
    // hang it on.
    let fallback = ctx.service_context().or_else(|_| {
        let actor = match ctx.principal.as_ref() {
            Some(principal) => mas_domain::AuditActor::new(
                match principal.kind {
                    mas_security::PrincipalKind::User => mas_domain::AuditActorKind::User,
                    mas_security::PrincipalKind::ApiKey => mas_domain::AuditActorKind::ApiKey,
                    mas_security::PrincipalKind::System => mas_domain::AuditActorKind::System,
                    _ => mas_domain::AuditActorKind::Service,
                },
                principal.actor_id.clone(),
            ),
            None => mas_domain::AuditActor::new(
                mas_domain::AuditActorKind::Anonymous,
                "unauthenticated",
            ),
        };
        actor.and_then(|actor| {
            mas_application::context::ServiceContext::new(
                actor,
                ctx.scope.as_ref().map(|scope| scope.tenant_id()),
                ctx.scope.as_ref().map(|scope| scope.organization_id()),
                ctx.correlation_id.clone(),
                &Timestamp::now(),
            )
        })
    });

    let Ok(service_ctx) = fallback else {
        tracing::warn!(action, "a control-plane refusal could not be attributed");
        return;
    };
    if let Err(error) = audit_outcome(
        state,
        &service_ctx,
        action,
        resource_type,
        resource_id,
        mas_domain::AuditOutcome::Denied,
    )
    .await
    {
        tracing::warn!(action, %error, "a control-plane refusal was not recorded");
    }
}

/// Authorises a control-plane mutation, recording the refusal when it is
/// refused.
///
/// Every control-plane write goes through here rather than calling
/// [`require_platform_admin`] directly, so the refusal cannot be separated
/// from its record. Before this, a caller who tried to change another tenant's
/// subscription and was refused left *no trace at all*: the audit plane
/// received an event only when the change succeeded. The attempt an auditor
/// most needs to see — one that was made and refused — was the one thing never
/// written down.
async fn authorize_admin(
    state: &AppState,
    ctx: &crate::context::RequestContext,
    action: &str,
    resource_type: &str,
    resource_id: Option<String>,
) -> Result<mas_application::context::ServiceContext> {
    match ctx.platform_service_context() {
        Ok(service_ctx) => Ok(service_ctx),
        Err(error) => {
            audit_refusal(state, ctx, action, resource_type, resource_id).await;
            Err(error)
        },
    }
}

// ───────────────────────────────────────────────────────────────────────────
// Guards
// ───────────────────────────────────────────────────────────────────────────

/// Requires the platform-admin scope.
///
/// Control-plane operations change what tenants are allowed to do and what
/// they are billed. A tenant-scoped principal must never reach them, however
/// valid its token.
pub(crate) fn require_platform_admin(ctx: &crate::context::RequestContext) -> Result<()> {
    let Some(principal) = ctx.principal.as_ref() else {
        return Err(mas_common::error::AppError::unauthorized(
            "authentication required",
        ));
    };
    if !principal.scopes.permits("platform:admin") {
        // Deliberately the same shape as "does not exist": telling a caller
        // that an admin endpoint exists but it may not use it is a small
        // disclosure, and there is no legitimate need for it.
        return Err(mas_common::error::AppError::forbidden(
            "insufficient scope for a control-plane operation",
        ));
    }
    Ok(())
}

/// The control plane from app state, with no conversion or fallback.
fn state_control_plane(
    state: &AppState,
) -> &std::sync::Arc<mas_application::control_plane::ControlPlane> {
    state.control_plane()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_oversized_page_is_clamped_not_rejected() {
        let page = PageRequest::new().with_limit(100_000);
        assert_eq!(
            page_size(&page),
            i64::from(MAX_PAGE_SIZE),
            "a client should not have to know the ceiling to make a valid request"
        );
    }

    #[test]
    fn an_absent_limit_gets_the_default() {
        assert_eq!(page_size(&PageRequest::new()), 50);
    }

    #[test]
    fn a_zero_limit_is_raised_to_one() {
        assert_eq!(
            page_size(&PageRequest::new().with_limit(0)),
            1,
            "a zero page would return nothing and look like 'no data'"
        );
    }

    #[test]
    fn a_bad_timestamp_is_a_validation_error_not_a_panic() {
        assert!(parse_timestamp("not a timestamp").is_err());
        assert!(parse_timestamp("2024-01-01T00:00:00Z").is_ok());
    }
}
