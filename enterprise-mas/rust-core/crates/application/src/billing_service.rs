//! PURPOSE: The billing application service -- provider webhook processing
//! and checkout orchestration, through ports only.
//!
//! Owning layer: `mas-application`.
//!
//! Production responsibility: turn one signature-verified provider delivery
//! into exactly one platform effect, and make checkout exactly-once. The
//! webhook is processed in two phases because the tenant is not known until
//! the body has been verified and the provider customer resolved:
//!
//!   1. [`BillingService::receive_stripe_event`] (pre-scope): verify the
//!      signature over the **raw body**, parse the envelope, claim the durable
//!      receipt, and resolve the owning tenant from platform truth.
//!   2. [`BillingService::apply_stripe_event`] (inside the authoritative
//!      scope the caller opens for the resolved tenant): claim the per-tenant
//!      application exactly once, apply the subscription transition, and
//!      finish the receipt.
//!
//! Security boundary: this crate performs no I/O and no SQL. Everything
//! reaches storage through the ports below (implemented by `mas-persistence`)
//! and the provider through [`CheckoutProviderPort`] (implemented by
//! `mas-integrations` behind guarded egress). A tenant id is only ever taken
//! from the [`BillingCustomerStorePort::resolve_owner`] lookup -- never from
//! the event body.
//!
//! It must NOT:
//!   * accept an event whose signature did not verify;
//!   * apply one event twice to one tenant (the application claim is the
//!     control);
//!   * hold a raw provider secret -- signing secrets arrive as
//!     `mas_security::SecretString` and are never logged.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use mas_billing_stripe::{verify_signature, CheckoutSession, StripeEvent, StripeEventType};
use mas_common::error::AppError;
use mas_common::ids::{BillingReceiptId, CheckoutSessionId, OrganizationId, PlanId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::billing::{
    BillingCustomer, BillingInterval, CheckoutSessionRecord, CheckoutState, EventApplication,
    PlanPrice,
};
use mas_domain::{Subscription, SubscriptionState};
use mas_security::SecretString;

/// The provider this service speaks to. Opaque to everything else.
pub const STRIPE_PROVIDER: &str = "stripe";

// ---------------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------------

/// What happened to one provider receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptOutcome {
    /// Claimed, not yet decided.
    Received,
    /// The effect was applied.
    Applied,
    /// Deliberately no effect (unknown event family).
    Ignored,
    /// Refused: bad signature, unknown tenant, malformed body.
    Rejected,
}

impl ReceiptOutcome {
    /// Storage form (matches the receipt table's CHECK constraint).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Received => "received",
            Self::Applied => "applied",
            Self::Ignored => "ignored",
            Self::Rejected => "rejected",
        }
    }
}

/// One claimed receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptClaim {
    /// Platform receipt id.
    pub id: BillingReceiptId,
    /// The provider event id this receipt files.
    pub event_id: String,
    /// Current state.
    pub state: ReceiptOutcome,
    /// Tenant linked to the receipt, once resolved.
    pub tenant_id: Option<TenantId>,
}

/// The durable provider-callback receipt log (platform-level, pre-scope).
#[async_trait]
pub trait BillingReceiptPort: fmt::Debug + Send + Sync {
    /// Claims one `(provider, event_id)`; a repeat returns the existing row.
    async fn claim(
        &self,
        provider: &str,
        event_id: &str,
        event_type: &str,
        payload_hash: &str,
        signature_valid: bool,
    ) -> Result<ReceiptClaim>;

    /// Records the decision. Idempotent for a repeated identical verdict.
    async fn finish(
        &self,
        receipt_id: BillingReceiptId,
        outcome: ReceiptOutcome,
        reason: Option<&str>,
        tenant: Option<TenantId>,
    ) -> Result<ReceiptClaim>;
}

/// The tenant <-> provider customer mapping.
#[async_trait]
pub trait BillingCustomerStorePort: fmt::Debug + Send + Sync {
    /// Pre-scope: which tenant (and organization) owns this provider
    /// customer. `None` = unknown customer; reject, never guess.
    async fn resolve_owner(
        &self,
        provider: &str,
        provider_customer_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>>;

    /// Scoped: records the mapping idempotently.
    async fn create_or_get(&self, customer: &BillingCustomer) -> Result<BillingCustomer>;

    /// Scoped: the tenant's customer for a provider, when one exists.
    async fn get_for_tenant_provider(
        &self,
        tenant: TenantId,
        provider: &str,
    ) -> Result<Option<BillingCustomer>>;
}

/// The plan <-> provider price catalogue.
#[async_trait]
pub trait PlanPriceStorePort: fmt::Debug + Send + Sync {
    /// Scoped: the active price for a plan slot.
    async fn get_active(
        &self,
        tenant: TenantId,
        provider: &str,
        plan_id: PlanId,
        interval: BillingInterval,
    ) -> Result<Option<PlanPrice>>;

    /// Scoped: lookup by provider price handle.
    async fn get_by_provider_price(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_price_id: &str,
    ) -> Result<Option<PlanPrice>>;
}

/// Durable checkout session records.
#[async_trait]
pub trait CheckoutSessionStorePort: fmt::Debug + Send + Sync {
    /// Scoped: create, or return the record an earlier request created for
    /// the same idempotency key.
    async fn create_or_get(
        &self,
        record: &CheckoutSessionRecord,
    ) -> Result<(CheckoutSessionRecord, bool)>;

    /// Scoped: records the provider session handle on a created row.
    async fn attach_provider_session(
        &self,
        tenant: TenantId,
        idempotency_key: &str,
        provider_session_id: &str,
    ) -> Result<CheckoutSessionRecord>;

    /// Scoped: lookup by provider session handle.
    async fn get_by_provider_session(
        &self,
        tenant: TenantId,
        provider: &str,
        provider_session_id: &str,
    ) -> Result<Option<CheckoutSessionRecord>>;

    /// Scoped: records a checkout lifecycle transition.
    async fn set_state(
        &self,
        tenant: TenantId,
        id: CheckoutSessionId,
        state: CheckoutState,
    ) -> Result<Option<CheckoutSessionRecord>>;
}

/// The per-tenant event application claim.
#[async_trait]
pub trait EventApplicationStorePort: fmt::Debug + Send + Sync {
    /// Scoped: claims the right to apply one event. `false` = already
    /// applied (a replay); that is success, not an error.
    async fn claim(&self, application: &EventApplication) -> Result<bool>;

    /// Scoped: an existing claim.
    async fn get(
        &self,
        tenant: TenantId,
        provider: &str,
        event_id: &str,
    ) -> Result<Option<EventApplication>>;
}

// The provider boundary is declared in `mas-integrations` (the lower-level
// integration crate) and composed here.
pub use mas_integrations::provider_ports::{
    CheckoutProviderPort, CheckoutRequest, CustomerRequest, PortalRequest, ProviderCustomer,
    ProviderPortal, ProviderSession,
};

// ---------------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------------

/// Outcome of the pre-scope webhook phase.
#[derive(Debug, Clone)]
pub enum StripeDelivery {
    /// The delivery is not authentic or not processable. No effect applied.
    Rejected {
        /// Why, for the receipt and the (safe) operator log.
        reason: String,
    },
    /// The receipt was already terminal: a replay. Acknowledge without
    /// re-applying.
    Duplicate {
        /// The provider event id.
        event_id: String,
    },
    /// Verified and resolved; the caller must open the authoritative scope
    /// and call [`BillingService::apply_stripe_event`].
    Resolved(ResolvedStripeEvent),
}

/// A verified, tenant-resolved provider event awaiting its effect.
#[derive(Debug, Clone)]
pub struct ResolvedStripeEvent {
    /// The parsed event.
    pub event: StripeEvent,
    /// Owning tenant and organization (from platform truth).
    pub owner: (TenantId, OrganizationId),
    /// The receipt claimed for this delivery.
    pub receipt_id: BillingReceiptId,
    /// The provider customer the event names, when it names one.
    pub provider_customer_id: Option<String>,
}

/// What the apply phase did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StripeApplyOutcome {
    /// The effect was applied exactly once.
    Applied {
        /// Short effect code recorded on the claim.
        effect: String,
    },
    /// A replay: the claim was already held.
    AlreadyApplied,
    /// Nothing to do for this event family.
    Ignored {
        /// Why.
        reason: String,
    },
}

/// One checkout the service created (or replayed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckoutCreated {
    /// The durable record.
    pub record: CheckoutSessionRecord,
    /// The provider checkout URL. Empty only when a replay found a record
    /// whose provider session was never attached; callers treat that as a
    /// conflict and retry with a new key.
    pub checkout_url: String,
}

/// The billing application service.
pub struct BillingService {
    receipts: Arc<dyn BillingReceiptPort>,
    customers: Arc<dyn BillingCustomerStorePort>,
    prices: Arc<dyn PlanPriceStorePort>,
    checkouts: Arc<dyn CheckoutSessionStorePort>,
    claims: Arc<dyn EventApplicationStorePort>,
    subscriptions: Arc<dyn mas_tenancy::EntitlementStorePort>,
    provider: Arc<dyn CheckoutProviderPort>,
    signing_secrets: Vec<SecretString>,
    tolerance_secs: u64,
}

impl fmt::Debug for BillingService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BillingService")
            .field("tolerance_secs", &self.tolerance_secs)
            .field("signing_secrets", &self.signing_secrets.len())
            .finish_non_exhaustive()
    }
}

impl BillingService {
    /// Builds the service.
    ///
    /// `signing_secrets` holds the active webhook signing secrets; pass the
    /// old and the new value during rotation. `tolerance_secs` bounds the
    /// signature timestamp skew in both directions.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        receipts: Arc<dyn BillingReceiptPort>,
        customers: Arc<dyn BillingCustomerStorePort>,
        prices: Arc<dyn PlanPriceStorePort>,
        checkouts: Arc<dyn CheckoutSessionStorePort>,
        claims: Arc<dyn EventApplicationStorePort>,
        subscriptions: Arc<dyn mas_tenancy::EntitlementStorePort>,
        provider: Arc<dyn CheckoutProviderPort>,
        signing_secrets: Vec<SecretString>,
        tolerance_secs: u64,
    ) -> Self {
        Self {
            receipts,
            customers,
            prices,
            checkouts,
            claims,
            subscriptions,
            provider,
            signing_secrets,
            tolerance_secs,
        }
    }

    /// Phase 1 (pre-scope): verify, parse, claim, resolve.
    ///
    /// # Errors
    ///
    /// [`AppError`] when the receipt log or the customer lookup fails at the
    /// storage layer. An unauthentic delivery is **not** an error: it is a
    /// [`StripeDelivery::Rejected`] with a rejection receipt written.
    pub async fn receive_stripe_event(
        &self,
        raw_body: &[u8],
        signature_header: &str,
        now_unix_secs: u64,
    ) -> Result<StripeDelivery> {
        // Signature first: nothing below reads a field of an unverified body
        // except the event id used to file the rejection evidence.
        let verified = {
            let secrets: Vec<&[u8]> = self
                .signing_secrets
                .iter()
                .map(|secret| secret.expose().as_bytes())
                .collect();
            verify_signature(
                raw_body,
                signature_header,
                &secrets,
                self.tolerance_secs,
                now_unix_secs,
            )
        };

        if let Err(error) = verified {
            // File the rejection as evidence when the body at least names an
            // event; otherwise there is no key to file it under.
            if let Ok(event) = StripeEvent::parse(raw_body) {
                let hash = payload_hash(raw_body);
                self.receipts
                    .claim(STRIPE_PROVIDER, &event.id, &event.raw_type, &hash, false)
                    .await?;
            }
            return Ok(StripeDelivery::Rejected {
                reason: error.to_string(),
            });
        }

        let event = StripeEvent::parse(raw_body).map_err(|error| {
            AppError::validation(format!("stripe event body rejected: {error}"))
        })?;
        let hash = payload_hash(raw_body);
        let claim = self
            .receipts
            .claim(STRIPE_PROVIDER, &event.id, &event.raw_type, &hash, true)
            .await?;
        if claim.state != ReceiptOutcome::Received {
            return Ok(StripeDelivery::Duplicate { event_id: event.id });
        }

        // Resolve the tenant from platform truth: the customer the event
        // names, looked up in our own mapping table. A customer id in the
        // body is never itself trusted for tenancy.
        let provider_customer_id = event_customer(&event);
        let Some(customer_id) = provider_customer_id.clone() else {
            self.receipts
                .finish(
                    claim.id,
                    ReceiptOutcome::Ignored,
                    Some("event names no provider customer"),
                    None,
                )
                .await?;
            return Ok(StripeDelivery::Rejected {
                reason: "event names no provider customer".to_owned(),
            });
        };
        let Some(owner) = self
            .customers
            .resolve_owner(STRIPE_PROVIDER, &customer_id)
            .await?
        else {
            self.receipts
                .finish(
                    claim.id,
                    ReceiptOutcome::Rejected,
                    Some("unknown provider customer"),
                    None,
                )
                .await?;
            return Ok(StripeDelivery::Rejected {
                reason: "unknown provider customer".to_owned(),
            });
        };

        Ok(StripeDelivery::Resolved(ResolvedStripeEvent {
            event,
            owner,
            receipt_id: claim.id,
            provider_customer_id: Some(customer_id),
        }))
    }

    /// Phase 2 (inside the resolved tenant's authoritative scope): claim the
    /// effect exactly once, apply it, finish the receipt.
    ///
    /// # Errors
    ///
    /// [`AppError`] on storage failure, or [`AppError::conflict`] when a
    /// subscription transition is not legal from the current state.
    pub async fn apply_stripe_event(
        &self,
        resolved: &ResolvedStripeEvent,
    ) -> Result<StripeApplyOutcome> {
        let (tenant, _organization) = resolved.owner;
        let claim_row = EventApplication {
            id: mas_common::ids::EventApplicationId::new(),
            tenant_id: tenant,
            provider: STRIPE_PROVIDER.to_owned(),
            event_id: resolved.event.id.clone(),
            receipt_id: Some(resolved.receipt_id),
            event_type: resolved.event.raw_type.clone(),
            effect: "pending".to_owned(),
            applied_at: Timestamp::now(),
        };
        if !self.claims.claim(&claim_row).await? {
            // A replay (or a concurrent second delivery): the effect already
            // happened. Finish the receipt as applied so the replay is also
            // acknowledged, and change nothing else.
            self.receipts
                .finish(
                    resolved.receipt_id,
                    ReceiptOutcome::Applied,
                    None,
                    Some(tenant),
                )
                .await?;
            return Ok(StripeApplyOutcome::AlreadyApplied);
        }

        let outcome = self.apply_effect(tenant, &resolved.event).await?;
        let (receipt_outcome, effect) = match &outcome {
            StripeApplyOutcome::Applied { effect } => (ReceiptOutcome::Applied, effect.clone()),
            StripeApplyOutcome::Ignored { reason } => (ReceiptOutcome::Ignored, reason.clone()),
            StripeApplyOutcome::AlreadyApplied => {
                (ReceiptOutcome::Applied, "already_applied".to_owned())
            },
        };
        self.receipts
            .finish(
                resolved.receipt_id,
                receipt_outcome,
                if receipt_outcome == ReceiptOutcome::Ignored {
                    Some(effect.as_str())
                } else {
                    None
                },
                Some(tenant),
            )
            .await?;
        Ok(outcome)
    }

    /// The one place an event becomes a platform effect.
    async fn apply_effect(
        &self,
        tenant: TenantId,
        event: &StripeEvent,
    ) -> Result<StripeApplyOutcome> {
        match event.event_type {
            StripeEventType::CheckoutSessionCompleted => {
                let Some(session) = event.checkout_session() else {
                    return Ok(StripeApplyOutcome::Ignored {
                        reason: "checkout event has no session object".to_owned(),
                    });
                };
                self.complete_checkout(tenant, &session).await
            },
            StripeEventType::CheckoutSessionExpired => {
                let Some(session) = event.checkout_session() else {
                    return Ok(StripeApplyOutcome::Ignored {
                        reason: "checkout event has no session object".to_owned(),
                    });
                };
                if let Some(record) = self
                    .checkouts
                    .get_by_provider_session(tenant, STRIPE_PROVIDER, &session.id)
                    .await?
                {
                    self.checkouts
                        .set_state(tenant, record.id, CheckoutState::Expired)
                        .await?;
                }
                Ok(StripeApplyOutcome::Applied {
                    effect: "checkout_expired".to_owned(),
                })
            },
            StripeEventType::CustomerSubscriptionCreated
            | StripeEventType::CustomerSubscriptionUpdated => {
                let Some(subscription) = event.subscription() else {
                    return Ok(StripeApplyOutcome::Ignored {
                        reason: "subscription event has no subscription object".to_owned(),
                    });
                };
                self.sync_subscription(tenant, &subscription).await
            },
            StripeEventType::CustomerSubscriptionDeleted => {
                let Some(subscription) = event.subscription() else {
                    return Ok(StripeApplyOutcome::Ignored {
                        reason: "subscription event has no subscription object".to_owned(),
                    });
                };
                self.upsert_state(tenant, SubscriptionState::Cancelled, None, None)
                    .await?;
                let _ = subscription;
                Ok(StripeApplyOutcome::Applied {
                    effect: "subscription_cancelled".to_owned(),
                })
            },
            StripeEventType::InvoicePaid => Ok(StripeApplyOutcome::Ignored {
                reason: "invoice observed; subscription events are authoritative".to_owned(),
            }),
            StripeEventType::InvoicePaymentFailed => {
                // The dunning signal: grace period, runtime keeps working.
                self.upsert_state(tenant, SubscriptionState::PastDue, None, None)
                    .await?;
                Ok(StripeApplyOutcome::Applied {
                    effect: "subscription_past_due".to_owned(),
                })
            },
            StripeEventType::Unknown(_) => Ok(StripeApplyOutcome::Ignored {
                reason: format!("unhandled event type {}", event.raw_type),
            }),
        }
    }

    /// Finishes a checkout: marks the record complete and opens (or updates)
    /// the subscription the plan price maps to.
    async fn complete_checkout(
        &self,
        tenant: TenantId,
        session: &CheckoutSession,
    ) -> Result<StripeApplyOutcome> {
        let Some(record) = self
            .checkouts
            .get_by_provider_session(tenant, STRIPE_PROVIDER, &session.id)
            .await?
        else {
            // No platform record claims this session: nothing this tenant
            // owns can be concluded from it.
            return Ok(StripeApplyOutcome::Ignored {
                reason: "checkout session is not a platform record".to_owned(),
            });
        };
        self.checkouts
            .set_state(tenant, record.id, CheckoutState::Completed)
            .await?;

        // The plan code comes from the record -- platform truth recorded at
        // checkout time, never from the event body.
        let plan_code = record.plan_code.clone();
        if plan_code.is_empty() {
            return Ok(StripeApplyOutcome::Applied {
                effect: "checkout_completed".to_owned(),
            });
        }
        self.upsert_state(tenant, SubscriptionState::Active, Some(plan_code), None)
            .await?;
        Ok(StripeApplyOutcome::Applied {
            effect: "checkout_completed_subscription_active".to_owned(),
        })
    }

    /// Maps a provider subscription object onto the platform subscription.
    async fn sync_subscription(
        &self,
        tenant: TenantId,
        subscription: &mas_billing_stripe::SubscriptionObject,
    ) -> Result<StripeApplyOutcome> {
        let state = match subscription.status.as_deref() {
            Some("trialing") => Some(SubscriptionState::Trialing),
            Some("active") => Some(SubscriptionState::Active),
            Some("past_due") | Some("unpaid") => Some(SubscriptionState::PastDue),
            Some("canceled") | Some("incomplete_expired") => Some(SubscriptionState::Cancelled),
            Some("paused") => Some(SubscriptionState::Suspended),
            // `incomplete` and unknown statuses change nothing yet.
            _ => None,
        };
        let Some(state) = state else {
            return Ok(StripeApplyOutcome::Ignored {
                reason: format!(
                    "subscription status {:?} maps to no platform state",
                    subscription.status
                ),
            });
        };

        // The plan code: resolved from the provider price through the
        // platform's price catalogue; when the mapping is missing the
        // existing subscription's plan code is kept.
        let plan_code = match &subscription.price_id {
            Some(price_id) => self
                .prices
                .get_by_provider_price(tenant, STRIPE_PROVIDER, price_id)
                .await?
                .map(|price| price.plan_code.clone()),
            None => None,
        };
        self.upsert_state(tenant, state, plan_code, subscription.cancel_at_period_end)
            .await?;
        Ok(StripeApplyOutcome::Applied {
            effect: format!("subscription_{state}"),
        })
    }

    /// Loads the tenant's subscription (if any), applies the state, and
    /// upserts it.
    async fn upsert_state(
        &self,
        tenant: TenantId,
        state: SubscriptionState,
        plan_code: Option<String>,
        cancel_at_period_end: Option<bool>,
    ) -> Result<()> {
        let existing = self.subscriptions.get_subscription(tenant).await?;
        let mut subscription = match existing {
            Some(mut current) => {
                if current.state != state {
                    current.transition_to(state)?;
                }
                if let Some(plan) = plan_code {
                    if plan != current.plan_code {
                        current.plan_code = plan;
                    }
                }
                current
            },
            None => {
                let plan = plan_code.ok_or_else(|| {
                    AppError::conflict(
                        "no subscription exists and the event names no resolvable plan",
                    )
                })?;
                Subscription::new(tenant, plan, state, None, None)?
            },
        };
        if cancel_at_period_end == Some(true) {
            // The provider will end it at the period boundary; the platform
            // keeps it entitled until the `deleted` event arrives.
            subscription.updated_at = Timestamp::now();
        }
        self.subscriptions.upsert_subscription(&subscription).await
    }

    /// Opens a checkout, exactly once per `(tenant, idempotency_key)`.
    ///
    /// # Errors
    ///
    /// [`AppError::validation`] when no active price maps the plan;
    /// [`AppError::conflict`] when a replayed record has no provider
    /// session to return; [`AppError`] on provider or storage failure.
    pub async fn create_checkout_session(
        &self,
        tenant: TenantId,
        plan_id: PlanId,
        interval: BillingInterval,
        email: Option<&str>,
        success_url: &str,
        cancel_url: &str,
        idempotency_key: &str,
    ) -> Result<CheckoutCreated> {
        let price = self
            .prices
            .get_active(tenant, STRIPE_PROVIDER, plan_id, interval)
            .await?
            .ok_or_else(|| {
                AppError::validation("no active provider price maps this plan and interval")
            })?;

        // The provider customer: reused when the tenant already has one.
        let customer = match self
            .customers
            .get_for_tenant_provider(tenant, STRIPE_PROVIDER)
            .await?
        {
            Some(existing) => existing,
            None => {
                let created = self
                    .provider
                    .create_customer(&CustomerRequest {
                        tenant_id: tenant,
                        email: email.map(str::to_owned),
                        idempotency_key: format!("{idempotency_key}:customer"),
                    })
                    .await?;
                self.customers
                    .create_or_get(&BillingCustomer {
                        id: mas_common::ids::BillingCustomerId::new(),
                        tenant_id: tenant,
                        provider: STRIPE_PROVIDER.to_owned(),
                        provider_customer_id: created.provider_customer_id,
                        email: email.map(str::to_owned),
                        created_at: Timestamp::now(),
                        updated_at: Timestamp::now(),
                    })
                    .await?
            },
        };

        let record = CheckoutSessionRecord {
            id: mas_common::ids::CheckoutSessionId::new(),
            tenant_id: tenant,
            idempotency_key: idempotency_key.to_owned(),
            plan_id,
            plan_price_id: Some(price.id),
            plan_code: price.plan_code.clone(),
            provider: STRIPE_PROVIDER.to_owned(),
            provider_session_id: None,
            state: CheckoutState::Created,
            success_url: success_url.to_owned(),
            cancel_url: cancel_url.to_owned(),
            created_at: Timestamp::now(),
            updated_at: Timestamp::now(),
        };
        let (record, created) = self.checkouts.create_or_get(&record).await?;
        if !created {
            // Replay: return the recorded session when one was attached.
            if let Some(session_id) = record.provider_session_id.clone() {
                // The URL is not re-fetched on a replay: the provider
                // session id is the durable answer.
                return Ok(CheckoutCreated {
                    record,
                    checkout_url: format!("session:{session_id}"),
                });
            }
            return Err(AppError::conflict(
                "an earlier checkout with this idempotency key never obtained a provider session; retry with a new key",
            ));
        }

        let session = self
            .provider
            .create_checkout_session(&CheckoutRequest {
                provider_customer_id: customer.provider_customer_id.clone(),
                provider_price_id: price.provider_price_id.clone(),
                success_url: success_url.to_owned(),
                cancel_url: cancel_url.to_owned(),
                idempotency_key: idempotency_key.to_owned(),
            })
            .await?;
        let record = self
            .checkouts
            .attach_provider_session(tenant, idempotency_key, &session.provider_session_id)
            .await?;
        Ok(CheckoutCreated {
            record,
            checkout_url: session.checkout_url,
        })
    }

    /// Opens a Stripe billing-portal session for the tenant's existing
    /// customer (scoped). No customer means no portal: we never create one
    /// as a side-effect of "manage billing".
    ///
    /// # Errors
    ///
    /// [`AppError::not_found`] when the tenant has no Stripe customer;
    /// [`AppError`] on provider failure.
    pub async fn create_portal_session(
        &self,
        tenant: TenantId,
        return_url: &str,
        idempotency_key: &str,
    ) -> Result<ProviderPortal> {
        let customer = self
            .customers
            .get_for_tenant_provider(tenant, STRIPE_PROVIDER)
            .await?
            .ok_or_else(|| AppError::not_found("billing customer", tenant.as_uuid().to_string()))?;
        self.provider
            .create_portal_session(&PortalRequest {
                provider_customer_id: customer.provider_customer_id,
                return_url: return_url.to_owned(),
                idempotency_key: idempotency_key.to_owned(),
            })
            .await
    }
}

/// SHA-256 of the raw body, hex-encoded -- the receipt's replay fingerprint.
fn payload_hash(raw_body: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(raw_body);
    hex::encode(digest)
}

/// The provider customer an event names, from whichever object it carries.
fn event_customer(event: &StripeEvent) -> Option<String> {
    if let Some(session) = event.checkout_session() {
        if let Some(customer) = session.customer {
            return Some(customer);
        }
    }
    if let Some(subscription) = event.subscription() {
        if let Some(customer) = subscription.customer {
            return Some(customer);
        }
    }
    event
        .data_object
        .get("customer")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    //! Service-level tests over in-memory fakes: the exact orchestration
    //! order (verify -> claim -> resolve -> apply-once), with no I/O.

    use super::*;
    use mas_common::ids::BillingCustomerId;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct FakeReceipts {
        rows: Mutex<Vec<ReceiptClaim>>,
    }

    #[async_trait]
    impl BillingReceiptPort for FakeReceipts {
        async fn claim(
            &self,
            _provider: &str,
            event_id: &str,
            _event_type: &str,
            _payload_hash: &str,
            signature_valid: bool,
        ) -> Result<ReceiptClaim> {
            let mut rows = self.rows.lock().expect("lock");
            if let Some(existing) = rows.iter().find(|row| row.event_id == event_id) {
                return Ok(existing.clone());
            }
            let row = ReceiptClaim {
                id: BillingReceiptId::new(),
                event_id: event_id.to_owned(),
                state: if signature_valid {
                    ReceiptOutcome::Received
                } else {
                    ReceiptOutcome::Rejected
                },
                tenant_id: None,
            };
            rows.push(row.clone());
            Ok(row)
        }

        async fn finish(
            &self,
            receipt_id: BillingReceiptId,
            outcome: ReceiptOutcome,
            _reason: Option<&str>,
            _tenant: Option<TenantId>,
        ) -> Result<ReceiptClaim> {
            let mut rows = self.rows.lock().expect("lock");
            let row = rows
                .iter_mut()
                .find(|row| row.id == receipt_id)
                .expect("claimed");
            row.state = outcome;
            Ok(row.clone())
        }
    }

    #[derive(Debug, Default)]
    struct FakeCustomers {
        owners: Mutex<Vec<(String, TenantId, OrganizationId)>>,
    }

    #[async_trait]
    impl BillingCustomerStorePort for FakeCustomers {
        async fn resolve_owner(
            &self,
            _provider: &str,
            provider_customer_id: &str,
        ) -> Result<Option<(TenantId, OrganizationId)>> {
            Ok(self
                .owners
                .lock()
                .expect("lock")
                .iter()
                .find(|(handle, _, _)| handle == provider_customer_id)
                .map(|(_, tenant, org)| (*tenant, *org)))
        }

        async fn create_or_get(&self, customer: &BillingCustomer) -> Result<BillingCustomer> {
            self.owners.lock().expect("lock").push((
                customer.provider_customer_id.clone(),
                customer.tenant_id,
                OrganizationId::new(),
            ));
            Ok(customer.clone())
        }

        async fn get_for_tenant_provider(
            &self,
            _tenant: TenantId,
            _provider: &str,
        ) -> Result<Option<BillingCustomer>> {
            Ok(None)
        }
    }

    #[derive(Debug, Default)]
    struct FakeClaims {
        held: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl EventApplicationStorePort for FakeClaims {
        async fn claim(&self, application: &EventApplication) -> Result<bool> {
            let mut held = self.held.lock().expect("lock");
            if held.contains(&application.event_id) {
                return Ok(false);
            }
            held.push(application.event_id.clone());
            Ok(true)
        }

        async fn get(
            &self,
            _tenant: TenantId,
            _provider: &str,
            event_id: &str,
        ) -> Result<Option<EventApplication>> {
            Ok(self
                .held
                .lock()
                .expect("lock")
                .contains(&event_id.to_owned())
                .then(|| EventApplication {
                    id: mas_common::ids::EventApplicationId::new(),
                    tenant_id: TenantId::nil(),
                    provider: STRIPE_PROVIDER.to_owned(),
                    event_id: event_id.to_owned(),
                    receipt_id: None,
                    event_type: String::new(),
                    effect: "pending".to_owned(),
                    applied_at: Timestamp::now(),
                }))
        }
    }

    #[derive(Debug, Default)]
    struct FakePrices;

    #[async_trait]
    impl PlanPriceStorePort for FakePrices {
        async fn get_active(
            &self,
            _tenant: TenantId,
            _provider: &str,
            _plan_id: PlanId,
            _interval: BillingInterval,
        ) -> Result<Option<PlanPrice>> {
            Ok(None)
        }

        async fn get_by_provider_price(
            &self,
            _tenant: TenantId,
            _provider: &str,
            _provider_price_id: &str,
        ) -> Result<Option<PlanPrice>> {
            Ok(None)
        }
    }

    #[derive(Debug, Default)]
    struct FakeCheckouts {
        rows: Mutex<Vec<CheckoutSessionRecord>>,
    }

    #[async_trait]
    impl CheckoutSessionStorePort for FakeCheckouts {
        async fn create_or_get(
            &self,
            record: &CheckoutSessionRecord,
        ) -> Result<(CheckoutSessionRecord, bool)> {
            let mut rows = self.rows.lock().expect("lock");
            if let Some(existing) = rows
                .iter()
                .find(|row| row.idempotency_key == record.idempotency_key)
            {
                return Ok((existing.clone(), false));
            }
            rows.push(record.clone());
            Ok((record.clone(), true))
        }

        async fn attach_provider_session(
            &self,
            _tenant: TenantId,
            idempotency_key: &str,
            provider_session_id: &str,
        ) -> Result<CheckoutSessionRecord> {
            let mut rows = self.rows.lock().expect("lock");
            let row = rows
                .iter_mut()
                .find(|row| row.idempotency_key == idempotency_key)
                .expect("record");
            row.provider_session_id = Some(provider_session_id.to_owned());
            Ok(row.clone())
        }

        async fn get_by_provider_session(
            &self,
            _tenant: TenantId,
            _provider: &str,
            provider_session_id: &str,
        ) -> Result<Option<CheckoutSessionRecord>> {
            Ok(self
                .rows
                .lock()
                .expect("lock")
                .iter()
                .find(|row| row.provider_session_id.as_deref() == Some(provider_session_id))
                .cloned())
        }

        async fn set_state(
            &self,
            _tenant: TenantId,
            id: CheckoutSessionId,
            state: CheckoutState,
        ) -> Result<Option<CheckoutSessionRecord>> {
            let mut rows = self.rows.lock().expect("lock");
            Ok(rows.iter_mut().find(|row| row.id == id).map(|row| {
                row.state = state;
                row.clone()
            }))
        }
    }

    #[derive(Debug, Default)]
    struct FakeSubscriptions {
        rows: Mutex<Vec<Subscription>>,
    }

    #[async_trait]
    impl mas_tenancy::EntitlementStorePort for FakeSubscriptions {
        async fn get_subscription(&self, tenant_id: TenantId) -> Result<Option<Subscription>> {
            Ok(self
                .rows
                .lock()
                .expect("lock")
                .iter()
                .find(|row| row.tenant_id == tenant_id)
                .cloned())
        }

        async fn upsert_subscription(&self, subscription: &Subscription) -> Result<()> {
            let mut rows = self.rows.lock().expect("lock");
            rows.retain(|row| row.tenant_id != subscription.tenant_id);
            rows.push(subscription.clone());
            Ok(())
        }

        async fn list_entitlements(
            &self,
            _tenant_id: TenantId,
        ) -> Result<Vec<mas_domain::Entitlement>> {
            Ok(vec![])
        }

        async fn upsert_entitlement(&self, _entitlement: &mas_domain::Entitlement) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct FakeProvider;

    #[async_trait]
    impl CheckoutProviderPort for FakeProvider {
        async fn create_customer(&self, _request: &CustomerRequest) -> Result<ProviderCustomer> {
            Ok(ProviderCustomer {
                provider_customer_id: "cus_fake".to_owned(),
            })
        }

        async fn create_checkout_session(
            &self,
            _request: &CheckoutRequest,
        ) -> Result<ProviderSession> {
            Ok(ProviderSession {
                provider_session_id: "cs_fake".to_owned(),
                checkout_url: "https://provider.test/checkout".to_owned(),
            })
        }

        async fn create_portal_session(&self, _request: &PortalRequest) -> Result<ProviderPortal> {
            Ok(ProviderPortal {
                portal_url: "https://provider.test/portal".to_owned(),
            })
        }
    }

    fn service(secrets: Vec<&str>) -> BillingService {
        BillingService::new(
            Arc::new(FakeReceipts::default()),
            Arc::new(FakeCustomers::default()),
            Arc::new(FakePrices),
            Arc::new(FakeCheckouts::default()),
            Arc::new(FakeClaims::default()),
            Arc::new(FakeSubscriptions::default()),
            Arc::new(FakeProvider),
            secrets.into_iter().map(SecretString::new).collect(),
            300,
        )
    }

    const SECRET: &str = "whsec_test";

    fn signed_delivery(body: &str, now: u64) -> (Vec<u8>, String) {
        use mas_billing_stripe::compute_v1_signature;
        let tag = hex::encode(compute_v1_signature(
            SECRET.as_bytes(),
            now,
            body.as_bytes(),
        ));
        (body.as_bytes().to_vec(), format!("t={now},v1={tag}"))
    }

    const NOW: u64 = 1_700_000_000;

    #[tokio::test]
    async fn an_unverifiable_delivery_is_rejected_not_applied() {
        let service = service(vec![SECRET]);
        let outcome = service
            .receive_stripe_event(b"{}", "t=1,v1=00", NOW)
            .await
            .expect("no storage failure");
        assert!(matches!(outcome, StripeDelivery::Rejected { .. }));
    }

    #[tokio::test]
    async fn a_verified_delivery_with_an_unknown_customer_is_rejected() {
        let service = service(vec![SECRET]);
        let body = r#"{"id":"evt_1","type":"checkout.session.completed",
            "data":{"object":{"id":"cs_1","customer":"cus_unknown"}}}"#;
        let (raw, header) = signed_delivery(body, NOW);
        let outcome = service
            .receive_stripe_event(&raw, &header, NOW)
            .await
            .expect("no storage failure");
        match outcome {
            StripeDelivery::Rejected { reason } => {
                assert!(reason.contains("unknown provider customer"));
            },
            other => panic!("expected rejection, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_verified_delivery_resolves_the_owner_and_applies_once() {
        // Register the customer->tenant mapping first.
        let customers = FakeCustomers::default();
        customers
            .create_or_get(&BillingCustomer {
                id: BillingCustomerId::new(),
                tenant_id: TenantId::nil(),
                provider: STRIPE_PROVIDER.to_owned(),
                provider_customer_id: "cus_known".to_owned(),
                email: None,
                created_at: Timestamp::now(),
                updated_at: Timestamp::now(),
            })
            .await
            .expect("seed customer");
        let service = BillingService::new(
            Arc::new(FakeReceipts::default()),
            Arc::new(customers),
            Arc::new(FakePrices),
            Arc::new(FakeCheckouts::default()),
            Arc::new(FakeClaims::default()),
            Arc::new(FakeSubscriptions::default()),
            Arc::new(FakeProvider),
            vec![SecretString::new(SECRET)],
            300,
        );

        let body = r#"{"id":"evt_2","type":"invoice.payment_failed",
            "data":{"object":{"id":"in_1","customer":"cus_known"}}}"#;
        let (raw, header) = signed_delivery(body, NOW);
        let outcome = service
            .receive_stripe_event(&raw, &header, NOW)
            .await
            .expect("receive");
        let StripeDelivery::Resolved(resolved) = outcome else {
            panic!("expected resolved delivery, got {outcome:?}");
        };
        assert_eq!(resolved.owner.0, TenantId::nil());

        // First apply: the dunning signal moves the subscription to past_due.
        // There is no existing subscription and the event names no plan, so
        // the effect is refused loudly rather than minting a phantom plan.
        let first = service.apply_stripe_event(&resolved).await;
        assert!(
            first.is_err(),
            "invoice without a plan must not fabricate one"
        );

        // The claim was still taken exactly once: a second apply reports the
        // replay instead of applying again.
        let second = service.apply_stripe_event(&resolved).await.expect("replay");
        assert_eq!(second, StripeApplyOutcome::AlreadyApplied);
    }

    #[tokio::test]
    async fn rotation_accepts_the_old_or_the_new_secret() {
        let body = r#"{"id":"evt_3","type":"customer.subscription.deleted",
            "data":{"object":{"id":"sub_1","customer":"cus_gone"}}}"#;
        for secret in [SECRET, "whsec_new"] {
            let service = service(vec![SECRET, "whsec_new"]);
            let tag = hex::encode(mas_billing_stripe::compute_v1_signature(
                secret.as_bytes(),
                NOW,
                body.as_bytes(),
            ));
            let header = format!("t={NOW},v1={tag}");
            let outcome = service
                .receive_stripe_event(body.as_bytes(), &header, NOW)
                .await
                .expect("receive");
            assert!(
                !matches!(outcome, StripeDelivery::Rejected { reason } if reason.contains("verification failed")),
                "secret {secret:?} must verify"
            );
        }
    }

    #[tokio::test]
    async fn a_portal_session_is_refused_when_the_tenant_has_no_customer() {
        let service = service(vec![SECRET]);
        let error = service
            .create_portal_session(TenantId::nil(), "https://example.test/account", "portal-1")
            .await
            .expect_err("no customer, no portal");
        assert_eq!(error.error_code(), "RESOURCE_NOT_FOUND");
    }
}
