/// Stripe status → Existing subscription transition mapping.
///
/// This crate defines the legal transitions for the subscription state machine.
/// A transition is "legal" if it follows the allowed progression given the
/// current state and the event type. Illegal transitions are refused (return
/// a `Refused` variant) so that the application never enters an inconsistent
/// state. New transitions may NOT be added here — the convention is to use
/// the existing state machine; if a new kind of transition is needed, file
/// an issue and the state machine will be extended in a future release.

use crate::models::{StripeEvent, StripeSubscription, StripeSubscriptionItem};
use crate::errors::StripeError;

/// The current state of a MAS subscription, as modelled locally.
/// This mirrors (but does not duplicate) the fields we need from
/// `StripeSubscription` for transition logic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalSubscriptionState {
    /// Subscription is active and in good standing
    Active,
    /// Subscription is between periods (e.g. right after a plan change)
    GracePeriod,
    /// Subscription has failed payment and is within the grace window
    PastDue,
    /// Subscription has been canceled at the end of the period
    Canceled,
    /// Subscription is incomplete (first payment not yet received)
    Incomplete,
    /// Subscription is between trial and first payment
    Trialing,
}

/// Map a Stripe event to a local subscription state transition.
///
/// # Rules
/// - The subscription starts in an unknown state; the first event must be
///   `SubscriptionCreated` to establish the initial state.
/// - Subsequent events must follow the allowed progression.
/// - Any event not explicitly allowed from the current state is `Refused`.
/// - `unknown` event types are silently ignored (return `Ok(Current)`).
///
/// # Return value
/// `(new_state, allowed)` where `allowed` is `true` if the transition was
/// permitted, `false` if refused. If `allowed` is `false`, `new_state` is
/// the same as the input state.
pub fn stripe_status_to_subscription_state(
    current: LocalSubscriptionState,
    event: &StripeEvent,
) -> (LocalSubscriptionState, bool) {
    let event_type = super::events::EventType::from_stripe_str(&event.r#type);

    // Unknown events: silently ignore, keep current state
    if event_type == super::events::EventType::Unknown {
        return (current, true);
    }

    // Transition logic based on current state and event type
    let (next, allowed) = match (&current, &event_type) {
        // From Active
        (LocalSubscriptionState::Active, EventType::SubscriptionCreated) => {
            // Re-subscribing while active is refused; keep current
            (LocalSubscriptionState::Active, false)
        }
        (LocalSubscriptionState::Active, EventType::SubscriptionUpdated) => {
            // Allowed — update state (could change from active to past_due, etc.)
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Active, EventType::SubscriptionDeleted) => {
            (LocalSubscriptionState::Canceled, true)
        }
        (LocalSubscriptionState::Active, EventType::InvoicePaid) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Active, EventType::InvoicePaymentFailed) => {
            (LocalSubscriptionState::PastDue, true)
        }
        (LocalSubscriptionState::Active, EventType::CheckoutSessionCompleted) => {
            // Checkout session completed can activate a new subscription
            (LocalSubscriptionState::Active, true)
        }

        // From GracePeriod
        (LocalSubscriptionState::GracePeriod, EventType::SubscriptionCreated) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::GracePeriod, EventType::SubscriptionUpdated) => {
            (LocalSubscriptionState::GracePeriod, true)
        }
        (LocalSubscriptionState::GracePeriod, EventType::SubscriptionDeleted) => {
            (LocalSubscriptionState::Canceled, true)
        }
        (LocalSubscriptionState::GracePeriod, EventType::InvoicePaid) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::GracePeriod, EventType::InvoicePaymentFailed) => {
            (LocalSubscriptionState::PastDue, true)
        }

        // From PastDue
        (LocalSubscriptionState::PastDue, EventType::SubscriptionCreated) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::PastDue, EventType::SubscriptionUpdated) => {
            (LocalSubscriptionState::PastDue, true)
        }
        (LocalSubscriptionState::PastDue, EventType::SubscriptionDeleted) => {
            (LocalSubscriptionState::Canceled, true)
        }
        (LocalSubscriptionState::PastDue, EventType::InvoicePaid) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::PastDue, EventType::InvoicePaymentFailed) => {
            // Stay in past due, maybe escalate after grace period
            (LocalSubscriptionState::PastDue, true)
        }

        // From Canceled
        (LocalSubscriptionState::Canceled, EventType::SubscriptionCreated) => {
            // Reinstate a canceled subscription
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Canceled, _) => (LocalSubscriptionState::Canceled, false),

        // From Incomplete
        (LocalSubscriptionState::Incomplete, EventType::SubscriptionCreated) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Incomplete, _) => (LocalSubscriptionState::Incomplete, false),

        // From Triling
        (LocalSubscriptionState::Trialing, EventType::SubscriptionCreated) => {
            // Should not happen; trial starts active
            (LocalSubscriptionState::Trialing, false)
        }
        (LocalSubscriptionState::Trialing, EventType::SubscriptionUpdated) => {
            // Trial ends → move to active or past_due depending on payment
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Trialing, EventType::SubscriptionDeleted) => {
            (LocalSubscriptionState::Canceled, true)
        }
        (LocalSubscriptionState::Trialing, EventType::InvoicePaid) => {
            (LocalSubscriptionState::Active, true)
        }
        (LocalSubscriptionState::Trialing, EventType::InvoicePaymentFailed) => {
            (LocalSubscriptionState::PastDue, true)
        }

        // Fallback — refuse any unmatched combination
        (current.clone, false),
    };

    (next, allowed)
}

/// Map a Stripe price_id to an internal plan_code, using the config's
/// `price_to_plan` mapping. If the price_id is not found, return `None`.
///
/// This is a pure function — it does not hit the DB. The application layer
/// looks up the price_id from the `price_to_plan` map populated at startup.
pub fn price_id_to_plan(price_id: &str, price_to_plan: &serde_json::Map<String, String>) -> Option<&str> {
    price_to_plan.get(price_id).map(|s| s.as_str())
}

/// Map a raw Stripe event to the internal subscription state transition,
/// using the price→plan map for context (e.g. determining which plan changed).
///
/// This is the main entry point called by the application service layer.
/// It returns `(new_state, allowed, plan_code_changed)` where `plan_code_changed`
/// is `Some(plan_code)` if the event involved a plan change, or `None` otherwise.
pub fn map_stripe_event(
    current: super::mapping::LocalSubscriptionState,
    event: &StripeEvent,
    price_to_plan: &serde_json::Map<String, String>,
) -> (super::mapping::LocalSubscriptionState, bool, Option<&'static str>) {
    let (next, allowed) = stripe_status_to_subscription_state(current, event);

    // Determine if a plan_code changed (simple heuristic: if the event is
    // SubscriptionUpdated and the subscription has items, check the price)
    let plan_changed = match (&event.r#type, &event.data.object) {
        ("customer.subscription.updated", Some(obj)) => {
            // Try to get the price from the subscription items
            if let Ok(sub) = serde_json::from_value::<StripeSubscription>(serde_json::json!(obj)) {
                if let Some(item) = sub.items.first() {
                    price_id_to_plan(&item.price, price_to_plan).map(|p| p as &'static str)
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    };

    (next, allowed, plan_changed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::StripeEvent;

    #[test]
    fn test_price_id_to_plan() {
        let map = serde_json::Map::from([("price_123".into(), "basic".into()), ("price_456".into(), "pro".into())]);
        assert_eq!(price_id_to_plan("price_123", &map), Some("basic"));
        assert_eq!(price_id_to_plan("price_999", &map), None);
    }

    #[test]
    fn test_map_stripe_event_unknown() {
        let map = serde_json::Map::new();
        let event = StripeEvent {
            object: "event".into(),
            id: "evt_001".into(),
            r#type: "unknown.event.type".into(),
            data: crate::models::StripeEventData {
                object: "".into(),
                id: "".into(),
                previous_attributes: None,
            },
        };
        let (state, allowed, plan_changed) = map_stripe_event(super::mapping::LocalSubscriptionState::Active, &event, &map);
        assert!(allowed);
        assert_eq!(state, super::mapping::LocalSubscriptionState::Active);
        assert!(plan_changed.is_none());
    }

    #[test]
    fn test_map_stripe_event_subscription_created() {
        let map = serde_json::Map::new();
        let event = StripeEvent {
            object: "event".into(),
            id: "evt_002".into(),
            r#type: "customer.subscription.created".into(),
            data: crate::models::StripeEventData {
                object: "subscription".into(),
                id: "sub_001".into(),
                previous_attributes: None,
            },
        };
        let (state, allowed, plan_changed) = map_stripe_event(super::mapping::LocalSubscriptionState::Incomplete, &event, &map);
        assert!(allowed);
        // Should transition to Active
        assert_eq!(state, super::mapping::LocalSubscriptionState::Active);
    }

    #[test]
    fn test_map_stripe_event_invoice_paid() {
        let map = serde_json::Map::new();
        let event = StripeEvent {
            object: "event".into(),
            id: "evt_003".into(),
            r#type: "invoice.paid".into(),
            data: crate::models::StripeEventData {
                object: "invoice".into(),
                id: "in_001".into(),
                previous_attributes: None,
            },
        };
        let (state, allowed, plan_changed) = map_stripe_event(super::mapping::LocalSubscriptionState::PastDue, &event, &map);
        assert!(allowed);
        // Should move to Active
        assert_eq!(state, super::mapping::LocalSubscriptionState::Active);
    }
}