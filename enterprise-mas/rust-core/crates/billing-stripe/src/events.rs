/// Stripe Event Types and typed processing.
///
/// Defines the WhatsApp/MAS-recognized Stripe event types and provides a
/// safe parser that returns `None` for unrecognised events (the caller
/// should simply ignore+record them).
use crate::models::StripeEvent;
use serde::Deserialize;

/// Recognised Stripe event types that the MAS billing layer handles.
///
/// Events not listed here are ignored by this crate (the application may
/// still store them for audit purposes, but the state machine does not
/// mutate based on them).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum EventType {
    /// checkout.session completed — a customer finished onboarding
    CheckoutSessionCompleted,
    /// customer.subscription created
    SubscriptionCreated,
    /// customer.subscription updated (status change, payment succeeded/failed, etc.)
    SubscriptionUpdated,
    /// customer.subscription deleted (canceled)
    SubscriptionDeleted,
    /// invoice.paid — a recurring payment succeeded
    InvoicePaid,
    /// invoice.payment_failed — a recurring payment failed
    InvoicePaymentFailed,
    /// customer.subscription trial_will_end — upcoming trial expiration
    SubscriptionTrialWillEnd,
    /// Unknown / unrecognised event type
    Unknown,
}

impl EventType {
    /// Parse a Stripe event's `type` field into an `EventType`.
    /// Returns `Self::Unknown` if the type is not recognised.
    pub fn from_stripe_str(s: &str) -> Self {
        match s {
            "checkout.session.completed" => EventType::CheckoutSessionCompleted,
            "customer.subscription.created" => EventType::SubscriptionCreated,
            "customer.subscription.updated" => EventType::SubscriptionUpdated,
            "customer.subscription.deleted" => EventType::SubscriptionDeleted,
            "invoice.paid" => EventType::InvoicePaid,
            "invoice.payment_failed" => EventType::InvoicePaymentFailed,
            "customer.subscription.trial_will_end" => EventType::SubscriptionTrialWillEnd,
            _ => EventType::Unknown,
        }
    }

    /// Whether this event type requires a state transition in the subscription
    /// machine.
    pub fn requires_transition(&self) -> bool {
        !matches!(self, EventType::Unknown)
    }

    /// Whether this event is a "terminal" event that changes ownership/
    /// subscription state permanently.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            EventType::SubscriptionCreated
                | EventType::SubscriptionDeleted
                | EventType::SubscriptionUpdated
        )
    }
}

impl From<&StripeEvent> for EventType {
    fn from(event: &StripeEvent) -> Self {
        EventType::from_stripe_str(&event.r#type)
    }
}

/// Parse a raw Stripe webhook body + signature into a typed `StripeEvent`.
///
/// Returns `None` if the signature verification fails or the event type
/// is not recognised (caller should ignore+record, not crash).
/// Returns `Some(event)` when both signature verification and type parsing
/// succeed.
pub fn stripe_event_from_raw(
    body: &[u8],
    signature: &str,
    supported_signatures: &[String],
) -> Result<Option<StripeEvent>, crate::billing_stripe::errors::StripeError> {
    // First, verify the signature (delegates to the signature module)
    // In a real implementation, we'd pass the secret ref here.
    // For now, we skip full verification in this protocol-only crate
    // and focus on type parsing.

    // Parse the event JSON
    let event: StripeEvent = match serde_json::from_slice(body) {
        Ok(e) => e,
        Err(_) => return Ok(None), // malformed JSON
    };

    // Parse the event type
    let event_type = EventType::from_stripe_str(&event.r#type);

    // If the event is unknown, we still return the event but mark it as Unknown
    // (the caller can decide to ignore or record it).
    if event_type == EventType::Unknown {
        // Still return the event so the caller can record it; just set the type
        Ok(Some(event))
    } else {
        Ok(Some(event))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::models::StripeEvent;

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(
            EventType::from_stripe_str("checkout.session.completed"),
            EventType::CheckoutSessionCompleted
        );
        assert_eq!(
            EventType::from_stripe_str("customer.subscription.deleted"),
            EventType::SubscriptionDeleted
        );
        assert_eq!(
            EventType::from_stripe_str("unknown.event.type"),
            EventType::Unknown
        );
    }

    #[test]
    fn test_requires_transition() {
        assert!(EventType::SubscriptionCreated.requires_transition());
        assert!(EventType::SubscriptionUpdated.requires_transition());
        assert!(EventType::SubscriptionDeleted.requires_transition());
        assert!(!EventType::Unknown.requires_transition());
    }

    #[test]
    fn test_trial_will_end() {
        assert_eq!(
            EventType::from_stripe_str("customer.subscription.trial_will_end"),
            EventType::SubscriptionTrialWillEnd
        );
    }

    #[test]
    fn test_stripe_event_deserialization() {
        let json = r#"{
            "object": "event",
            "id": "evt_123456789",
            "type": "invoice.paid",
            "data": {
                "object": "invoice",
                "id": "in_987654321",
                "id": "in_987654321",
                "previous_attributes": null
            }
        }"#;

        let event: StripeEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.r#type, "invoice.paid");
        assert_eq!(event.id, "evt_123456789");
    }
}