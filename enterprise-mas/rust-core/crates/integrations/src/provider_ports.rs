//! PURPOSE: The provider boundary ports for billing checkout and WhatsApp
//! messaging, declared here (the lower-level integration crate) so the
//! application layer composes them and this crate provides the production
//! implementations behind guarded egress.
//!
//! Owning layer: `mas-integrations`.
//!
//! Production responsibility: say exactly which provider calls the billing
//! and WhatsApp use cases may make, no more.
//!
//! It must NOT:
//!   * carry secrets -- signing keys and access tokens arrive as
//!     [`SecretString`](mas_security::SecretString) at the implementations;
//!   * know anything about tenants, receipts, or platform state.

use std::fmt;

use async_trait::async_trait;
use mas_common::ids::TenantId;
use mas_common::result::Result;

/// A provider customer the provider accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderCustomer {
    /// Provider handle (`cus_...`).
    pub provider_customer_id: String,
}

/// A checkout session the provider accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderSession {
    /// Provider session handle (`cs_...`).
    pub provider_session_id: String,
    /// The URL to send the browser to.
    pub checkout_url: String,
}

/// A customer creation request.
#[derive(Debug, Clone)]
pub struct CustomerRequest {
    /// Owning tenant (provider-side metadata only).
    pub tenant_id: TenantId,
    /// Contact email, when known.
    pub email: Option<String>,
    /// Idempotency key for the provider call.
    pub idempotency_key: String,
}

/// A checkout session creation request.
#[derive(Debug, Clone)]
pub struct CheckoutRequest {
    /// The provider customer to charge.
    pub provider_customer_id: String,
    /// The provider price to charge.
    pub provider_price_id: String,
    /// Browser return URL on success.
    pub success_url: String,
    /// Browser return URL on cancellation.
    pub cancel_url: String,
    /// Idempotency key for the provider call.
    pub idempotency_key: String,
}

/// A billing-portal session creation request.
#[derive(Debug, Clone)]
pub struct PortalRequest {
    /// The provider customer whose billing portal to open.
    pub provider_customer_id: String,
    /// Browser return URL when they leave the portal.
    pub return_url: String,
    /// Idempotency key for the provider call.
    pub idempotency_key: String,
}

/// A billing-portal session the provider accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPortal {
    /// Where to send the browser.
    pub portal_url: String,
}

/// The billing provider boundary: the REST calls checkout and the portal need.
#[async_trait]
pub trait CheckoutProviderPort: fmt::Debug + Send + Sync {
    /// Creates (or reuses) the provider customer.
    async fn create_customer(&self, request: &CustomerRequest) -> Result<ProviderCustomer>;

    /// Opens a provider checkout session.
    async fn create_checkout_session(&self, request: &CheckoutRequest) -> Result<ProviderSession>;

    /// Opens a provider billing-portal session.
    async fn create_portal_session(&self, request: &PortalRequest) -> Result<ProviderPortal>;
}

/// What the provider confirmed about a send.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewaySendReceipt {
    /// The provider message id (`wamid....`).
    pub wamid: String,
    /// The provider's creation timestamp (unix seconds), when given.
    pub timestamp_unix: Option<i64>,
}

/// One message template the provider listed for a WABA.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayTemplate {
    /// Provider template name.
    pub name: String,
    /// Language code the template is approved for.
    pub language: String,
    /// Provider status (`APPROVED`, `PENDING`, ...), stored lowercased.
    pub status: String,
    /// Body preview, when the listing carried one.
    pub body: Option<String>,
}

/// The WhatsApp provider boundary for outbound sends.
#[async_trait]
pub trait WhatsAppGatewayPort: fmt::Debug + Send + Sync {
    /// Sends one text message; the provider names it `wamid`.
    async fn send_text(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
        to: &str,
        body: &str,
    ) -> Result<GatewaySendReceipt>;

    /// Sends one template message (the only legal outbound after the 24h window).
    async fn send_template(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
        to: &str,
        name: &str,
        language: &str,
    ) -> Result<GatewaySendReceipt>;

    /// Lists the WABA's message templates.
    async fn list_templates(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
    ) -> Result<Vec<GatewayTemplate>>;
}
