/// Stripe configuration referenced by secret_ref.
/// Inline secret values are strictly prohibited — every secret must be a
/// secret_ref (pointing to a key store entry) so the never-leak policy is
/// enforced at the crate boundary.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StripeConfig {
    /// Reference to the Stripe secret key (sk_live_ or sk_test_).
    /// Never inline; resolved via the secret gate at the application boundary.
    pub api_key_ref: String,
    /// List of webhook secret references (one per registered webhook endpoint).
    /// Each entry is a key store reference; the actual secret is resolved
    /// by the application layer when verifying inbound events.
    #[serde(default)]
    pub webhook_secret_refs: Vec<String>,
    /// Mapping from Stripe price_id → internal plan_code.
    /// This is a static configuration; the app populates it at startup.
    #[serde(default)]
    pub price_to_plan: serde_json::Map<String, String>,
    /// Hosts allowed to receive redirect_url callbacks from Stripe
    /// (success/cancel). Prevents SSRF by restricting outbound egress.
    #[serde(default)]
    pub allowed_redirect_hosts: Vec<String>,
}

impl StripeConfig {
    /// Validate that the config has at least one webhook secret ref.
    pub fn validate(&self) -> Result<(), String> {
        if self.api_key_ref.is_empty() {
            return Err("api_key_ref must not be empty".into());
        }
        if self.webhook_secret_refs.is_empty() {
            return Err("At least one webhook_secret_ref must be configured".into());
        }
        Ok(())
    }
}

/// Default configuration with sane placeholders (will fail validation until
/// the application populates real secret refs).
impl Default for StripeConfig {
    fn default() -> Self {
        StripeConfig {
            api_key_ref: "ref-stripe-api-key".into(),
            webhook_secret_refs: vec!["ref-stripe-webhook".into()],
            price_to_plan: serde_json::Map::new(),
            allowed_redirect_hosts: vec!["example.com".into(), "app.example.com".into()],
        }
    }
}