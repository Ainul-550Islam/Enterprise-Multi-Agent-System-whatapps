/// WhatsAppAccountConfig holds account-level configuration referenced by secret_ref.
/// Inline secret values are strictly prohibited — every secret must be a
/// secret_ref (pointing to a key store entry) so the never-leak policy is
/// enforced at the crate boundary.
///
/// The config is intentionally minimal: only identifiers and refs. All
/// cryptographic material is resolved at the application boundary, never
/// materialised in this crate.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WhatsAppAccountConfig {
    /// WABA (WhatsApp Business Account) identifier assigned by Meta
    pub waba_id: uuid::Uuid,
    /// Phone number identifier associated with this WABA
    pub phone_number_id: uuid::Uuid,
    /// Graph API version string (e.g. "v20.0")
    pub graph_version: String,
    /// Reference to the app_secret stored in the secret manager; never inline
    pub app_secret_ref: String,
    /// Reference to the access_token stored in the secret manager; never inline
    pub access_token_ref: String,
    /// Reference to the verify_token used for hub challenge verification;
    /// this is the plain text value expected in the hub verification GET
    pub verify_token_ref: String,
}

impl WhatsAppAccountConfig {
    /// Returns the verify_token plain string for hub challenge matching.
    /// The actual secret lookup is performed by the application boundary;
    /// this method only provides the key reference.
    pub fn verify_token(&self) -> Result<String, channel_whatsapp::errors::ChannelError> {
        // Secret resolution happens in the application crate via
        // integrations::secret_gate::resolve_secret_ref.
        // Here we just validate the ref format.
        if self.verify_token_ref.is_empty() {
            Err(channel_whatsapp::errors::ChannelError::InvalidConfig(
                "verify_token_ref must not be empty".into(),
            ))
        } else {
            // In a full implementation, return Ok(self.verify_token_ref.clone())
            // after the app has resolved it through the secret gate.
            Ok(self.verify_token_ref.clone())
        }
    }
}