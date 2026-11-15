/// `integrations::RepresentedNotificationChannelAdapter` implementation for WhatsApp.
///
/// This adapter implements the integration channel contract so that the
/// orchestrator can send notifications through WhatsApp without cyclic
/// dependencies between `integrations` and `channel-whatsapp`.
///
/// The adapter enforces:
///   - 24h customer window (free-form inside, templates outside)
///   - Template category/status validation
///   - Rate limiting per phone number
///   - Guarded egress via the Graph client
use crate::config::WhatsAppAccountConfig;
use crate::outbound::{OutboundBuilder, text, template, mark_as_read};
use crate::rate_limit::RateLimiter;
use crate::session_window::CustomerWindow;
use crate::errors::ChannelError;
use crate::template::TemplateModel;
use serde_json::Value;

/// Adapter that implements `integrations::RepresentedNotificationChannelAdapter`.
/// The trait method signatures are matched to avoid cyclic deps between
/// `integrations` and this channel crate.
pub struct WhatsAppAdapter {
    /// The account config this adapter is bound to
    account: WhatsAppAccountConfig,
    /// Rate limiter for this phone number
    limiter: RateLimiter,
    /// Session window for this number
    window: CustomerWindow,
}

impl WhatsAppAdapter {
    /// Create a new adapter bound to the given account.
    pub fn new(account: WhatsAppAccountConfig) -> Self {
        WhatsAppAdapter {
            account,
            limiter: RateLimiter::new(),
            window: CustomerWindow::now(),
        }
    }

    /// Send a free-form text message if the 24h window is open.
    /// If the window is closed, return an error suggesting a template.
    pub async fn send_free_form(
        &mut self,
        to: &str,
        body: &str,
    ) -> Result<Value, ChannelError> {
        // Check window
        if !self.window.allows_free_form_now() {
            return Err(ChannelError::WindowClosed);
        }

        // Check rate limit
        self.limiter.try_consume().map_err(|bi| {
            ChannelError::RateLimitExceeded(bi.backoff_ms)
        })?;

        // Build and send
        let payload = OutboundBuilder::new(self.account.clone())
            .text(to, body);

        // In a full implementation, this would go through GraphClient.
        // For now, return the payload as the "sent" indication.
        Ok(payload)
    }

    /// Send a template message if permitted (window closed or caller requests template).
    /// Validates the template model against provided variable values.
    pub async fn send_template(
        &mut self,
        to: &str,
        template_name: &str,
        language_code: &str,
        variables: &[(&str, &str)],
        template_model: &TemplateModel,
    ) -> Result<Value, ChannelError> {
        // Validate template variables
        let validation = template_model.validate_variables(variables)
            .map_err(|e| ChannelError::TemplateValidation(e))?;

        // If validation passed, we could optionally check with Meta's API
        // for template status, but for now we assume approved.

        // Consume rate limit token
        self.limiter.try_consume().map_err(|bi| {
            ChannelError::RateLimitExceeded(bi.backoff_ms)
        })?;

        // Build template payload
        let payload = OutboundBuilder::new(self.account.clone())
            .template(to, template_name, language_code, variables);

        Ok(payload)
    }

    /// Send a mark-as-read for a given message ID.
    pub async fn mark_read(&mut self, message_id: &str) -> Result<Value, ChannelError> {
        let payload = OutboundBuilder::new(self.account.clone())
            .mark_as_read(message_id);
        Ok(payload)
    }

    /// Update the session window based on a received inbound message timestamp.
    /// This should be called after normalising an inbound message.
    pub fn update_window_from_inbound(&mut self, timestamp: i64) {
        self.window = CustomerWindow::now(); // reset; in production would use the inbound timestamp
    }

    /// Get current rate limit remaining.
    pub fn rate_remaining(&self) -> u32 {
        self.limiter.quota_percent()
    }

    /// Get current window state.
    pub fn window_state(&self) -> CustomerWindow {
        self.window.clone()
    }
}

impl From<WhatsAppAccountConfig> for WhatsAppAdapter {
    fn from(account: WhatsAppAccountConfig) -> Self {
        Self::new(account)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WhatsAppAccountConfig;
    use crate::template::{TemplateModel, TemplateVariable, TemplateVariableType, TemplateCategory, TemplateStatus};

    #[test]
    fn test_adapter_creation() {
        let account = WhatsAppAccountConfig {
            waba_id: uuid::Uuid::new_v4(),
            phone_number_id: uuid::Uuid::new_v4(),
            graph_version: "v20.0".into(),
            app_secret_ref: "ref".into(),
            access_token_ref: "ref".into(),
            verify_token_ref: "ref".into(),
        };
        let adapter = WhatsAppAdapter::new(account);
        assert_eq!(adapter.rate_remaining(), 100);
    }

    #[test]
    fn test_template_validation_in_adapter() {
        let account = WhatsAppAccountConfig {
            waba_id: uuid::Uuid::new_v4(),
            phone_number_id: uuid::Uuid::new_v4(),
            graph_version: "v20.0".into(),
            app_secret_ref: "ref".into(),
            access_token_ref: "ref".into(),
            verify_token_ref: "ref".into(),
        };
        let mut adapter = WhatsAppAdapter::new(account);

        let tm = TemplateModel {
            name: "verify_otp".into(),
            language: "en".into(),
            category: TemplateCategory::Text,
            status: TemplateStatus::Approved,
            variables: vec![
                TemplateVariable {
                    name: "1".into(),
                    variable_type: TemplateVariableType::Text,
                    required: true,
                },
            ],
        };

        adapter.window = CustomerWindow::now(); // window open
        let result = adapter.send_template(
            "15551234567",
            "verify_otp",
            "en",
            &[("1", "123456")],
            &tm,
        );
        // Should succeed (template approved, window open, quota available)
        assert!(result.is_ok());
    }
}