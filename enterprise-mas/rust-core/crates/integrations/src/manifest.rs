//! Provider manifests: declarative descriptors telling the integration
//! layer what a provider IS before any tenant installs it.
//!
//! A manifest carries the provider key, the integration [`ConnectorCategory`]
//! (which drives config validation), the auth modes an installation may
//! choose, and the config key contract. Manifests are configuration, never
//! credentials — a manifest must not hold secret material.

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_domain::ConnectorAuthMode;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

string_enum! {
    /// Broad integration category; determines which config validation rules
    /// the connector factory applies.
    ConnectorCategory {
        Database => "database",
        Storage => "storage",
        Messaging => "messaging",
        Crm => "crm",
        Ticketing => "ticketing",
        Email => "email",
        Webhook => "webhook",
        Custom => "custom",
    }
}

/// Upper bound for provider keys.
pub const MAX_PROVIDER_KEY_LEN: usize = 64;

/// A validated provider descriptor.
///
/// `supported_auth` is a `Vec` (not set) to keep serde shapes simple; it is
/// de-duplicated and validated at construction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderManifest {
    /// Unique provider key, e.g. `slack` (`[a-z0-9][a-z0-9._-]+`).
    pub provider: String,
    /// Human-readable name, e.g. `Slack`.
    pub name: String,
    /// Category driving config validation for installations.
    pub category: ConnectorCategory,
    /// Auth modes a tenant instance may select.
    pub supported_auth: Vec<ConnectorAuthMode>,
    /// Config keys every installation MUST provide.
    #[serde(default)]
    pub required_config: BTreeSet<String>,
    /// Config keys an installation MAY provide (operator docs/validation).
    #[serde(default)]
    pub optional_config: BTreeSet<String>,
    /// Free-form operator notes (rendered in docs/UI).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ProviderManifest {
    /// Creates a manifest, validating the provider key and name.
    pub fn new(
        provider: impl Into<String>,
        name: impl Into<String>,
        category: ConnectorCategory,
        supported_auth: Vec<ConnectorAuthMode>,
    ) -> Result<Self> {
        let provider = provider.into();
        let name = name.into();

        if provider.len() < 2 || provider.len() > MAX_PROVIDER_KEY_LEN {
            return Err(AppError::invalid_field(
                "provider",
                "invalid_length",
                format!("provider key must be 2..={MAX_PROVIDER_KEY_LEN} characters"),
            ));
        }
        let first_ok = provider
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphanumeric());
        let chars_ok = provider
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
        if !first_ok || !chars_ok {
            return Err(AppError::invalid_field(
                "provider",
                "invalid_format",
                "provider keys must match [a-z0-9][a-z0-9._-]+",
            ));
        }
        mas_common::validation::validate_length("name", &name, 1, 128)?;
        if supported_auth.is_empty() {
            return Err(AppError::invalid_field(
                "supported_auth",
                "required",
                "a manifest must declare at least one auth mode",
            ));
        }

        let mut deduped = Vec::with_capacity(supported_auth.len());
        for mode in supported_auth {
            if !deduped.contains(&mode) {
                deduped.push(mode);
            }
        }

        Ok(Self {
            provider,
            name,
            category,
            supported_auth: deduped,
            required_config: BTreeSet::new(),
            optional_config: BTreeSet::new(),
            description: None,
        })
    }

    /// Builder: declare required config keys.
    #[must_use]
    pub fn require_config<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required_config = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Builder: declare optional config keys.
    #[must_use]
    pub fn allow_config<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.optional_config = keys.into_iter().map(Into::into).collect();
        self
    }

    /// Builder: attach operator notes.
    #[must_use]
    pub fn describe(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Whether `mode` is a supported auth choice for this provider.
    #[must_use]
    pub fn supports_auth(&self, mode: &ConnectorAuthMode) -> bool {
        self.supported_auth.contains(mode)
    }

    /// Enforces the config key contract: every required key must be present
    /// with a non-null, non-empty-string value.
    pub fn validate_key_contract(
        &self,
        config: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<()> {
        for key in &self.required_config {
            match config.get(key) {
                None => {
                    return Err(AppError::invalid_field(
                        "config",
                        "missing_required_key",
                        format!(
                            "config key '{key}' is required for provider '{}'",
                            self.provider
                        ),
                    ));
                },
                Some(serde_json::Value::Null) => {
                    return Err(AppError::invalid_field(
                        "config",
                        "null_value",
                        format!("config key '{key}' must not be null"),
                    ));
                },
                Some(serde_json::Value::String(s)) if s.trim().is_empty() => {
                    return Err(AppError::invalid_field(
                        "config",
                        "empty_value",
                        format!("config key '{key}' must not be empty"),
                    ));
                },
                Some(_) => {},
            }
        }
        Ok(())
    }
}

/// Built-in provider manifests shipped with the platform. Operators may
/// register their own on top via
/// [`crate::registry::ProviderRegistryPort::register`].
#[must_use]
pub fn builtin_manifests() -> Vec<ProviderManifest> {
    vec![
        ProviderManifest::new(
            "slack",
            "Slack",
            ConnectorCategory::Messaging,
            vec![ConnectorAuthMode::OAuth2, ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest slack")
        .allow_config(["default_channel"])
        .describe("Slack workspace messaging connector"),
        ProviderManifest::new(
            "salesforce",
            "Salesforce",
            ConnectorCategory::Crm,
            vec![ConnectorAuthMode::OAuth2, ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest salesforce")
        .require_config(["api_version"])
        .describe("Salesforce CRM connector"),
        ProviderManifest::new(
            "postgres",
            "PostgreSQL",
            ConnectorCategory::Database,
            vec![ConnectorAuthMode::Basic, ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest postgres")
        .require_config(["host", "port"])
        .allow_config(["database", "connect_timeout_ms"])
        .describe("PostgreSQL database connector (DSN assembled at the edge; credentials via SecretReference)"),
        ProviderManifest::new(
            "s3",
            "Amazon S3",
            ConnectorCategory::Storage,
            vec![ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest s3")
        .require_config(["bucket"])
        .allow_config(["region", "endpoint", "prefix"])
        .describe("S3 object storage connector"),
        ProviderManifest::new(
            "rabbitmq",
            "RabbitMQ",
            ConnectorCategory::Messaging,
            vec![ConnectorAuthMode::Basic],
        )
        .expect("builtin manifest rabbitmq")
        .require_config(["broker_url"])
        .allow_config(["exchange", "vhost"])
        .describe("RabbitMQ AMQP connector"),
        ProviderManifest::new(
            "jira",
            "Jira",
            ConnectorCategory::Ticketing,
            vec![ConnectorAuthMode::OAuth2, ConnectorAuthMode::ApiKey, ConnectorAuthMode::Basic],
        )
        .expect("builtin manifest jira")
        .allow_config(["base_url", "project_key"])
        .describe("Jira ticketing connector"),
        ProviderManifest::new(
            "sendgrid",
            "SendGrid",
            ConnectorCategory::Email,
            vec![ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest sendgrid")
        .allow_config(["from_address", "region"])
        .describe("SendGrid email connector"),
        ProviderManifest::new(
            "generic-webhook",
            "Generic Webhook",
            ConnectorCategory::Webhook,
            vec![ConnectorAuthMode::None, ConnectorAuthMode::ApiKey],
        )
        .expect("builtin manifest generic-webhook")
        .allow_config(["default_url"])
        .describe("Outbound HTTP webhook connector (signed payloads)"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_key_and_name_are_validated() {
        assert!(ProviderManifest::new(
            "Slack",
            "Slack",
            ConnectorCategory::Messaging,
            vec![ConnectorAuthMode::ApiKey]
        )
        .is_err());
        assert!(ProviderManifest::new(
            "a",
            "short",
            ConnectorCategory::Custom,
            vec![ConnectorAuthMode::None]
        )
        .is_err());
        assert!(ProviderManifest::new(
            "ok-slack",
            "",
            ConnectorCategory::Custom,
            vec![ConnectorAuthMode::None]
        )
        .is_err());
        assert!(
            ProviderManifest::new("ok-slack", "Slack", ConnectorCategory::Custom, vec![]).is_err()
        );
        let dup = ProviderManifest::new(
            "ok-slack",
            "Slack",
            ConnectorCategory::Custom,
            vec![ConnectorAuthMode::ApiKey, ConnectorAuthMode::ApiKey],
        )
        .expect("manifest");
        assert_eq!(
            dup.supported_auth.len(),
            1,
            "duplicates de-duplicated at construction"
        );
        assert!(dup.supports_auth(&ConnectorAuthMode::ApiKey));
        assert!(!dup.supports_auth(&ConnectorAuthMode::OAuth2));
    }

    #[test]
    fn key_contract_rejects_missing_null_and_empty_required_keys() {
        let manifest = ProviderManifest::new(
            "postgres",
            "PostgreSQL",
            ConnectorCategory::Database,
            vec![ConnectorAuthMode::Basic],
        )
        .expect("manifest")
        .require_config(["host", "port"]);

        let mut config = serde_json::Map::new();
        config.insert(
            "host".to_owned(),
            serde_json::Value::String("db.example.com".to_owned()),
        );
        assert!(
            manifest.validate_key_contract(&config).is_err(),
            "missing 'port'"
        );

        config.insert("port".to_owned(), serde_json::Value::Null);
        assert!(
            manifest.validate_key_contract(&config).is_err(),
            "null value"
        );

        config.insert("port".to_owned(), serde_json::Value::String(" ".to_owned()));
        assert!(
            manifest.validate_key_contract(&config).is_err(),
            "empty string"
        );

        config.insert("port".to_owned(), serde_json::json!(5432));
        assert!(
            manifest.validate_key_contract(&config).is_ok(),
            "complete contract accepted"
        );
    }

    #[test]
    fn builtins_cover_all_spec_categories() {
        let builtins = builtin_manifests();
        assert!(builtins.len() >= 8);
        for category in [
            ConnectorCategory::Database,
            ConnectorCategory::Storage,
            ConnectorCategory::Messaging,
            ConnectorCategory::Crm,
            ConnectorCategory::Ticketing,
            ConnectorCategory::Email,
            ConnectorCategory::Webhook,
        ] {
            assert!(
                builtins.iter().any(|m| m.category == category),
                "category {category:?} must have a builtin manifest"
            );
        }
    }
}
