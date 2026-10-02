//! Category-driven connector factory and connection probes.
//!
//! The factory turns a provider [`crate::manifest::ProviderManifest`] plus
//! tenant-supplied, **non-secret** configuration into a persisted
//! [`mas_domain::Connector`] in `PendingVerification`:
//!
//! 1. manifest lookup (unknown providers reject),
//! 2. auth-mode check against the manifest,
//! 3. config key contract + category rules
//!    ([`validate_category_config`]) — with a blanket
//!    *no-secret-material-in-config* rule backed by
//!    [`mas_common::redaction::is_sensitive_key`]: credentials bind through
//!    `SecretReference`, never through connector config,
//! 4. persistence via [`crate::registry::ConnectorRegistryPort`].
//!
//! Connection probes ([`ConnectionProbePort`]) advance the status
//! lifecycle to `Active` or `Error` with a sanitized, single-line error —
//! probing results never leak provider responses wholesale.

use async_trait::async_trait;
use mas_common::enums::ConnectorStatus;
use mas_common::error::AppError;
use mas_common::ids::{ConnectorId, OrganizationId, TenantId};
use mas_common::redaction::is_sensitive_key;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::value_objects::SafeUrl;
use mas_domain::{Connector, ConnectorAuthMode};
use serde_json::{Map, Value};

use crate::manifest::ConnectorCategory;
use crate::registry::{ConnectorRegistryPort, ProviderRegistryPort};

/// Maximum characters kept from a provider error, single-line cleaned.
pub const MAX_STORED_ERROR_CHARS: usize = 512;

/// Validates `config` for `category`:
///
/// * global rule — any key classified sensitive
///   (`password`, `secret`, `token`, `api_key`, …) rejects with guidance
///   to use secret references;
/// * database — `host` printable without '@' (no userinfo smuggling),
///   `port` in `1..=65535`, any `dsn` key must not embed credentials;
/// * storage — `bucket` follows S3-style naming (3..=63 lowercase
///   alnum/'-'/'.', no IP literals, no adjacent separators);
/// * messaging — `broker_url` uses an approved scheme
///   (`amqp(s)`,`kafka`,`nats`,`tls`,`tcp`) without embedded credentials;
/// * CRM/ticketing — `base_url`/`default_url` must parse as [`SafeUrl`]
///   when present; `project_key` matches `[A-Z][A-Z0-9]{1,9}`;
/// * email — `from_address` has a single '@' with non-empty local/domain,
///   sender domain contains a dot.
pub fn validate_category_config(
    category: &ConnectorCategory,
    config: &Map<String, Value>,
) -> Result<()> {
    for key in config.keys() {
        if is_sensitive_key(key) {
            return Err(AppError::invalid_field(
                "config",
                "secret_in_config",
                format!(
                    "key '{key}' looks like secret material; bind credentials through a SecretReference, never connector config"
                ),
            ));
        }
    }

    let get_str = |key: &str| -> Option<&str> { config.get(key).and_then(Value::as_str) };

    match category {
        ConnectorCategory::Database => {
            if let Some(host) = get_str("host") {
                if host.contains('@') {
                    return Err(AppError::invalid_field(
                        "config.host",
                        "credentials_not_allowed",
                        "host must not embed userinfo; use a SecretReference",
                    ));
                }
            }
            if let Some(port) = config.get("port").and_then(Value::as_u64) {
                if !(1..=65535).contains(&port) {
                    return Err(AppError::invalid_field(
                        "config.port",
                        "out_of_range",
                        "port must be in 1..=65535",
                    ));
                }
            }
            if let Some(dsn) = get_str("dsn") {
                let lowered = dsn.to_ascii_lowercase();
                if lowered.contains("password=") || lowered.contains("://") && lowered.contains('@')
                {
                    return Err(AppError::invalid_field(
                        "config.dsn",
                        "credentials_not_allowed",
                        "DSNs must not embed credentials; assemble at the edge from a SecretReference",
                    ));
                }
            }
        },
        ConnectorCategory::Storage => {
            if let Some(bucket) = get_str("bucket") {
                let chars_ok = bucket
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.');
                let ip_shaped =
                    !bucket.is_empty() && bucket.chars().all(|c| c.is_ascii_digit() || c == '.');
                let adjacent_separators =
                    bucket.contains("..") || bucket.contains(".-") || bucket.contains("-.");
                if !(3..=63).contains(&bucket.len())
                    || !chars_ok
                    || ip_shaped
                    || adjacent_separators
                {
                    return Err(AppError::invalid_field(
                        "config.bucket",
                        "invalid_bucket_name",
                        "bucket must be 3..=63 lowercase alnum/'-'/'.', not IP-shaped, without adjacent separators",
                    ));
                }
            }
            if let Some(endpoint) = get_str("endpoint") {
                SafeUrl::parse(endpoint)?;
            }
        },
        ConnectorCategory::Messaging => {
            if let Some(broker) = get_str("broker_url") {
                let (scheme, rest) = broker.split_once("://").ok_or_else(|| {
                    AppError::invalid_field(
                        "config.broker_url",
                        "invalid_format",
                        "broker_url must be scheme://host[:port]/…",
                    )
                })?;
                if !matches!(scheme, "amqp" | "amqps" | "kafka" | "nats" | "tls" | "tcp") {
                    return Err(AppError::invalid_field(
                        "config.broker_url",
                        "invalid_scheme",
                        format!("scheme '{scheme}' is not an approved messaging scheme"),
                    ));
                }
                if rest.contains('@') {
                    return Err(AppError::invalid_field(
                        "config.broker_url",
                        "credentials_not_allowed",
                        "broker_url must not embed credentials; use a SecretReference",
                    ));
                }
                if rest.is_empty() {
                    return Err(AppError::invalid_field(
                        "config.broker_url",
                        "missing_host",
                        "broker_url must name a host",
                    ));
                }
            }
        },
        ConnectorCategory::Crm | ConnectorCategory::Ticketing | ConnectorCategory::Webhook => {
            for key in ["base_url", "default_url", "api_base"] {
                if let Some(link) = get_str(key) {
                    SafeUrl::parse(link)?;
                }
            }
            if let Some(project) = get_str("project_key") {
                let valid = project.len() >= 2
                    && project.len() <= 10
                    && project
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_uppercase())
                    && project
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
                if !valid {
                    return Err(AppError::invalid_field(
                        "config.project_key",
                        "invalid_format",
                        "project keys match [A-Z][A-Z0-9]{1,9}",
                    ));
                }
            }
        },
        ConnectorCategory::Email => {
            if let Some(from) = get_str("from_address") {
                let parts: Vec<&str> = from.split('@').collect();
                let valid_shape = parts.len() == 2
                    && !parts[0].is_empty()
                    && parts[1].contains('.')
                    && parts[1].len() > 3;
                if !valid_shape {
                    return Err(AppError::invalid_field(
                        "config.from_address",
                        "invalid_email",
                        "from_address must be a mailbox on a dotted domain",
                    ));
                }
            }
        },
        ConnectorCategory::Custom => {},
    }
    Ok(())
}

/// Result-agnostic connection probe, implemented per provider family
/// (real implementations travel through the guarded egress client).
#[async_trait]
pub trait ConnectionProbePort: std::fmt::Debug + Send + Sync {
    /// Probes `connector`. `Ok(())` = reachable and authenticating.
    async fn probe(&self, connector: &Connector) -> Result<()>;
}

/// Provider-manifest-aware factory producing tenant connectors.
#[derive(Debug)]
pub struct ConnectorFactory<P: ProviderRegistryPort, C: ConnectorRegistryPort> {
    providers: P,
    connectors: C,
}

impl<P: ProviderRegistryPort, C: ConnectorRegistryPort> ConnectorFactory<P, C> {
    /// Builds a factory over the two registries.
    #[must_use]
    pub const fn new(providers: P, connectors: C) -> Self {
        Self {
            providers,
            connectors,
        }
    }

    /// Creates and persists a tenant connector after the full validation
    /// pipeline (manifest → auth → config contract → category rules).
    pub async fn create_instance(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        provider: &str,
        name: &str,
        auth_mode: ConnectorAuthMode,
        config: Map<String, Value>,
    ) -> Result<Connector> {
        let manifest = self
            .providers
            .get(provider)
            .await?
            .ok_or_else(|| AppError::not_found("provider manifest", provider))?;
        if !manifest.supports_auth(&auth_mode) {
            return Err(AppError::invalid_field(
                "auth_mode",
                "unsupported",
                format!("provider '{provider}' does not support auth mode {auth_mode}"),
            ));
        }
        manifest.validate_key_contract(&config)?;
        validate_category_config(&manifest.category, &config)?;

        let mut connector = Connector::register(tenant, organization, provider, name, auth_mode)?;
        connector.config = config;
        connector.updated_at = Timestamp::now();
        self.connectors.save(&connector).await?;
        tracing::debug!(connector = %connector.id, tenant = %tenant, provider = provider, "connector instance created");
        Ok(connector)
    }

    /// Runs a connection probe and advances the status lifecycle:
    /// `Ok` → `Active`, `Err` → `Error` with a sanitized single-line
    /// fragment. Either way `last_tested_at` is stamped and persisted.
    pub async fn connection_test<T: ConnectionProbePort>(
        &self,
        tenant: TenantId,
        id: ConnectorId,
        probe: &T,
        at: &Timestamp,
    ) -> Result<Connector> {
        let mut connector = self
            .connectors
            .get(tenant, id)
            .await?
            .ok_or_else(|| AppError::not_found("connector", id.to_string()))?;
        connector.last_tested_at = Some(*at);
        match probe.probe(&connector).await {
            Ok(()) => {
                connector.status = ConnectorStatus::Active;
                connector.last_error = None;
                tracing::debug!(connector = %id, "connection probe succeeded");
            },
            Err(err) => {
                connector.status = ConnectorStatus::Error;
                connector.last_error = Some(clean_error_fragment(&err.to_string()));
                tracing::warn!(connector = %id, "connection probe failed");
            },
        }
        connector.updated_at = Timestamp::now();
        self.connectors.save(&connector).await?;
        Ok(connector)
    }
}

/// Renders an error for durable storage: single-line, length-capped. The
/// source error text is treated as untrusted (may embed provider payloads).
pub fn clean_error_fragment(raw: &str) -> String {
    let clean: String = raw
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    clean.chars().take(MAX_STORED_ERROR_CHARS).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::{InMemoryConnectorRegistry, InMemoryProviderRegistry};

    fn config(pairs: &[(&str, Value)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect()
    }

    #[test]
    fn sensitive_keys_never_enter_connector_config() {
        let sneaky = config(&[("db_password", Value::String("hunter2".to_owned()))]);
        for category in [
            ConnectorCategory::Database,
            ConnectorCategory::Storage,
            ConnectorCategory::Crm,
            ConnectorCategory::Custom,
        ] {
            assert!(
                validate_category_config(&category, &sneaky).is_err(),
                "category {category:?} rejects secretish keys"
            );
        }
    }

    #[test]
    fn database_rules_reject_credentials_and_bad_ports() {
        let good = config(&[
            ("host", Value::String("db.internal.test".to_owned())),
            ("port", Value::from(5432_u64)),
        ]);
        assert!(validate_category_config(&ConnectorCategory::Database, &good).is_ok());

        let userinfo = config(&[("host", Value::String("u@db.internal.test".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Database, &userinfo).is_err());

        let port0 = config(&[("port", Value::from(70_000_u64))]);
        assert!(validate_category_config(&ConnectorCategory::Database, &port0).is_err());

        let dsn = config(&[("dsn", Value::String("postgres://u:p@h/db".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Database, &dsn).is_err());
    }

    #[test]
    fn storage_messaging_ticketing_and_email_rules() {
        let ip_bucket = config(&[("bucket", Value::String("192.168.0.1".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Storage, &ip_bucket).is_err());
        let good_bucket = config(&[("bucket", Value::String("tenant-assets-01".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Storage, &good_bucket).is_ok());

        let bad_scheme = config(&[("broker_url", Value::String("ftp://mq.test:5672".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Messaging, &bad_scheme).is_err());
        let credentialed = config(&[(
            "broker_url",
            Value::String("amqps://user:pw@mq.test:5671".to_owned()),
        )]);
        assert!(validate_category_config(&ConnectorCategory::Messaging, &credentialed).is_err());
        let good_broker = config(&[(
            "broker_url",
            Value::String("amqps://mq.test:5671/prod".to_owned()),
        )]);
        assert!(validate_category_config(&ConnectorCategory::Messaging, &good_broker).is_ok());

        let bad_project = config(&[("project_key", Value::String("lowercase".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Ticketing, &bad_project).is_err());
        let good_project = config(&[("project_key", Value::String("OPS42".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Ticketing, &good_project).is_ok());

        let nodomain = config(&[("from_address", Value::String("ops@localhost".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Email, &nodomain).is_err());
        let good_from = config(&[("from_address", Value::String("ops@acme.example".to_owned()))]);
        assert!(validate_category_config(&ConnectorCategory::Email, &good_from).is_ok());
    }

    #[tokio::test]
    async fn factory_pipe_line_creates_validated_instances() {
        let providers = InMemoryProviderRegistry::with_builtin();
        let connectors = InMemoryConnectorRegistry::new();
        let factory = ConnectorFactory::new(providers, connectors);
        let tenant = TenantId::new();
        let org = OrganizationId::new();

        // Unknown provider rejects.
        assert!(
            factory
                .create_instance(
                    tenant,
                    org,
                    "ghost",
                    "x",
                    ConnectorAuthMode::None,
                    config(&[])
                )
                .await
                .is_err(),
            "unknown provider"
        );
        // Unsupported auth mode rejects before config validation.
        assert!(
            factory
                .create_instance(
                    tenant,
                    org,
                    "sendgrid",
                    "mail",
                    ConnectorAuthMode::OAuth2,
                    config(&[])
                )
                .await
                .is_err(),
            "unsupported auth"
        );
        // Missing required manifest keys reject.
        assert!(
            factory
                .create_instance(
                    tenant,
                    org,
                    "postgres",
                    "db-a",
                    ConnectorAuthMode::Basic,
                    config(&[("host", Value::String("db.internal.test".to_owned()))]),
                )
                .await
                .is_err(),
            "port is required by the manifest contract"
        );
        // Secrets in config reject even when keys match the contract.
        assert!(
            factory
                .create_instance(
                    tenant,
                    org,
                    "postgres",
                    "db-a",
                    ConnectorAuthMode::Basic,
                    config(&[
                        ("host", Value::String("db.internal.test".to_owned())),
                        ("port", Value::from(5432_u64)),
                        ("password", Value::String("nope".to_owned())),
                    ]),
                )
                .await
                .is_err(),
            "passwords never travel via config"
        );

        let created = factory
            .create_instance(
                tenant,
                org,
                "postgres",
                "db-primary",
                ConnectorAuthMode::Basic,
                config(&[
                    ("host", Value::String("db.internal.test".to_owned())),
                    ("port", Value::from(5432_u64)),
                    ("database", Value::String("mas".to_owned())),
                ]),
            )
            .await
            .expect("valid install");
        assert_eq!(created.status, ConnectorStatus::PendingVerification);
        assert!(created.credential_id.is_none());

        // Tenant isolation applies to reads through the factory.
        assert!(factory
            .connection_test(tenant_b(), created.id, &OkProbe, &Timestamp::now())
            .await
            .is_err());
    }

    fn tenant_b() -> TenantId {
        TenantId::new()
    }

    #[derive(Debug)]
    struct OkProbe;
    #[async_trait]
    impl ConnectionProbePort for OkProbe {
        async fn probe(&self, _connector: &Connector) -> Result<()> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct FailProbe;
    #[async_trait]
    impl ConnectionProbePort for FailProbe {
        async fn probe(&self, _connector: &Connector) -> Result<()> {
            Err(AppError::external_service(
                "postgres",
                "provider said: connection refused\nusername=svc-mas password=broccoli @ 10.0.0.9"
                    .to_owned(),
            ))
        }
    }

    #[tokio::test]
    async fn probes_advance_status_and_store_sanitized_errors() {
        let providers = InMemoryProviderRegistry::with_builtin();
        let connectors = InMemoryConnectorRegistry::new();
        let factory = ConnectorFactory::new(providers, connectors);
        let tenant = TenantId::new();
        let org = OrganizationId::new();
        let created = factory
            .create_instance(
                tenant,
                org,
                "postgres",
                "db-primary",
                ConnectorAuthMode::Basic,
                config(&[
                    ("host", Value::String("db.internal.test".to_owned())),
                    ("port", Value::from(5432_u64)),
                ]),
            )
            .await
            .expect("install");

        let ok_at = Timestamp::from_unix_seconds(1_700_000_000).expect("ts");
        let active = factory
            .connection_test(tenant, created.id, &OkProbe, &ok_at)
            .await
            .expect("probe");
        assert_eq!(active.status, ConnectorStatus::Active);
        assert_eq!(active.last_tested_at, Some(ok_at));
        assert!(active.last_error.is_none());

        let failing = factory
            .connection_test(tenant, created.id, &FailProbe, &ok_at)
            .await
            .expect("probe ran");
        assert_eq!(failing.status, ConnectorStatus::Error);
        let stored = failing.last_error.expect("error stored");
        assert!(!stored.contains('\n'), "single-line only");
        assert!(stored.len() <= MAX_STORED_ERROR_CHARS);

        let reactivated = factory
            .connection_test(tenant, created.id, &OkProbe, &ok_at)
            .await
            .expect("probe");
        assert_eq!(
            reactivated.status,
            ConnectorStatus::Active,
            "probes recover connectors"
        );
        assert!(reactivated.last_error.is_none());
    }
}
