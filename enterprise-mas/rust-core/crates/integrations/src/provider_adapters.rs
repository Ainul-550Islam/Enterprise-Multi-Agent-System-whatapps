//! PURPOSE: Real guarded HTTP connection probes for supported HTTP providers.
//!
//! Owning layer: `mas-integrations` infrastructure edge.
//!
//! Production role: provide non-fake connectivity probes for Slack, SendGrid,
//! Jira, Salesforce, and generic HTTP-webhook connectors. Each probe resolves
//! credential authority through an injected port and sends a real request only
//! through the canonical guarded HTTP client. Unsupported protocol providers
//! explicitly fail instead of claiming success.
//!
//! Security boundary: secrets are `SecretString` values only at header
//! construction, connector configuration is never trusted for credentials,
//! tenant-influenced base URLs are parsed as `SafeUrl`, and guarded egress
//! validates destination/redirects before the concrete transport owns a socket.
//!
//! Data ownership: connector persistence owns config and credential links;
//! the credential port owns reference-to-material resolution. This module owns
//! no durable provider state.
//!
//! Prohibited responsibilities: persisting tokens, inventing success for an
//! unimplemented provider, accepting credentials in config, bypassing egress,
//! or leaking provider response bodies into errors/logs.

use std::fmt;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::ids::TenantId;
use mas_common::result::Result;
use mas_domain::value_objects::SafeUrl;
use mas_domain::{Connector, ConnectorAuthMode, SecretReference};
use mas_security::SecretString;

use crate::connectors::ConnectionProbePort;
use crate::http_guard::{GuardedHttpClient, HttpClientPort, HttpRequest};

/// Authorization material resolved just-in-time for one provider request.
/// Debug intentionally redacts every material-bearing variant.
pub enum ProviderAuthorization {
    /// No header authority; legal only for `ConnectorAuthMode::None`.
    None,
    /// OAuth/provider bearer token.
    Bearer(SecretString),
    /// API key supplied in a named provider header.
    Header { name: String, value: SecretString },
}

impl fmt::Debug for ProviderAuthorization {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("ProviderAuthorization::None"),
            Self::Bearer(_) => formatter.write_str("ProviderAuthorization::Bearer([redacted])"),
            Self::Header { name, .. } => formatter
                .debug_struct("ProviderAuthorization::Header")
                .field("name", name)
                .field("value", &"[redacted]")
                .finish(),
        }
    }
}

/// Resolves the currently bound connector credential just before a probe.
/// Production implementations use `SecretGate` + credential/reference metadata;
/// they must return an error for missing/revoked/expired credentials.
#[async_trait]
pub trait ConnectorCredentialPort: fmt::Debug + Send + Sync {
    /// Resolves provider request authority for this exact connector.
    async fn resolve_authorization(&self, connector: &Connector) -> Result<ProviderAuthorization>;
}

/// Metadata needed to resolve one bound connector credential. The secret is
/// deliberately still external; this record can safely cross repository and
/// application boundaries without exposing material.
#[derive(Debug, Clone)]
pub struct ConnectorCredentialReference {
    pub tenant_id: TenantId,
    pub connector_id: mas_common::ids::ConnectorId,
    pub secret_reference: SecretReference,
}

/// Tenant-scoped credential/reference lookup implemented by persistence.
#[async_trait]
pub trait ConnectorCredentialLookupPort: fmt::Debug + Send + Sync {
    /// Returns the active bound reference for `connector`, or `None` when no
    /// valid binding exists. A foreign connector is indistinguishable from
    /// missing at this boundary.
    async fn active_reference(
        &self,
        connector: &Connector,
    ) -> Result<Option<ConnectorCredentialReference>>;
}

/// Guarded real HTTP probe for explicitly supported HTTP providers.
#[derive(Debug)]
pub struct HttpProviderProbe<C, A>
where
    C: HttpClientPort,
    A: ConnectorCredentialPort,
{
    client: GuardedHttpClient<C>,
    credentials: A,
}

impl<C, A> HttpProviderProbe<C, A>
where
    C: HttpClientPort,
    A: ConnectorCredentialPort,
{
    /// Composes one probe from the canonical guarded transport and credential
    /// resolver. There is intentionally no "successful mock" constructor.
    #[must_use]
    pub const fn new(client: GuardedHttpClient<C>, credentials: A) -> Self {
        Self {
            client,
            credentials,
        }
    }

    fn endpoint_for(&self, connector: &Connector) -> Result<SafeUrl> {
        match connector.provider.as_str() {
            "slack" => SafeUrl::parse("https://slack.com/api/auth.test"),
            "sendgrid" => SafeUrl::parse("https://api.sendgrid.com/v3/user/profile"),
            "jira" => append_path(required_safe_config_url(connector, "base_url")?, "/rest/api/3/myself"),
            "salesforce" => {
                let base = config_url(connector, "api_base")
                    .or_else(|| config_url(connector, "base_url"))
                    .ok_or_else(|| {
                        AppError::invalid_field(
                            "config.api_base",
                            "required",
                            "Salesforce probes require a tenant-safe api_base or base_url",
                        )
                    })?;
                let version = connector
                    .config
                    .get("api_version")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| {
                        AppError::invalid_field(
                            "config.api_version",
                            "required",
                            "Salesforce probes require api_version",
                        )
                    })?;
                if version.len() > 32
                    || !version
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
                {
                    return Err(AppError::invalid_field(
                        "config.api_version",
                        "invalid_format",
                        "Salesforce API version must be a short alphanumeric/dot token",
                    ));
                }
                append_path(base, &format!("/services/data/{version}/limits"))
            },
            "generic-webhook" => required_safe_config_url(connector, "default_url"),
            // Non-HTTP protocol adapters must be installed explicitly. This
            // hard error is safer than a TCP/DNS-only "probe" that would mark
            // a connector active while authentication/query semantics fail.
            "postgres" | "s3" | "rabbitmq" => Err(AppError::invalid_field(
                "provider",
                "adapter_not_configured",
                format!(
                    "provider '{}' requires its protocol-specific production adapter; HTTP probing is refused",
                    connector.provider
                ),
            )),
            provider => Err(AppError::invalid_field(
                "provider",
                "unsupported",
                format!("no production probe adapter is registered for provider '{provider}'"),
            )),
        }
    }

    async fn probe_inner(&self, connector: &Connector) -> Result<()> {
        let endpoint = self.endpoint_for(connector)?;
        let authorization = self.credentials.resolve_authorization(connector).await?;
        let mut request = HttpRequest::get(endpoint);
        match (&connector.auth_mode, authorization) {
            (ConnectorAuthMode::None, ProviderAuthorization::None) => {},
            (ConnectorAuthMode::None, _) => {
                return Err(AppError::conflict(
                    "an unauthenticated connector must not resolve credential material",
                ));
            },
            (_, ProviderAuthorization::None) => {
                return Err(AppError::conflict(
                    "authenticated connector probe has no resolved credential authority",
                ));
            },
            (_, ProviderAuthorization::Bearer(token)) => {
                request =
                    request.with_header("authorization", &format!("Bearer {}", token.expose()));
            },
            (_, ProviderAuthorization::Header { name, value }) => {
                validate_provider_header_name(&name)?;
                request = request.with_header(&name, value.expose());
            },
        }
        let response = self.client.send(&request).await?;
        if !response.is_success() {
            return Err(AppError::external_service(
                &connector.provider,
                format!("provider probe returned HTTP status {}", response.status),
            ));
        }
        // Slack can respond 200 for API-level errors. Treat an explicit `ok:
        // false` as a real failure without reflecting its error payload.
        if connector.provider == "slack" {
            if let Ok(document) = serde_json::from_slice::<serde_json::Value>(&response.body) {
                if document.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
                    return Err(AppError::external_service(
                        "slack",
                        "Slack auth.test rejected the supplied credential",
                    ));
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<C, A> ConnectionProbePort for HttpProviderProbe<C, A>
where
    C: HttpClientPort + 'static,
    A: ConnectorCredentialPort + 'static,
{
    async fn probe(&self, connector: &Connector) -> Result<()> {
        self.probe_inner(connector).await
    }
}

fn config_url(connector: &Connector, key: &str) -> Option<SafeUrl> {
    connector
        .config
        .get(key)
        .and_then(serde_json::Value::as_str)
        .and_then(|raw| SafeUrl::parse(raw).ok())
}

fn required_safe_config_url(connector: &Connector, key: &str) -> Result<SafeUrl> {
    let raw = connector
        .config
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            AppError::invalid_field(
                format!("config.{key}"),
                "required",
                format!("provider '{}' requires config.{key}", connector.provider),
            )
        })?;
    SafeUrl::parse(raw)
}

fn append_path(base: SafeUrl, suffix: &str) -> Result<SafeUrl> {
    let mut url = base.as_url().clone();
    let prefix = url.path().trim_end_matches('/');
    url.set_path(&format!("{prefix}{suffix}"));
    url.set_query(None);
    url.set_fragment(None);
    SafeUrl::parse(url.as_str())
}

fn validate_provider_header_name(name: &str) -> Result<()> {
    let normalized = name.to_ascii_lowercase();
    if normalized.is_empty()
        || normalized.len() > 128
        || !normalized
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        || matches!(
            normalized.as_str(),
            "host" | "content-length" | "transfer-encoding" | "connection" | "proxy-authorization"
        )
    {
        return Err(AppError::invalid_field(
            "credential_header",
            "invalid_header_name",
            "provider credential header is not a permitted HTTP field name",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_guard::EgressPolicy;
    use mas_common::ids::{OrganizationId, TenantId};

    #[derive(Debug)]
    struct NoCredential;

    #[async_trait]
    impl ConnectorCredentialPort for NoCredential {
        async fn resolve_authorization(
            &self,
            _connector: &Connector,
        ) -> Result<ProviderAuthorization> {
            Ok(ProviderAuthorization::None)
        }
    }

    #[derive(Debug)]
    struct FakeHttp;

    #[async_trait]
    impl HttpClientPort for FakeHttp {
        async fn send(&self, _request: &HttpRequest) -> Result<crate::http_guard::HttpResponse> {
            Ok(crate::http_guard::HttpResponse::new(200))
        }
    }

    #[tokio::test]
    async fn unsupported_protocol_probe_refuses_instead_of_returning_fake_success() {
        let connector = Connector::register(
            TenantId::new(),
            OrganizationId::new(),
            "postgres",
            "primary",
            ConnectorAuthMode::Basic,
        )
        .expect("connector");
        let probe = HttpProviderProbe::new(
            GuardedHttpClient::new(FakeHttp, EgressPolicy::default()),
            NoCredential,
        );
        assert!(probe.probe(&connector).await.is_err());
    }
}
