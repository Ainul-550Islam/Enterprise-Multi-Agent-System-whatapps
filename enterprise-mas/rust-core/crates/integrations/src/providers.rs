//! PURPOSE: The production provider implementations: Stripe billing
//! checkout and the WhatsApp Cloud API gateway, both behind the guarded
//! egress transport.
//!
//! Owning layer: `mas-integrations`.
//!
//! Production responsibility: speak the two providers' REST dialects over
//! policy-checked HTTP. Every call names its host in the egress allowlist;
//! the platform secret is attached as a request header by these impls and
//! never logged, echoed, or stored.
//!
//! It must NOT:
//!   * bypass `GuardedHttpClient` (the SSRF/enforcement boundary);
//!   * retry mutating calls on its own -- idempotency keys make the safe
//!     retries (the provider's) exact;
//!   * interpret provider responses as anything but the small fields named
//!     below.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_domain::value_objects::SafeUrl;
use mas_security::SecretString;

use crate::http_guard::{EgressPolicy, GuardedHttpClient, HttpClientPort, HttpRequest};
use crate::provider_ports::{
    CheckoutProviderPort, CheckoutRequest, CustomerRequest, GatewaySendReceipt, GatewayTemplate,
    PortalRequest, ProviderCustomer, ProviderPortal, ProviderSession, WhatsAppGatewayPort,
};

/// Stripe's REST API over guarded egress.
#[derive(Debug)]
pub struct StripeCheckoutProvider<C: HttpClientPort> {
    client: GuardedHttpClient<C>,
    api_base: String,
    /// The secret key (`sk_...`) as a bearer credential. Never logged.
    secret_key: SecretString,
}

impl<C: HttpClientPort> StripeCheckoutProvider<C> {
    /// Builds the provider over one guarded client.
    #[must_use]
    pub fn new(client: GuardedHttpClient<C>, api_base: String, secret_key: SecretString) -> Self {
        Self {
            client,
            api_base,
            secret_key,
        }
    }

    /// The egress policy this provider requires: HTTPS to the Stripe API
    /// host and nothing else.
    #[must_use]
    pub fn egress_policy() -> EgressPolicy {
        let mut allowlist = std::collections::BTreeSet::new();
        allowlist.insert("api.stripe.com".to_owned());
        EgressPolicy {
            https_only: true,
            hostname_allowlist: Some(allowlist),
            max_redirects: 0,
            ..EgressPolicy::default()
        }
    }

    /// One form-encoded POST against the Stripe API.
    async fn post_form(
        &self,
        path: &str,
        form: &[(&str, String)],
        idempotency: &str,
    ) -> Result<serde_json::Value> {
        let body: String = form
            .iter()
            .map(|(key, value)| format!("{key}={}", form_urlencode(value)))
            .collect::<Vec<_>>()
            .join("&");
        let url = SafeUrl::parse(&format!("{}{}", self.api_base, path))?;
        let request = HttpRequest::post(url, body.into_bytes())
            .with_header(
                "authorization",
                &format!("Bearer {}", self.secret_key.expose()),
            )
            .with_header("content-type", "application/x-www-form-urlencoded")
            .with_header("idempotency-key", idempotency);
        let response = self.client.send(&request).await?;
        let parsed: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|error| {
                AppError::external_service("stripe", format!("response was not JSON: {error}"))
            })?;
        if !response.is_success() {
            let message = parsed
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown stripe error");
            return Err(AppError::external_service(
                "stripe",
                format!("refused the call: {message}"),
            ));
        }
        Ok(parsed)
    }
}

/// Minimal form-urlencoding (the Stripe form dialect: `key=value` pairs).
fn form_urlencode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(*byte as char);
            },
            b' ' => encoded.push('+'),
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

#[async_trait]
impl<C: HttpClientPort> CheckoutProviderPort for StripeCheckoutProvider<C> {
    async fn create_customer(&self, request: &CustomerRequest) -> Result<ProviderCustomer> {
        let mut form: Vec<(&str, String)> = vec![(
            "metadata[tenant_id]",
            request.tenant_id.as_uuid().to_string(),
        )];
        if let Some(email) = &request.email {
            form.push(("email", email.clone()));
        }
        let parsed = self
            .post_form("/v1/customers", &form, &request.idempotency_key)
            .await?;
        let id = parsed
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("stripe", "customer response carried no id")
            })?;
        Ok(ProviderCustomer {
            provider_customer_id: id.to_owned(),
        })
    }

    async fn create_checkout_session(&self, request: &CheckoutRequest) -> Result<ProviderSession> {
        let form = [
            ("mode", "subscription".to_owned()),
            ("customer", request.provider_customer_id.clone()),
            ("line_items[0][price]", request.provider_price_id.clone()),
            ("line_items[0][quantity]", "1".to_owned()),
            ("success_url", request.success_url.clone()),
            ("cancel_url", request.cancel_url.clone()),
        ];
        let parsed = self
            .post_form("/v1/checkout/sessions", &form, &request.idempotency_key)
            .await?;
        let id = parsed
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("stripe", "session response carried no id")
            })?;
        let url = parsed
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("stripe", "session response carried no url")
            })?;
        Ok(ProviderSession {
            provider_session_id: id.to_owned(),
            checkout_url: url.to_owned(),
        })
    }

    async fn create_portal_session(&self, request: &PortalRequest) -> Result<ProviderPortal> {
        let form = [
            ("customer", request.provider_customer_id.clone()),
            ("return_url", request.return_url.clone()),
        ];
        let parsed = self
            .post_form(
                "/v1/billing_portal/sessions",
                &form,
                &request.idempotency_key,
            )
            .await?;
        let url = parsed
            .get("url")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("stripe", "portal response carried no url")
            })?;
        Ok(ProviderPortal {
            portal_url: url.to_owned(),
        })
    }
}

/// The WhatsApp Cloud API text gateway over guarded egress.
#[derive(Debug)]
pub struct WhatsAppGraphGateway<C: HttpClientPort> {
    client: GuardedHttpClient<C>,
    graph_base: String,
    /// The system-user access token. Never logged.
    access_token: SecretString,
}

impl<C: HttpClientPort> WhatsAppGraphGateway<C> {
    /// Builds the gateway over one guarded client.
    #[must_use]
    pub fn new(
        client: GuardedHttpClient<C>,
        graph_base: String,
        access_token: SecretString,
    ) -> Self {
        Self {
            client,
            graph_base,
            access_token,
        }
    }

    /// The egress policy this gateway requires: HTTPS to the Graph API
    /// host and nothing else.
    #[must_use]
    pub fn egress_policy() -> EgressPolicy {
        let mut allowlist = std::collections::BTreeSet::new();
        allowlist.insert("graph.facebook.com".to_owned());
        EgressPolicy {
            https_only: true,
            hostname_allowlist: Some(allowlist),
            max_redirects: 0,
            ..EgressPolicy::default()
        }
    }
}

#[async_trait]
impl<C: HttpClientPort> WhatsAppGatewayPort for WhatsAppGraphGateway<C> {
    async fn send_text(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
        to: &str,
        body: &str,
    ) -> Result<GatewaySendReceipt> {
        let payload = serde_json::json!({
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "text",
            "text": { "preview_url": false, "body": body },
        });
        // The path must name the channel's own phone number id; the Graph
        // API routes by it.
        let url = SafeUrl::parse(&format!(
            "{}/{}/{}/messages",
            self.graph_base, channel.api_version, channel.phone_number_id
        ))?;
        let request = HttpRequest::post(
            url,
            serde_json::to_vec(&payload).map_err(|error| {
                AppError::external_service(
                    "whatsapp",
                    format!("payload did not serialize: {error}"),
                )
            })?,
        )
        .with_header(
            "authorization",
            &format!("Bearer {}", self.access_token.expose()),
        )
        .with_header("content-type", "application/json");
        let response = self.client.send(&request).await?;
        let parsed: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|error| {
                AppError::external_service("whatsapp", format!("response was not JSON: {error}"))
            })?;
        if !response.is_success() {
            let message = parsed
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown whatsapp error");
            return Err(AppError::external_service(
                "whatsapp",
                format!("refused the send: {message}"),
            ));
        }
        let wamid = parsed
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("id"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("whatsapp", "send response carried no message id")
            })?;
        Ok(GatewaySendReceipt {
            wamid: wamid.to_owned(),
            timestamp_unix: None,
        })
    }

    async fn send_template(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
        to: &str,
        name: &str,
        language: &str,
    ) -> Result<GatewaySendReceipt> {
        let payload = serde_json::json!({
            "messaging_product": "whatsapp",
            "recipient_type": "individual",
            "to": to,
            "type": "template",
            "template": {
                "name": name,
                "language": { "code": language },
            },
        });
        let url = SafeUrl::parse(&format!(
            "{}/{}/{}/messages",
            self.graph_base, channel.api_version, channel.phone_number_id
        ))?;
        let request = HttpRequest::post(
            url,
            serde_json::to_vec(&payload).map_err(|error| {
                AppError::external_service(
                    "whatsapp",
                    format!("payload did not serialize: {error}"),
                )
            })?,
        )
        .with_header(
            "authorization",
            &format!("Bearer {}", self.access_token.expose()),
        )
        .with_header("content-type", "application/json");
        let response = self.client.send(&request).await?;
        let parsed: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|error| {
                AppError::external_service("whatsapp", format!("response was not JSON: {error}"))
            })?;
        if !response.is_success() {
            let message = parsed
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown whatsapp error");
            return Err(AppError::external_service(
                "whatsapp",
                format!("refused the send: {message}"),
            ));
        }
        let wamid = parsed
            .get("messages")
            .and_then(serde_json::Value::as_array)
            .and_then(|messages| messages.first())
            .and_then(|message| message.get("id"))
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                AppError::external_service("whatsapp", "send response carried no message id")
            })?;
        Ok(GatewaySendReceipt {
            wamid: wamid.to_owned(),
            timestamp_unix: None,
        })
    }

    async fn list_templates(
        &self,
        channel: &mas_domain::whatsapp::WhatsAppChannel,
    ) -> Result<Vec<GatewayTemplate>> {
        let waba_id = channel
            .waba_id
            .as_deref()
            .ok_or_else(|| AppError::validation("channel has no waba_id; cannot list templates"))?;
        let url = SafeUrl::parse(&format!(
            "{}/{}/{}/message_templates?limit=100",
            self.graph_base, channel.api_version, waba_id
        ))?;
        let request = HttpRequest::get(url).with_header(
            "authorization",
            &format!("Bearer {}", self.access_token.expose()),
        );
        let response = self.client.send(&request).await?;
        let parsed: serde_json::Value =
            serde_json::from_slice(&response.body).map_err(|error| {
                AppError::external_service("whatsapp", format!("response was not JSON: {error}"))
            })?;
        if !response.is_success() {
            let message = parsed
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown whatsapp error");
            return Err(AppError::external_service(
                "whatsapp",
                format!("refused the template list: {message}"),
            ));
        }
        let Some(rows) = parsed.get("data").and_then(serde_json::Value::as_array) else {
            return Ok(Vec::new());
        };
        let mut templates = Vec::with_capacity(rows.len());
        for row in rows {
            let Some(name) = row.get("name").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let language = row
                .get("language")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("en")
                .to_owned();
            let status = row
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("pending")
                .to_ascii_lowercase();
            let body = row
                .get("components")
                .and_then(serde_json::Value::as_array)
                .and_then(|components| {
                    components.iter().find_map(|component| {
                        let kind = component.get("type").and_then(serde_json::Value::as_str)?;
                        if !kind.eq_ignore_ascii_case("BODY") {
                            return None;
                        }
                        component
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_owned)
                    })
                });
            templates.push(GatewayTemplate {
                name: name.to_owned(),
                language,
                status,
                body,
            });
        }
        Ok(templates)
    }
}

#[cfg(test)]
mod tests {
    //! Provider dialect tests over a scripted fake client: form shapes and
    //! response parsing, no sockets.

    use super::*;
    use crate::http_guard::HttpResponse;
    use crate::provider_ports::CheckoutProviderPort;
    use mas_common::ids::TenantId;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct ScriptedClient {
        sent: Mutex<Vec<HttpRequest>>,
        respond_with: Mutex<HttpResponse>,
    }

    impl ScriptedClient {
        fn responding(status: u16, body: &str) -> Self {
            let mut response = HttpResponse::new(status);
            response.body = body.as_bytes().to_vec();
            Self {
                sent: Mutex::new(Vec::new()),
                respond_with: Mutex::new(response),
            }
        }
    }

    #[async_trait]
    impl HttpClientPort for ScriptedClient {
        async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
            self.sent.lock().expect("lock").push(request.clone());
            Ok(self.respond_with.lock().expect("lock").clone())
        }
    }

    fn stripe(client: ScriptedClient) -> StripeCheckoutProvider<ScriptedClient> {
        let policy = StripeCheckoutProvider::<ScriptedClient>::egress_policy();
        StripeCheckoutProvider::new(
            GuardedHttpClient::new(client, policy),
            "https://api.stripe.com".to_owned(),
            SecretString::new("sk_test"),
        )
    }

    #[tokio::test]
    async fn stripe_customer_creation_posts_the_form_and_parses_the_id() {
        let client = ScriptedClient::responding(200, r#"{"id":"cus_1"}"#);
        let provider = stripe(client);
        let customer = provider
            .create_customer(&CustomerRequest {
                tenant_id: TenantId::nil(),
                email: Some("ops@example.test".to_owned()),
                idempotency_key: "key-1".to_owned(),
            })
            .await
            .expect("customer");
        assert_eq!(customer.provider_customer_id, "cus_1");
    }

    #[tokio::test]
    async fn a_refusal_surfaces_the_provider_message() {
        let client =
            ScriptedClient::responding(400, r#"{"error":{"message":"No such price: price_x"}}"#);
        let provider = stripe(client);
        let error = provider
            .create_checkout_session(&CheckoutRequest {
                provider_customer_id: "cus_1".to_owned(),
                provider_price_id: "price_x".to_owned(),
                success_url: "https://example.test/ok".to_owned(),
                cancel_url: "https://example.test/cancel".to_owned(),
                idempotency_key: "key-2".to_owned(),
            })
            .await
            .expect_err("must refuse");
        let message = error.to_string();
        assert!(message.contains("No such price"), "got: {message}");
    }

    #[tokio::test]
    async fn stripe_portal_posts_the_form_and_parses_the_url() {
        let client = ScriptedClient::responding(
            200,
            r#"{"id":"bps_1","url":"https://billing.stripe.com/session/1"}"#,
        );
        let provider = stripe(client);
        let portal = provider
            .create_portal_session(&PortalRequest {
                provider_customer_id: "cus_1".to_owned(),
                return_url: "https://example.test/account".to_owned(),
                idempotency_key: "key-portal".to_owned(),
            })
            .await
            .expect("portal");
        assert_eq!(portal.portal_url, "https://billing.stripe.com/session/1");
    }
}
