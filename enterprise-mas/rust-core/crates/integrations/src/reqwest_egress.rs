//! PURPOSE: Canonical pinned Reqwest egress adapter for every Rust integration
//! HTTP call.
//!
//! Owning layer: `mas-integrations` infrastructure adapter.
//!
//! Production role: resolve an already policy-validated hostname immediately
//! before each request, reject an unsafe DNS answer set, and inject the chosen
//! address through Reqwest's resolver override while preserving the original
//! URL hostname for HTTP Host, TLS SNI, and certificate verification. It also
//! disables ambient proxies and redirects and bounds response bytes at the
//! socket-owning layer.
//!
//! Security boundary: this is the only concrete HTTP transport intended for
//! tenant-influenced connector, tool, and webhook traffic. A caller must still
//! wrap it in `GuardedHttpClient`; this adapter does not grant authorization,
//! parse tenant input, follow redirects, read secrets, or persist delivery
//! state. It must never rewrite an HTTPS URL to an IP literal, because doing
//! so loses the hostname TLS security boundary.
//!
//! Data ownership: no durable data; the adapter owns only one request's
//! resolver override and bounded response buffer.
//!
//! Prohibited responsibilities: business retries, webhook settlement,
//! credential storage, OAuth exchange state, provider-specific request shape,
//! and any unguarded socket path.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;

use crate::http_guard::{
    validate_request_headers, EgressPolicy, HttpClientPort, HttpMethod, HttpRequest, HttpResponse,
};

/// A Reqwest adapter that pins each connection to a policy-approved address.
///
/// The client is constructed per request because the resolver override is a
/// property of a client instance in Reqwest. That is intentional: caching a
/// host-to-address mapping beyond the request would silently turn DNS rotation
/// and expiry semantics into an unreviewed in-process policy. Reqwest retains
/// the URL hostname when an override is installed, so HTTPS SNI and certificate
/// verification remain bound to the configured hostname rather than a literal
/// IP address.
#[derive(Debug, Clone)]
pub struct ReqwestEgress {
    policy: EgressPolicy,
}

impl ReqwestEgress {
    /// Creates the canonical adapter for one immutable egress policy.
    #[must_use]
    pub const fn new(policy: EgressPolicy) -> Self {
        Self { policy }
    }

    /// The policy applied to DNS pinning and response bounds.
    #[must_use]
    pub const fn policy(&self) -> &EgressPolicy {
        &self.policy
    }

    /// Resolves every address currently returned for `host:port` and chooses
    /// one only after the policy has examined the whole answer set.
    async fn pin(&self, host: &str, port: u16, internal_endpoint: bool) -> Result<IpAddr> {
        let host = host.to_owned();
        let resolved = tokio::net::lookup_host((host.as_str(), port))
            .await
            .map_err(|error| {
                AppError::external_service(
                    "http-egress",
                    format!("DNS resolution failed for {host}: {error}"),
                )
            })?
            .collect::<Vec<SocketAddr>>();
        let addresses = resolved.iter().map(SocketAddr::ip).collect::<Vec<IpAddr>>();
        self.policy
            .pin_resolution_for(&addresses, internal_endpoint)
    }

    /// Builds one client whose resolver can return only `pinned` for `host`.
    fn client_for(&self, host: &str, port: u16, pinned: IpAddr) -> Result<reqwest::Client> {
        reqwest::Client::builder()
            // Redirects are handled only by GuardedHttpClient, which validates
            // every redirect target under the same policy.
            .redirect(reqwest::redirect::Policy::none())
            // Environment proxy variables create an alternate egress route
            // that bypasses pinned DNS, so guarded transport never inherits
            // them implicitly.
            .no_proxy()
            .https_only(self.policy.https_only)
            // This preserves `host` as URL authority / TLS SNI while making the
            // TCP dial land on the address that was just policy-checked.
            .resolve(host, SocketAddr::new(pinned, port))
            .build()
            .map_err(|error| AppError::external_service("http-client", error.to_string()))
    }

    /// Reads a response incrementally and rejects it before a body can exceed
    /// the policy budget in memory.
    async fn read_bounded(&self, mut response: reqwest::Response, host: &str) -> Result<Vec<u8>> {
        if let Some(length) = response.content_length() {
            let limit = u64::try_from(self.policy.max_body_bytes).unwrap_or(u64::MAX);
            if length > limit {
                return Err(AppError::external_service(
                    "http-egress",
                    format!(
                        "response body from {host} declared {length} bytes, exceeding the {} byte cap",
                        self.policy.max_body_bytes
                    ),
                ));
            }
        }

        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|error| {
            AppError::external_service(
                "http-egress",
                format!("reading the response from {host} failed: {error}"),
            )
        })? {
            let next_len = body.len().saturating_add(chunk.len());
            if next_len > self.policy.max_body_bytes {
                return Err(AppError::external_service(
                    "http-egress",
                    format!(
                        "response body from {host} exceeded the {} byte cap",
                        self.policy.max_body_bytes
                    ),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

#[async_trait]
impl HttpClientPort for ReqwestEgress {
    async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
        // Defense in depth: production callers compose this under
        // GuardedHttpClient, but the socket-owning adapter independently
        // validates the destination immediately before DNS resolution/dial.
        // A future direct caller therefore cannot bypass scheme/port/host
        // policy merely by skipping the wrapper.
        self.policy.check_destination(&request.url)?;
        validate_request_headers(&request.headers)?;
        if request.timeout_ms == 0 {
            return Err(AppError::invalid_field(
                "timeout_ms",
                "out_of_range",
                "outbound HTTP timeout must be greater than zero",
            ));
        }
        let url = request.url.as_url().clone();
        let host = url
            .host_str()
            .ok_or_else(|| AppError::validation("egress URL has no host"))?
            .to_owned();
        let port = url.port_or_known_default().ok_or_else(|| {
            AppError::invalid_field("url", "missing_port", "egress URL has no effective port")
        })?;
        let pinned = self
            .pin(&host, port, request.url.is_internal_endpoint())
            .await?;
        let client = self.client_for(&host, port, pinned)?;

        let method = match request.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
            HttpMethod::Delete => reqwest::Method::DELETE,
        };
        let mut builder = client
            .request(method, url)
            .timeout(Duration::from_millis(request.timeout_ms));
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }

        let response = builder.send().await.map_err(|error| {
            AppError::external_service("http-egress", format!("request to {host} failed: {error}"))
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.as_str().to_ascii_lowercase(),
                    value.to_str().unwrap_or_default().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        let body = self.read_bounded(response, &host).await?;

        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::*;
    use crate::http_guard::is_publicly_routable;

    fn addr(raw: &str) -> IpAddr {
        raw.parse().expect("valid IP")
    }

    #[test]
    fn forbidden_address_classes_are_never_pinnable() {
        let policy = EgressPolicy::default();
        for forbidden in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "0.0.0.0",
            "255.255.255.255",
            "fd00::1",
            "fe80::1",
            "::ffff:8.8.8.8",
            "2001:db8::1",
        ] {
            assert!(
                !is_publicly_routable(&addr(forbidden)),
                "{forbidden} must be classified as blocked"
            );
            assert!(
                policy.pin_resolution(&[addr(forbidden)]).is_err(),
                "{forbidden} must not be pinnable"
            );
        }
    }

    #[test]
    fn global_addresses_are_pinnable() {
        let policy = EgressPolicy::default();
        for allowed in ["93.184.216.34", "2606:4700:4700::1111"] {
            assert!(is_publicly_routable(&addr(allowed)));
            assert!(policy.pin_resolution(&[addr(allowed)]).is_ok());
        }
    }

    #[test]
    fn response_cap_and_policy_are_carried_by_the_adapter() {
        let adapter = ReqwestEgress::new(EgressPolicy {
            max_body_bytes: 1234,
            ..EgressPolicy::default()
        });
        assert_eq!(adapter.policy().max_body_bytes, 1234);
        let _ = Ipv4Addr::LOCALHOST;
        let _ = Ipv6Addr::LOCALHOST;
    }
}
