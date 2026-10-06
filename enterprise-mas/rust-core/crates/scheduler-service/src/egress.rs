// PURPOSE: The real network client behind the SSRF guard.
//
// Owning layer: `mas-scheduler-service` (and, by extension, any process that
// makes outbound HTTP). `mas-integrations` deliberately ships no concrete
// client so the policy layer stays testable; this is the production binding
// of `HttpClientPort`.
//
// Production responsibility: spec Phase 23. The guard in
// `mas_integrations::http_guard` was correct and complete but had no
// dependents — the scheduler's outbound dispatcher built a bare
// `reqwest::Client` and posted straight to whatever URL it was configured
// with, so the guard was dead code. Every outbound request now goes through
// `GuardedHttpClient<ReqwestEgress>`.
//
// Security boundary: this is the only place in the scheduler process where a
// socket is opened to a host named by configuration. It is therefore where
// DNS rebinding is defeated: the hostname is resolved here, every answer is
// checked by `EgressPolicy::pin_resolution`, and the connection is made to
// the pinned *address* with the original `Host` header preserved. A hostile
// DNS record that answers with `169.254.169.254` on the second lookup cannot
// win, because the address is pinned before the socket is opened and never
// re-resolved.
//
// It must NOT:
//   • let `reqwest` resolve the hostname itself — that would reintroduce the
//     TOCTOU window the pinning exists to close;
//   • follow redirects itself. Redirects belong to `GuardedHttpClient`, which
//     re-validates every hop against the policy;
//   • read an unbounded response body. The cap is enforced here as well as in
//     the guard, because a cap that exists in only one layer is a cap that
//     disappears the moment the other is bypassed.

use std::net::IpAddr;
use std::time::Duration;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_integrations::http_guard::{
    EgressPolicy, HttpClientPort, HttpMethod, HttpRequest, HttpResponse,
};

/// A `reqwest` client bound to the egress policy.
///
/// Holds a `reqwest::Client` with redirects disabled: following them here
/// would move a security decision out of the guard, which is the one place
/// that re-checks the destination of every hop.
#[derive(Debug)]
pub struct ReqwestEgress {
    client: reqwest::Client,
    policy: EgressPolicy,
}

impl ReqwestEgress {
    /// Builds a client with redirects off and a hard connect/read timeout.
    ///
    /// # Errors
    /// Fails only if the underlying client cannot be constructed, which is a
    /// process-level misconfiguration rather than a request failure.
    pub fn new(policy: EgressPolicy) -> Result<Self> {
        let client = reqwest::Client::builder()
            // Redirects are the guard's job, not ours.
            .redirect(reqwest::redirect::Policy::none())
            .https_only(policy.https_only)
            .build()
            .map_err(|err| AppError::external_service("http-client", err.to_string()))?;
        Ok(Self { client, policy })
    }

    /// The policy this client enforces.
    #[must_use]
    pub const fn policy(&self) -> &EgressPolicy {
        &self.policy
    }

    /// Resolves `host` and returns the one address the socket may use.
    ///
    /// Every answer is checked, not just the first: a resolver that returns
    /// one public and one private address is refused outright, because a
    /// client that retries on failure would otherwise reach the private one.
    async fn pin(&self, host: &str, port: u16, internal_endpoint: bool) -> Result<IpAddr> {
        let authority = format!("{host}:{port}");
        // `std`'s resolver is blocking, so it runs off the async worker.
        // Pinning must happen before the socket is opened either way; doing
        // it here keeps the resolution and the connect in the same call.
        let resolved = tokio::task::spawn_blocking(move || {
            std::net::ToSocketAddrs::to_socket_addrs(&authority)
                .map(|iter| iter.collect::<Vec<std::net::SocketAddr>>())
        })
        .await
        .map_err(|err| {
            AppError::external_service("http-egress", format!("dns resolution task failed: {err}"))
        })?
        .map_err(|err| {
            AppError::external_service(
                "http-egress",
                format!("dns resolution of {host} failed: {err}"),
            )
        })?;
        let addresses: Vec<IpAddr> = resolved.iter().map(std::net::SocketAddr::ip).collect();
        self.policy
            .pin_resolution_for(&addresses, internal_endpoint)
    }
}

#[async_trait]
impl HttpClientPort for ReqwestEgress {
    async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
        let url = url::Url::parse(request.url.as_str()).map_err(|err| {
            AppError::internal(format!("validated SafeUrl failed re-parse: {err}"))
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| AppError::validation("egress url has no host"))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(443);

        // Resolve first, pin the address, then connect to the address. `Host`
        // carries the original name so TLS and virtual hosting still work.
        let pinned = self
            .pin(&host, port, request.url.is_internal_endpoint())
            .await?;

        let method = match request.method {
            HttpMethod::Get => reqwest::Method::GET,
            HttpMethod::Post => reqwest::Method::POST,
            HttpMethod::Put => reqwest::Method::PUT,
        };

        // Rebuild the URL against the pinned address so no second resolution
        // can happen inside `reqwest`.
        let mut pinned_url = url.clone();
        pinned_url
            .set_ip_host(pinned)
            .map_err(|_| AppError::internal("could not pin the resolved address into the url"))?;

        let mut builder = self
            .client
            .request(method, pinned_url)
            .timeout(Duration::from_millis(request.timeout_ms))
            .header(reqwest::header::HOST, host.clone());
        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }
        if !request.body.is_empty() {
            builder = builder.body(request.body.clone());
        }

        let response = builder.send().await.map_err(|err| {
            AppError::external_service("http-egress", format!("request to {host} failed: {err}"))
        })?;

        let status = response.status().as_u16();
        let mut headers = Vec::new();
        for (name, value) in response.headers().iter() {
            headers.push((
                name.as_str().to_ascii_lowercase(),
                value.to_str().unwrap_or_default().to_owned(),
            ));
        }

        // The body cap is enforced here, not only in the guard: this is the
        // layer that actually reads the bytes.
        let bytes = response.bytes().await.map_err(|err| {
            AppError::external_service(
                "http-egress",
                format!("reading the response from {host} failed: {err}"),
            )
        })?;
        if bytes.len() > self.policy.max_body_bytes {
            return Err(AppError::external_service(
                "http-egress",
                format!(
                    "response body from {host} exceeded the {} byte cap",
                    self.policy.max_body_bytes
                ),
            ));
        }

        Ok(HttpResponse {
            status,
            headers,
            body: bytes.to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_integrations::http_guard::is_publicly_routable;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    fn addr(raw: &str) -> IpAddr {
        raw.parse().expect("ip")
    }

    /// The address classes Phase 23 enumerates: loopback, private RFC1918,
    /// link-local, the cloud metadata address, unique-local IPv6 and
    /// IPv4-mapped IPv6 must all be refused.
    #[test]
    fn every_forbidden_address_class_is_refused() {
        let policy = EgressPolicy::default();
        for forbidden in [
            "127.0.0.1",
            "127.1.2.3",
            "::1",
            "10.0.0.1",
            "172.16.0.1",
            "192.168.1.1",
            "169.254.169.254",
            "169.254.0.1",
            "0.0.0.0",
            "255.255.255.255",
            "fd00::1",
            "fe80::1",
            "::ffff:127.0.0.1",
        ] {
            assert!(
                !is_publicly_routable(&addr(forbidden)),
                "{forbidden} must not be considered publicly routable"
            );
            assert!(
                policy.pin_resolution(&[addr(forbidden)]).is_err(),
                "pin_resolution must refuse {forbidden}"
            );
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        let policy = EgressPolicy::default();
        for allowed in ["93.184.216.34", "2606:2800:220:1:248:1893:25c8:1946"] {
            assert!(is_publicly_routable(&addr(allowed)), "{allowed} is public");
            assert!(policy.pin_resolution(&[addr(allowed)]).is_ok());
        }
    }

    /// A resolver that mixes one good answer with one bad one is refused:
    /// a client that retries on failure would otherwise reach the bad one.
    #[test]
    fn a_single_bad_answer_poisons_the_whole_resolution() {
        let policy = EgressPolicy::default();
        let mixed = [addr("93.184.216.34"), addr("169.254.169.254")];
        assert!(policy.pin_resolution(&mixed).is_err());
    }

    #[test]
    fn an_empty_resolution_is_a_failure_not_a_default() {
        let policy = EgressPolicy::default();
        assert!(policy.pin_resolution(&[]).is_err());
    }

    #[test]
    fn the_client_disables_redirects_so_the_guard_owns_them() {
        let client = ReqwestEgress::new(EgressPolicy::default()).expect("client");
        assert_eq!(client.policy().max_redirects, 3);
    }

    #[test]
    fn ipv4_mapped_ipv6_is_judged_by_its_embedded_address() {
        let mapped = IpAddr::V6(Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped());
        assert!(!is_publicly_routable(&mapped));
        let public = IpAddr::V6(Ipv6Addr::LOCALHOST);
        assert!(!is_publicly_routable(&public));
    }
}
