//! Hardened egress: the SSRF enforcement layer promised by `SafeUrl`.
//!
//! `SafeUrl` performs first-line validation at parse time (scheme, no
//! embedded credentials, no private/loopback IP literals). This module adds
//! everything parse time *cannot* know:
//!
//! * [`EgressPolicy`] — operator-configured scheme/port/hostname allowlists
//!   plus `pin_resolution`, which a production client MUST call with the
//!   DNS answers and refuse the request when any answer is non-global (the
//!   conservative DNS-rebinding/TOCTOU rule).
//! * [`GuardedHttpClient`] — wraps an [`HttpClientPort`], re-validates
//!   EVERY redirect hop (a redirect to `http://169.254.169.254/…` is a
//!   classic SSRF pivot), applies redirect count and response body caps.
//!
//! The crate deliberately ships no concrete network client; worker/api
//! processes bind a real HTTP stack to [`HttpClientPort`]. A scriptable
//! recorder implementation lives in the tests of this module.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_domain::value_objects::SafeUrl;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use url::Url;

/// Default maximum redirect hops a guarded request may follow.
pub const DEFAULT_MAX_REDIRECTS: u8 = 3;
/// Default response body cap (1 MiB).
pub const DEFAULT_MAX_BODY_BYTES: usize = 1 << 20;
/// Default per-request timeout budget.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Operator-configured egress policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressPolicy {
    /// Require TLS for every outbound request (default: `true`). Plain
    /// `http` is then allowed only for hosts named in
    /// `permitted_plain_http_hosts`.
    pub https_only: bool,
    /// Exact hostnames (or parent domains) permitted to be reached over
    /// plain HTTP despite `https_only` — dev/test fixtures only.
    #[serde(default)]
    pub permitted_plain_http_hosts: BTreeSet<String>,
    /// Explicit port allowlist. `None` ⇒ only the scheme's default port
    /// (443 for https, 80 for http).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_ports: Option<BTreeSet<u16>>,
    /// Optional hostname allowlist applied on top of everything else;
    /// exact match or subdomain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname_allowlist: Option<BTreeSet<String>>,
    pub max_redirects: u8,
    pub max_body_bytes: usize,
}

impl Default for EgressPolicy {
    fn default() -> Self {
        Self {
            https_only: true,
            permitted_plain_http_hosts: BTreeSet::new(),
            allowed_ports: None,
            hostname_allowlist: None,
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
        }
    }
}

impl EgressPolicy {
    /// Validates that `url` may be contacted under this policy.
    ///
    /// `SafeUrl` guarantees parse-level safety; this layer applies the
    /// operator policy on scheme, port and hostname.
    pub fn check_destination(&self, url: &SafeUrl) -> Result<()> {
        let parsed = Url::parse(url.as_str()).map_err(|err| {
            AppError::internal(format!("validated SafeUrl failed re-parse: {err}"))
        })?;
        let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();

        match parsed.scheme() {
            "https" => {},
            "http" => {
                let permitted = !self.https_only
                    || self
                        .permitted_plain_http_hosts
                        .iter()
                        .any(|allowed| host == *allowed || host.ends_with(&format!(".{allowed}")));
                if !permitted {
                    return Err(AppError::invalid_field(
                        "url",
                        "plain_http_forbidden",
                        format!("plain HTTP to '{host}' is not permitted by the egress policy"),
                    ));
                }
            },
            scheme => {
                return Err(AppError::invalid_field(
                    "url",
                    "invalid_scheme",
                    format!("scheme '{scheme}' is not allowed (http/https only)"),
                ));
            },
        }

        let port = parsed.port_or_known_default().ok_or_else(|| {
            AppError::invalid_field("url", "missing_port", "no port could be determined")
        })?;
        match &self.allowed_ports {
            Some(set) if !set.contains(&port) => {
                return Err(AppError::invalid_field(
                    "url",
                    "forbidden_port",
                    format!("port {port} is not in the egress allowlist"),
                ));
            },
            Some(_) => {},
            None => {
                let default_port = url.is_tls();
                let expected = if default_port { 443 } else { 80 };
                if port != expected {
                    return Err(AppError::invalid_field(
                        "url",
                        "forbidden_port",
                        format!(
                            "port {port} is not the scheme default without an explicit allowlist"
                        ),
                    ));
                }
            },
        }

        if let Some(allowlist) = &self.hostname_allowlist {
            let allowed = allowlist
                .iter()
                .any(|entry| host == *entry || host.ends_with(&format!(".{entry}")));
            if !allowed {
                return Err(AppError::invalid_field(
                    "url",
                    "host_not_allowlisted",
                    format!("host '{host}' is not in the egress hostname allowlist"),
                ));
            }
        }
        Ok(())
    }

    /// DNS-resolution gate (anti-rebinding). The real HTTP client resolves
    /// the hostname, calls this with every answer, and connects ONLY to the
    /// returned address. Conservative rule: if ANY answer is non-global,
    /// the whole request is refused — a hostile DNS record must not smuggle
    /// a private address past us among good ones.
    pub fn pin_resolution(&self, resolved: &[IpAddr]) -> Result<IpAddr> {
        if resolved.is_empty() {
            return Err(AppError::external_service(
                "http-egress",
                "destination resolved to no addresses",
            ));
        }
        for addr in resolved {
            if !is_publicly_routable(addr) {
                return Err(AppError::invalid_field(
                    "url",
                    "forbidden_host",
                    format!("destination resolves to non-public address {addr}"),
                ));
            }
        }
        Ok(resolved[0])
    }
}

/// Whether an address is safe to connect to from the platform. Complements
/// `SafeUrl`'s literal-time checks with full IPv6 ranges: unique-local
/// (`fc00::/7`), link-local (`fe80::/10`) and IPv4-mapped addresses are
/// classified via their embedded IPv4 rules.
#[must_use]
pub fn is_publicly_routable(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => ipv4_is_public(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return ipv4_is_public(&mapped);
            }
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (ip.segments()[0] & 0xffc0) == 0xfe80) // fe80::/10 link-local
        },
    }
}

fn ipv4_is_public(ip: &Ipv4Addr) -> bool {
    !(ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        || ip.octets()[0] == 0
        // CGNAT 100.64.0.0/10
        || (ip.octets()[0] == 100 && (ip.octets()[1] & 0xC0) == 64)
        // benchmarking 198.18.0.0/15
        || (ip.octets()[0] == 198 && (ip.octets()[1] == 18 || ip.octets()[1] == 19))
        // reserved + broadcast space 240.0.0.0/4
        || ip.octets()[0] >= 240)
}

/// HTTP method supported by the egress plane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    /// `GET`.
    Get,
    /// `POST`.
    Post,
    /// `PUT`.
    Put,
}

impl HttpMethod {
    /// Canonical wire token.
    #[must_use]
    pub const fn as_token(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
        }
    }
}

/// A validated outbound request.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: HttpMethod,
    pub url: SafeUrl,
    /// Lowercased header names; credential headers are only ever attached
    /// by callers, never echoed in errors.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub timeout_ms: u64,
}

impl HttpRequest {
    /// A bare GET.
    #[must_use]
    pub fn get(url: SafeUrl) -> Self {
        Self {
            method: HttpMethod::Get,
            url,
            headers: Vec::new(),
            body: Vec::new(),
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    /// A POST with a body.
    #[must_use]
    pub fn post(url: SafeUrl, body: Vec<u8>) -> Self {
        Self {
            method: HttpMethod::Post,
            url,
            headers: Vec::new(),
            body,
            timeout_ms: DEFAULT_TIMEOUT_MS,
        }
    }

    /// Builder: set a header (name lowercased).
    #[must_use]
    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers
            .push((name.to_ascii_lowercase(), value.to_owned()));
        self
    }

    /// Builder: override the timeout budget.
    #[must_use]
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
}

/// A received response (already size-capped by the guard).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    /// Header names are lowercased.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Synthetic response for tests/fakes.
    #[must_use]
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    /// Whether the status is 2xx.
    #[must_use]
    pub const fn is_success(&self) -> bool {
        self.status >= 200 && self.status < 300
    }

    /// Whether the status is a redirect.
    #[must_use]
    pub const fn is_redirect(&self) -> bool {
        matches!(self.status, 301 | 302 | 303 | 307 | 308)
    }

    /// Case-insensitive header lookup.
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let wanted = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(key, _)| *key == wanted)
            .map(|(_, value)| value.as_str())
    }

    /// Parsed `Retry-After` seconds, when present and well-formed.
    /// HTTP-date form is intentionally unsupported by the egress plane;
    /// callers fall back to their own backoff.
    #[must_use]
    pub fn retry_after_seconds(&self) -> Option<u64> {
        self.header("retry-after")?.trim().parse().ok()
    }
}

/// The outbound HTTP port real clients bind to.
#[async_trait]
pub trait HttpClientPort: std::fmt::Debug + Send + Sync {
    /// Sends a pre-validated request. Implementations MUST enforce
    /// `request.timeout_ms`, resolve DNS through
    /// [`EgressPolicy::pin_resolution`], and cap reads at
    /// `EgressPolicy::max_body_bytes`.
    async fn send(&self, request: &HttpRequest) -> Result<HttpResponse>;
}

/// Policy-enforcing wrapper around an [`HttpClientPort`].
#[derive(Debug)]
pub struct GuardedHttpClient<S: HttpClientPort> {
    inner: S,
    policy: EgressPolicy,
}

impl<S: HttpClientPort> GuardedHttpClient<S> {
    /// Wraps `inner` under `policy`.
    #[must_use]
    pub const fn new(inner: S, policy: EgressPolicy) -> Self {
        Self { inner, policy }
    }

    /// The active egress policy.
    #[must_use]
    pub const fn policy(&self) -> &EgressPolicy {
        &self.policy
    }

    /// The wrapped client.
    #[must_use]
    pub const fn inner(&self) -> &S {
        &self.inner
    }

    /// Sends `request`, enforcing:
    /// 1. destination policy (scheme/port/host),
    /// 2. redirect re-validation of EVERY `Location` hop (relative or
    ///    absolute), 303 → GET with an empty body,
    /// 3. redirect and body caps.
    pub async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
        let mut current = request.clone();
        let mut hops: u8 = 0;
        loop {
            self.policy.check_destination(&current.url)?;
            let response = self.inner.send(&current).await.map_err(|err| {
                err.with_context(format!("egress call to {} failed", current.url.host_str()))
            })?;

            if !(response.is_redirect() && response.header("location").is_some()) {
                if response.body.len() > self.policy.max_body_bytes {
                    return Err(AppError::external_service(
                        "http-egress",
                        format!(
                            "response body exceeded the {} byte cap",
                            self.policy.max_body_bytes
                        ),
                    ));
                }
                return Ok(response);
            }

            hops += 1;
            if hops > self.policy.max_redirects {
                return Err(AppError::external_service(
                    "http-egress",
                    format!(
                        "redirect limit of {} hops exceeded",
                        self.policy.max_redirects
                    ),
                ));
            }
            let location = response.header("location").unwrap_or_default();
            let base = Url::parse(current.url.as_str()).map_err(|err| {
                AppError::internal(format!("validated SafeUrl failed re-parse: {err}"))
            })?;
            let joined = base.join(location).map_err(|err| {
                AppError::external_service(
                    "http-egress",
                    format!("invalid redirect location: {err}"),
                )
            })?;
            let next_url = SafeUrl::parse(joined.as_str())?;
            if response.status == 303 {
                current.method = HttpMethod::Get;
                current.body.clear();
            }
            current.url = next_url;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;
    use std::sync::Mutex;

    fn safe(raw: &str) -> SafeUrl {
        SafeUrl::parse(raw).expect("safe url")
    }

    #[test]
    fn policy_enforces_scheme_port_and_allowlist() {
        let policy = EgressPolicy::default();
        assert!(policy
            .check_destination(&safe("https://api.example.com/x"))
            .is_ok());
        assert!(
            policy
                .check_destination(&safe("http://api.example.com/x"))
                .is_err(),
            "https_only default"
        );
        assert!(
            policy
                .check_destination(&safe("https://api.example.com:8443/x"))
                .is_err(),
            "non-default port blocked"
        );

        let mut dev = EgressPolicy::default();
        dev.permitted_plain_http_hosts
            .insert("local.test".to_owned());
        assert!(dev
            .check_destination(&safe("http://api.local.test/x"))
            .is_ok());

        let ported = EgressPolicy {
            allowed_ports: Some(BTreeSet::from([8443_u16])),
            ..EgressPolicy::default()
        };
        assert!(ported
            .check_destination(&safe("https://api.example.com:8443/x"))
            .is_ok());
        assert!(
            ported
                .check_destination(&safe("https://api.example.com/x"))
                .is_err(),
            "443 not in port allowlist"
        );

        let scoped = EgressPolicy {
            hostname_allowlist: Some(BTreeSet::from(["example.com".to_owned()])),
            ..EgressPolicy::default()
        };
        assert!(scoped
            .check_destination(&safe("https://api.example.com/x"))
            .is_ok());
        assert!(scoped
            .check_destination(&safe("https://attacker.io/x"))
            .is_err());
    }

    #[test]
    fn resolution_pinning_rejects_non_global_and_accepts_public() {
        let policy = EgressPolicy::default();
        assert!(policy.pin_resolution(&[]).is_err());
        assert!(policy
            .pin_resolution(&[IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))])
            .is_ok());
        assert!(
            policy
                .pin_resolution(&[
                    IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                    IpAddr::V4(Ipv4Addr::new(10, 0, 0, 7)),
                ])
                .is_err(),
            " ONE private answer poisons the whole set"
        );
        assert!(policy
            .pin_resolution(&[IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))])
            .is_err());
        assert!(
            policy
                .pin_resolution(&[IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254))])
                .is_err(),
            "cloud metadata"
        );
        assert!(
            policy
                .pin_resolution(&[IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1))])
                .is_err(),
            "CGNAT"
        );
        let mapped: Ipv6Addr = "::ffff:192.168.1.5".parse().expect("v6 literal");
        assert!(
            !is_publicly_routable(&IpAddr::V6(mapped)),
            "v4-mapped falls back to v4 rules"
        );
        let ula: Ipv6Addr = "fd00::1".parse().expect("v6 literal");
        assert!(!is_publicly_routable(&IpAddr::V6(ula)), "unique-local");
        let link: Ipv6Addr = "fe80::1".parse().expect("v6 literal");
        assert!(!is_publicly_routable(&IpAddr::V6(link)), "link-local");
        let global: Ipv6Addr = "2606:4700:4700::1111".parse().expect("v6 literal");
        assert!(is_publicly_routable(&IpAddr::V6(global)));
    }

    /// Scriptable recorder client used by guard and webhook tests alike?
    /// (webhook tests define their own; this one serves the guard tests).
    #[derive(Debug, Default)]
    struct ScriptClient {
        recorded: Mutex<Vec<HttpRequest>>,
        routes: Mutex<Vec<(SafeUrl, HttpResponse)>>,
    }

    impl ScriptClient {
        fn route(&self, url: &str, response: HttpResponse) {
            self.routes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((safe(url), response));
        }
    }

    #[async_trait]
    impl HttpClientPort for ScriptClient {
        async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
            self.recorded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(request.clone());
            let mut routes = self.routes.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(position) = routes
                .iter()
                .position(|(u, _)| u.as_str() == request.url.as_str())
            {
                return Ok(routes.remove(position).1);
            }
            drop(routes);
            Ok(HttpResponse::new(200))
        }
    }

    #[tokio::test]
    async fn redirect_hops_are_revalidated() {
        let client = ScriptClient::default();
        let mut to_private = HttpResponse::new(302);
        to_private.headers.push((
            "location".to_owned(),
            "http://169.254.169.254/latest".to_owned(),
        ));
        client.route("https://hook.example.test/a", to_private);

        let guarded = GuardedHttpClient::new(client, EgressPolicy::default());
        let result = guarded
            .send(&HttpRequest::post(
                safe("https://hook.example.test/a"),
                vec![1],
            ))
            .await;
        assert!(result.is_err(), "redirect to link-local must be refused");
        // 169.254.169.254 fails `SafeUrl::parse` (link-local literal), so the
        // hop is refused before any second request is sent.
        let sent = guarded
            .inner()
            .recorded
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len();
        assert_eq!(sent, 1, "no request was sent to the redirect target");
    }

    #[tokio::test]
    async fn redirect_follows_allowed_hops_and_caps_bodies() {
        let client = ScriptClient::default();
        let mut bounce = HttpResponse::new(302);
        bounce.headers.push((
            "location".to_owned(),
            "https://final.example.test/ok".to_owned(),
        ));
        client.route("https://hook.example.test/a", bounce);

        let guarded = GuardedHttpClient::new(client, EgressPolicy::default());
        let response = guarded
            .send(&HttpRequest::post(
                safe("https://hook.example.test/a"),
                vec![1, 2, 3],
            ))
            .await
            .expect("valid chain allowed");
        assert_eq!(response.status, 200);
        assert_eq!(
            guarded
                .inner()
                .recorded
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .len(),
            2
        );

        // Body cap
        let heavy = ScriptClient::default();
        let mut big = HttpResponse::new(200);
        big.body = vec![0u8; 64];
        heavy.route("https://big.example.test/", big);
        let tiny = EgressPolicy {
            max_body_bytes: 8,
            ..EgressPolicy::default()
        };
        let guarded_tiny = GuardedHttpClient::new(heavy, tiny);
        assert!(guarded_tiny
            .send(&HttpRequest::get(safe("https://big.example.test/")))
            .await
            .is_err());

        // Redirect loop — routes are consumed on use, so seed enough copies
        // to sustain the circle past the hop limit.
        let looped = ScriptClient::default();
        for _ in 0..8 {
            let mut again = HttpResponse::new(302);
            again.headers.push((
                "location".to_owned(),
                "https://loop.example.test/spin".to_owned(),
            ));
            looped.route("https://loop.example.test/spin", again);
        }
        let guarded_loop = GuardedHttpClient::new(looped, EgressPolicy::default());
        assert!(
            guarded_loop
                .send(&HttpRequest::get(safe("https://loop.example.test/spin")))
                .await
                .is_err(),
            "never-ending redirects die at the hop limit"
        );
    }
}
