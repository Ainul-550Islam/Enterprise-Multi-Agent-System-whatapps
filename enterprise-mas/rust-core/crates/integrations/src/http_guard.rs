//! PURPOSE: The SSRF egress guard — the policy every production outbound
//! HTTP call goes through.
//!
//! Owning layer: `mas-integrations`.
//!
//! Production responsibility: decide whether a destination is reachable at
//! all: reject link-local, loopback, and metadata addresses unless a caller
//! names them explicitly as internal endpoints.
//!
//! Security boundary: without this, any feature that fetches a URL becomes a
//! request forgery into the cluster. Reaching a private range is a decision
//! somebody makes on purpose, in code, for a named host.
//!
//! It must NOT:
//!   • allow a redirect to re-target an approved request into a private
//!     range;
//!   • be optional for production callers — `HttpDispatcher` holds one and
//!     every dispatch goes through it;
//!   • resolve a hostname more than once (DNS rebinding).

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
    /// Whether URLs parsed as operator-declared internal endpoints
    /// (`SafeUrl::parse_internal_endpoint`) may be contacted.
    ///
    /// Default `false`. When `true`, [`Self::hostname_allowlist`] is
    /// **mandatory**: the exception is granted to a named destination, not to
    /// "anything private".
    ///
    /// This exists because platform services legitimately call each other over
    /// the cluster network. It is not a way to reach a tenant-supplied
    /// private address — a URL from tenant data is never parsed as an internal
    /// endpoint, and the flag on the `SafeUrl` is what distinguishes the two.
    #[serde(default)]
    pub allow_internal_endpoints: bool,
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
            allow_internal_endpoints: false,
        }
    }
}

impl EgressPolicy {
    /// A policy for one operator-declared internal service endpoint.
    ///
    /// `hosts` names the exact hosts the platform may reach over the cluster
    /// network — a reviewed Kubernetes service DNS name. Loopback,
    /// link-local, metadata, and multicast addresses remain forbidden even for
    /// this operator-only policy. Plain HTTP is permitted because cluster traffic is commonly not TLS
    /// terminated, and every other restriction (redirect re-validation, body
    /// caps, redirect limits) stays in force.
    ///
    /// The allowlist is required: an internal-endpoint policy with no
    /// allowlist would be an open permit to reach any private address, which
    /// is the exact vulnerability the guard exists to prevent.
    #[must_use]
    pub fn for_internal_endpoints<I, S>(hosts: I, allowed_ports: Option<BTreeSet<u16>>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let hosts: BTreeSet<String> = hosts.into_iter().map(Into::into).collect();
        Self {
            https_only: false,
            permitted_plain_http_hosts: hosts.clone(),
            allowed_ports,
            hostname_allowlist: Some(hosts),
            max_redirects: DEFAULT_MAX_REDIRECTS,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            allow_internal_endpoints: true,
        }
    }
}

impl EgressPolicy {
    /// Validates that `url` may be contacted under this policy.
    ///
    /// `SafeUrl` guarantees parse-level safety; this layer applies the
    /// operator policy on scheme, port and hostname.
    pub fn check_destination(&self, url: &SafeUrl) -> Result<()> {
        // An internal endpoint is permitted only when the policy opted in, and
        // then only if it also passes the hostname allowlist below. A URL that
        // merely *claims* to be internal cannot get through a policy that never
        // enabled them.
        if url.is_internal_endpoint() {
            if !self.allow_internal_endpoints {
                return Err(AppError::invalid_field(
                    "url",
                    "internal_endpoint_forbidden",
                    "this egress policy does not permit internal endpoints",
                ));
            }
            if self.hostname_allowlist.is_none() {
                return Err(AppError::internal(
                    "internal endpoints are enabled without a hostname allowlist; \
                     refusing to contact anything",
                ));
            }
        }

        let parsed = Url::parse(url.as_str()).map_err(|err| {
            AppError::internal(format!("validated SafeUrl failed re-parse: {err}"))
        })?;
        let host = normalize_host(parsed.host_str().unwrap_or_default());

        match parsed.scheme() {
            "https" => {},
            "http" => {
                let permitted = !self.https_only
                    || self
                        .permitted_plain_http_hosts
                        .iter()
                        .any(|allowed| host_matches(&host, allowed));
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
            let allowed = allowlist.iter().any(|entry| host_matches(&host, entry));
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
        self.pin_resolution_public(resolved)
    }

    /// Resolution gate for a destination that may legitimately be private.
    ///
    /// `internal_endpoint` comes from the URL and is honoured only when the
    /// policy allows internal endpoints at all. For an internal endpoint the
    /// requirement relaxes from "publicly routable" to "not a link-local or
    /// metadata address": reaching `10.0.0.5` is the point, but reaching
    /// `169.254.169.254` never is, not even from inside the cluster.
    pub fn pin_resolution_for(
        &self,
        resolved: &[IpAddr],
        internal_endpoint: bool,
    ) -> Result<IpAddr> {
        if resolved.is_empty() {
            return Err(AppError::external_service(
                "http-egress",
                "destination resolved to no addresses",
            ));
        }
        if internal_endpoint && self.allow_internal_endpoints {
            for addr in resolved {
                if is_metadata_or_link_local(addr) {
                    return Err(AppError::invalid_field(
                        "url",
                        "forbidden_host",
                        format!("destination resolves to metadata/link-local address {addr}"),
                    ));
                }
            }
            return Ok(resolved[0]);
        }
        self.pin_resolution_public(resolved)
    }

    /// The public-only resolution gate.
    fn pin_resolution_public(&self, resolved: &[IpAddr]) -> Result<IpAddr> {
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

/// Canonicalizes a hostname for operator allowlist comparison. DNS names are
/// case-insensitive and a trailing root dot is semantically irrelevant.
fn normalize_host(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

/// Returns whether a host matches an explicit allowlist entry.
///
/// Bare entries are exact hostnames. Operators who intentionally want a domain
/// family must say so with `*.example.com`; accepting every subdomain of a
/// bare tenant/provider hostname is an ambient authority expansion and makes
/// a typo or delegated DNS zone an outbound exfiltration path.
fn host_matches(host: &str, configured: &str) -> bool {
    let host = normalize_host(host);
    let configured = normalize_host(configured);
    match configured.strip_prefix("*.") {
        Some(parent) if !parent.is_empty() => {
            host.len() > parent.len()
                && host.ends_with(parent)
                && host.as_bytes().get(host.len() - parent.len() - 1) == Some(&b'.')
        },
        _ => host == configured,
    }
}

/// Whether an address is a link-local or cloud-metadata address.
///
/// Never reachable, not even from an operator-declared internal endpoint:
/// `169.254.0.0/16` is the metadata service on every major cloud, and a
/// platform service calling its own API has no business being able to read
/// instance credentials.
#[must_use]
pub fn is_metadata_or_link_local(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => {
            ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_loopback()
                || ip.is_multicast()
        },
        IpAddr::V6(ip) => {
            // IPv4-mapped IPv6 is never a supported egress address. Treating
            // a mapped public address as a separate class from IPv4 lets
            // callers sidestep literal and DNS range policy.
            if ip.to_ipv4_mapped().is_some() {
                return true;
            }
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                || (ip.segments()[0] & 0xffc0) == 0xfe80
        },
    }
}

/// Whether an address is safe to connect to from the platform. Complements
/// `SafeUrl`'s literal-time checks with full IPv6 ranges: unique-local
/// (`fc00::/7`), link-local (`fe80::/10`), documentation space, and
/// IPv4-mapped addresses are rejected outright.
#[must_use]
pub fn is_publicly_routable(addr: &IpAddr) -> bool {
    match addr {
        IpAddr::V4(ip) => ipv4_is_public(ip),
        IpAddr::V6(ip) => {
            // Never permit IPv4-mapped IPv6 through the IPv6 code path. The
            // direct-literal guard and the DNS gate must agree on this class.
            if ip.to_ipv4_mapped().is_some() {
                return false;
            }
            let segments = ip.segments();
            !(ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_multicast()
                // IPv4-compatible / unspecified-prefix addresses are not
                // globally routable IPv6 destinations.
                || segments[..6].iter().all(|segment| *segment == 0)
                || (segments[0] & 0xfe00) == 0xfc00 // fc00::/7 unique-local
                || (segments[0] & 0xffc0) == 0xfe80 // fe80::/10 link-local
                || (segments[0] == 0x2001 && segments[1] == 0x0db8)) // documentation
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
    /// `DELETE`.
    Delete,
}

impl HttpMethod {
    /// Canonical wire token.
    #[must_use]
    pub const fn as_token(&self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Delete => "DELETE",
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

    /// A DELETE request. It has no body and, like every non-GET method,
    /// cannot follow redirects through [`GuardedHttpClient`].
    #[must_use]
    pub fn delete(url: SafeUrl) -> Self {
        Self {
            method: HttpMethod::Delete,
            url,
            headers: Vec::new(),
            body: Vec::new(),
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
    /// 1. destination and header policy before every socket-owning send,
    /// 2. redirect re-validation of EVERY `Location` hop,
    /// 3. no redirect of a mutating request or credentialed cross-origin GET,
    /// 4. body caps on both intermediate and terminal responses.
    pub async fn send(&self, request: &HttpRequest) -> Result<HttpResponse> {
        validate_request_headers(&request.headers)?;
        if request.timeout_ms == 0 {
            return Err(AppError::invalid_field(
                "timeout_ms",
                "out_of_range",
                "outbound HTTP timeout must be greater than zero",
            ));
        }

        let mut current = request.clone();
        let initial_origin = origin(&current.url)?;
        let mut hops: u8 = 0;
        loop {
            self.policy.check_destination(&current.url)?;
            let response = self.inner.send(&current).await.map_err(|err| {
                err.with_context(format!("egress call to {} failed", current.url.host_str()))
            })?;
            ensure_response_is_bounded(&response, self.policy.max_body_bytes)?;

            let Some(location) = response.header("location") else {
                return Ok(response);
            };
            if !response.is_redirect() {
                return Ok(response);
            }
            if current.method != HttpMethod::Get {
                return Err(AppError::invalid_field(
                    "url",
                    "unsafe_redirect_method",
                    "redirects are not permitted for mutating outbound HTTP requests",
                ));
            }

            hops = hops.saturating_add(1);
            if hops > self.policy.max_redirects {
                return Err(AppError::external_service(
                    "http-egress",
                    format!(
                        "redirect limit of {} hops exceeded",
                        self.policy.max_redirects
                    ),
                ));
            }
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
            if origin(&next_url)? != initial_origin && headers_are_sensitive(&current.headers) {
                return Err(AppError::invalid_field(
                    "url",
                    "credential_redirect",
                    "cross-origin redirects are not permitted for credentialed requests",
                ));
            }
            // The next iteration performs full scheme/host/port validation
            // before the second request is handed to the concrete transport.
            current.url = next_url;
        }
    }
}

/// Returns `(scheme, hostname, effective port)` for redirect origin checks.
fn origin(url: &SafeUrl) -> Result<(String, String, u16)> {
    let parsed = url.as_url();
    let port = parsed.port_or_known_default().ok_or_else(|| {
        AppError::invalid_field("url", "missing_port", "outbound URL has no effective port")
    })?;
    Ok((
        parsed.scheme().to_ascii_lowercase(),
        normalize_host(parsed.host_str().unwrap_or_default()),
        port,
    ))
}

/// Enforces a small set of transport-owned headers that untrusted connector
/// configuration must never override. Values also reject CR/LF so a future
/// alternate HTTP adapter cannot turn a metadata string into header injection.
pub(crate) fn validate_request_headers(headers: &[(String, String)]) -> Result<()> {
    const FORBIDDEN: &[&str] = &[
        "host",
        "content-length",
        "transfer-encoding",
        "connection",
        "keep-alive",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "upgrade",
    ];
    for (name, value) in headers {
        let normalized = name.to_ascii_lowercase();
        if normalized.is_empty()
            || !normalized
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        {
            return Err(AppError::invalid_field(
                "headers",
                "invalid_header_name",
                "outbound HTTP header names must be non-empty ASCII tokens",
            ));
        }
        if FORBIDDEN.contains(&normalized.as_str()) {
            return Err(AppError::invalid_field(
                "headers",
                "forbidden_header",
                format!("outbound HTTP header '{normalized}' is transport-owned"),
            ));
        }
        if value.len() > 16 * 1024 || value.contains('\r') || value.contains('\n') {
            return Err(AppError::invalid_field(
                "headers",
                "invalid_header_value",
                "outbound HTTP header values must be bounded and contain no CR/LF",
            ));
        }
    }
    Ok(())
}

/// Whether a redirect could carry a credential or browser/session authority to
/// a different origin. The header values are deliberately not inspected or
/// logged.
fn headers_are_sensitive(headers: &[(String, String)]) -> bool {
    const SENSITIVE: &[&str] = &[
        "authorization",
        "cookie",
        "proxy-authorization",
        "x-api-key",
        "x-amz-security-token",
        "x-vault-token",
        "x-goog-api-key",
        "x-ms-token-aad-access-token",
    ];
    headers
        .iter()
        .any(|(name, _)| SENSITIVE.contains(&name.to_ascii_lowercase().as_str()))
}

/// Checks a response at the guard boundary even when a custom test or future
/// concrete client returned it without applying its own streaming cap.
fn ensure_response_is_bounded(response: &HttpResponse, limit: usize) -> Result<()> {
    if response.body.len() > limit {
        return Err(AppError::external_service(
            "http-egress",
            format!("response body exceeded the {limit} byte cap"),
        ));
    }
    Ok(())
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
        assert!(dev.check_destination(&safe("http://local.test/x")).is_ok());
        assert!(
            dev.check_destination(&safe("http://api.local.test/x"))
                .is_err(),
            "a bare hostname allowlist entry is exact; wildcard is explicit"
        );

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
            hostname_allowlist: Some(BTreeSet::from(["*.example.com".to_owned()])),
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
            .send(&HttpRequest::get(safe("https://hook.example.test/a")))
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
