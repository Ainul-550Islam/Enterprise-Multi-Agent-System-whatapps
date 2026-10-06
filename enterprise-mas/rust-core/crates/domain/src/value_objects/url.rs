//! Safe URL value object with scheme/host validation and basic SSRF guards.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use url::{Host, Url};

/// An absolute `http(s)://` URL that passed baseline safety checks:
///
/// * scheme restricted to `http` / `https`,
/// * explicit host (no opaque/weird authorities),
/// * no embedded credentials,
/// * host is not a loopback/private/link-local/unspecified **IP literal** and
///   not `localhost` (DNS-name checks belong to the egress layer).
///
/// This is a first-line guard; the integration HTTP client enforces the full
/// egress policy (DNS resolution checks, redirect re-validation, allowlists).
///
/// # Internal endpoints
///
/// Loopback and RFC1918 destinations are refused by [`Self::parse`], and that
/// is right for anything a tenant can influence. Platform services, however,
/// legitimately call each other over the cluster network, and an operator
/// needs to be able to configure `MAS_API_URL=http://mas-api:8080`.
///
/// [`Self::parse_internal_endpoint`] exists for exactly that case and carries
/// a flag the egress layer reads. The exception is therefore *visible at the
/// point of use*: a URL that skips the private-address check says so about
/// itself, and the egress policy still has to opt in before such a URL may be
/// contacted.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SafeUrl {
    url: Url,
    /// Whether this URL was parsed as an operator-declared internal endpoint.
    /// `false` for everything else, and never settable from a tenant-supplied
    /// value.
    internal_endpoint: bool,
}

impl SafeUrl {
    /// Parses and validates a URL, refusing private and loopback hosts.
    ///
    /// This is the correct parse for anything a tenant can influence.
    pub fn parse(raw: &str) -> Result<Self> {
        Self::parse_with(raw, false)
    }

    /// Parses a URL for an **operator-declared internal service endpoint**.
    ///
    /// Identical to [`Self::parse`] except that loopback and private hosts are
    /// accepted. Use it only for destinations that come from process
    /// configuration and that the egress policy has explicitly allow-listed —
    /// never for a URL derived from tenant data, a webhook endpoint, a
    /// connector config, or anything a request can reach.
    ///
    /// The returned URL reports [`Self::is_internal_endpoint`], and the egress
    /// policy refuses to contact it unless it was built with
    /// internal endpoints enabled.
    pub fn parse_internal_endpoint(raw: &str) -> Result<Self> {
        Self::parse_with(raw, true)
    }

    /// Whether this URL was parsed as an internal endpoint.
    #[must_use]
    pub const fn is_internal_endpoint(&self) -> bool {
        self.internal_endpoint
    }

    fn parse_with(raw: &str, internal_endpoint: bool) -> Result<Self> {
        let trimmed = raw.trim();
        let url = Url::parse(trimmed).map_err(|err| {
            AppError::invalid_field("url", "invalid_url", format!("not a valid URL: {err}"))
        })?;

        match url.scheme() {
            "http" | "https" => {},
            scheme => {
                return Err(AppError::invalid_field(
                    "url",
                    "invalid_scheme",
                    format!("scheme '{scheme}' is not allowed (http/https only)"),
                ));
            },
        }

        if !url.username().is_empty() || url.password().is_some() {
            return Err(AppError::invalid_field(
                "url",
                "credentials_not_allowed",
                "URL must not contain embedded credentials",
            ));
        }

        let host = url.host().ok_or_else(|| {
            AppError::invalid_field("url", "missing_host", "URL must contain an explicit host")
        })?;

        if internal_endpoint {
            // The scheme, credential and host-presence checks above still
            // applied. Only the private/loopback refusal is lifted, and only
            // for the address the operator named.
            return Ok(Self {
                url,
                internal_endpoint: true,
            });
        }

        match host {
            Host::Domain(domain) => {
                let lowered = domain.to_ascii_lowercase();
                if lowered == "localhost"
                    || lowered.ends_with(".localhost")
                    || lowered.ends_with(".internal")
                    || lowered == "metadata.google.internal"
                {
                    return Err(AppError::invalid_field(
                        "url",
                        "forbidden_host",
                        "local and internal metadata hosts are not allowed",
                    ));
                }
            },
            Host::Ipv4(ip) => {
                if Self::ipv4_is_forbidden(ip) {
                    return Err(AppError::invalid_field(
                        "url",
                        "forbidden_host",
                        format!("IP address {ip} is not reachable from this platform"),
                    ));
                }
            },
            Host::Ipv6(ip) => {
                if Self::ipv6_is_forbidden(ip) {
                    return Err(AppError::invalid_field(
                        "url",
                        "forbidden_host",
                        format!("IP address {ip} is not reachable from this platform"),
                    ));
                }
            },
        }

        Ok(Self {
            url,
            internal_endpoint: false,
        })
    }

    fn ipv4_is_forbidden(ip: Ipv4Addr) -> bool {
        ip.is_loopback()
            || ip.is_private()
            || ip.is_link_local()
            || ip.is_unspecified()
            || ip.is_broadcast()
            || ip.is_documentation()
            || ip.is_multicast()
            // CGNAT / carrier-grade NAT range 100.64.0.0/10
            || (ip.octets()[0] == 100 && (ip.octets()[1] & 0xC0) == 64)
    }

    fn ipv6_is_forbidden(ip: Ipv6Addr) -> bool {
        ip.is_loopback() || ip.is_unspecified() || ip.is_multicast()
    }

    /// The validated URL as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.url.as_str()
    }

    /// The host portion (always present after validation).
    #[must_use]
    pub fn host_str(&self) -> &str {
        self.url.host_str().unwrap_or("")
    }

    /// `http` or `https`.
    #[must_use]
    pub fn scheme(&self) -> &str {
        self.url.scheme()
    }

    /// Whether TLS is used.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        self.url.scheme() == "https"
    }

    /// Access the parsed URL (read-only).
    #[must_use]
    pub const fn as_url(&self) -> &Url {
        &self.url
    }
}

impl fmt::Display for SafeUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.url.as_str())
    }
}

impl FromStr for SafeUrl {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl TryFrom<String> for SafeUrl {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}

impl From<SafeUrl> for String {
    fn from(url: SafeUrl) -> Self {
        url.url.into()
    }
}
