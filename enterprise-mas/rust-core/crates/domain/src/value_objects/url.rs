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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SafeUrl(Url);

impl SafeUrl {
    /// Parses and validates a URL.
    pub fn parse(raw: &str) -> Result<Self> {
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

        Ok(Self(url))
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
        self.0.as_str()
    }

    /// The host portion (always present after validation).
    #[must_use]
    pub fn host_str(&self) -> &str {
        self.0.host_str().unwrap_or("")
    }

    /// `http` or `https`.
    #[must_use]
    pub fn scheme(&self) -> &str {
        self.0.scheme()
    }

    /// Whether TLS is used.
    #[must_use]
    pub fn is_tls(&self) -> bool {
        self.0.scheme() == "https"
    }

    /// Access the parsed URL (read-only).
    #[must_use]
    pub const fn as_url(&self) -> &Url {
        &self.0
    }
}

impl fmt::Display for SafeUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
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
        url.0.into()
    }
}
