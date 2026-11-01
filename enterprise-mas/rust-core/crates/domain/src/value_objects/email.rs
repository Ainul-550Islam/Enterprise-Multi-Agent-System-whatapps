//! Validated email address value object.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A syntactically validated, normalized email address.
///
/// Normalization: trimmed; domain part lowercased; local part preserved
/// (mailbox providers treat it case-sensitively in theory, case-insensitively
/// in practice — we keep the original to avoid identity drift).
///
/// Validation is deliberately pragmatic (RFC 5322 is intentionally *not*
/// fully implemented): non-empty local part of allowed characters, single `@`,
/// domain with at least one dot, valid labels, total length ≤ 320.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Email(String);

impl Email {
    /// Maximum accepted length per RFC 5321.
    pub const MAX_LENGTH: usize = 320;
    /// Maximum local-part length.
    pub const MAX_LOCAL_LENGTH: usize = 64;

    /// Parses and validates an email address.
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();

        if trimmed.is_empty() || trimmed.len() > Self::MAX_LENGTH {
            return Err(AppError::invalid_field(
                "email",
                "invalid_length",
                format!("email must be 1..={} characters", Self::MAX_LENGTH),
            ));
        }
        if trimmed.chars().any(char::is_whitespace) {
            return Err(AppError::invalid_field(
                "email",
                "invalid_format",
                "email must not contain whitespace",
            ));
        }

        let mut parts = trimmed.split('@');
        let (Some(local), Some(domain), None) = (parts.next(), parts.next(), parts.next()) else {
            return Err(AppError::invalid_field(
                "email",
                "invalid_format",
                "email must contain exactly one '@'",
            ));
        };

        if local.is_empty() || local.len() > Self::MAX_LOCAL_LENGTH {
            return Err(AppError::invalid_field(
                "email",
                "invalid_format",
                format!(
                    "local part must be 1..={} characters",
                    Self::MAX_LOCAL_LENGTH
                ),
            ));
        }
        let local_ok = local
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || "!#$%&'*+-/=?^_`{|}~.".contains(ch));
        if !local_ok || local.starts_with('.') || local.ends_with('.') || local.contains("..") {
            return Err(AppError::invalid_field(
                "email",
                "invalid_format",
                "invalid local part",
            ));
        }

        if !Self::domain_is_valid(domain) {
            return Err(AppError::invalid_field(
                "email",
                "invalid_format",
                "invalid domain part",
            ));
        }

        Ok(Self(format!("{local}@{}", domain.to_ascii_lowercase())))
    }

    fn domain_is_valid(domain: &str) -> bool {
        if domain.len() > 253 || !domain.contains('.') {
            return false;
        }
        domain.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
    }

    /// Canonical string form (normalized).
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The normalized domain part (after `@`).
    #[must_use]
    pub fn domain(&self) -> &str {
        // Invariant guaranteed by `parse`: exactly one '@'.
        self.0.split('@').nth(1).unwrap_or("")
    }

    /// The (case-preserved) local part (before `@`).
    #[must_use]
    pub fn local(&self) -> &str {
        self.0.split('@').next().unwrap_or("")
    }

    /// Redacted representation safe for logs (`a***@example.com`).
    #[must_use]
    pub fn redacted(&self) -> String {
        mas_common::redaction::redact_email(&self.0)
    }
}

impl fmt::Display for Email {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Email {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl TryFrom<String> for Email {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}

impl From<Email> for String {
    fn from(email: Email) -> Self {
        email.0
    }
}
