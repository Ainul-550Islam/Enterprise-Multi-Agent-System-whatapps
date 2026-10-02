//! Secret/PII redaction primitives.
//!
//! **Invariant:** secret material must never appear in logs, metrics, events,
//! error messages or audit payloads. These helpers are applied by the
//! observability layer and anywhere structured metadata is attached.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Replacement string written in place of any redacted value.
pub const REDACTED: &str = "[REDACTED]";

/// Canonical classification of sensitive data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SensitiveField {
    Password,
    Token,
    ApiKey,
    Secret,
    PrivateKey,
    Credential,
    SessionMaterial,
    PiiEmail,
}

impl SensitiveField {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::Token => "token",
            Self::ApiKey => "api_key",
            Self::Secret => "secret",
            Self::PrivateKey => "private_key",
            Self::Credential => "credential",
            Self::SessionMaterial => "session_material",
            Self::PiiEmail => "pii_email",
        }
    }
}

/// Exact-match keys treated as sensitive in structured payloads
/// (compared case-insensitively, `-`/`_` normalized).
const SENSITIVE_KEYS: &[&str] = &[
    "password",
    "passwd",
    "pwd",
    "secret",
    "client_secret",
    "webhook_secret",
    "signing_secret",
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "session_token",
    "api_key",
    "apikey",
    "x_api_key",
    "authorization",
    "proxy_authorization",
    "auth",
    "credential",
    "credentials",
    "private_key",
    "privatekey",
    "cookie",
    "set_cookie",
];

/// Substring markers: a key containing one of these is always sensitive.
const SENSITIVE_MARKERS: &[&str] = &["password", "secret", "private_key", "api_key", "token"];

/// Normalized (`lowercase`, `-`→`_`) check against the built-in key set.
#[must_use]
pub fn is_sensitive_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    if SENSITIVE_KEYS.contains(&normalized.as_str()) {
        return true;
    }
    SENSITIVE_MARKERS
        .iter()
        .any(|marker| normalized.contains(marker))
}

/// Classifies a key into a [`SensitiveField`] category, if sensitive.
#[must_use]
pub fn classify_key(key: &str) -> Option<SensitiveField> {
    if !is_sensitive_key(key) {
        return None;
    }
    let normalized = key.to_ascii_lowercase().replace('-', "_");
    if normalized.contains("password") || normalized.contains("passwd") || normalized == "pwd" {
        Some(SensitiveField::Password)
    } else if normalized.contains("api_key") || normalized.contains("apikey") {
        Some(SensitiveField::ApiKey)
    } else if normalized.contains("private_key") || normalized.contains("privatekey") {
        Some(SensitiveField::PrivateKey)
    } else if normalized.contains("session") {
        Some(SensitiveField::SessionMaterial)
    } else if normalized.contains("credential") {
        Some(SensitiveField::Credential)
    } else if normalized.contains("token")
        || normalized.contains("authorization")
        || normalized == "auth"
    {
        Some(SensitiveField::Token)
    } else {
        Some(SensitiveField::Secret)
    }
}

/// Redacts an email address, preserving the first character and the domain:
/// `alice.smith@example.com` → `a***@example.com`. Anything not resembling an
/// email becomes `[REDACTED]`.
#[must_use]
pub fn redact_email(email: &str) -> String {
    let email = email.trim();
    let Some((local, domain)) = email.split_once('@') else {
        return REDACTED.to_owned();
    };
    if local.is_empty() || domain.is_empty() {
        return REDACTED.to_owned();
    }
    let first = local.chars().next().unwrap_or('*');
    format!("{first}***@{domain}")
}

/// Redacts an opaque token, disclosing only its length:
/// `mysecrettoken` → `[REDACTED token, 13 chars]`.
#[must_use]
pub fn redact_token(token: &str) -> String {
    let len = token.chars().count();
    if len == 0 {
        REDACTED.to_owned()
    } else {
        format!("[REDACTED token, {len} chars]")
    }
}

/// Redacts an API key while preserving the well-known visible prefix used for
/// lookups: `mas_a1b2c3d4…` → `mas_a1b2…[REDACTED]`.
#[must_use]
pub fn redact_api_key(key: &str) -> String {
    let key = key.trim();
    if let Some(rest) = key.strip_prefix(crate::constants::API_KEY_PREFIX) {
        let visible: String = rest
            .chars()
            .take(crate::constants::API_KEY_VISIBLE_PREFIX_LEN.saturating_sub(4))
            .collect();
        if visible.is_empty() {
            return REDACTED.to_owned();
        }
        format!(
            "{}{}…{}",
            crate::constants::API_KEY_PREFIX,
            visible,
            REDACTED
        )
    } else {
        REDACTED.to_owned()
    }
}

/// In-place redaction of a JSON document: any object member whose key is
/// sensitive (exact match or marker substring, case-insensitive) is replaced
/// with `"[REDACTED]"`. Arrays and nested objects are traversed.
pub fn redact_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => redact_object(map, &|key, _| is_sensitive_key(key)),
        Value::Array(items) => {
            for item in items {
                redact_json_value(item);
            }
        },
        _ => {},
    }
}

fn redact_object(map: &mut Map<String, Value>, is_sensitive: &dyn Fn(&str, &Value) -> bool) {
    for (key, member) in map.iter_mut() {
        if is_sensitive(key, member) {
            *member = Value::String(REDACTED.to_owned());
        } else {
            match member {
                Value::Object(child) => redact_object(child, is_sensitive),
                Value::Array(items) => {
                    for item in items.iter_mut() {
                        redact_json_value(item);
                    }
                },
                _ => {},
            }
        }
    }
}

/// Configured redactor with optional extra keys and email-PII detection.
///
/// This is the policy object; the free functions above are the stateless
/// fast path using default settings.
#[derive(Debug, Clone)]
pub struct SecretRedactor {
    extra_keys: Vec<String>,
    redact_email_values: bool,
}

impl Default for SecretRedactor {
    fn default() -> Self {
        Self {
            extra_keys: Vec::new(),
            redact_email_values: true,
        }
    }
}

impl SecretRedactor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds custom keys (normalized like built-ins) to the sensitive set.
    #[must_use]
    pub fn with_extra_keys<I, S>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.extra_keys.extend(
            keys.into_iter()
                .map(|key| key.into().to_ascii_lowercase().replace('-', "_")),
        );
        self
    }

    /// Disables value-level email detection (keys still redact normally).
    #[must_use]
    pub fn without_email_value_redaction(mut self) -> Self {
        self.redact_email_values = false;
        self
    }

    /// Whether `key` is sensitive under this policy (built-ins + extras).
    #[must_use]
    pub fn is_sensitive(&self, key: &str) -> bool {
        is_sensitive_key(key) || {
            let normalized = key.to_ascii_lowercase().replace('-', "_");
            self.extra_keys.contains(&normalized)
        }
    }

    /// In-place redaction of a JSON document under this policy.
    pub fn redact_value(&self, value: &mut Value) {
        match value {
            Value::Object(map) => self.redact_object(map),
            Value::Array(items) => {
                for item in items.iter_mut() {
                    self.redact_value(item);
                }
            },
            Value::String(text) if self.redact_email_values && looks_like_email(text) => {
                *text = redact_email(text);
            },
            _ => {},
        }
    }

    fn redact_object(&self, map: &mut Map<String, Value>) {
        for (key, member) in map.iter_mut() {
            if self.is_sensitive(key) {
                *member = Value::String(REDACTED.to_owned());
            } else {
                self.redact_value(member);
            }
        }
    }

    /// Redacts a serialized JSON string. Returns the input unchanged when it
    /// is not valid JSON (callers must never log unparseable secrets anyway).
    #[must_use]
    pub fn redact_json_str(&self, input: &str) -> String {
        match serde_json::from_str::<Value>(input) {
            Ok(mut value) => {
                self.redact_value(&mut value);
                serde_json::to_string(&value).unwrap_or_else(|_| REDACTED.to_owned())
            },
            Err(_) => input.to_owned(),
        }
    }
}

/// Cheap heuristic for value-level email detection (contains `@` and a dot
/// after it, no whitespace).
fn looks_like_email(value: &str) -> bool {
    if value.len() < 5 || value.len() > 320 || value.chars().any(char::is_whitespace) {
        return false;
    }
    match value.split_once('@') {
        Some((local, domain)) => {
            !local.is_empty()
                && domain.contains('.')
                && !domain.starts_with('.')
                && !domain.ends_with('.')
        },
        None => false,
    }
}
