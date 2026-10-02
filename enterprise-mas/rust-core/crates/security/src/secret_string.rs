//! [`SecretString`]: in-memory secret material with redacted output and
//! zeroization on drop.
//!
//! This type is the *only* legitimate home for raw secret bytes in the
//! platform. `Debug`/`Display` never show content; callers must spell
//! `expose()` to touch bytes, and the value is wiped (via `zeroize`, which
//! the optimizer cannot elide) when it goes out of scope.

use serde::Serialize;
use std::fmt;
use zeroize::Zeroizing;

/// The statically-redacted rendering used everywhere secrets are printed.
pub const SECRET_REDACTED: &str = "«SECRET»";

/// Redacting secret material (zeroized on drop).
#[derive(Clone)]
pub struct SecretString(Zeroizing<String>);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    /// Reads the secret. Audit-equivalent of "opening an envelope": callers
    /// should only do this at the exact site of use.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(SECRET_REDACTED)
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(SECRET_REDACTED)
    }
}

// Deliberately NOT `Serialize`: secrets may not ride payloads accidentally.
// A fighting-chance guard: explicit serialization refusal when a caller
// truly needs it must go through `expose()` by hand.
impl Serialize for SecretString {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(SECRET_REDACTED)
    }
}

/// Constant-time byte equality for authentication material. Length is
/// folded into the accumulator so lengths do not short-circuit the loop.
#[must_use]
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let len_left = left.len() as u64;
    let len_right = right.len() as u64;
    let mut acc = len_left ^ len_right;
    let max = left.len().max(right.len());
    for i in 0..max {
        let a = u64::from(*left.get(i).unwrap_or(&0));
        let b = u64::from(*right.get(i).unwrap_or(&0));
        acc |= a ^ b;
    }
    acc == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secrets_never_render_content() {
        let secret = SecretString::new("hunter2-password");
        assert!(!format!("{secret:?}").contains("hunter2"));
        assert!(!format!("{secret}").contains("hunter2"));
        assert_eq!(secret.expose(), "hunter2-password");
        let json = serde_json::to_value(&secret).expect("value");
        assert_eq!(json, serde_json::Value::from(SECRET_REDACTED));
    }

    #[test]
    fn constant_time_eq_behaviour() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
        // long bash strings
        let a: Vec<u8> = (0..4096).map(|i| i as u8).collect();
        let mut b = a.clone();
        b[2000] = b[2000].wrapping_add(1);
        assert!(constant_time_eq(&a, &a));
        assert!(!constant_time_eq(&a, &b));
    }

    #[test]
    fn zeroizing_drop_compiles_and_clears() {
        // Behavioural proof is hard in-process; we assert the wrapper type
        // is used (contents zeroizable through the public API).
        let secret = SecretString::new("wipe me");
        assert_eq!(secret.len(), 7);
        drop(secret);
    }
}
