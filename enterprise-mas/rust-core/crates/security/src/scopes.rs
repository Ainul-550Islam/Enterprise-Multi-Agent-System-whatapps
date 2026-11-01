//! Scope vocabulary and matching: `resource:verb` with a trailing
//! `resource:*` wildcard, `*` granting everything (break-glass only).

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// A normalized set of scopes owned by a principal.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeSet(BTreeSet<String>);

impl ScopeSet {
    /// Builds and validates a set. Scope syntax: `resource:verb` where both
    /// segments are `[a-z0-9_-]{1,64}`, `verb` may end in `*` for the
    /// resource-wide wildcard, and the literal `*` is the global wildcard.
    pub fn new<I, S>(scopes: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let set: BTreeSet<String> = scopes.into_iter().map(Into::into).collect();
        for scope in &set {
            validate_scope(scope)?;
        }
        Ok(Self(set))
    }

    /// An empty set (denies everything).
    #[must_use]
    pub fn denied() -> Self {
        Self::default()
    }

    /// Global admin scope; never persist casually.
    pub fn all() -> Self {
        Self(BTreeSet::from(["*".to_owned()]))
    }

    /// Whether `required` is permitted by this set.
    ///
    /// Grants when: an exact scope exists, a `resource:*` wildcard covers
    /// it, or the global `*` is present.
    #[must_use]
    pub fn permits(&self, required: &str) -> bool {
        if self.0.contains("*") || self.0.contains(required) {
            return true;
        }
        match required.rsplit_once(':') {
            Some((resource, _verb)) => self.0.contains(&format!("{resource}:*")),
            None => false,
        }
    }

    #[must_use]
    pub fn permits_all<'a>(&self, required: impl IntoIterator<Item = &'a str>) -> bool {
        required.into_iter().all(|scope| self.permits(scope))
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }

    /// Owned view as a Vec (for embedding into audit metadata/principal structs).
    #[must_use]
    pub fn to_vec(&self) -> Vec<String> {
        self.0.iter().cloned().collect()
    }
}

impl fmt::Display for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            self.0.iter().cloned().collect::<Vec<_>>().join(",")
        )
    }
}

/// Validates a single scope string.
pub fn validate_scope(scope: &str) -> Result<()> {
    if scope == "*" {
        return Ok(());
    }
    let invalid = |code: &'static str| {
        AppError::invalid_field(
            "scope",
            code,
            format!("'{scope}' is not a valid scope (expect 'resource:verb', 'resource:*' or '*')"),
        )
    };
    let (resource, verb) = scope
        .split_once(':')
        .ok_or_else(|| invalid("missing_colon"))?;
    let segment_ok = |segment: &str, wildcard_tail: bool| {
        (!segment.is_empty() && segment.len() <= 64)
            && segment.chars().enumerate().all(|(i, ch)| {
                ch.is_ascii_alphanumeric()
                    || matches!(ch, '-' | '_')
                    || (ch == '*' && wildcard_tail && i == segment.len() - 1)
            })
            // `*` allowed only as a standalone verb tail, not embedded.
            && (!wildcard_tail || !segment[..segment.len().saturating_sub(1)].contains('*'))
    };
    if scope.contains(':') {
        let extra_colons = scope.matches(':').count();
        if extra_colons > 1 {
            return Err(invalid("too_many_segments"));
        }
    }
    if !segment_ok(resource, false) {
        return Err(invalid("invalid_resource"));
    }
    if !segment_ok(verb, true) && !(verb.len() == 1 && verb == "*") {
        return Err(invalid("invalid_verb"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_covers_exact_wildcard_and_global() {
        let scopes =
            ScopeSet::new(["agents:read", "executions:*", "tools:invoke"]).expect("scopes");
        for (required, expected) in [
            ("agents:read", true),
            ("agents:write", false),
            ("executions:read", true),
            ("executions:cancel", true),
            ("tools:invoke", true),
            ("tools:read", false),
            ("billing:read", false),
        ] {
            assert_eq!(scopes.permits(required), expected, "for {required}");
        }
        assert!(ScopeSet::all().permits("anything:here"));
        assert!(!ScopeSet::denied().permits("agents:read"));
        assert!(scopes.permits_all(["agents:read", "executions:read"]));
        assert!(!scopes.permits_all(["agents:read", "agents:write"]));
    }

    #[test]
    fn grammar_is_strict() {
        for good in [
            "*",
            "agents:read",
            "agents:*",
            "mcp-v2:invoke",
            "billing_users:read",
        ] {
            assert!(validate_scope(good).is_ok(), "accept {good}");
        }
        for bad in [
            "",
            "agents",
            "agents:read:extra",
            ":read",
            "agents:",
            "agents:r*ad",
            "**:",
            "agents:*read",
            "agents:read write",
            &format!("agents:{}", "x".repeat(65)),
        ] {
            assert!(validate_scope(bad).is_err(), "reject {bad:?}");
        }
    }
}
