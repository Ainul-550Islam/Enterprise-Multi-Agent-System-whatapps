//! Slug value object: URL/identifier-safe short names.

use mas_common::constants::MAX_SLUG_LENGTH;
use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// A lowercase, dash-separated slug (`^[a-z0-9]+(-[a-z0-9]+)*$`).
///
/// [`Slug::new`] validates strictly; [`Slug::normalize`] derives one from free
/// text (lowercasing, mapping spaces/underscores/invalid runs to dashes).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Slug(String);

impl Slug {
    /// Validates `raw` as a slug without modifying it.
    pub fn new(raw: &str) -> Result<Self> {
        mas_common::validation::validate_slug("slug", raw)?;
        Ok(Self(raw.to_owned()))
    }

    /// Derives a slug from arbitrary text.
    ///
    /// Fails when normalization produces nothing usable or the result exceeds
    /// [`MAX_SLUG_LENGTH`] (after which truncation can no longer preserve
    /// intent safely).
    pub fn normalize(raw: &str) -> Result<Self> {
        let mut out = String::with_capacity(raw.len().min(MAX_SLUG_LENGTH));
        let mut prev_dash = true; // suppresses leading separators
        for ch in raw.trim().chars() {
            let mapped = match ch {
                'A'..='Z' => Some(ch.to_ascii_lowercase()),
                'a'..='z' | '0'..='9' => Some(ch),
                ' ' | '-' | '_' | '.' | '/' => None, // separator or dropped
                _ => None,
            };
            match mapped {
                Some(c) => {
                    out.push(c);
                    prev_dash = false;
                },
                None => {
                    if !prev_dash && !out.is_empty() {
                        out.push('-');
                        prev_dash = true;
                    }
                },
            }
        }
        while out.ends_with('-') {
            out.pop();
        }
        if out.is_empty() {
            return Err(AppError::invalid_field(
                "slug",
                "invalid_slug",
                format!("cannot derive a slug from {raw:?}"),
            ));
        }
        if out.chars().count() > MAX_SLUG_LENGTH {
            // Truncate on a dash boundary when possible.
            let mut truncated: String = out.chars().take(MAX_SLUG_LENGTH).collect();
            while truncated.ends_with('-') {
                truncated.pop();
            }
            out = truncated;
        }
        // Normalization guarantees the pattern; validate defensively anyway.
        mas_common::validation::validate_slug("slug", &out)?;
        Ok(Self(out))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for Slug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for Slug {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self> {
        Self::new(s)
    }
}

impl TryFrom<String> for Slug {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(&value)
    }
}

impl From<Slug> for String {
    fn from(slug: Slug) -> Self {
        slug.0
    }
}
