//! Semantic version wrapper for agent/workflow versions.

use mas_common::error::AppError;
use mas_common::result::Result;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Strict semantic version (`MAJOR.MINOR.PATCH` + optional pre-release/build)
/// used for agent and workflow versions.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SemanticVersion(Version);

impl SemanticVersion {
    /// Parses `MAJOR.MINOR.PATCH[-prerelease][+build]`.
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        Version::parse(trimmed).map(Self).map_err(|err| {
            AppError::invalid_field(
                "version",
                "invalid_semver",
                format!("invalid semantic version: {err}"),
            )
        })
    }

    #[must_use]
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self(Version::new(major, minor, patch))
    }

    #[must_use]
    pub fn major(&self) -> u64 {
        self.0.major
    }

    #[must_use]
    pub fn minor(&self) -> u64 {
        self.0.minor
    }

    #[must_use]
    pub fn patch(&self) -> u64 {
        self.0.patch
    }

    #[must_use]
    pub fn is_prerelease(&self) -> bool {
        !self.0.pre.is_empty()
    }

    /// Caret-compatibility check: `other` may satisfy requirements of `self`.
    /// Pre-1.0 versions are only compatible when minor also matches.
    #[must_use]
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        if self.0.major != other.0.major {
            return false;
        }
        if self.0.major == 0 && self.0.minor != other.0.minor {
            return false;
        }
        !self.is_prerelease() && !other.is_prerelease()
    }

    #[must_use]
    pub fn next_major(&self) -> Self {
        Self(Version::new(self.0.major + 1, 0, 0))
    }

    #[must_use]
    pub fn next_minor(&self) -> Self {
        Self(Version::new(self.0.major, self.0.minor + 1, 0))
    }

    #[must_use]
    pub fn next_patch(&self) -> Self {
        Self(Version::new(self.0.major, self.0.minor, self.0.patch + 1))
    }

    #[must_use]
    pub fn as_version(&self) -> &Version {
        &self.0
    }
}

impl fmt::Display for SemanticVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for SemanticVersion {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

impl TryFrom<String> for SemanticVersion {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self> {
        Self::parse(&value)
    }
}

impl From<SemanticVersion> for String {
    fn from(version: SemanticVersion) -> Self {
        version.0.to_string()
    }
}

impl From<Version> for SemanticVersion {
    fn from(version: Version) -> Self {
        Self(version)
    }
}
