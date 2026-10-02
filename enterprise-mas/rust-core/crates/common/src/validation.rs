//! Reusable validation helpers shared by DTOs and domain aggregates.
//!
//! Single-field helpers fail fast with [`AppError::Validation`];
//! [`ValidationBuilder`] accumulates issues when a whole payload should be
//! reported at once.

use crate::constants::{MAX_NAME_LENGTH, MAX_SLUG_LENGTH};
use crate::error::{AppError, ValidationIssue};
use crate::result::Result;
use url::Url;

/// Value must contain at least one non-whitespace character.
pub fn validate_non_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(AppError::invalid_field(
            field,
            "required",
            "value must not be empty",
        ));
    }
    Ok(())
}

/// Unicode-scalar length must lie within `[min, max]`.
pub fn validate_length(field: &'static str, value: &str, min: usize, max: usize) -> Result<()> {
    let len = value.chars().count();
    if len < min {
        return Err(AppError::invalid_field(
            field,
            "too_short",
            format!("length {len} is below minimum {min}"),
        ));
    }
    if len > max {
        return Err(AppError::invalid_field(
            field,
            "too_long",
            format!("length {len} exceeds maximum {max}"),
        ));
    }
    Ok(())
}

/// Validates a slug: `^[a-z0-9]+(-[a-z0-9]+)*$`, max [`MAX_SLUG_LENGTH`].
pub fn validate_slug(field: &'static str, value: &str) -> Result<()> {
    let len = value.chars().count();
    if len == 0 || len > MAX_SLUG_LENGTH {
        return Err(AppError::invalid_field(
            field,
            "invalid_length",
            format!("slug must be 1..={MAX_SLUG_LENGTH} characters"),
        ));
    }
    let mut prev_dash = true; // reject leading dash implicitly
    for ch in value.chars() {
        match ch {
            'a'..='z' | '0'..='9' => prev_dash = false,
            '-' if !prev_dash => prev_dash = true,
            '-' => {
                return Err(AppError::invalid_field(
                    field,
                    "invalid_slug",
                    "slug must not contain consecutive or leading dashes",
                ));
            },
            _ => {
                return Err(AppError::invalid_field(
                    field,
                    "invalid_slug",
                    "slug may only contain lowercase letters, digits and single dashes",
                ));
            },
        }
    }
    if prev_dash {
        return Err(AppError::invalid_field(
            field,
            "invalid_slug",
            "slug must not end with a dash",
        ));
    }
    Ok(())
}

/// Validates an absolute http(s) URL without user-info, fragments or
/// credentials. Does *not* perform SSRF checks (those live in
/// `integrations::http` / the `SafeUrl` domain value object).
pub fn validate_url(field: &'static str, value: &str) -> Result<()> {
    let url = Url::parse(value).map_err(|err| {
        AppError::invalid_field(field, "invalid_url", format!("not a valid URL: {err}"))
    })?;
    match url.scheme() {
        "http" | "https" => {},
        scheme => {
            return Err(AppError::invalid_field(
                field,
                "invalid_scheme",
                format!("scheme '{scheme}' is not allowed (http/https only)"),
            ));
        },
    }
    if url.host_str().is_none() {
        return Err(AppError::invalid_field(
            field,
            "invalid_url",
            "URL must contain a host",
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(AppError::invalid_field(
            field,
            "invalid_url",
            "URL must not contain credentials",
        ));
    }
    Ok(())
}

/// Validates and parses a UUID string.
pub fn validate_uuid(field: &'static str, value: &str) -> Result<uuid::Uuid> {
    uuid::Uuid::try_parse(value.trim()).map_err(|_| {
        AppError::invalid_field(field, "invalid_format", "expected a canonical UUID string")
    })
}

/// Validates a display resource name (non-empty, bounded, no control chars).
pub fn validate_resource_name(field: &'static str, value: &str) -> Result<()> {
    validate_non_empty(field, value)?;
    validate_length(field, value, 1, MAX_NAME_LENGTH)?;
    if value.chars().any(char::is_control) {
        return Err(AppError::invalid_field(
            field,
            "invalid_characters",
            "name must not contain control characters",
        ));
    }
    if value.trim() != value {
        return Err(AppError::invalid_field(
            field,
            "invalid_characters",
            "name must not have leading or trailing whitespace",
        ));
    }
    Ok(())
}

/// Accumulates [`ValidationIssue`]s and fails once with all of them.
#[derive(Debug, Default)]
pub struct ValidationBuilder {
    issues: Vec<ValidationIssue>,
}

impl ValidationBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an issue.
    pub fn add(&mut self, field: &str, code: &str, message: impl Into<String>) -> &mut Self {
        self.issues.push(ValidationIssue::new(field, code, message));
        self
    }

    /// Records all issues carried by a failed single-field validation.
    /// Non-validation errors are re-thrown conceptually by being recorded as
    /// `internal` issues — they indicate bugs, but must not panic validation.
    pub fn add_result(&mut self, result: &Result<()>) -> &mut Self {
        if let Err(err) = result {
            match err {
                AppError::Validation { issues, message } if !issues.is_empty() => {
                    self.issues.extend(issues.iter().cloned());
                    if issues.is_empty() {
                        self.issues
                            .push(ValidationIssue::new("_", "invalid", message.clone()));
                    }
                },
                other => self
                    .issues
                    .push(ValidationIssue::new("_", "error", other.to_string())),
            }
        }
        self
    }

    /// Runs `f` and records any produced error.
    pub fn check(&mut self, f: impl FnOnce() -> Result<()>) -> &mut Self {
        let result = f();
        self.add_result(&result)
    }

    #[must_use]
    pub fn has_issues(&self) -> bool {
        !self.issues.is_empty()
    }

    #[must_use]
    pub fn issues(&self) -> &[ValidationIssue] {
        &self.issues
    }

    /// `Ok(())` when no issues were collected, otherwise one
    /// [`AppError::Validation`] carrying every issue.
    pub fn finish(self) -> Result<()> {
        if self.issues.is_empty() {
            Ok(())
        } else {
            Err(AppError::Validation {
                message: format!("validation failed with {} issue(s)", self.issues.len()),
                issues: self.issues,
            })
        }
    }
}
