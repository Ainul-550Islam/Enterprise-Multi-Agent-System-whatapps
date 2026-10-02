//! NATS-shaped subject names: construction, validation, wildcard matching.
//!
//! Subjects are dot-separated token streams (`mas.events.task.submitted`).
//! Publishers may only use *concrete* subjects; subscriptions may use the
//! `*` wildcard (exactly one token) and `>` (one or more trailing tokens).
//!
//! Conventions used across the platform:
//!
//! * domain events: `mas.events.<aggregate_type>.<event.type>` where the
//!   event type is the validated dotted form from
//!   [`mas_events::envelope::EventEnvelope`];
//! * dead-letter subjects are the original subject plus a `.dlq` suffix;
//! * tenant-partitioned streams embed the tenant id as a plain token.

use std::fmt;

/// A subject may never exceed 255 bytes (broker ceiling).
pub const SUBJECT_MAX_LEN: usize = 255;

/// Subject construction failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectError {
    /// Human-readable reason, safe for logs.
    pub reason: String,
}

impl SubjectError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

impl fmt::Display for SubjectError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid subject: {}", self.reason)
    }
}

impl std::error::Error for SubjectError {}

impl From<SubjectError> for mas_common::error::AppError {
    fn from(value: SubjectError) -> Self {
        mas_common::error::AppError::validation(value.to_string())
    }
}

/// True when a character is allowed inside a concrete subject token.
fn is_token_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_')
}

fn validate_common(raw: &str) -> Result<(), SubjectError> {
    if raw.is_empty() {
        return Err(SubjectError::new("subject must not be empty"));
    }
    if raw.len() > SUBJECT_MAX_LEN {
        return Err(SubjectError::new(format!(
            "subject exceeds {} bytes",
            SUBJECT_MAX_LEN
        )));
    }
    if raw.starts_with('.') || raw.ends_with('.') || raw.contains("..") {
        return Err(SubjectError::new("empty tokens are not allowed"));
    }
    Ok(())
}

/// A concrete, publishable subject (no wildcards).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Subject(String);

impl Subject {
    /// Parses and validates a concrete subject.
    pub fn parse(raw: &str) -> Result<Self, SubjectError> {
        validate_common(raw)?;
        if !raw.chars().all(|c| is_token_char(c) || c == '.') {
            return Err(SubjectError::new(
                "subjects may contain ASCII alphanumerics, '-', '_', '.' only",
            ));
        }
        Ok(Self(raw.to_owned()))
    }

    /// The subject string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Looks like a dead-letter subject.
    #[must_use]
    pub fn is_dead_letter(&self) -> bool {
        self.0.ends_with(".dlq")
    }

    /// The `.dlq` variant of this subject.
    #[must_use]
    pub fn dead_letter(&self) -> Self {
        // `+ ".dlq"` cannot exceed the max: checked explicitly.
        let combined = format!("{}.dlq", self.0);
        debug_assert!(combined.len() <= SUBJECT_MAX_LEN + 4);
        Self(combined)
    }

    /// Splits into tokens.
    #[must_use]
    pub fn tokens(&self) -> Vec<&str> {
        self.0.split('.').collect()
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A subscription filter subject with optional `*` / `>` wildcards.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectFilter(String);

impl SubjectFilter {
    /// Parses and validates a filter subject.
    ///
    /// Rules: `*` must occupy a whole token; `>` must be the *last* token and
    /// matches one or more tokens (NATS semantics).
    pub fn parse(raw: &str) -> Result<Self, SubjectError> {
        validate_common(raw)?;
        let tokens: Vec<&str> = raw.split('.').collect();
        for (index, token) in tokens.iter().enumerate() {
            if token.contains('*') && *token != "*" {
                return Err(SubjectError::new("'*' must occupy a whole token"));
            }
            if token.contains('>') {
                if *token != ">" {
                    return Err(SubjectError::new("'>' must occupy a whole token"));
                }
                if index != tokens.len() - 1 {
                    return Err(SubjectError::new("'>' must be the final token"));
                }
            }
            if *token != "*" && *token != ">" && !token.chars().all(is_token_char) {
                return Err(SubjectError::new("invalid characters in token"));
            }
        }
        Ok(Self(raw.to_owned()))
    }

    /// The filter string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// True if this filter matches the concrete subject.
    ///
    /// Same algorithm as NATS: iterate token pairs; `*` consumes exactly one
    /// token; `>` matches the remaining one-or-more tokens.
    #[must_use]
    pub fn matches(&self, subject: &Subject) -> bool {
        let filter_tokens: Vec<&str> = self.0.split('.').collect();
        let subject_tokens = subject.tokens();
        let mut i = 0;
        let mut j = 0;
        while i < filter_tokens.len() {
            match filter_tokens[i] {
                ">" => {
                    // `>` matches one or more trailing tokens.
                    return j < subject_tokens.len();
                },
                "*" => {
                    if j >= subject_tokens.len() {
                        return false;
                    }
                    i += 1;
                    j += 1;
                },
                token => {
                    if j >= subject_tokens.len() || subject_tokens[j] != token {
                        return false;
                    }
                    i += 1;
                    j += 1;
                },
            }
        }
        j == subject_tokens.len()
    }

    /// Convenience: parse + match in one call.
    pub fn matches_str(filter: &str, subject: &Subject) -> bool {
        SubjectFilter::parse(filter).is_ok_and(|f| f.matches(subject))
    }
}

impl fmt::Display for SubjectFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Convention helpers for platform subject names.
pub mod conventions {
    use super::{Subject, SubjectError};

    /// Root prefix for every domain-event stream.
    pub const EVENT_PREFIX: &str = "mas.events";

    /// Canonical domain-event subject: `mas.events.<aggregate>.<event.type>`.
    ///
    /// The event type must already be the validated dotted form; aggregate
    /// tokens are lowercase alnum/`-`.
    pub fn event_subject(aggregate_type: &str, event_type: &str) -> Result<Subject, SubjectError> {
        Subject::parse(&format!("{EVENT_PREFIX}.{aggregate_type}.{event_type}"))
    }

    /// A task-work subject: `mas.tasks.<priority>` where priority is a
    /// lowercase label such as `critical` / `normal` / `low`.
    pub fn task_subject(priority_label: &str) -> Result<Subject, SubjectError> {
        Subject::parse(&format!("mas.tasks.{priority_label}"))
    }

    /// Dead-letter subject of any concrete subject.
    #[must_use]
    pub fn dead_letter_subject(base: &Subject) -> Subject {
        base.dead_letter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concrete_subjects_validate() {
        assert!(Subject::parse("mas.events.task.submitted").is_ok());
        assert!(Subject::parse("a").is_ok());
        for bad in ["", ".a.b", "a.b.", "a..b", "a b", "a.b*c", "a.>", "a.b$d"] {
            assert!(Subject::parse(bad).is_err(), "{bad:?} must be rejected");
        }
        let too_long = "a".repeat(SUBJECT_MAX_LEN + 1);
        assert!(Subject::parse(&too_long).is_err());
    }

    #[test]
    fn wildcard_filters_match_nats_semantics() {
        let subject = Subject::parse("mas.events.task.submitted").expect("subject");
        assert!(SubjectFilter::matches_str(
            "mas.events.task.submitted",
            &subject
        ));
        assert!(SubjectFilter::matches_str(
            "mas.events.*.submitted",
            &subject
        ));
        assert!(SubjectFilter::matches_str("mas.events.>", &subject));
        assert!(SubjectFilter::matches_str(">", &subject));
        assert!(!SubjectFilter::matches_str("mas.events.*.failed", &subject));
        assert!(!SubjectFilter::matches_str("mas.events", &subject));
        assert!(
            !SubjectFilter::matches_str("mas.events.task.submitted.>", &subject),
            "'>' requires at least one more token"
        );

        // Invalid filters
        assert!(SubjectFilter::parse("a.b>*").is_err());
        assert!(SubjectFilter::parse("a.*x").is_err());
        assert!(SubjectFilter::parse("a.>.b").is_err());
    }

    #[test]
    fn conventions_and_dead_letter_subjects() {
        let event = conventions::event_subject("task", "task.submitted").expect("subject");
        assert_eq!(event.to_string(), "mas.events.task.task.submitted");
        assert!(!event.is_dead_letter());
        let dlq = conventions::dead_letter_subject(&event);
        assert!(dlq.is_dead_letter());
        assert_eq!(dlq.to_string(), "mas.events.task.task.submitted.dlq");
        assert!(conventions::task_subject("critical").is_ok());
        assert!(conventions::task_subject("CRITICAL!").is_err());
    }
}
