//! Structured JSON logging with mandatory redaction.
//!
//! The design hard-codes the platform rule *values must be redacted before
//! they touch the wire*: [`JsonLogger::log`] runs every event field through
//! the shared `SecretRedactor` after merging static enrichment fields.
//! AppError enrichment serializes only the stable public surface
//! (`error.code`, public message) — internal details must be passed
//! field-by-field by callers who own them.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::redaction::SecretRedactor;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde_json::{Map, Value};
use std::sync::Mutex;

string_enum! {
    /// Log severity levels, in ascending order.
    LogLevel {
        Trace => "trace",
        Debug => "debug",
        Info => "info",
        Warn => "warn",
        Error => "error",
    }
}

impl LogLevel {
    /// Ascending severity rank used for min-level filtering.
    #[must_use]
    pub const fn rank(&self) -> u8 {
        match self {
            Self::Trace => 0,
            Self::Debug => 1,
            Self::Info => 2,
            Self::Warn => 3,
            Self::Error => 4,
        }
    }
}

/// Upper bound for a rendered log message.
pub const MAX_MESSAGE_CHARS: usize = 2048;
/// Upper bound for field keys.
pub const MAX_FIELD_KEY_CHARS: usize = 128;
/// Upper bound for the number of fields on one event.
pub const MAX_FIELDS: usize = 64;

/// One structured log event.
#[derive(Debug, Clone)]
pub struct LogEvent {
    pub timestamp: Timestamp,
    pub level: LogLevel,
    /// Logger target (typically `module_path!()`); dots/underscores/dashes.
    pub target: String,
    pub message: String,
    pub fields: Map<String, Value>,
}

impl LogEvent {
    /// Creates an event; the message is capped and control-flattened.
    pub fn new(
        level: LogLevel,
        target: impl Into<String>,
        message: &str,
        at: &Timestamp,
    ) -> Result<Self> {
        let target = target.into();
        mas_common::validation::validate_length("target", &target, 1, 128)?;
        if !target
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'))
        {
            return Err(AppError::invalid_field(
                "target",
                "invalid_format",
                "targets use [A-Za-z0-9._:-] only",
            ));
        }
        let mut message: String = message
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        message.truncate(MAX_MESSAGE_CHARS);
        if message.trim().is_empty() {
            return Err(AppError::invalid_field(
                "message",
                "required",
                "log messages must be non-empty",
            ));
        }
        Ok(Self {
            timestamp: *at,
            level,
            target,
            message,
            fields: Map::new(),
        })
    }

    /// Builder: attach one field. Keys are validated
    /// (`[a-z0-9._-]`, ≤ 128 chars); values are redacted by the logger,
    /// not here (so callers may build events without a redactor handy).
    pub fn field(mut self, key: &str, value: Value) -> Result<Self> {
        validate_field_key(key)?;
        if self.fields.len() >= MAX_FIELDS {
            return Err(AppError::invalid_field(
                "fields",
                "too_many",
                format!("log events carry at most {MAX_FIELDS} fields"),
            ));
        }
        self.fields.insert(key.to_owned(), value);
        Ok(self)
    }

    /// Builder: attach an AppError's public surface
    /// (`error.code`, `error.message`).
    pub fn with_error(self, err: &AppError) -> Result<Self> {
        self.field("error.code", Value::String(err.error_code().to_owned()))?
            .field("error.message", Value::String(err.public_message()))
    }
}

fn validate_field_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.len() > MAX_FIELD_KEY_CHARS
        || !key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'))
    {
        return Err(AppError::invalid_field(
            "fields",
            "invalid_key",
            format!("invalid log field key {key:?}"),
        ));
    }
    Ok(())
}

/// Renders an event as a single-line JSON object:
/// `{"ts":…,"level":…,"target":…,"message":…,"<fields…>"}`.
pub fn render_json(event: &LogEvent) -> Result<String> {
    let mut root = Map::with_capacity(event.fields.len() + 4);
    root.insert(
        "ts".to_owned(),
        Value::String(event.timestamp.to_rfc3339_millis()),
    );
    root.insert(
        "level".to_owned(),
        Value::String(event.level.as_str().to_owned()),
    );
    root.insert("target".to_owned(), Value::String(event.target.clone()));
    root.insert("message".to_owned(), Value::String(event.message.clone()));
    for (key, value) in &event.fields {
        root.insert(key.clone(), value.clone());
    }
    serde_json::to_string(&Value::Object(root))
        .map_err(|err| AppError::serialization(format!("log event not serializable: {err}")))
}

/// Drain for rendered log lines.
#[async_trait]
pub trait LogSinkPort: std::fmt::Debug + Send + Sync {
    /// Emits one fully-rendered JSON line.
    async fn emit(&self, line: &str, event: &LogEvent) -> Result<()>;
}

/// The JSON logger: level filtering, static enrichment, mandatory
/// redaction, single-line rendering, sink emission.
#[derive(Debug)]
pub struct JsonLogger<S: LogSinkPort> {
    sink: S,
    redactor: SecretRedactor,
    min_level: LogLevel,
    static_fields: Map<String, Value>,
}

impl<S: LogSinkPort> JsonLogger<S> {
    /// Creates a logger emitting at `min_level` and above.
    #[must_use]
    pub fn new(sink: S, min_level: LogLevel) -> Self {
        Self {
            sink,
            redactor: SecretRedactor::new(),
            min_level,
            static_fields: Map::new(),
        }
    }

    /// Builder: static enrichment (service name, version, environment, …).
    /// Event fields win over same-named static fields.
    pub fn with_static_field(mut self, key: &str, value: Value) -> Result<Self> {
        validate_field_key(key)?;
        self.static_fields.insert(key.to_owned(), value);
        Ok(self)
    }

    /// The active minimum level.
    #[must_use]
    pub const fn min_level(&self) -> &LogLevel {
        &self.min_level
    }

    /// The bound sink (introspection/testing).
    #[must_use]
    pub const fn sink(&self) -> &S {
        &self.sink
    }

    /// Whether `level` would be emitted.
    #[must_use]
    pub fn enabled(&self, level: &LogLevel) -> bool {
        level.rank() >= self.min_level.rank()
    }

    /// Logs `event` if enabled: merges static fields (event wins),
    /// redacts every value, renders a single JSON line, emits it.
    pub async fn log(&self, event: &LogEvent) -> Result<bool> {
        if !self.enabled(&event.level) {
            return Ok(false);
        }
        let mut merged = event.clone();
        let mut fields = Map::with_capacity(event.fields.len() + self.static_fields.len());
        // Redaction is KEY-AWARE like the shared policy: sensitive keys are
        // replaced wholesale; other values are scanned recursively (so
        // email-shaped strings etc. are still neutralized).
        let redact = |key: &str, value: &Value| -> Value {
            if self.redactor.is_sensitive(key) {
                Value::String(mas_common::redaction::REDACTED.to_owned())
            } else {
                let mut value = value.clone();
                self.redactor.redact_value(&mut value);
                value
            }
        };
        for (key, value) in &self.static_fields {
            fields.insert(key.clone(), redact(key, value));
        }
        for (key, value) in &event.fields {
            fields.insert(key.clone(), redact(key, value));
        }
        merged.fields = fields;
        let line = render_json(&merged)?;
        self.sink.emit(&line, &merged).await?;
        Ok(true)
    }
}

/// In-memory sink for tests and deferred bootstrapping.
#[derive(Debug, Default)]
pub struct InMemoryLogSink {
    lines: Mutex<Vec<String>>,
}

impl InMemoryLogSink {
    /// Empty sink.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// All captured lines, in emission order.
    #[must_use]
    pub fn lines(&self) -> Vec<String> {
        self.lines.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Number of captured lines.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    /// Whether nothing has been captured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears the capture buffer.
    pub fn clear(&self) {
        self.lines.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

#[async_trait]
impl LogSinkPort for InMemoryLogSink {
    async fn emit(&self, line: &str, _event: &LogEvent) -> Result<()> {
        self.lines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(line.to_owned());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    #[tokio::test]
    async fn levels_filter_and_static_fields_enrich() {
        let logger = JsonLogger::new(InMemoryLogSink::new(), LogLevel::Info)
            .with_static_field("service", Value::from("mas-worker"))
            .expect("static field");

        let debug = LogEvent::new(LogLevel::Debug, "mas.worker", "hidden", &at()).expect("event");
        assert!(
            !logger.log(&debug).await.expect("log"),
            "debug below min level"
        );
        assert!(logger.sink().is_empty(), "nothing emitted below min level");

        let info = LogEvent::new(LogLevel::Info, "mas.worker", "run started", &at())
            .expect("event")
            .field("task.id", Value::from("t-1"))
            .expect("field");
        assert!(logger.log(&info).await.expect("log"));

        let lines = logger.sink().lines();
        assert_eq!(lines.len(), 1);
        let parsed: Value = serde_json::from_str(&lines[0]).expect("valid json");
        assert_eq!(parsed["level"], Value::from("info"));
        assert_eq!(parsed["service"], Value::from("mas-worker"));
        assert_eq!(parsed["task.id"], Value::from("t-1"));
        assert_eq!(parsed["message"], Value::from("run started"));
        assert!(!lines[0].contains('\n'), "strictly single-line");

        // Event fields win over static fields.
        let winner = LogEvent::new(LogLevel::Info, "mas.worker", "override", &at())
            .expect("event")
            .field("service", Value::from("event-owned"))
            .expect("field");
        logger.log(&winner).await.expect("log");
        let parsed: Value = serde_json::from_str(&logger.sink().lines()[1]).expect("json");
        assert_eq!(parsed["service"], Value::from("event-owned"));
    }

    #[tokio::test]
    async fn redaction_scrubs_secrets_and_emails_from_fields() {
        let logger = JsonLogger::new(InMemoryLogSink::new(), LogLevel::Trace);
        let event = LogEvent::new(LogLevel::Info, "mas.security", "credential resolved", &at())
            .expect("event")
            .field("password", Value::from("hunter2"))
            .expect("field")
            .field("api_key", Value::from("sk-live-abcdef123456"))
            .expect("field")
            .field("actor", Value::from("jane.doe@acme.example"))
            .expect("field");
        logger.log(&event).await.expect("log");

        let line = &logger.sink().lines()[0];
        assert!(!line.contains("hunter2"), "password never reaches the wire");
        assert!(
            !line.contains("sk-live-abcdef123456"),
            "api keys never reach the wire"
        );
        assert!(
            !line.contains("jane.doe@acme.example"),
            "email values are redacted"
        );
        assert!(
            line.contains("REDACTED") || line.contains('@'),
            "redaction marker present: {line}"
        );
    }

    #[tokio::test]
    async fn error_enrichment_exposes_only_public_surface() {
        let logger = JsonLogger::new(InMemoryLogSink::new(), LogLevel::Info);
        let err = AppError::database(
            "sqlstate 23505 duplicate key value violates unique constraint uq_tenants_slug",
        )
        .with_context("insert into tenants (slug)");
        let event = LogEvent::new(
            LogLevel::Error,
            "mas.persistence",
            "tenant create failed",
            &at(),
        )
        .expect("event")
        .with_error(&err)
        .expect("with_error");
        // Also route the event body through the logger's redactor pipeline.
        logger.log(&event).await.expect("log");
        let parsed: Value = serde_json::from_str(&logger.sink().lines()[0]).expect("json");
        assert_eq!(parsed["error.code"], Value::from(err.error_code()));
        // public_message must already be the operator-safe surface; internal
        // sqlstate noise must not be echoed verbatim.
        let message = parsed["error.message"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        assert!(!message.is_empty());
        assert!(
            !message.contains("sqlstate"),
            "public message must not leak driver internals: {message}"
        );
    }

    #[test]
    fn log_event_validates_shape() {
        assert!(LogEvent::new(LogLevel::Info, "bad target space", "x", &at()).is_err());
        assert!(LogEvent::new(LogLevel::Info, "mas.worker", "   ", &at()).is_err());
        let flattened =
            LogEvent::new(LogLevel::Info, "mas.worker", "line1\nline2\tend", &at()).expect("event");
        assert_eq!(flattened.message, "line1 line2 end");
        let event = LogEvent::new(LogLevel::Info, "mas.worker", "x", &at()).expect("event");
        assert!(event.clone().field("UPPER", Value::Null).is_err());
        assert!(event.field("", Value::Null).is_err());
    }
}
