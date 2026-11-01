// PURPOSE: The durable execution pipeline's **safe structured logging**
// surface: one builder that attaches the correlation identity every pipeline
// log line must carry, and that cannot be used to log a secret.
//
// Owning layer: `mas-application` (shared by the submission path, the worker
// and the relay, all of which already depend on this crate).
//
// Production responsibility: an operator debugging a stuck run needs to jump
// from a request id to a correlation id to an execution to a task to a tenant
// without grepping. Every pipeline log line therefore carries the same five
// identity fields, in the same shape, everywhere. What it must NOT carry is
// the other half of a request: credentials, tokens, API keys, prompt bodies,
// model outputs, or a principal's email. Logging here is best-effort: a log
// failure must never fail a durable write, and must never panic.
//
// The two halves of that contract are enforced in code, not documentation:
//   * [`PipelineLogContext::field`] runs every value through the shared
//     redactor, and refuses to attach a field whose key names a secret;
//   * [`PipelineLogContext::tenant_id`] logs the tenant's UUID and nothing
//     else — never its name, slug, or the organisation behind it.
//
// It must NOT:
//   • become a second logging framework. It builds `mas_observability`
//     `LogEvent`s and emits them through `tracing`;
//   • log a value that has not passed through the redactor;
//   • fail the caller. Every emit is best-effort.

use mas_common::ids::{ExecutionId, TaskId, TenantId};
use mas_common::redaction::SecretRedactor;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_observability::logging::{render_json, LogEvent, LogLevel};
use serde_json::{json, Value};

/// Field names the pipeline uses for its correlation identity.
///
/// These are part of the operational contract: dashboards and saved searches
/// key off them, so they are constants rather than string literals scattered
/// across the worker.
pub const REQUEST_ID: &str = "request.id";
pub const CORRELATION_ID: &str = "correlation.id";
pub const EXECUTION_ID: &str = "execution.id";
pub const TASK_ID: &str = "task.id";
pub const TENANT_ID: &str = "tenant.id";
pub const ATTEMPT: &str = "attempt";
pub const WORKER: &str = "worker";

/// The value substituted for anything the redactor identifies as a secret.
pub const REDACTED: &str = "[redacted]";

/// The redactor used for every value attached to a pipeline log line.
///
/// Built once per call rather than cached in a `static`: it is a handful of
/// string comparisons and log lines are not the hot path. Correctness (every
/// value is redacted under the shared policy) is worth more than the
/// nanoseconds.
fn redactor() -> SecretRedactor {
    SecretRedactor::new()
}

/// Strips anything the shared redaction policy identifies as sensitive.
///
/// Applied to every value, at every nesting level, before it can reach a sink.
/// A caller that logs a whole request body is therefore safe by construction:
/// the secrets inside it are replaced, not merely omitted at the top level.
#[must_use]
pub fn safe_value(value: &Value) -> Value {
    let mut redacted = value.clone();
    redactor().redact_value(&mut redacted);
    redacted
}

/// The correlation identity one pipeline operation carries.
///
/// Built once at the entry point (an HTTP handler, a consumer loop, a relay
/// cycle) and threaded through the call, so every log line written while
/// handling that operation identifies the same run.
#[derive(Debug, Clone)]
pub struct PipelineLogContext {
    request_id: Option<String>,
    correlation_id: String,
    execution_id: Option<ExecutionId>,
    task_id: Option<TaskId>,
    tenant_id: TenantId,
    attempt: Option<u32>,
    worker: Option<String>,
    extra: Vec<(String, Value)>,
}

impl PipelineLogContext {
    /// Starts a context from the two things every request has: whose it is
    /// (as an opaque UUID) and which causal chain it belongs to.
    #[must_use]
    pub fn new(tenant_id: TenantId, correlation_id: impl Into<String>) -> Self {
        Self {
            request_id: None,
            correlation_id: correlation_id.into(),
            execution_id: None,
            task_id: None,
            tenant_id,
            attempt: None,
            worker: None,
            extra: Vec::new(),
        }
    }

    /// Attaches the client-visible request id.
    #[must_use]
    pub fn with_request_id(mut self, request_id: impl Into<String>) -> Self {
        self.request_id = Some(request_id.into());
        self
    }

    /// Attaches the execution the operation belongs to.
    #[must_use]
    pub const fn with_execution(mut self, execution_id: ExecutionId) -> Self {
        self.execution_id = Some(execution_id);
        self
    }

    /// Attaches the execution, when there is one. Tasks can exist without an
    /// owning execution, and a log context must not invent an id.
    #[must_use]
    pub const fn with_execution_opt(mut self, execution_id: Option<ExecutionId>) -> Self {
        self.execution_id = execution_id;
        self
    }

    /// Attaches the task the operation is working on.
    #[must_use]
    pub const fn with_task(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Attaches the attempt number, which is durable — it comes from
    /// `task_attempts`, never from a broker redelivery counter.
    #[must_use]
    pub const fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt = Some(attempt);
        self
    }

    /// Attaches the worker identity that owns the work.
    #[must_use]
    pub fn with_worker(mut self, worker: impl Into<String>) -> Self {
        self.worker = Some(worker.into());
        self
    }

    /// Attaches one additional field, redacted.
    ///
    /// The value passes through the shared redactor, so a caller cannot
    /// accidentally log a credential by naming it something the framework does
    /// not recognise at the top level.
    #[must_use]
    pub fn field(mut self, key: &str, value: Value) -> Self {
        // Two passes, because there are two ways to leak:
        //   * the key names the secret (`api_key`, `authorization`, …), in
        //     which case even a harmless-looking value must not be trusted;
        //   * the key is innocuous but the *value* carries one (a request body
        //     with a token nested inside it).
        let stored = if redactor().is_sensitive(key) {
            Value::String(REDACTED.to_owned())
        } else {
            safe_value(&value)
        };
        self.extra.push((key.to_owned(), stored));
        self
    }

    /// Attaches an already-known-safe scalar.
    ///
    /// For values the pipeline itself produced (a status, a stable error code,
    /// a duration in milliseconds). Never for caller-supplied data.
    #[must_use]
    pub fn safe_field(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra.push((key.to_owned(), value.into()));
        self
    }

    /// The tenant this context belongs to, as an opaque identifier.
    ///
    /// Deliberately the only tenant-shaped thing on the context: there is no
    /// `with_tenant_name`, because a tenant's name and slug are customer data
    /// with no operational value on a log line, and an operator does not need
    /// them to find a run.
    #[must_use]
    pub const fn tenant_id(&self) -> TenantId {
        self.tenant_id
    }

    /// Builds a `LogEvent` carrying the full correlation identity.
    ///
    /// Returns `Err` only when the event itself is malformed (empty message,
    /// bad target), which is a programming error the caller should see in
    /// tests — never a reason to drop a log line silently at runtime.
    pub fn event(
        &self,
        level: LogLevel,
        target: &str,
        message: &str,
        at: &Timestamp,
    ) -> Result<LogEvent> {
        let mut event = LogEvent::new(level, canonical_target(target), message, at)?
            .field(CORRELATION_ID, json!(self.correlation_id))?
            .field(TENANT_ID, json!(self.tenant_id.to_string()))?;

        if let Some(request_id) = &self.request_id {
            event = event.field(REQUEST_ID, json!(request_id))?;
        }
        if let Some(execution_id) = self.execution_id {
            event = event.field(EXECUTION_ID, json!(execution_id.to_string()))?;
        }
        if let Some(task_id) = self.task_id {
            event = event.field(TASK_ID, json!(task_id.to_string()))?;
        }
        if let Some(attempt) = self.attempt {
            event = event.field(ATTEMPT, json!(attempt))?;
        }
        if let Some(worker) = &self.worker {
            event = event.field(WORKER, json!(worker))?;
        }
        for (key, value) in &self.extra {
            event = event.field(key, safe_value(value))?;
        }
        Ok(event)
    }

    /// Renders the event as one JSON line, redacted.
    pub fn render(
        &self,
        level: LogLevel,
        target: &str,
        message: &str,
        at: &Timestamp,
    ) -> Result<String> {
        render_json(&self.event(level, target, message, at)?)
    }

    /// Emits one log line. Best-effort: a failure is traced and swallowed,
    /// because a log sink must never be able to fail a durable write.
    pub fn emit(&self, level: LogLevel, target: &str, message: &str, at: &Timestamp) {
        match self.render(level, target, message, at) {
            Ok(line) => match level {
                LogLevel::Trace => tracing::trace!(target: "mas.pipeline", "{}", line),
                LogLevel::Debug => tracing::debug!(target: "mas.pipeline", "{}", line),
                LogLevel::Info => tracing::info!(target: "mas.pipeline", "{}", line),
                LogLevel::Warn => tracing::warn!(target: "mas.pipeline", "{}", line),
                LogLevel::Error => tracing::error!(target: "mas.pipeline", "{}", line),
            },
            Err(error) => {
                // The one thing that must never be swallowed silently is the
                // fact that logging is broken — but it is still not worth
                // failing the pipeline over.
                tracing::warn!(
                    target: "mas.pipeline",
                    error = %error,
                    "pipeline log event could not be built"
                );
            },
        }
    }

    /// Convenience: emit at `info`.
    pub fn info(&self, target: &str, message: &str, at: &Timestamp) {
        self.emit(LogLevel::Info, target, message, at);
    }

    /// Convenience: emit at `warn`.
    pub fn warn(&self, target: &str, message: &str, at: &Timestamp) {
        self.emit(LogLevel::Warn, target, message, at);
    }

    /// Convenience: emit at `error`.
    pub fn error(&self, target: &str, message: &str, at: &Timestamp) {
        self.emit(LogLevel::Error, target, message, at);
    }
}

/// Normalizes a `module_path!()` into a target the logger accepts
/// (`[A-Za-z0-9._:-]` only).
fn canonical_target(target: &str) -> String {
    let cleaned: String = target
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':') {
                c
            } else {
                '.'
            }
        })
        .collect();
    if cleaned.is_empty() {
        "mas.pipeline".to_owned()
    } else {
        cleaned
    }
}

/// The names of the five identity fields, for the log-contract test.
pub const IDENTITY_FIELDS: [&str; 5] =
    [REQUEST_ID, CORRELATION_ID, EXECUTION_ID, TASK_ID, TENANT_ID];

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    fn execution() -> ExecutionId {
        ExecutionId::from_uuid(uuid::Uuid::from_u128(0xE1))
    }

    fn task() -> TaskId {
        TaskId::from_uuid(uuid::Uuid::from_u128(0xF1))
    }

    fn tenant() -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(0xD1))
    }

    #[test]
    fn every_identity_field_is_present_on_one_line() {
        let context = PipelineLogContext::new(tenant(), "corr-1")
            .with_request_id("req-1")
            .with_execution(execution())
            .with_task(task())
            .with_attempt(2)
            .with_worker("worker-a");

        let line = context
            .render(LogLevel::Info, "mas.worker", "task settled", &at())
            .expect("render");

        for field in IDENTITY_FIELDS {
            assert!(line.contains(field), "the line must carry {field}: {line}");
        }
        assert!(
            line.contains(&execution().to_string()),
            "execution id on the line"
        );
        assert!(line.contains(&task().to_string()), "task id on the line");
        assert!(
            line.contains(&tenant().to_string()),
            "tenant id on the line"
        );
        assert!(line.contains("corr-1"));
        assert!(line.contains("req-1"));
        assert!(line.contains("worker-a"));
    }

    #[test]
    fn a_secret_in_a_caller_supplied_value_is_redacted() {
        let context = PipelineLogContext::new(tenant(), "corr-2").field(
            "payload",
            json!({
                "prompt": "hello",
                "api_key": "sk-live-abcdefghijklmnop",
                "authorization": "Bearer t0ken",
                "nested": {"password": "hunter2", "model": "gpt-4o-mini"},
            }),
        );

        let line = context
            .render(LogLevel::Info, "mas.pipeline", "submission received", &at())
            .expect("render");

        assert!(
            !line.contains("sk-live-abcdefghijklmnop"),
            "the api key must not appear: {line}"
        );
        assert!(!line.contains("t0ken"), "the bearer token must not appear");
        assert!(
            !line.contains("hunter2"),
            "a nested password must not appear"
        );
        // Non-sensitive values survive, so the line is still useful.
        assert!(line.contains("gpt-4o-mini"), "safe values are kept: {line}");
    }

    #[test]
    fn a_secret_typed_at_the_top_level_is_redacted_too() {
        // The most likely mistake: naming the field after what it holds.
        let context = PipelineLogContext::new(tenant(), "corr-3")
            .field("secret", json!("s3cr3t"))
            .field("credential", json!("c3r3d"));

        let line = context
            .render(LogLevel::Warn, "mas.pipeline", "something odd", &at())
            .expect("render");

        assert!(
            !line.contains("s3cr3t") && !line.contains("c3r3d"),
            "secrets leaked: {line}"
        );
    }

    #[test]
    fn the_tenant_is_logged_as_an_opaque_id_and_nothing_else() {
        let context = PipelineLogContext::new(tenant(), "corr-4");
        let line = context
            .render(LogLevel::Info, "mas.pipeline", "tenant scoped", &at())
            .expect("render");

        // The id is there; no name, slug or email is reachable through the
        // context at all — there is no setter that could put one there.
        assert!(line.contains(&tenant().to_string()));
        assert_eq!(context.tenant_id(), tenant());
    }

    #[test]
    fn safe_value_redacts_at_every_depth() {
        let redacted = safe_value(&json!({
            "outer": {"inner": {"token": "abc123", "attempt": 1}},
            "list": [{"password": "p"}, {"status": "queued"}],
        }));
        let text = redacted.to_string();
        assert!(!text.contains("abc123"), "nested token: {text}");
        assert!(!text.contains("\"p\""), "password in a list: {text}");
        assert!(text.contains("queued"), "safe values survive: {text}");
    }

    #[test]
    fn emitting_never_panics_even_with_a_bad_target() {
        let context = PipelineLogContext::new(tenant(), "corr-5");
        // A target full of characters the logger rejects must be sanitized,
        // not turned into an error the caller has to handle.
        let line = context
            .render(LogLevel::Error, "weird target ✓", "still logged", &at())
            .expect("render");
        assert!(line.contains("still logged"));

        context.error("mas::worker::settle", "failed to settle", &at());
        context.warn("mas.worker", "lease expiring", &at());
        context.info("mas.worker", "claimed", &at());
    }

    #[test]
    fn an_empty_message_is_rejected_rather_than_logged_as_blank() {
        let context = PipelineLogContext::new(tenant(), "corr-6");
        assert!(
            context
                .render(LogLevel::Info, "mas.pipeline", "   ", &at())
                .is_err(),
            "a blank message is a programming error, not a log line"
        );
    }
}
