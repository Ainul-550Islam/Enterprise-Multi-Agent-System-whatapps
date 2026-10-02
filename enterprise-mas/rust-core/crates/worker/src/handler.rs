//! Work execution seam: data arrives as a decoded [`TaskQueueMessage`],
//! the application service context comes from the message's scope fields,
//! and concrete runtimes (execution engine, maintenance jobs, webhooks)
//! plug in per-operation.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_contracts::task::TaskQueueMessage;

/// What one handler invocation produced. `Ok(output)` settles `Ack`;
/// `Err(AppError)` is classified by the consumer via
/// `mas_execution`-compatible `classify_failure` semantics (implemented
/// generically here to keep this crate free of the execution crate).
#[async_trait]
pub trait TaskHandlerPort: std::fmt::Debug + Send + Sync {
    /// Runs the operation. Implementations MUST be written for re-drive
    /// (the broker may redeliver after a crash; upstream idempotency keys
    /// are in `message`).
    async fn execute(&self, message: &TaskQueueMessage) -> Result<serde_json::Value>;
}

/// Operation-string → handler routing with an explicit: last registration
/// wins; unknown operations are a load-time config bug and therefore poison
/// messages at runtime (never retried — they cannot ever succeed on
/// redrive).
#[derive(Debug, Default, Clone)]
pub struct OperationDispatcher {
    handlers: HashMap<String, Arc<dyn TaskHandlerPort>>,
}

impl OperationDispatcher {
    /// Empty dispatcher (no operation routable).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `handler` for `operation` (returns self for builders).
    /// Whitespace/empty operation names are programming errors and panic.
    #[must_use]
    pub fn with_handler(mut self, operation: &str, handler: Arc<dyn TaskHandlerPort>) -> Self {
        assert!(
            !operation.trim().is_empty(),
            "operation name must not be empty"
        );
        self.handlers.insert(operation.to_owned(), handler);
        self
    }

    /// The registered operations (for startup logs/assertions).
    #[must_use]
    pub fn operations(&self) -> Vec<&str> {
        let mut ops: Vec<_> = self.handlers.keys().map(String::as_str).collect();
        ops.sort_unstable();
        ops
    }

    /// Whether `operation` is routable.
    #[must_use]
    pub fn has(&self, operation: &str) -> bool {
        self.handlers.contains_key(operation)
    }
}

#[async_trait]
impl TaskHandlerPort for OperationDispatcher {
    async fn execute(&self, message: &TaskQueueMessage) -> Result<serde_json::Value> {
        let Some(handler) = self.handlers.get(&message.operation) else {
            return Err(AppError::validation(format!(
                "no handler registered for operation '{}'",
                message.operation
            )));
        };
        handler.execute(message).await
    }
}

/// A canned handler for fixtures: sequence of outcomes consumed in order,
/// then success. Test code composes precise retry/DLQ scenarios with it.
#[cfg(any(test, feature = "testutils"))]
pub mod canned {
    use super::*;
    use std::sync::Mutex;

    /// Scripted handler: fails N times with each queued [`AppError`], then
    /// answers `Ok` forever. Tracks invocation count for assertions.
    #[derive(Debug, Default)]
    pub struct CannedHandler {
        failures: Mutex<Vec<AppError>>,
        invocations: Mutex<u32>,
    }

    impl CannedHandler {
        /// Script: fail with these errors (oldest first), then succeed.
        #[must_use]
        pub fn scripted(mut failures: Vec<AppError>) -> Self {
            failures.reverse();
            Self {
                failures: Mutex::new(failures),
                invocations: Mutex::new(0),
            }
        }

        /// Appends a scripted failure (runs after already-queued ones).
        pub fn push_failure(&self, error: AppError) {
            self.failures.lock().expect("lock").insert(0, error);
        }

        /// How many times it ran.
        #[must_use]
        pub fn invocations(&self) -> u32 {
            *self.invocations.lock().expect("lock")
        }
    }

    #[async_trait]
    impl TaskHandlerPort for CannedHandler {
        async fn execute(&self, message: &TaskQueueMessage) -> Result<serde_json::Value> {
            *self.invocations.lock().expect("lock") += 1;
            let mut failures = self.failures.lock().expect("lock");
            if let Some(err) = failures.pop() {
                return Err(err);
            }
            Ok(serde_json::json!({
                "echo": message.input.clone(),
                "attempt": message.attempt_count,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dispatcher_routes_and_rejects_unknown_operations() {
        #[derive(Debug)]
        struct Probe;
        #[async_trait]
        impl TaskHandlerPort for Probe {
            async fn execute(&self, msg: &TaskQueueMessage) -> Result<serde_json::Value> {
                Ok(serde_json::json!({"got": msg.operation}))
            }
        }

        let dispatcher = OperationDispatcher::new().with_handler("execution.run", Arc::new(Probe));
        assert!(dispatcher.has("execution.run"));
        assert_eq!(dispatcher.operations(), vec!["execution.run"]);

        let msg = TaskQueueMessage {
            task_id: mas_common::ids::TaskId::new(),
            tenant_id: mas_common::ids::TenantId::new(),
            organization_id: mas_common::ids::OrganizationId::new(),
            project_id: mas_common::ids::ProjectId::new(),
            operation: "execution.run".to_owned(),
            input: serde_json::json!({}),
            execution_id: Some(mas_common::ids::ExecutionId::new()),
            agent_id: None,
            workflow_id: None,
            idempotency_key: "idem-1".to_owned(),
            priority: mas_common::enums::TaskPriority::Normal,
            attempt_count: 1,
            max_attempts: 5,
            deadline: None,
            correlation_id: "corr-disp".to_owned(),
            enqueued_at: mas_common::timestamps::Timestamp::now(),
        };
        let out = dispatcher.execute(&msg).await.expect("routed");
        assert_eq!(out["got"], "execution.run");

        let mut unknown = msg.clone();
        unknown.operation = "does.not.exist".to_owned();
        let err = dispatcher.execute(&unknown).await.expect_err("unknown op");
        assert!(
            matches!(err, AppError::Validation { .. }),
            "poison, not retry"
        );
    }
}
