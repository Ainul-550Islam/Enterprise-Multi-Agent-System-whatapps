//! Broker health probes.
//!
//! Produces [`DependencyHealth`] entries for readiness endpoints. Messages are
//! deliberately sanitized operators' hints ("broker is currently
//! unavailable") — never connection URLs, hostnames, or credentials.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use mas_contracts::health::DependencyHealth;

use crate::broker::BrokerPort;

/// The logical dependency name used in readiness responses.
pub const BROKER_DEPENDENCY_NAME: &str = "nats";

/// Wraps any [`BrokerPort`] and reports its liveness as dependency health.
pub struct BrokerHealthProbe {
    broker: Arc<dyn BrokerPort>,
    name: String,
}

impl fmt::Debug for BrokerHealthProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerHealthProbe")
            .field("name", &self.name)
            .finish()
    }
}

impl BrokerHealthProbe {
    /// Probe over a shared broker, named `nats` by default.
    #[must_use]
    pub fn new(broker: Arc<dyn BrokerPort>) -> Self {
        Self {
            broker,
            name: BROKER_DEPENDENCY_NAME.to_owned(),
        }
    }

    /// Overrides the dependency label (e.g. `rabbitmq`, `kafka`).
    #[must_use]
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Runs the probe: cheap `ping`, measuring round-trip latency.
    pub async fn check(&self) -> DependencyHealth {
        let start = Instant::now();
        match self.broker.ping().await {
            Ok(()) => {
                let latency_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                DependencyHealth::healthy(self.name.clone(), Some(latency_ms))
            },
            Err(err) => DependencyHealth::unhealthy(self.name.clone(), sanitized_reason(&err)),
        }
    }
}

/// Strips anything beyond the public error class — the underlying error may
/// embed cluster addresses the readiness endpoint must not leak.
fn sanitized_reason(error: &mas_common::error::AppError) -> String {
    match error.error_code() {
        "EXTERNAL_SERVICE_ERROR" => "broker is currently unavailable".to_owned(),
        code => format!("unexpected broker probe failure ({code})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::InMemoryBroker;

    #[tokio::test]
    async fn healthy_when_broker_is_available() {
        let broker = Arc::new(InMemoryBroker::new());
        broker.set_available(true);
        let probe = BrokerHealthProbe::new(broker);
        let health = probe.check().await;
        assert!(health.healthy);
        assert_eq!(health.name, "nats");
        assert!(health.latency_ms.is_some());
        assert!(health.message.is_none());
    }

    #[tokio::test]
    async fn unhealthy_message_is_sanitized() {
        let broker = Arc::new(InMemoryBroker::new()); // starts unavailable
        let probe = BrokerHealthProbe::new(broker).named("rabbitmq");
        let health = probe.check().await;
        assert!(!health.healthy);
        assert_eq!(health.name, "rabbitmq");
        assert_eq!(
            health.message.as_deref(),
            Some("broker is currently unavailable")
        );
        assert!(health.latency_ms.is_none());
    }
}
