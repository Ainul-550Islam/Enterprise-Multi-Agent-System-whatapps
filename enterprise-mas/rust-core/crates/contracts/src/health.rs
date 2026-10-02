//! Health and readiness DTOs. Must never leak secrets/internal topology
//! beyond what an operator needs.

use mas_common::timestamps::Timestamp;
use serde::{Deserialize, Serialize};

/// Liveness: process is up and its event loop works.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivenessResponse {
    /// Always `"ok"` when the process can answer.
    pub status: String,
    pub service: String,
    pub version: String,
    pub timestamp: Timestamp,
}

impl LivenessResponse {
    #[must_use]
    pub fn ok(service: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            status: "ok".to_owned(),
            service: service.into(),
            version: version.into(),
            timestamp: Timestamp::now(),
        }
    }
}

/// One dependency's health.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyHealth {
    /// Logical name: `postgres`, `redis`, `nats`, `python-orchestrator`, `worker`.
    pub name: String,
    pub healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    /// Safe reason when unhealthy (never connection strings/credentials).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl DependencyHealth {
    #[must_use]
    pub fn healthy(name: impl Into<String>, latency_ms: Option<u64>) -> Self {
        Self {
            name: name.into(),
            healthy: true,
            latency_ms,
            message: None,
        }
    }

    #[must_use]
    pub fn unhealthy(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            healthy: false,
            latency_ms: None,
            message: Some(message.into()),
        }
    }
}

/// Readiness: the service may accept traffic (all *required* deps healthy).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadinessResponse {
    pub ready: bool,
    pub service: String,
    pub version: String,
    #[serde(default)]
    pub dependencies: Vec<DependencyHealth>,
    pub timestamp: Timestamp,
}

impl ReadinessResponse {
    #[must_use]
    pub fn compute(
        service: impl Into<String>,
        version: impl Into<String>,
        dependencies: Vec<DependencyHealth>,
    ) -> Self {
        let ready = dependencies.iter().all(|dep| dep.healthy);
        Self {
            ready,
            service: service.into(),
            version: version.into(),
            dependencies,
            timestamp: Timestamp::now(),
        }
    }
}

/// Full-system diagnostics (admin/operator view).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealthResponse {
    pub service: String,
    pub version: String,
    pub uptime_seconds: u64,
    pub healthy: bool,
    #[serde(default)]
    pub dependencies: Vec<DependencyHealth>,
    /// Per-worker/queue liveness from the scheduler/consumer perspective.
    #[serde(default)]
    pub components: Vec<DependencyHealth>,
    pub timestamp: Timestamp,
}
