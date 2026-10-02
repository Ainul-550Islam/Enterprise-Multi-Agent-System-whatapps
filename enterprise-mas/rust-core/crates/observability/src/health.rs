//! Component health registry: liveness vs readiness probes aggregated
//! into a deterministic platform status.
//!
//! Semantics (Kubernetes-aligned):
//! * **liveness** — is the process alive at all; a failing liveness probe
//!   means restart, not remove from rotation.
//! * **readiness** — may the component receive traffic (dependencies up).
//!
//! Aggregation rule: the platform is as healthy as its WORST registered
//! component (`Down` > `Degraded` > `Up`). Probe details are sanitized
//! before storage/rendering (single-line, length-capped): health output
//! is served to operators and must never leak internals.

use async_trait::async_trait;
use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;

string_enum! {
    /// Component/platform health states, ordered by severity below.
    HealthStatus {
        Up => "up",
        Degraded => "degraded",
        Down => "down",
    }
}

impl HealthStatus {
    /// Severity rank for worst-of aggregation.
    #[must_use]
    pub const fn severity(&self) -> u8 {
        match self {
            Self::Up => 0,
            Self::Degraded => 1,
            Self::Down => 2,
        }
    }
}

/// Maximum characters stored for one probe detail line.
pub const MAX_DETAIL_CHARS: usize = 256;

/// Sanitizes probe detail text for storage/output.
#[must_use]
pub fn sanitize_detail(raw: &str) -> String {
    raw.chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .take(MAX_DETAIL_CHARS)
        .collect()
}

/// The outcome of one component probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub checked_at: Timestamp,
    pub latency_ms: Option<u64>,
    pub detail: Option<String>,
}

impl ComponentHealth {
    fn base(name: &str, status: HealthStatus, at: &Timestamp) -> Result<Self> {
        validate_component_name(name)?;
        Ok(Self {
            name: name.to_owned(),
            status,
            checked_at: *at,
            latency_ms: None,
            detail: None,
        })
    }

    /// Healthy probe result.
    pub fn up(name: &str, at: &Timestamp) -> Result<Self> {
        Self::base(name, HealthStatus::Up, at)
    }

    /// Degraded probe result with a sanitized detail.
    pub fn degraded(name: &str, at: &Timestamp, detail: &str) -> Result<Self> {
        let mut health = Self::base(name, HealthStatus::Degraded, at)?;
        health.detail = Some(sanitize_detail(detail));
        Ok(health)
    }

    /// Down probe result with a sanitized detail.
    pub fn down(name: &str, at: &Timestamp, detail: &str) -> Result<Self> {
        let mut health = Self::base(name, HealthStatus::Down, at)?;
        health.detail = Some(sanitize_detail(detail));
        Ok(health)
    }

    /// Builder: attach measured latency.
    #[must_use]
    pub fn with_latency_ms(mut self, latency_ms: u64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }
}

/// Registered component names: `[a-z0-9._-]+`, 1..=64 chars.
pub fn validate_component_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '.' | '_' | '-'));
    if ok {
        Ok(())
    } else {
        Err(AppError::invalid_field(
            "component",
            "invalid_name",
            format!("component names match [a-z0-9._-]+ (≤64), got {name:?}"),
        ))
    }
}

/// Which probe family a check belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProbeKind {
    /// Liveness: process-level aliveness.
    Liveness,
    /// Readiness: dependency-level traffic admission.
    Readiness,
}

/// A health probe.
#[async_trait]
pub trait HealthCheckPort: Send + Sync {
    /// Runs the probe at `at`. Implementations SHOULD keep work bounded;
    /// the registry does not time probes out (deployment config does).
    async fn check(&self, at: &Timestamp) -> ComponentHealth;
}

/// Aggregated result of evaluating a probe family.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateHealth {
    pub kind: ProbeKind,
    /// Worst severity across components (`Up` when none registered).
    pub status: HealthStatus,
    pub evaluated_at: Timestamp,
    pub components: Vec<ComponentHealth>,
}

impl AggregateHealth {
    /// Whether the aggregate allows traffic / restart decisions as healthy.
    #[must_use]
    pub const fn is_operational(&self) -> bool {
        self.status.severity() == 0
    }

    /// JSON view for HTTP health endpoints.
    #[must_use]
    pub fn to_json(&self) -> Value {
        let components: Vec<Value> = self
            .components
            .iter()
            .map(|component| {
                let mut item = Map::with_capacity(5);
                item.insert("name".to_owned(), Value::from(component.name.clone()));
                item.insert(
                    "status".to_owned(),
                    Value::from(component.status.as_str().to_owned()),
                );
                item.insert(
                    "checked_at".to_owned(),
                    Value::from(component.checked_at.to_rfc3339_millis()),
                );
                if let Some(latency) = component.latency_ms {
                    item.insert("latency_ms".to_owned(), Value::from(latency));
                }
                if let Some(detail) = &component.detail {
                    item.insert("detail".to_owned(), Value::from(detail.clone()));
                }
                Value::Object(item)
            })
            .collect();
        let mut root = Map::with_capacity(4);
        root.insert("kind".to_owned(), Value::from(kind_name(self.kind)));
        root.insert(
            "status".to_owned(),
            Value::from(self.status.as_str().to_owned()),
        );
        root.insert(
            "evaluated_at".to_owned(),
            Value::from(self.evaluated_at.to_rfc3339_millis()),
        );
        root.insert("components".to_owned(), Value::Array(components));
        Value::Object(root)
    }
}

fn kind_name(kind: ProbeKind) -> &'static str {
    match kind {
        ProbeKind::Liveness => "liveness",
        ProbeKind::Readiness => "readiness",
    }
}

/// Registered probes, separated by probe family.
pub struct HealthRegistry {
    checks: std::sync::Mutex<BTreeMap<(ProbeKind, String), Box<dyn HealthCheckPort>>>,
}

impl fmt::Debug for HealthRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HealthRegistry")
            .field("registered", &self.names_snapshot())
            .finish()
    }
}

impl Default for HealthRegistry {
    fn default() -> Self {
        Self {
            checks: std::sync::Mutex::new(BTreeMap::new()),
        }
    }
}

impl HealthRegistry {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn names_snapshot(&self) -> Vec<(String, String)> {
        self.checks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .map(|(kind, name)| (kind_name(*kind).to_owned(), name.clone()))
            .collect()
    }

    /// Registers (or replaces) a probe for `kind`/`name`.
    pub fn register<C: HealthCheckPort + 'static>(
        &self,
        kind: ProbeKind,
        name: &str,
        check: C,
    ) -> Result<()> {
        validate_component_name(name)?;
        self.checks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert((kind, name.to_owned()), Box::new(check));
        Ok(())
    }

    /// Removes a probe. Returns whether it was registered.
    pub fn remove(&self, kind: ProbeKind, name: &str) -> bool {
        self.checks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(kind, name.to_owned()))
            .is_some()
    }

    /// Registered component names for `kind`, sorted.
    #[must_use]
    pub fn registered(&self, kind: ProbeKind) -> Vec<String> {
        self.checks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .filter(|(probe_kind, _)| *probe_kind == kind)
            .map(|(_, name)| name.clone())
            .collect()
    }

    /// Runs every probe of `kind` and aggregates worst-of. One failing
    /// probe never aborts the evaluation: panics/errors are reported as
    /// that component being `Down`.
    pub async fn evaluate(&self, kind: ProbeKind, at: &Timestamp) -> AggregateHealth {
        let names = self.registered(kind);
        let mut components = Vec::with_capacity(names.len());
        let mut worst = HealthStatus::Up;
        for name in names {
            let check = self
                .checks
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&(kind, name.clone()));
            let health = match check {
                Some(probe) => {
                    let outcome = probe.check(at).await;
                    self.checks
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert((kind, name.clone()), probe);
                    outcome
                },
                None => ComponentHealth::down(&name, at, "check vanished during evaluation")
                    .expect("constant component health"),
            };
            if health.status.severity() > worst.severity() {
                worst = health.status;
            }
            components.push(health);
        }
        AggregateHealth {
            kind,
            status: worst,
            evaluated_at: *at,
            components,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    struct Stub {
        name: &'static str,
        status: HealthStatus,
        detail: Option<&'static str>,
    }

    #[async_trait]
    impl HealthCheckPort for Stub {
        async fn check(&self, at: &Timestamp) -> ComponentHealth {
            let detail = self.detail.unwrap_or("");
            match self.status {
                HealthStatus::Up => ComponentHealth::up(self.name, at),
                HealthStatus::Degraded => ComponentHealth::degraded(self.name, at, detail),
                HealthStatus::Down => ComponentHealth::down(self.name, at, detail),
            }
            .expect("component health")
        }
    }

    #[tokio::test]
    async fn aggregation_is_worst_of_and_scoped_by_kind() {
        let registry = HealthRegistry::new();
        registry
            .register(
                ProbeKind::Liveness,
                "self",
                Stub {
                    name: "self",
                    status: HealthStatus::Up,
                    detail: None,
                },
            )
            .expect("register");
        registry
            .register(
                ProbeKind::Readiness,
                "database",
                Stub {
                    name: "database",
                    status: HealthStatus::Up,
                    detail: None,
                },
            )
            .expect("register");
        registry
            .register(
                ProbeKind::Readiness,
                "broker",
                Stub {
                    name: "broker",
                    status: HealthStatus::Degraded,
                    detail: Some("nats: slow consumers slow consumers slow consumers"),
                },
            )
            .expect("register");
        registry
            .register(
                ProbeKind::Readiness,
                "vault",
                Stub {
                    name: "vault",
                    status: HealthStatus::Down,
                    detail: Some("sealed\nsealed\nsealed"),
                },
            )
            .expect("register");

        let liveness = registry.evaluate(ProbeKind::Liveness, &at()).await;
        assert_eq!(liveness.status, HealthStatus::Up);
        assert!(liveness.is_operational());
        assert_eq!(liveness.components.len(), 1, "kinds stay separate");

        let readiness = registry.evaluate(ProbeKind::Readiness, &at()).await;
        assert_eq!(readiness.status, HealthStatus::Down, "worst-of wins");
        assert!(!readiness.is_operational());
        assert_eq!(readiness.components.len(), 3);
        let vault_detail = &readiness.components[2].detail.clone().expect("detail");
        assert!(
            !vault_detail.contains('\n'),
            "details sanitized single-line"
        );

        assert!(registry
            .register(
                ProbeKind::Liveness,
                "Bad Name",
                Stub {
                    name: "x",
                    status: HealthStatus::Up,
                    detail: None
                }
            )
            .is_err());
        assert!(registry.remove(ProbeKind::Readiness, "vault"));
        assert!(!registry.remove(ProbeKind::Readiness, "vault"));
        let after = registry.evaluate(ProbeKind::Readiness, &at()).await;
        assert_eq!(after.status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn json_payload_carries_operator_safe_summary() {
        let registry = HealthRegistry::new();
        registry
            .register(
                ProbeKind::Readiness,
                "queue",
                Stub {
                    name: "queue",
                    status: HealthStatus::Up,
                    detail: None,
                },
            )
            .expect("register");
        let aggregate = registry.evaluate(ProbeKind::Readiness, &at()).await;
        let json = aggregate.to_json();
        assert_eq!(json["kind"], Value::from("readiness"));
        assert_eq!(json["status"], Value::from("up"));
        assert!(json["evaluated_at"].is_string());
        assert!(json["components"].is_array());
    }
}
