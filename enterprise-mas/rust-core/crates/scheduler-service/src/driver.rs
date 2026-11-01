//! The interval driver: `recover` once, `tick` every interval, drain on
//! shutdown.

use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_scheduling::lease::LeaseStorePort;
use mas_scheduling::runner::{DispatchPort, SchedulerRuntime, TickReport};
use mas_scheduling::store::ScheduleStorePort;
use mas_worker::grace::ShutdownWatch;

/// Driver configuration.
#[derive(Debug, Clone)]
pub struct DriverConfig {
    /// Wall-clock distance between ticks. The deterministic runtime clamps
    /// its own internals; this is purely how often the wall advances.
    pub tick_interval: Duration,
    /// Run [`SchedulerRuntime::recover`] before the first tick.
    pub recover_on_start: bool,
    /// Ceiling for the backoff between successive failing ticks.
    pub error_backoff_cap: Duration,
}

impl Default for DriverConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(1),
            recover_on_start: true,
            error_backoff_cap: Duration::from_secs(30),
        }
    }
}

impl DriverConfig {
    /// Knob sanity.
    pub fn validated(self) -> Result<Self> {
        if self.tick_interval < Duration::from_millis(10) {
            return Err(AppError::validation("tick_interval must be >= 10ms"));
        }
        if self.error_backoff_cap < self.tick_interval {
            return Err(AppError::validation(
                "error_backoff_cap must be >= tick_interval",
            ));
        }
        Ok(self)
    }
}

/// Cumulative observability across the driver's lifetime.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriverStats {
    /// Ticks started (shutdown-triggered final tick excluded).
    pub ticks: u64,
    /// Aggregate of every folded [`TickReport`].
    pub dispatched: u64,
    /// Total dispatch failures (each re-queued).
    pub dispatch_errors: u64,
    /// Per-schedule lease losses (normal under multi-replica operation).
    pub lease_lost: u64,
    /// Runs skipped because the previous run was still in flight.
    pub overlap_skipped: u64,
    /// Runs skipped by the catch-up policy.
    pub catch_up_skipped: u64,
    /// Ticks that failed wholesale (store/load-level errors), consecutively.
    /// Resets on the first successful tick.
    pub consecutive_errors: u64,
    /// Leases reaped during startup recovery.
    pub leases_reaped: u64,
    /// True when the loop exited via the shutdown watch.
    pub stopped_by_shutdown: bool,
}

impl DriverStats {
    fn fold(&mut self, report: &TickReport) {
        self.ticks += 1;
        self.dispatched += report.dispatched as u64;
        self.dispatch_errors += report.dispatch_errors as u64;
        self.lease_lost += report.lease_lost as u64;
        self.overlap_skipped += report.overlap_skipped as u64;
        self.catch_up_skipped += report.catch_up_skipped as u64;
    }
}

/// Bounded exponential delay after the Nth consecutive erroring tick
/// (1-indexed: first error ⇒ one interval, then 2×, 4×, …, capped at `cap`
/// and at a 1024× growth limit).
#[must_use]
pub fn backoff_for(interval: Duration, cap: Duration, errors: u64) -> Duration {
    if errors == 0 {
        return Duration::ZERO;
    }
    let shift = errors.min(11) - 1; // << beyond 2^10 saturates
    interval
        .checked_mul(1u32.checked_shl(shift as u32).unwrap_or(1 << 10))
        .unwrap_or(cap)
        .min(cap)
}

/// The interval loop around a [`SchedulerRuntime`]. Owns its stats; the
/// runtime stays owned by the caller so two driver runs can chain
/// (first-run recovery, later-restart runtime).
#[derive(Debug)]
pub struct TickDriver {
    config: DriverConfig,
    stats: DriverStats,
}

impl TickDriver {
    /// Validates the config up front (fail fast, not at first tick).
    pub fn new(config: DriverConfig) -> Result<Self> {
        Ok(Self {
            config: config.validated()?,
            stats: DriverStats::default(),
        })
    }

    /// Cumulative stats so far.
    #[must_use]
    pub fn stats(&self) -> &DriverStats {
        &self.stats
    }

    /// Runs the loop until the shutdown watch fires. A tick already in
    /// progress always completes before the loop exits (a half-dispatched
    /// run would wedge the delayed queue).
    pub async fn run<S, L, D>(
        &mut self,
        runtime: &mut SchedulerRuntime<S, L, D>,
        mut watch: ShutdownWatch,
    ) -> Result<DriverStats>
    where
        S: ScheduleStorePort,
        L: LeaseStorePort,
        D: DispatchPort,
    {
        if self.config.recover_on_start {
            match runtime.recover(&Timestamp::now()).await {
                Ok(report) => {
                    self.stats.leases_reaped = report.leases_reaped as u64;
                    tracing::info!(
                        leases_reaped = report.leases_reaped,
                        schedules_rescanned = report.schedules_rescanned,
                        "scheduler recovery complete",
                    );
                },
                Err(err) => {
                    // Recovery failure is fatal: without lease reaping a
                    // crashed predecessor's schedules would stick.
                    return Err(err);
                },
            }
        }

        let mut interval = tokio::time::interval(self.config.tick_interval);
        // First tick fires immediately — no dead first-interval in dev.
        loop {
            tokio::select! {
                biased;
                _ = watch.changed() => {
                    self.stats.stopped_by_shutdown = true;
                    break;
                }
                _ = interval.tick() => {
                    let tick_at = Timestamp::now();
                    match runtime.tick(&tick_at).await {
                        Ok(report) => {
                            if report.scanned > 0 || report.dispatched > 0 {
                                tracing::debug!(
                                    scanned = report.scanned,
                                    due = report.due,
                                    dispatched = report.dispatched,
                                    dropped = report.lease_lost + report.overlap_skipped,
                                    "tick",
                                );
                            }
                            self.stats.fold(&report);
                            self.stats.consecutive_errors = 0;
                        },
                        Err(err) => {
                            self.stats.ticks += 1;
                            self.stats.consecutive_errors += 1;
                            let backoff = backoff_for(
                                self.config.tick_interval,
                                self.config.error_backoff_cap,
                                self.stats.consecutive_errors,
                            );
                            tracing::warn!(
                                %err,
                                consecutive = self.stats.consecutive_errors,
                                backoff_ms = backoff.as_millis() as u64,
                                "tick failed; backing off before next tick",
                            );
                            // Sleep the backoff but stay interruptible.
                            tokio::select! {
                                biased;
                                _ = watch.changed() => {
                                    self.stats.stopped_by_shutdown = true;
                                    return Ok(self.stats.clone());
                                }
                                _ = tokio::time::sleep(backoff) => {}
                            }
                        },
                    }
                }
            }
        }
        Ok(self.stats.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let interval = Duration::from_millis(100);
        let cap = Duration::from_secs(2);
        assert_eq!(backoff_for(interval, cap, 0), Duration::ZERO);
        assert_eq!(backoff_for(interval, cap, 1), interval);
        assert_eq!(backoff_for(interval, cap, 2), Duration::from_millis(200));
        assert_eq!(backoff_for(interval, cap, 3), Duration::from_millis(400));
        assert_eq!(backoff_for(interval, cap, 50), cap);
    }

    #[test]
    fn config_rejects_bad_knobs() {
        assert!(DriverConfig {
            tick_interval: Duration::from_millis(5),
            ..DriverConfig::default()
        }
        .validated()
        .is_err());
        assert!(DriverConfig {
            error_backoff_cap: Duration::from_millis(1),
            ..DriverConfig::default()
        }
        .validated()
        .is_err());
    }
}
