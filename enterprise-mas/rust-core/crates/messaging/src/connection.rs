//! Connection lifecycle: explicit state machine with bounded backoff reconnection.
//!
//! Real transports (NATS, gRPC channels) all share the same lifecycle:
//! `Disconnected → Connecting → Connected → Draining → Closed`, with losses
//! taking `Connecting/Connected` back to `Disconnected` where the reconnect
//! policy computes the next delay. The controller performs no I/O — adapters
//! drive it and sleep the returned delays — which keeps the policy unit
//! testable and identical across transports.

use std::fmt;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;

/// Where a connection is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Never connected, or lost and not currently retrying.
    Disconnected,
    /// A connect attempt is in flight.
    Connecting,
    /// Usable for publishing/subscribing.
    Connected,
    /// No new work; finishing in-flight requests before close.
    Draining,
    /// Terminal: will not reconnect.
    Closed,
}

impl ConnectionState {
    /// Whether publishes/subscribes may use this connection.
    #[must_use]
    pub fn is_usable(&self) -> bool {
        matches!(self, Self::Connected)
    }

    /// Whether this state may still reach `Closed` only (no recovery).
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Closed)
    }

    /// Legal successors under the lifecycle rules.
    #[must_use]
    pub fn successors(&self) -> &'static [ConnectionState] {
        match self {
            Self::Disconnected => &[Self::Connecting, Self::Closed],
            Self::Connecting => &[Self::Connected, Self::Disconnected, Self::Closed],
            Self::Connected => &[Self::Draining, Self::Disconnected, Self::Closed],
            Self::Draining => &[Self::Disconnected, Self::Closed],
            Self::Closed => &[],
        }
    }

    /// Whether `self → next` is a legal transition.
    #[must_use]
    pub fn can_transition_to(&self, next: ConnectionState) -> bool {
        self.successors().contains(&next)
    }
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Disconnected => "disconnected",
            Self::Connecting => "connecting",
            Self::Connected => "connected",
            Self::Draining => "draining",
            Self::Closed => "closed",
        };
        f.write_str(name)
    }
}

/// Bounded exponential backoff for reconnection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// Maximum reconnect attempts before giving up (`0` = no retries).
    pub max_attempts: u32,
    /// Delay before attempt 1.
    pub base_delay: Duration,
    /// Absolute ceiling for any single delay.
    pub max_delay: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        }
    }
}

impl ReconnectPolicy {
    /// Validates: `base_delay > 0`, `max_delay >= base_delay`, attempts ≤ 1000.
    pub fn validated(self) -> Result<Self> {
        if self.base_delay.is_zero() {
            return Err(AppError::validation("reconnect base_delay must be > 0"));
        }
        if self.max_delay < self.base_delay {
            return Err(AppError::validation(
                "reconnect max_delay must be >= base_delay",
            ));
        }
        if self.max_attempts > 1000 {
            return Err(AppError::validation(
                "reconnect max_attempts must be <= 1000",
            ));
        }
        Ok(self)
    }

    /// Delay before reconnect attempt number `attempt` (1-based), or `None`
    /// once attempts are exhausted.
    #[must_use]
    pub fn delay_for(&self, attempt: u32) -> Option<Duration> {
        if attempt == 0 || attempt > self.max_attempts {
            return None;
        }
        // base * 2^(attempt-1), capped — fully checked arithmetic.
        let factor = 1u64.checked_shl(attempt - 1).unwrap_or(u64::MAX);
        let scaled = self
            .base_delay
            .checked_mul(u32::try_from(factor.min(u64::from(u32::MAX))).unwrap_or(u32::MAX))
            .unwrap_or(self.max_delay);
        Some(scaled.min(self.max_delay))
    }
}

/// Drives one connection's lifecycle; I/O-free by design.
#[derive(Debug)]
pub struct ConnectionController {
    state: ConnectionState,
    policy: ReconnectPolicy,
    attempts: u32,
    last_error: Option<String>,
    last_connected_at: Option<Timestamp>,
    total_reconnects: u64,
}

impl ConnectionController {
    /// A fresh, disconnected controller.
    pub fn new(policy: ReconnectPolicy) -> Result<Self> {
        Ok(Self {
            state: ConnectionState::Disconnected,
            policy: policy.validated()?,
            attempts: 0,
            last_error: None,
            last_connected_at: None,
            total_reconnects: 0,
        })
    }

    /// Current lifecycle state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// The validated reconnect policy.
    #[must_use]
    pub fn policy(&self) -> ReconnectPolicy {
        self.policy
    }

    /// How many consecutive reconnect attempts have failed so far.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Last failure reason (sanitized by callers; never a connection string).
    #[must_use]
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// When the connection last became usable.
    #[must_use]
    pub fn last_connected_at(&self) -> Option<Timestamp> {
        self.last_connected_at
    }

    /// Lifetime reconnect count (observability).
    #[must_use]
    pub fn total_reconnects(&self) -> u64 {
        self.total_reconnects
    }

    /// Publishes/subscribes are allowed.
    #[must_use]
    pub fn is_publishable(&self) -> bool {
        self.state.is_usable()
    }

    fn transition(&mut self, next: ConnectionState) -> Result<()> {
        if !self.state.can_transition_to(next) {
            return Err(AppError::conflict(format!(
                "illegal connection transition {} -> {}",
                self.state, next
            )));
        }
        self.state = next;
        Ok(())
    }

    /// `Disconnected → Connecting`: the adapter starts a dial.
    pub fn begin_connect(&mut self) -> Result<()> {
        self.transition(ConnectionState::Connecting)
    }

    /// `Connecting → Connected`: dial succeeded; failure budget resets.
    pub fn mark_connected(&mut self) -> Result<()> {
        let was_reconnect = self.attempts > 0;
        self.transition(ConnectionState::Connected)?;
        if was_reconnect {
            self.total_reconnects += 1;
        }
        self.attempts = 0;
        self.last_error = None;
        self.last_connected_at = Some(Timestamp::now());
        Ok(())
    }

    /// A dial failed or a live connection dropped. Returns the backoff delay
    /// before the next `begin_connect`, or `None` when attempts are exhausted
    /// (the adapter should escalate to dead-letter/operator alert).
    pub fn mark_lost(&mut self, reason: impl Into<String>) -> Result<Option<Duration>> {
        self.last_error = Some(reason.into());
        if self.state.is_usable() || self.state == ConnectionState::Connecting {
            self.transition(ConnectionState::Disconnected)?;
        }
        if self.state != ConnectionState::Disconnected {
            return Err(AppError::conflict(format!(
                "cannot mark connection lost from state {}",
                self.state
            )));
        }
        self.attempts = self.attempts.saturating_add(1);
        Ok(self.policy.delay_for(self.attempts))
    }

    /// `Connected → Draining`: stop accepting new work.
    pub fn begin_drain(&mut self) -> Result<()> {
        self.transition(ConnectionState::Draining)
    }

    /// `* → Closed`: final shutdown. Legal from every non-terminal state.
    pub fn close(&mut self) -> Result<()> {
        // Draining/dropping mid-Connecting is always allowed by construction.
        if self.state.is_terminal() {
            return Err(AppError::conflict("connection is already closed"));
        }
        self.state = ConnectionState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_lifecycle() {
        let mut controller = ConnectionController::new(ReconnectPolicy::default()).expect("policy");
        assert_eq!(controller.state(), ConnectionState::Disconnected);
        assert!(!controller.is_publishable());

        controller.begin_connect().expect("dial");
        assert_eq!(controller.state(), ConnectionState::Connecting);
        controller.mark_connected().expect("up");
        assert!(controller.is_publishable());
        assert!(controller.last_connected_at().is_some());

        controller.begin_drain().expect("drain");
        assert!(!controller.is_publishable());
        controller.close().expect("closed");
        assert!(controller.state().is_terminal());
        assert!(controller.close().is_err(), "double close is an error");
    }

    #[test]
    fn illegal_transitions_are_rejected() {
        let mut controller = ConnectionController::new(ReconnectPolicy::default()).expect("policy");
        assert!(controller.mark_connected().is_err(), "must dial first");
        assert!(controller.begin_drain().is_err());
        controller.begin_connect().expect("dial");
        assert!(controller.begin_drain().is_err(), "connecting cannot drain");
    }

    #[test]
    fn reconnect_backoff_is_bounded_and_exhausts() {
        let policy = ReconnectPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(120),
        };
        assert_eq!(policy.delay_for(0), None);
        assert_eq!(policy.delay_for(1), Some(Duration::from_millis(50)));
        assert_eq!(policy.delay_for(2), Some(Duration::from_millis(100)));
        assert_eq!(
            policy.delay_for(3),
            Some(Duration::from_millis(120)),
            "capped at ceiling"
        );
        assert_eq!(policy.delay_for(4), None, "exhausted");

        let mut controller = ConnectionController::new(policy).expect("policy");
        controller.begin_connect().expect("dial");
        controller.mark_connected().expect("up");

        let first = controller.mark_lost("socket reset").expect("lost #1");
        assert_eq!(controller.attempts(), 1);
        assert_eq!(first, Some(Duration::from_millis(50)));
        assert_eq!(controller.last_error(), Some("socket reset"));

        controller.begin_connect().expect("re-dial");
        let second = controller.mark_lost("still down").expect("lost #2");
        assert_eq!(second, Some(Duration::from_millis(100)));
        controller.begin_connect().expect("re-dial");
        let third = controller.mark_lost("still down").expect("lost #3");
        assert_eq!(third, Some(Duration::from_millis(120)));
        controller.begin_connect().expect("re-dial");
        let fourth = controller.mark_lost("permanently down").expect("lost #4");
        assert_eq!(fourth, None, "no further delay once exhausted");

        // A clean connect resets the budget and counts as a reconnect.
        controller.begin_connect().expect("dial");
        controller.mark_connected().expect("up again");
        assert_eq!(controller.attempts(), 0);
        assert_eq!(controller.total_reconnects(), 1);
        assert!(controller.last_error().is_none());
    }
}
