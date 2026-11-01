//! PURPOSE: The quota reservation aggregate — the in-flight half of
//! `check → reserve → execute → settle → release`.
//!
//! Owning layer: `mas-domain`.
//!
//! Production responsibility: represent a *held* spend that has been
//! authorised against a distributed counter but not yet reconciled with what
//! was actually consumed. Because it carries a lease expiry, a process that
//! dies mid-execution does not consume the tenant's quota forever: the sweeper
//! reclaims the reservation when the lease lapses.
//!
//! Data ownership: reservations are tenant-scoped, RLS-protected, and
//! Rust-owned. The Python orchestrator never creates one.
//!
//! It must NOT:
//!   • be used to bill — a reservation is authorised headroom, not consumption;
//!     only the settled amount becomes usage;
//!   • allow `settled_amount > reserved_amount` (the database enforces the
//!     same invariant; this type refuses it earlier with a better message);
//!   • be minted without an idempotency key — a caller that retries the same
//!     logical work must get the same reservation back, not a second one.

use mas_common::error::AppError;
use mas_common::ids::{ExecutionId, ReservationId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::string_enum;
use mas_common::timestamps::Timestamp;
use mas_common::validation;
use serde::{Deserialize, Serialize};

use crate::quota::QuotaDimension;

string_enum! {
    /// Lifecycle of a reservation.
    ReservationState {
        /// Authorised, work not finished. Counts against the tenant.
        Held => "held",
        /// Reconciled against actual consumption. Terminal.
        Settled => "settled",
        /// Returned to the tenant because the work did not happen. Terminal.
        Released => "released",
        /// Lease lapsed with no settlement; reclaimed by the sweeper. Terminal.
        Expired => "expired",
    }
}

impl ReservationState {
    /// Whether the reservation still consumes headroom.
    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(self, Self::Held)
    }

    /// Whether no further mutation is legitimate.
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        !self.is_open()
    }
}

/// One held spend against one metered dimension.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaReservation {
    pub id: ReservationId,
    pub tenant_id: TenantId,
    pub dimension: QuotaDimension,
    /// Fixed-window bucket this reservation was charged to.
    pub bucket: i64,
    /// Units authorised.
    pub reserved_amount: u64,
    /// Units actually consumed once the work finished.
    pub settled_amount: u64,
    pub state: ReservationState,
    /// Caller-supplied idempotency key. Two calls with the same key for the
    /// same tenant must resolve to one reservation.
    pub reservation_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<ExecutionId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<TaskId>,
    /// When an un-settled reservation may be reclaimed.
    pub lease_expires_at: Timestamp,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<Timestamp>,
}

impl QuotaReservation {
    /// Creates a reservation in the [`ReservationState::Held`] state.
    #[allow(clippy::too_many_arguments)]
    pub fn hold(
        tenant_id: TenantId,
        dimension: QuotaDimension,
        bucket: i64,
        reserved_amount: u64,
        reservation_key: impl Into<String>,
        execution_id: Option<ExecutionId>,
        task_id: Option<TaskId>,
        lease_expires_at: Timestamp,
        now: &Timestamp,
    ) -> Result<Self> {
        let reservation_key = reservation_key.into();
        validation::validate_length("reservation_key", &reservation_key, 1, 200)?;
        if reserved_amount == 0 {
            return Err(AppError::invalid_field(
                "reserved_amount",
                "out_of_range",
                "a reservation must authorise a positive amount",
            ));
        }
        if !lease_expires_at.is_after(now) {
            return Err(AppError::invalid_field(
                "lease_expires_at",
                "invalid_period",
                "a reservation lease must expire in the future",
            ));
        }
        Ok(Self {
            id: ReservationId::new(),
            tenant_id,
            dimension,
            bucket,
            reserved_amount,
            settled_amount: 0,
            state: ReservationState::Held,
            reservation_key,
            execution_id,
            task_id,
            lease_expires_at,
            created_at: *now,
            updated_at: *now,
            settled_at: None,
        })
    }

    /// Units still held but not consumed — what releasing would return.
    #[must_use]
    pub const fn unused(&self) -> u64 {
        self.reserved_amount.saturating_sub(self.settled_amount)
    }

    /// Whether the lease has lapsed at `at`, making the reservation
    /// reclaimable. An already-terminal reservation is never reclaimable:
    /// reclaiming a settled reservation would hand the tenant back headroom
    /// for work that was already metered.
    #[must_use]
    pub fn is_reclaimable_at(&self, at: &Timestamp) -> bool {
        self.state.is_open() && !at.is_before(&self.lease_expires_at)
    }

    /// Records actual consumption. The settled amount may not exceed what was
    /// reserved: consumption beyond the authorisation is a new reservation,
    /// not an amendment to this one.
    pub fn settle(&mut self, actual: u64, at: &Timestamp) -> Result<()> {
        if !self.state.is_open() {
            return Err(AppError::conflict(format!(
                "reservation {} is {} and can no longer be settled",
                self.id, self.state
            )));
        }
        if actual > self.reserved_amount {
            return Err(AppError::invalid_field(
                "settled_amount",
                "out_of_range",
                format!(
                    "settling {} exceeds the {} units this reservation authorised",
                    actual, self.reserved_amount
                ),
            ));
        }
        self.settled_amount = actual;
        self.state = ReservationState::Settled;
        self.updated_at = *at;
        self.settled_at = Some(*at);
        Ok(())
    }

    /// Extends the lease on a still-held reservation.
    ///
    /// A run that outlives its lease must be able to say so, or the only way
    /// to hold a slot for a long run is to reserve it for the worst case up
    /// front — and the worst case is then what a crashed worker holds the
    /// slot for.
    ///
    /// The lease may only move **later**. Shortening it is refused, because
    /// the lease is what a sweeper reads to decide this work is dead: a lease
    /// that moved backwards would let a concurrent sweep reclaim a slot for
    /// work that is still running, which is exactly the over-admission the
    /// reservation exists to prevent.
    ///
    /// # Errors
    /// * `Conflict` when the reservation is no longer held — a lease cannot be
    ///   extended on something already settled, released or reclaimed;
    /// * `Validation` when `until` is not after the current expiry.
    pub fn extend_lease(&mut self, until: &Timestamp, at: &Timestamp) -> Result<()> {
        if !self.state.is_open() {
            return Err(AppError::conflict(format!(
                "reservation {} is {} and can no longer be extended",
                self.id, self.state
            )));
        }
        if !until.is_after(&self.lease_expires_at) {
            return Err(AppError::invalid_field(
                "lease_expires_at",
                "not_after_current_expiry",
                format!(
                    "a lease may only be extended; {} is not after {}",
                    until, self.lease_expires_at
                ),
            ));
        }
        self.lease_expires_at = *until;
        self.updated_at = *at;
        Ok(())
    }

    /// Returns the un-consumed headroom without recording usage.
    pub fn release(&mut self, at: &Timestamp) -> Result<u64> {
        if !self.state.is_open() {
            return Err(AppError::conflict(format!(
                "reservation {} is {} and can no longer be released",
                self.id, self.state
            )));
        }
        let unused = self.unused();
        self.state = ReservationState::Released;
        self.updated_at = *at;
        self.settled_at = Some(*at);
        Ok(unused)
    }

    /// Reclaims a lapsed reservation (crash recovery).
    pub fn expire(&mut self, at: &Timestamp) -> Result<u64> {
        if !self.is_reclaimable_at(at) {
            return Err(AppError::conflict(format!(
                "reservation {} is not reclaimable at {at} (state {}, lease until {})",
                self.id, self.state, self.lease_expires_at
            )));
        }
        let unused = self.unused();
        self.state = ReservationState::Expired;
        self.updated_at = *at;
        self.settled_at = Some(*at);
        Ok(unused)
    }
}

/// Outcome of one settle-or-reclaim mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Settlement {
    /// Units that were held but not consumed — what the caller must return to
    /// the counter.
    pub released: u64,
    /// Units actually consumed, which become usage.
    pub consumed: u64,
    /// Whether this call was the one that moved the reservation to terminal.
    ///
    /// `false` means the reservation was already terminal: a duplicate
    /// delivery of the same completion. The caller must **not** meter again —
    /// and must not refund again either, or a redelivery would both bill and
    /// credit the same work.
    pub applied: bool,
}

impl Settlement {
    /// A first, effective settlement.
    #[must_use]
    pub const fn applied(consumed: u64, released: u64) -> Self {
        Self {
            released,
            consumed,
            applied: true,
        }
    }

    /// A duplicate: nothing moved, nothing to meter, nothing to refund.
    #[must_use]
    pub const fn duplicate(consumed: u64) -> Self {
        Self {
            released: 0,
            consumed,
            applied: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> Timestamp {
        Timestamp::from_unix_seconds(1_700_000_000).expect("ts")
    }

    fn held(amount: u64, lease_seconds: i64) -> QuotaReservation {
        let at = now();
        let lease = Timestamp::from_unix_seconds(1_700_000_000 + lease_seconds).expect("lease");
        QuotaReservation::hold(
            TenantId::new(),
            QuotaDimension::Tokens,
            7,
            amount,
            "key-1",
            None,
            None,
            lease,
            &at,
        )
        .expect("hold")
    }

    #[test]
    fn a_reservation_must_be_positive_and_leased_into_the_future() {
        let at = now();
        let past = Timestamp::from_unix_seconds(1).expect("past");
        assert!(QuotaReservation::hold(
            TenantId::new(),
            QuotaDimension::Tokens,
            0,
            10,
            "k",
            None,
            None,
            past,
            &at,
        )
        .is_err());
        assert!(QuotaReservation::hold(
            TenantId::new(),
            QuotaDimension::Tokens,
            0,
            0,
            "k",
            None,
            None,
            Timestamp::from_unix_seconds(1_800_000_000).expect("future"),
            &at,
        )
        .is_err());
    }

    #[test]
    fn settling_more_than_was_reserved_is_refused() {
        let mut reservation = held(100, 60);
        assert!(reservation.settle(101, &now()).is_err());
        reservation.settle(40, &now()).expect("settle");
        assert_eq!(reservation.settled_amount, 40);
        assert_eq!(reservation.unused(), 60);
        assert!(reservation.state.is_terminal());
    }

    #[test]
    fn a_terminal_reservation_cannot_be_settled_or_released_twice() {
        let mut reservation = held(100, 60);
        reservation.settle(100, &now()).expect("settle");
        assert!(reservation.settle(10, &now()).is_err());
        assert!(reservation.release(&now()).is_err());
        assert!(reservation.expire(&now()).is_err());
    }

    #[test]
    fn release_returns_the_unconsumed_headroom() {
        let mut reservation = held(100, 60);
        assert_eq!(reservation.release(&now()).expect("release"), 100);
        assert_eq!(reservation.state, ReservationState::Released);
    }

    #[test]
    fn an_unexpired_lease_is_not_reclaimable() {
        let reservation = held(100, 600);
        assert!(!reservation.is_reclaimable_at(&now()));
        let later = Timestamp::from_unix_seconds(1_700_001_000).expect("later");
        assert!(reservation.is_reclaimable_at(&later));
    }
}
