//! PURPOSE:
//! Worker capacity accounting: how many *more* tasks this worker may take.
//!
//! `max_in_flight` is only meaningful if it is measured against what is
//! genuinely in flight. Counting tasks this process has spawned is wrong after
//! a restart (the process forgot) and wrong while another replica drains the
//! same queue. Counting durable, unfinished attempts owned by this worker is
//! right in both cases.
//!
//! Layer / ownership
//! -----------------
//! `mas-worker`. The consumer asks this module for a number before every fetch
//! and never fetches more than the answer.
//!
//! Production responsibility
//! -------------------------
//! * Derive remaining capacity from **durable active attempts**, not from
//!   in-process counters and not from broker delivery state.
//! * Never return more than the configured ceiling, and never underflow: an
//!   over-count must clamp to zero rather than wrap to `usize::MAX`.
//! * Be honest about zero: at capacity the worker fetches nothing.
//!
//! What this module must NOT do
//! ----------------------------
//! * It must NOT track capacity in a way that survives only in memory — a
//!   restarted worker would then believe it is idle while durable attempts are
//!   still open under its name.
//! * It must NOT treat "the broker gave us N messages" as permission to run
//!   N tasks.

use mas_common::error::AppError;
use mas_common::result::Result;

/// The configured ceiling and the arithmetic around it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityGate {
    max_in_flight: usize,
}

impl CapacityGate {
    /// Builds a gate. A zero ceiling is legal and means "drain only": it makes
    /// the refusal explicit instead of being a division-by-zero-shaped accident.
    pub fn new(max_in_flight: usize) -> Result<Self> {
        if max_in_flight > u32::MAX as usize {
            return Err(AppError::invalid_field(
                "max_in_flight",
                "out_of_range",
                "max_in_flight must fit in u32",
            ));
        }
        Ok(Self { max_in_flight })
    }

    /// The configured ceiling.
    #[must_use]
    pub const fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// How many more tasks may be claimed, given `active` durable attempts.
    ///
    /// Saturating: an `active` count larger than the ceiling yields zero, never
    /// a wrapped value that would look like unlimited capacity.
    #[must_use]
    pub fn remaining(&self, active: u64) -> usize {
        let active = usize::try_from(active).unwrap_or(self.max_in_flight);
        self.max_in_flight.saturating_sub(active)
    }

    /// Whether the worker may take on any more work at `active`.
    #[must_use]
    pub fn has_room(&self, active: u64) -> bool {
        self.remaining(active) > 0
    }

    /// Clamps a broker fetch size to the remaining capacity.
    ///
    /// A batch size larger than the remaining room is the classic over-claim:
    /// the worker would take work it cannot run, and every message past the
    /// ceiling would sit unacked until its ack window expired.
    #[must_use]
    pub fn clamp_batch(&self, active: u64, batch_size: usize) -> usize {
        batch_size.min(self.remaining(active))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gate(max: usize) -> CapacityGate {
        CapacityGate::new(max).expect("gate")
    }

    #[test]
    fn capacity_zero_claims_nothing() {
        let gate = gate(0);
        assert_eq!(gate.remaining(0), 0);
        assert!(!gate.has_room(0));
        assert_eq!(
            gate.clamp_batch(0, 16),
            0,
            "a drain-only worker fetches nothing"
        );
    }

    #[test]
    fn capacity_one_allows_exactly_one() {
        let gate = gate(1);
        assert_eq!(gate.remaining(0), 1);
        assert!(gate.has_room(0));
        assert_eq!(gate.remaining(1), 0);
        assert!(!gate.has_room(1));
        assert_eq!(gate.clamp_batch(0, 8), 1);
    }

    #[test]
    fn capacity_exactly_n_saturates_at_n() {
        let gate = gate(4);
        assert_eq!(gate.remaining(0), 4);
        assert_eq!(gate.remaining(2), 2);
        assert_eq!(
            gate.remaining(4),
            0,
            "at capacity, nothing more may be taken"
        );
        assert!(!gate.has_room(4));
    }

    #[test]
    fn capacity_n_plus_one_never_exceeds_the_ceiling() {
        let gate = gate(4);
        // Five durable attempts is more than the ceiling allows — recovery from
        // a restart or a reassigned lease can legitimately produce this.
        assert_eq!(gate.remaining(5), 0, "over-count saturates at zero");
        assert_eq!(
            gate.remaining(u64::MAX),
            0,
            "an absurd active count must not wrap into unlimited capacity"
        );
        assert_eq!(gate.clamp_batch(9, 64), 0);
    }

    #[test]
    fn completion_during_claim_frees_exactly_one_slot() {
        let gate = gate(3);
        // A task finished while the worker was mid-claim.
        assert_eq!(gate.remaining(3), 0);
        assert_eq!(
            gate.remaining(2),
            1,
            "one completion frees exactly one slot"
        );
        assert_eq!(gate.clamp_batch(2, 32), 1);
    }

    #[test]
    fn restart_during_claim_is_measured_from_durable_attempts() {
        let gate = gate(8);
        // After a restart the process-local counter would read zero; durable
        // attempts are the only honest input, and they say 5.
        assert_eq!(
            gate.remaining(5),
            3,
            "recovery does not grant free capacity"
        );
        assert_eq!(gate.clamp_batch(5, 16), 3);
    }

    #[test]
    fn batch_is_never_larger_than_capacity_nor_than_the_request() {
        let gate = gate(2);
        assert_eq!(gate.clamp_batch(0, 1), 1, "a smaller batch stays smaller");
        assert_eq!(gate.clamp_batch(0, 64), 2, "a larger batch is clamped");
        assert_eq!(gate.clamp_batch(2, 64), 0);
    }

    #[test]
    fn absurd_ceilings_are_rejected() {
        assert!(CapacityGate::new(usize::MAX).is_err());
        assert!(CapacityGate::new(1_024).is_ok());
    }
}
