//! PURPOSE: The concurrency slot a worker holds for the duration of one run —
//! the "reserve" and "release" halves of `check → reserve → execute → settle →
//! release`, expressed as a port the worker can call without knowing any quota
//! arithmetic.
//!
//! Owning layer: `mas-worker` (the durable worker plane).
//!
//! Production responsibility: a tenant's plan can cap how many runs it may
//! have in flight at once. That ceiling is only meaningful if the thing that
//! counts against it is held for exactly as long as the run — taken before the
//! work starts, renewed while it is genuinely still going, and returned when
//! it stops, whether it stopped by finishing, by failing, or by the worker
//! dying. `mas-application`'s control plane already knows how to do all three
//! against a distributed counter; this module is the seam that lets the worker
//! use that without re-implementing it.
//!
//! The seam exists because of a dependency direction, not for decoration:
//! `mas-worker` deliberately does not depend on `mas-persistence`, and the
//! transaction a slot is taken in must be the runtime pool's, under the
//! tenant's authoritative scope. So the port lives here, and the composition
//! root (`mas-worker-service`, which can see both crates) supplies the
//! implementation.
//!
//! Data ownership: none. The worker owns no quota state. It asks for a slot,
//! says when it is still using one, and says when it has stopped. Every number
//! behind those answers belongs to the control plane.
//!
//! What it must NOT do:
//!   • compute, cache, or "optimistically" assume a quota decision. Spec 23:
//!     the worker consumes, reports and settles; it never invents quota math.
//!     A worker that decided for itself that "one more run is probably fine"
//!     would be a second quota engine, and a second quota engine cannot see
//!     the other replicas';
//!   • let a failure to release leak a slot. Every path that acquires must
//!     also release, including the paths where the claim yielded no work;
//!   • block a run when no ceiling is configured. [`RunSlot::NotMetered`] is a
//!     success, and treating it as a refusal would impose a limit nobody set.

//! The concurrency slot one run holds, as a port the worker can call.

use mas_common::ids::{ExecutionId, OrganizationId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use std::fmt::Debug;

/// What the concurrency gate decided for one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunSlot {
    /// This call took a slot. It counts against the tenant's ceiling, and
    /// **this** caller is the one that must release it when the run stops.
    Acquired,
    /// A slot already exists for this run, held by a different attempt that is
    /// still live.
    ///
    /// Three things follow, and all three matter:
    ///   • the caller must not start the work — another attempt is running it;
    ///   • the caller must not release the slot — it belongs to a run that is
    ///     still executing, and releasing it would hand the tenant's headroom
    ///     to whoever asks next while the original work continues;
    ///   • this is not an error. A redelivery that arrives while the first
    ///     attempt still holds its lease is normal broker behaviour, and
    ///     deferring it is the correct response.
    HeldElsewhere,
    /// The tenant has no concurrency ceiling configured, so there is no slot
    /// to hold and nothing to release.
    ///
    /// This is a success. Refusing to run because a limit was never set would
    /// be the platform inventing a restriction; metering the run and not
    /// gating it is the documented behaviour of a dimension with no
    /// definition.
    NotMetered,
}

impl RunSlot {
    /// Whether the caller that received this decision owns a slot it must
    /// return.
    ///
    /// The one question the consumer has to answer on every exit path, and the
    /// reason [`Self::HeldElsewhere`] exists as a distinct state rather than
    /// being folded into an error: "release what you took" is a rule about
    /// ownership, and ownership is not the same thing as existence.
    #[must_use]
    pub const fn must_be_released(self) -> bool {
        matches!(self, Self::Acquired)
    }
}

/// The concurrency slot gate.
///
/// Every method is scoped to one `(tenant, organization, execution, task)`
/// tuple. The tenant and organization are what the runtime transaction arms
/// row-level security with — the slot is taken inside the tenant's own scope,
/// not by a role that can see across tenants — and the execution and task are
/// the identity of the slot itself: a retry of the same run resolves to the
/// same slot rather than taking a second one.
#[async_trait::async_trait]
pub trait RunSlotPort: Debug + Send + Sync {
    /// Takes a slot for one run, before the work starts.
    ///
    /// # Errors
    /// * `quota_exceeded` — the tenant is already running as many as its plan
    ///   allows. The caller must not start the run; the correct response is
    ///   to retry it later, not to proceed.
    /// * any infrastructure error — reported, and the caller must not start
    ///   the run either. Failing closed is the only safe reading of "I could
    ///   not tell whether this tenant has headroom".
    async fn acquire(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<RunSlot>;

    /// Says the run is still going, and extends the slot's lease.
    ///
    /// Called from the same heartbeat that renews the durable task lease, so
    /// the slot and the run it belongs to expire on the same clock. A no-op
    /// when no slot is held.
    async fn renew(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        until: &Timestamp,
        at: &Timestamp,
    ) -> Result<()>;

    /// Returns the slot: the run finished, failed, was deferred, or never
    /// started.
    ///
    /// Idempotent. A caller that cannot remember whether it acquired one —
    /// because it crashed in between, or because the tenant has no ceiling —
    /// must be safe calling this unconditionally.
    async fn release(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<()>;
}

/// The gate that holds nothing: the lane with no control plane wired.
///
/// Used by tests and by any composition root that has not been given a runtime
/// pool. It is deliberately not an error state: a worker that cannot meter
/// concurrency still runs work correctly, it simply does not gate it — and a
/// binary that *needs* gating fails at boot on the missing configuration
/// rather than here.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoRunSlots;

#[async_trait::async_trait]
impl RunSlotPort for NoRunSlots {
    async fn acquire(
        &self,
        _tenant: TenantId,
        _organization: OrganizationId,
        _execution: Option<ExecutionId>,
        _task: TaskId,
        _at: &Timestamp,
    ) -> Result<RunSlot> {
        Ok(RunSlot::NotMetered)
    }

    async fn renew(
        &self,
        _tenant: TenantId,
        _organization: OrganizationId,
        _execution: Option<ExecutionId>,
        _task: TaskId,
        _until: &Timestamp,
        _at: &Timestamp,
    ) -> Result<()> {
        Ok(())
    }

    async fn release(
        &self,
        _tenant: TenantId,
        _organization: OrganizationId,
        _execution: Option<ExecutionId>,
        _task: TaskId,
        _at: &Timestamp,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gate that records what it was asked, so the coordinator's contract
    /// can be asserted without a database.
    #[derive(Debug)]
    struct RecordingRunSlots {
        acquired: std::sync::Mutex<Vec<String>>,
        released: std::sync::Mutex<Vec<String>>,
        renewed: std::sync::Mutex<Vec<String>>,
        decision: RunSlot,
    }

    impl Default for RecordingRunSlots {
        fn default() -> Self {
            Self {
                acquired: std::sync::Mutex::new(Vec::new()),
                released: std::sync::Mutex::new(Vec::new()),
                renewed: std::sync::Mutex::new(Vec::new()),
                decision: RunSlot::NotMetered,
            }
        }
    }

    #[async_trait::async_trait]
    impl RunSlotPort for RecordingRunSlots {
        async fn acquire(
            &self,
            tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<ExecutionId>,
            task: TaskId,
            _at: &Timestamp,
        ) -> Result<RunSlot> {
            self.acquired
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(format!("{tenant}/{task}"));
            Ok(self.decision)
        }

        async fn renew(
            &self,
            tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<ExecutionId>,
            task: TaskId,
            _until: &Timestamp,
            _at: &Timestamp,
        ) -> Result<()> {
            self.renewed
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(format!("{tenant}/{task}"));
            Ok(())
        }

        async fn release(
            &self,
            tenant: TenantId,
            _organization: OrganizationId,
            _execution: Option<ExecutionId>,
            task: TaskId,
            _at: &Timestamp,
        ) -> Result<()> {
            self.released
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .push(format!("{tenant}/{task}"));
            Ok(())
        }
    }

    fn id(seed: u128) -> TaskId {
        TaskId::from_uuid(uuid::Uuid::from_u128(seed))
    }

    fn tenant(seed: u128) -> TenantId {
        TenantId::from_uuid(uuid::Uuid::from_u128(seed))
    }

    fn org(seed: u128) -> OrganizationId {
        OrganizationId::from_uuid(uuid::Uuid::from_u128(seed))
    }

    #[tokio::test]
    async fn the_absent_gate_holds_nothing_and_refuses_nothing() {
        let gate = NoRunSlots;
        assert_eq!(
            gate.acquire(tenant(1), org(1), None, id(1), &Timestamp::now())
                .await
                .expect("acquire"),
            RunSlot::NotMetered,
            "no control plane means no ceiling, not a refusal to run"
        );
        gate.renew(
            tenant(1),
            org(1),
            None,
            id(1),
            &Timestamp::now(),
            &Timestamp::now(),
        )
        .await
        .expect("renew");
        gate.release(tenant(1), org(1), None, id(1), &Timestamp::now())
            .await
            .expect("release");
    }

    #[tokio::test]
    async fn a_released_slot_is_released_exactly_once_per_acquire() {
        let gate = RecordingRunSlots {
            decision: RunSlot::Acquired,
            ..RecordingRunSlots::default()
        };
        let at = Timestamp::now();
        for task in [id(1), id(2)] {
            assert_eq!(
                gate.acquire(tenant(1), org(1), None, task, &at)
                    .await
                    .expect("acquire"),
                RunSlot::Acquired
            );
            gate.release(tenant(1), org(1), None, task, &at)
                .await
                .expect("release");
        }
        assert_eq!(
            gate.acquired
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            2,
            "one acquire per run"
        );
        assert_eq!(
            gate.released
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .len(),
            2,
            "and one release per run — a slot that is acquired twice but released \\
             once is a leak that only shows up as a tenant mysteriously at its ceiling"
        );
    }

    #[tokio::test]
    async fn releasing_without_acquiring_is_not_an_error() {
        let gate = RecordingRunSlots::default();
        // A worker that crashed between acquire and release, or a tenant with
        // no ceiling, must be safe calling release unconditionally.
        gate.release(tenant(1), org(1), None, id(1), &Timestamp::now())
            .await
            .expect("release");
        assert_eq!(
            gate.acquire(tenant(1), org(1), None, id(1), &Timestamp::now())
                .await
                .expect("acquire"),
            RunSlot::NotMetered
        );
    }
}
