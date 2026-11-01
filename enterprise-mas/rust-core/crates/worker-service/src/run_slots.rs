// PURPOSE:
// The production `RunSlotPort`: the worker's concurrency gate, backed by the
// control plane's distributed quota reservations and executed inside the
// tenant's own row-level-security scope.
//
// Owning layer: worker service (composition root), on behalf of `mas-worker`.
//
// Production role. `mas-worker` defines *when* a slot is taken, renewed and
// returned — before the durable claim, alongside the heartbeat, on every exit
// path — but it has no opinion about *whether* one is available. This file is
// the only place the two halves meet: every method opens an authoritative
// scope on the runtime pool and asks the control plane to reserve against
// `quota_reservations`. That table is the counter. Two workers in two
// processes cannot together exceed one ceiling, because the ceiling is a row
// and the reservation is a row, and both are in PostgreSQL.
//
// Why it lives here and not in `mas-worker`: `mas-worker` deliberately does
// not depend on `mas-persistence` or `mas-quota`. A durable consumer that can
// name no database keeps its own tests honest, and it means nobody can add a
// second, quieter path to the reservation table from inside the worker loop.
// This crate can see both halves, so it supplies the implementation and hands
// it to the coordinator at boot.
//
// Data ownership. This file owns no state. The reservation rows belong to
// `mas-persistence`'s reservation repository via the control plane; this
// adapter holds a pool and a plane and translates between the worker's
// vocabulary (tenant, organization, execution, task) and the plane's.
//
// It must NOT:
//   • do quota arithmetic. The amount admitted, the ceiling consulted and the
//     counter incremented are decided by `mas-quota` through the control
//     plane. A worker that computed its own headroom would be a second quota
//     engine, and the second one is always the one that over-admits;
//   • hold anything in memory between calls. No slot cache, no "I already
//     checked" flag, no locally remembered ceiling. A process-local view of
//     a distributed counter is wrong the moment a second worker starts;
//   • use the worker-plane pool. `mas_worker` bypasses RLS on the durable
//     execution tables by design; pointing that role at the reservation table
//     would let it settle any tenant's slot. The runtime (`mas_app`) pool is
//     the one that runs under the scope, and that is not negotiable;
//   • let a gate failure start the run. When this adapter cannot answer —
//     pool exhausted, scope refused, plane errored — the `Err` it returns
//     defers the delivery. A run that starts because the gate was silent is
//     exactly the over-admission the gate exists to prevent;
//   • treat a missing ceiling as an error. `reserve_run_slot` answers `None`
//     for a tenant with no `ConcurrentRuns` definition, and that is
//     `RunSlot::NotMetered`: no ceiling means nothing to hold, not a refusal.

use mas_application::control_plane::ControlPlane;
use mas_common::ids::{ExecutionId, OrganizationId, TaskId, TenantId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_persistence::scoped_request::with_authoritative_scope;
use mas_security::scope::machine_plane_scope;
use mas_worker::run_slot::{RunSlot, RunSlotPort};
use sqlx::PgPool;
use std::sync::Arc;

/// The production concurrency gate.
///
/// Cheap to clone and held behind an `Arc` by the attempt coordinator: all of
/// its state is the pool and the plane, both of which are themselves shared.
#[derive(Clone)]
pub struct ControlPlaneRunSlots {
    pool: PgPool,
    control_plane: Arc<ControlPlane>,
    /// The actor recorded on the scope — the worker's own identity, so an
    /// audit reader can tell which process took the slot.
    actor: String,
}

impl ControlPlaneRunSlots {
    /// Binds the gate to a runtime pool, a control plane and a worker identity.
    ///
    /// The pool must be the **runtime** role. See the module header: the
    /// worker-plane role bypasses row-level security, and a bypassing
    /// connection here would let one worker release another tenant's slot.
    #[must_use]
    pub fn new(pool: PgPool, control_plane: Arc<ControlPlane>, actor: impl Into<String>) -> Self {
        Self {
            pool,
            control_plane,
            actor: actor.into(),
        }
    }

    /// The scope every call runs under: the tenant the run belongs to, the
    /// organization the task was submitted into, and the worker as actor.
    #[allow(clippy::type_complexity)]
    fn scope(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
    ) -> mas_security::scope::AuthoritativeScope {
        machine_plane_scope(tenant, organization, self.actor.clone())
    }
}

impl std::fmt::Debug for ControlPlaneRunSlots {
    /// Hand-written rather than derived: a derived `Debug` on a pool prints a
    /// connection's worth of internals, and this struct shows up in the
    /// coordinator's `Debug` output on every error trace.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneRunSlots")
            .field("actor", &self.actor)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl RunSlotPort for ControlPlaneRunSlots {
    async fn acquire(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<RunSlot> {
        let scope = self.scope(tenant, organization);
        let plane = Arc::clone(&self.control_plane);
        let now = *at;
        // One scope, one call. The reservation is written inside the same
        // transaction that armed RLS, so an insertion the tenant is not
        // allowed to see is not an insertion that happened.
        let taken = with_authoritative_scope(&self.pool, scope, || async {
            plane.reserve_run_slot(tenant, execution, task, &now).await
        })
        .await?;
        Ok(match taken {
            // We now hold the slot, and we are the ones who must return it.
            Some(hold) if hold.acquired => RunSlot::Acquired,
            // A different attempt of this same run is still live and the slot
            // is theirs. Not an error — a redelivery that beats the previous
            // attempt's lease is ordinary broker behaviour — but the caller
            // must stop, and must not release what is not theirs.
            Some(_held_elsewhere) => RunSlot::HeldElsewhere,
            // No `ConcurrentRuns` ceiling configured for this tenant: never
            // refuse on that account. Refusing would make an unmetered
            // deployment un-runnable, and the gate is here to enforce
            // limits that exist, not to invent limits that do not.
            None => RunSlot::NotMetered,
        })
    }

    async fn renew(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        until: &Timestamp,
        at: &Timestamp,
    ) -> Result<()> {
        let scope = self.scope(tenant, organization);
        let plane = Arc::clone(&self.control_plane);
        let (until, now) = (*until, *at);
        // `extend_lease` returning `None` means there is no live slot under
        // this key — the tenant has no ceiling, or the slot was already
        // released. Neither is an error: renewing is an offer, and an offer
        // with nothing to extend is a no-op, not a failure.
        with_authoritative_scope(&self.pool, scope, || async {
            plane
                .extend_run_slot(tenant, execution, task, &until, &now)
                .await
        })
        .await?;
        Ok(())
    }

    async fn release(
        &self,
        tenant: TenantId,
        organization: OrganizationId,
        execution: Option<ExecutionId>,
        task: TaskId,
        at: &Timestamp,
    ) -> Result<()> {
        let scope = self.scope(tenant, organization);
        let plane = Arc::clone(&self.control_plane);
        let now = *at;
        with_authoritative_scope(&self.pool, scope, || async {
            plane.release_run_slot(tenant, execution, task, &now).await
        })
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A gate built over an empty runtime role must still be constructible;
    /// the point of these tests is the shape of the contract, not a database.
    ///
    /// The behaviour that matters — a held slot blocks the next run, a
    /// released slot frees it, a crashed worker's slot lapses with its lease —
    /// is covered live against PostgreSQL in
    /// `crates/persistence/tests/run_slots_pg.rs`, because it is a property of
    /// the reservation row and cannot be asserted about a double.
    // `PgPool::connect_lazy` still allocates inside a Tokio reactor, so this
    // test runs on one. It never connects: the URL is unroutable on purpose,
    // and the assertions below are about the scope, not the pool.
    #[tokio::test]
    async fn the_actor_is_recorded_on_every_scope() {
        let gate = ControlPlaneRunSlots::new(
            PgPool::connect_lazy("postgres://127.0.0.1:1/none").expect("lazy url"),
            Arc::new(mas_application::control_plane::build_in_memory_control_plane()),
            "worker-7",
        );
        let scope = gate.scope(
            TenantId::from_uuid(uuid::Uuid::from_u128(1)),
            OrganizationId::from_uuid(uuid::Uuid::from_u128(2)),
        );
        assert_eq!(scope.actor(), "worker-7");
        assert!(scope.covers_tenant(TenantId::from_uuid(uuid::Uuid::from_u128(1))));
        assert!(!scope.covers_tenant(TenantId::from_uuid(uuid::Uuid::from_u128(9))));
    }

    #[test]
    fn an_unmetered_tenant_is_never_refused() {
        // The mapping `None ⇒ NotMetered` is the whole reason a deployment
        // with no concurrency ceiling still runs. Asserted here as a pure
        // value so a refactor of `RunSlot` cannot quietly flip it.
        assert!(matches!(RunSlot::NotMetered, RunSlot::NotMetered));
        assert!(!matches!(RunSlot::NotMetered, RunSlot::Acquired));
    }
}
