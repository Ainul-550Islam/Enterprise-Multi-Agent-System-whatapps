//! Generic state machine with an explicit transition table.
//!
//! Illegal transitions are data (never panics) and surface as
//! [`AppError::Conflict`]. States and events are plain enums — the machine is
//! deliberately free of async/infrastructure concerns.

use mas_common::error::AppError;
use mas_common::result::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

/// Declarative transition table: source state + event → target state.
///
/// Construct via [`TransitionTable::new`] / [`TransitionTable::allow`].
#[derive(Debug, Clone)]
pub struct TransitionTable<S, E>
where
    S: Copy + Eq + Hash + Debug,
    E: Copy + Eq + Hash + Debug,
{
    /// (from, event) → to
    transitions: HashMap<(S, E), S>,
    /// Every declared event per state (for diagnostics).
    events_by_state: HashMap<S, HashSet<E>>,
}

/// Serializable description of a single transition (useful for config-driven
/// tables and for validating hand-written ones).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transition<S, E> {
    pub from: S,
    pub event: E,
    pub to: S,
}

impl<S, E> Default for TransitionTable<S, E>
where
    S: Copy + Eq + Hash + Debug,
    E: Copy + Eq + Hash + Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S, E> TransitionTable<S, E>
where
    S: Copy + Eq + Hash + Debug,
    E: Copy + Eq + Hash + Debug,
{
    #[must_use]
    pub fn new() -> Self {
        Self {
            transitions: HashMap::new(),
            events_by_state: HashMap::new(),
        }
    }

    /// Adds a legal transition; self-transitions must be declared explicitly.
    #[must_use]
    pub fn allow(mut self, from: S, event: E, to: S) -> Self {
        self.transitions.insert((from, event), to);
        self.events_by_state.entry(from).or_default().insert(event);
        self
    }

    /// Bulk-adds transitions from a description list.
    #[must_use]
    pub fn from_transitions(mut self, transitions: &[Transition<S, E>]) -> Self {
        for transition in transitions {
            self.transitions
                .insert((transition.from, transition.event), transition.to);
            self.events_by_state
                .entry(transition.from)
                .or_default()
                .insert(transition.event);
        }
        self
    }

    /// The target for `(from, event)` if the transition is legal.
    #[must_use]
    pub fn target(&self, from: S, event: E) -> Option<S> {
        self.transitions.get(&(from, event)).copied()
    }

    /// Events declared for `state` (diagnostics / introspection).
    #[must_use]
    pub fn allowed_events(&self, state: S) -> Vec<E> {
        self.events_by_state
            .get(&state)
            .map(|events| events.iter().copied().collect())
            .unwrap_or_default()
    }
}

/// A running state machine: an initial state plus its table.
#[derive(Debug, Clone)]
pub struct StateMachine<S, E>
where
    S: Copy + Eq + Hash + Debug,
    E: Copy + Eq + Hash + Debug,
{
    table: TransitionTable<S, E>,
    current: S,
    /// Strictly increasing count of applied transitions (audit/debug).
    applied: u64,
}

impl<S, E> StateMachine<S, E>
where
    S: Copy + Eq + Hash + Debug,
    E: Copy + Eq + Hash + Debug,
{
    #[must_use]
    pub fn new(table: TransitionTable<S, E>, initial: S) -> Self {
        Self {
            table,
            current: initial,
            applied: 0,
        }
    }

    /// Current state.
    #[must_use]
    pub fn current_state(&self) -> S {
        self.current
    }

    /// Number of applied transitions.
    #[must_use]
    pub const fn transition_count(&self) -> u64 {
        self.applied
    }

    /// Whether `event` may fire from the current state.
    #[must_use]
    pub fn can_transition(&self, event: E) -> bool {
        self.table.target(self.current, event).is_some()
    }

    /// Target state for `event` from the current state, if legal.
    #[must_use]
    pub fn peek(&self, event: E) -> Option<S> {
        self.table.target(self.current, event)
    }

    /// Fires `event` and moves to the resulting state.
    ///
    /// # Errors
    /// [`AppError::Conflict`] when the transition is not declared — message
    /// includes both states and the event for debugging.
    pub fn transition(&mut self, event: E) -> Result<S> {
        let target = self.table.target(self.current, event).ok_or_else(|| {
            AppError::conflict(format!(
                "illegal transition: {:?} --({:?})--> ? (no transition declared)",
                self.current, event
            ))
        })?;
        self.current = target;
        self.applied += 1;
        Ok(target)
    }

    /// Fires `event` only if legal; otherwise this is a no-op returning `None`.
    pub fn try_transition(&mut self, event: E) -> Option<S> {
        let target = self.table.target(self.current, event)?;
        self.current = target;
        self.applied += 1;
        Some(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Door {
        Closed,
        Open,
        Locked,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum DoorEvent {
        Open,
        Close,
        Lock,
        Unlock,
    }

    fn table() -> TransitionTable<Door, DoorEvent> {
        TransitionTable::new()
            .allow(Door::Closed, DoorEvent::Open, Door::Open)
            .allow(Door::Open, DoorEvent::Close, Door::Closed)
            .allow(Door::Closed, DoorEvent::Lock, Door::Locked)
            .allow(Door::Locked, DoorEvent::Unlock, Door::Closed)
    }

    #[test]
    fn legal_transitions_move_state() {
        let mut machine = StateMachine::new(table(), Door::Closed);
        assert!(machine.can_transition(DoorEvent::Open));
        assert_eq!(machine.transition(DoorEvent::Open).unwrap(), Door::Open);
        assert_eq!(machine.current_state(), Door::Open);
        assert_eq!(machine.transition_count(), 1);
        assert!(!machine.can_transition(DoorEvent::Unlock));
        assert_eq!(machine.transition(DoorEvent::Close).unwrap(), Door::Closed);
    }

    #[test]
    fn illegal_transitions_are_conflicts_and_do_not_move() {
        let mut machine = StateMachine::new(table(), Door::Locked);
        let err = machine.transition(DoorEvent::Open).unwrap_err();
        assert!(matches!(err, AppError::Conflict(_)));
        assert_eq!(err.error_code(), "CONFLICT");
        assert_eq!(machine.current_state(), Door::Locked);
        assert_eq!(machine.transition_count(), 0);
    }

    #[test]
    fn bulk_construction_and_introspection() {
        let table = TransitionTable::new().from_transitions(&[
            Transition {
                from: Door::Closed,
                event: DoorEvent::Open,
                to: Door::Open,
            },
            Transition {
                from: Door::Open,
                event: DoorEvent::Close,
                to: Door::Closed,
            },
        ]);
        assert_eq!(
            table.target(Door::Closed, DoorEvent::Open),
            Some(Door::Open)
        );
        assert!(table.target(Door::Open, DoorEvent::Lock).is_none());
        assert!(table
            .allowed_events(Door::Closed)
            .contains(&DoorEvent::Open));
    }
}
