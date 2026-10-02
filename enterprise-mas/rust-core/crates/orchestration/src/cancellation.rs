//! Cooperative cancellation propagation.
//!
//! A [`CancellationToken`] is cancelled once, is cheap to clone, and cancels
//! its whole subtree: API request → execution → workflow node → tool/agent
//! invocation.

use mas_common::error::AppError;
use mas_common::result::Result;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug)]
struct Shared {
    /// Guarded by `cancel()`; children are notified recursively.
    cancelled: AtomicBool,
    /// Wakes tasks awaiting cancellation.
    notify: tokio::sync::Notify,
    /// Subtree tokens (child execution/node scopes).
    children: Mutex<Vec<Weak<Shared>>>,
}

impl Shared {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
            children: Mutex::new(Vec::new()),
        }
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    fn cancel_tree(&self) {
        // Only the first cancellation does the work.
        if self.cancelled.swap(true, Ordering::SeqCst) {
            return;
        }
        self.notify.notify_waiters();
        let children: Vec<Arc<Shared>> = {
            let mut guard = self.children.lock().unwrap_or_else(|e| e.into_inner());
            let live: Vec<Arc<Shared>> = guard.iter().filter_map(Weak::upgrade).collect();
            guard.clear();
            live
        };
        for child in children {
            child.cancel_tree();
        }
    }
}

/// Hierarchical cooperative cancellation token.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    shared: Arc<Shared>,
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl CancellationToken {
    /// A fresh, uncancelled root token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Shared::new()),
        }
    }

    /// A child token: cancelled when this token (or any ancestor) cancels,
    /// but cancelling the child never affects the parent.
    #[must_use]
    pub fn child(&self) -> Self {
        let child = Arc::new(Shared::new());
        // If parent is already cancelled, start cancelled without registration.
        if self.shared.is_cancelled() {
            child.cancel_tree();
            return Self { shared: child };
        }
        self.shared
            .children
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(Arc::downgrade(&child));
        Self { shared: child }
    }

    /// Requests cancellation of this token and its whole subtree.
    /// Idempotent — safe to call from multiple triggers.
    pub fn cancel(&self) {
        self.shared.cancel_tree();
    }

    /// Whether cancellation was requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.shared.is_cancelled()
    }

    /// Fails with [`AppError::Cancelled`] when cancelled.
    pub fn throw_if_cancelled(&self) -> Result<()> {
        if self.is_cancelled() {
            return Err(AppError::cancelled("operation was cancelled"));
        }
        Ok(())
    }

    /// Resolves once cancellation is requested (immediately if already
    /// cancelled). Does not consume the token.
    pub async fn cancelled(&self) {
        if self.shared.is_cancelled() {
            return;
        }
        // `notify_waiters` wake-ups are sticky per registered waiter: enabling
        // the future before checking closes the lost-wakeup race between the
        // check above and the await.
        let notified = self.shared.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if self.shared.is_cancelled() {
            return;
        }
        notified.await;
    }

    /// Runs `future` to completion or abandons it on cancellation, returning
    /// [`AppError::Cancelled`] in the latter case.
    pub async fn run_until_cancelled<F, T>(&self, future: F) -> Result<T>
    where
        F: Future<Output = T>,
    {
        tokio::select! {
            biased;
            () = self.cancelled() => Err(AppError::cancelled("operation aborted by cancellation")),
            output = future => Ok(output),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn cancel_is_visible_and_idempotent() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        token.cancel(); // must not panic or misbehave
        assert!(token.is_cancelled());
        assert!(token.throw_if_cancelled().is_err());
        // Waiting resolves immediately on an already-cancelled token.
        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .expect("cancelled wait must resolve");
    }

    #[tokio::test]
    async fn parent_cancels_children_but_not_vice_versa() {
        let parent = CancellationToken::new();
        let child = parent.child();
        let grandchild = child.child();

        child.cancel();
        assert!(
            !parent.is_cancelled(),
            "child cancellation must not reach parent"
        );
        assert!(child.is_cancelled());
        assert!(
            grandchild.is_cancelled(),
            "cancellation must reach grandchildren"
        );

        let parent2 = CancellationToken::new();
        let child2 = parent2.child();
        parent2.cancel();
        assert!(child2.is_cancelled());
    }

    #[tokio::test]
    async fn child_of_pre_cancelled_parent_starts_cancelled() {
        let parent = CancellationToken::new();
        parent.cancel();
        assert!(parent.child().is_cancelled());
    }

    #[tokio::test]
    async fn run_until_cancelled_yields_cancelled_error() {
        let token = CancellationToken::new();
        let trigger = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            trigger.cancel();
        });
        let outcome = token
            .run_until_cancelled(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                1_u8
            })
            .await;
        assert_eq!(outcome.unwrap_err().error_code(), "CANCELLED");
    }

    #[tokio::test]
    async fn run_until_cancelled_passes_through_completed_work() {
        let token = CancellationToken::new();
        let value = token.run_until_cancelled(async { 7_u8 }).await.unwrap();
        assert_eq!(value, 7);
    }
}
