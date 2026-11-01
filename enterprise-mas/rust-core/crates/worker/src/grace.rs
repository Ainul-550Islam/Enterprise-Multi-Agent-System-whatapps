//! Shutdown propagation: a `tokio::sync::watch` flipped by SIGINT/SIGTERM
//! (or explicitly by tests). The consumer stops fetching the moment the
//! flag flips and drains in-flight work before returning.

/// Receiving end: `changed()` resolves when shutdown begins; `is_set()`
/// polls cheaply inside hot loops.
#[derive(Debug, Clone)]
pub struct ShutdownWatch {
    pub(crate) flag: tokio::sync::watch::Receiver<bool>,
}

impl ShutdownWatch {
    /// True once shutdown has been requested.
    #[must_use]
    pub fn is_set(&self) -> bool {
        *self.flag.borrow()
    }

    /// Wait for the flag (returns immediately when already set).
    pub async fn changed(&mut self) {
        if *self.flag.borrow() {
            return;
        }
        let _ = self.flag.changed().await;
    }
}

/// The producer end — `trigger()` flips all watches.
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    flag: tokio::sync::watch::Sender<bool>,
}

impl ShutdownHandle {
    /// Requests shutdown on every watch.
    pub fn trigger(&self) {
        let _ = self.flag.send(true);
    }
}

/// Creates the (handle, watch) pair.
#[must_use]
pub fn channel() -> (ShutdownHandle, ShutdownWatch) {
    let (tx, rx) = tokio::sync::watch::channel(false);
    (ShutdownHandle { flag: tx }, ShutdownWatch { flag: rx })
}

/// Watches OS signals (ctrl-c + SIGTERM) and triggers the handle. Spawn
/// once per process.
pub async fn watch_signals(handle: ShutdownHandle) {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            tracing::error!(%err, "ctrl-c listener failed");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            },
            Err(err) => tracing::error!(%err, "sigterm listener failed"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => tracing::info!("worker shutdown requested (SIGINT)"),
        () = terminate => tracing::info!("worker shutdown requested (SIGTERM)"),
    }
    handle.trigger();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn watch_flips_and_changed_resolves() {
        let (handle, mut watch) = channel();
        assert!(!watch.is_set());
        let changed = tokio::spawn(async move {
            watch.changed().await;
            true
        });
        tokio::task::yield_now().await;
        handle.trigger();
        assert!(changed.await.expect("join"));
    }
}
