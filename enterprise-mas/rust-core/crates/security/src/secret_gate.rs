//! The secret-resolution gate: domain `SecretReference` → material, with
//! lifecycle enforcement, optional TTL caching and mandatory auditing of
//! every resolution attempt (success AND failure — never the material).

use crate::audit::{security_event, AuditSinkPort};
use crate::secret_string::SecretString;
use mas_common::error::AppError;
use mas_common::ids::{SecretReferenceId, TenantId};
use mas_common::result::Result;
use mas_domain::{AuditActor, AuditOutcome, SecretReference};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Backend resolver port — implemented by the integrations crate(s) for
/// Vault/KMS/cloud managers; the in-memory one is dev/test only.
#[async_trait::async_trait]
pub trait SecretResolverPort: Send + Sync + fmt::Debug {
    /// Resolves a reference to its material (`None` = unknown reference).
    async fn resolve(&self, reference: &SecretReference) -> Result<Option<SecretString>>;
}

/// Dev/test resolver holding material in process memory by path.
#[derive(Debug, Default)]
pub struct InMemorySecretResolver {
    secrets: Mutex<HashMap<String, SecretString>>,
}

impl InMemorySecretResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, path: impl Into<String>, value: impl Into<String>) {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.into(), SecretString::new(value.into()));
    }

    pub fn remove(&self, path: &str) {
        self.secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(path);
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.secrets.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

#[async_trait::async_trait]
impl SecretResolverPort for InMemorySecretResolver {
    async fn resolve(&self, reference: &SecretReference) -> Result<Option<SecretString>> {
        Ok(self
            .secrets
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&reference.path)
            .cloned())
    }
}

struct CacheEntry {
    secret: SecretString,
    expires_at: mas_common::timestamps::Timestamp,
}

/// The one sanctioned way to turn references into material.
pub struct SecretGate {
    resolver: Arc<dyn SecretResolverPort>,
    audit: Arc<dyn AuditSinkPort>,
    ttl: Option<Duration>,
    cache: Mutex<HashMap<(SecretReferenceId, String), CacheEntry>>,
    max_cache: usize,
}

impl fmt::Debug for SecretGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretGate")
            .field("resolver", &self.resolver)
            .field("audit", &self.audit)
            .field("ttl", &self.ttl)
            .field("cached", &self.cache.lock().map(|c| c.len()).unwrap_or(0))
            .finish()
    }
}

impl SecretGate {
    pub fn new(resolver: Arc<dyn SecretResolverPort>, audit: Arc<dyn AuditSinkPort>) -> Self {
        Self {
            resolver,
            audit,
            ttl: None,
            cache: Mutex::new(HashMap::new()),
            max_cache: 10_000,
        }
    }

    /// Enables caching of *resolved material* for `ttl` — use only in hot
    /// paths with a threat-balanced TTL.
    #[must_use]
    pub const fn with_material_cache(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    #[must_use]
    pub const fn with_max_cache_entries(mut self, max_cache: usize) -> Self {
        self.max_cache = max_cache;
        self
    }

    /// Resolves a reference (audit by `actor`, under `purpose`).
    pub async fn resolve_for(
        &self,
        reference: &SecretReference,
        purpose: &str,
        tenant_id: Option<TenantId>,
        actor: &AuditActor,
        correlation_id: &str,
    ) -> Result<SecretString> {
        mas_common::validation::validate_non_empty("purpose", purpose)?;
        // 1. Lifecycle first: retired/deleted references fail closed.
        if !reference.is_readable() {
            self.audit_access(
                reference,
                purpose,
                tenant_id,
                actor,
                correlation_id,
                AuditOutcome::Denied,
            )
            .await;
            return Err(AppError::forbidden(format!(
                "secret reference {} is not readable (availability {:?}, rotation {:?})",
                reference.id, reference.availability, reference.rotation_state
            )));
        }
        // 2. Cache — probe briefly and drop the lock BEFORE any await.
        let key = (reference.id, reference.path.clone());
        let cached = if self.ttl.is_some() {
            self.cache
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .filter(|entry| entry.expires_at.is_future())
                .map(|entry| entry.secret.clone())
        } else {
            None
        };
        if let Some(secret) = cached {
            self.audit_access(
                reference,
                purpose,
                tenant_id,
                actor,
                correlation_id,
                AuditOutcome::Success,
            )
            .await;
            return Ok(secret);
        }
        // 3. Resolve.
        let resolved = self.resolver.resolve(reference).await;
        match resolved {
            Ok(Some(secret)) => {
                self.audit_access(
                    reference,
                    purpose,
                    tenant_id,
                    actor,
                    correlation_id,
                    AuditOutcome::Success,
                )
                .await;
                if let Some(ttl) = self.ttl {
                    self.insert_cache(key, secret.clone(), ttl);
                }
                Ok(secret)
            },
            Ok(None) => {
                self.audit_access(
                    reference,
                    purpose,
                    tenant_id,
                    actor,
                    correlation_id,
                    AuditOutcome::Denied,
                )
                .await;
                Err(AppError::not_found(
                    "secret",
                    format!("reference {} (path redacted)", reference.id),
                ))
            },
            Err(error) => {
                self.audit_access(
                    reference,
                    purpose,
                    tenant_id,
                    actor,
                    correlation_id,
                    AuditOutcome::Failure,
                )
                .await;
                Err(error)
            },
        }
    }

    fn insert_cache(&self, key: (SecretReferenceId, String), secret: SecretString, ttl: Duration) {
        let Some(expiry) = mas_common::timestamps::Timestamp::now().checked_add(ttl) else {
            tracing::warn!("secret cache TTL overflow; skipping cache insert");
            return;
        };
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.len() >= self.max_cache {
            // Sweep expired; else refuse to grow (fail open toward fresh resolve).
            cache.retain(|_, entry| entry.expires_at.is_future());
            if cache.len() >= self.max_cache {
                return;
            }
        }
        cache.insert(
            key,
            CacheEntry {
                secret,
                expires_at: expiry,
            },
        );
    }

    /// Drops any cached material for a reference (on rotation endpoints).
    pub fn invalidate(&self, reference_id: SecretReferenceId) {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|(id, _), _| *id != reference_id);
    }

    async fn audit_access(
        &self,
        reference: &SecretReference,
        purpose: &str,
        tenant_id: Option<TenantId>,
        actor: &AuditActor,
        correlation_id: &str,
        outcome: AuditOutcome,
    ) {
        let event = security_event(
            tenant_id,
            None,
            actor.clone(),
            "secret.resolve",
            "secret_reference",
            Some(reference.id.to_string()),
            outcome,
            correlation_id,
        )
        .map(|event| {
            event.with_metadata(serde_json::Map::from_iter([
                ("purpose".to_owned(), serde_json::Value::from(purpose)),
                (
                    "provider".to_owned(),
                    serde_json::Value::from(reference.provider.to_string()),
                ),
            ]))
        });
        match event {
            Ok(event) => {
                if let Err(error) = self.audit.record(&event).await {
                    tracing::warn!(%error, "failed to audit secret resolution");
                }
            },
            Err(error) => tracing::warn!(%error, "failed to build secret audit event"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::RecordingAuditSink;
    use mas_domain::SecretProviderKind;

    async fn gate() -> (
        SecretGate,
        Arc<InMemorySecretResolver>,
        Arc<RecordingAuditSink>,
    ) {
        let resolver = Arc::new(InMemorySecretResolver::new());
        let audit = Arc::new(RecordingAuditSink::new());
        (
            SecretGate::new(resolver.clone(), audit.clone()),
            resolver,
            audit,
        )
    }

    fn make_ref(path: &str) -> SecretReference {
        SecretReference::new(SecretProviderKind::Vault, path, None).expect("reference")
    }

    #[tokio::test]
    async fn resolves_only_readable_references_and_audits_both_outcomes() {
        let (gate, resolver, audit) = gate().await;
        resolver.set("secret/slack", "xoxb-real");
        let reference = make_ref("secret/slack");
        let tenant = TenantId::new();
        let actor = AuditActor::system();

        let secret = gate
            .resolve_for(&reference, "notify.webhook", Some(tenant), &actor, "corr-1")
            .await
            .expect("resolve");
        assert_eq!(secret.expose(), "xoxb-real");
        assert_eq!(audit.count_action("secret.resolve"), 1);

        // Retired reference → denied + audited.
        let mut retired = make_ref("secret/old");
        retired.retire();
        assert!(gate
            .resolve_for(&retired, "anything", Some(tenant), &actor, "corr-2")
            .await
            .is_err());
        // Unknown path → not found + audited.
        assert!(gate
            .resolve_for(
                &make_ref("secret/missing"),
                "anything",
                Some(tenant),
                &actor,
                "corr-3"
            )
            .await
            .is_err());
        let denied_and_failed = audit
            .events()
            .iter()
            .filter(|e| e.action == "secret.resolve" && e.outcome != AuditOutcome::Success)
            .count();
        assert_eq!(denied_and_failed, 2);
        // Audit metadata carries purpose; NEVER the material.
        let events = audit.events();
        let event = events
            .iter()
            .find(|e| e.action == "secret.resolve")
            .expect("event");
        assert_eq!(
            event.metadata.get("purpose"),
            Some(&serde_json::Value::from("notify.webhook"))
        );
        assert!(!event
            .metadata
            .values()
            .any(|v| v.to_string().contains("xoxb")));
    }

    #[tokio::test]
    async fn material_cache_hits_without_re_resolving_and_invalidates() {
        let resolver = Arc::new(InMemorySecretResolver::new());
        let audit = Arc::new(RecordingAuditSink::new());
        let gate = SecretGate::new(resolver.clone(), audit.clone())
            .with_material_cache(Duration::from_secs(60));
        resolver.set("secret/cache", "cached-value");
        let reference = make_ref("secret/cache");
        let actor = AuditActor::system();

        let first = gate
            .resolve_for(&reference, "read", None, &actor, "c1")
            .await
            .expect("first");
        // Backend change → cache must still serve the old material.
        resolver.set("secret/cache", "changed-value");
        let second = gate
            .resolve_for(&reference, "read", None, &actor, "c2")
            .await
            .expect("cached");
        assert_eq!(first.expose(), second.expose());
        gate.invalidate(reference.id);
        let third = gate
            .resolve_for(&reference, "read", None, &actor, "c3")
            .await
            .expect("re-resolved");
        assert_eq!(third.expose(), "changed-value");
        // Successful resolutions are audited even on cache hits.
        assert_eq!(audit.count_action("secret.resolve"), 3);
    }
}
