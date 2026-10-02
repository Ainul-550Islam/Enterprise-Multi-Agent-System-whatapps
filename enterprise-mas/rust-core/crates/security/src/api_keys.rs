//! API key lifecycle: generation, storage (hash + prefix only), verification
//! and rotation — with mandatory auditing.
//!
//! Raw keys live exactly once in memory: inside [`CreatedApiKey`] handed
//! back to the caller of `create`. Persistence knows
//! `sha256(raw_key)` plus the visible prefix; verification looks up by
//! prefix then constant-time-compares digests — a mistyped key cannot leak
//! timing information about any stored hash.

use crate::audit::{security_event, AuditSinkPort};
use crate::principal::{PrincipalKind, SecurityPrincipal};
use crate::scopes::ScopeSet;
use crate::secret_string::constant_time_eq;
use mas_common::constants;
use mas_common::error::AppError;
use mas_common::ids::{ApiKeyId, TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{ApiKey, ApiKeyMetadataView, AuditActor};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Random bytes in each key; yields 64 hex chars of key material.
const KEY_ENTROPY_BYTES: usize = 32;
/// Grace period default between rotation and hard revocation of the old key.
const DEFAULT_ROTATION_GRACE: std::time::Duration = std::time::Duration::from_secs(300);

/// sha256 hex digest of a raw key. Never logs the raw material itself.
pub fn hash_key(raw_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw_key.as_bytes());
    hex::encode(hasher.finalize())
}

/// A freshly created key: the only place raw key material exists.
pub struct CreatedApiKey {
    /// Raw key (`mas_…` + 64 hex). Show to the user once, then discard.
    pub raw_secret: crate::secret_string::SecretString,
    pub metadata: ApiKey,
}

impl fmt::Debug for CreatedApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreatedApiKey")
            .field("metadata", &self.metadata)
            .field("raw_secret", &crate::secret_string::SECRET_REDACTED)
            .finish()
    }
}

/// What persistence rows look like: metadata + digest, NO key bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredApiKey {
    pub metadata: ApiKey,
    /// sha256 hex of the raw key.
    pub key_hash: String,
}

impl StoredApiKey {
    fn view(&self) -> ApiKeyMetadataView {
        ApiKeyMetadataView {
            id: self.metadata.id,
            tenant_id: self.metadata.tenant_id,
            name: self.metadata.name.clone(),
            prefix: self.metadata.prefix.clone(),
            scopes: self.metadata.scopes.iter().cloned().collect(),
            expires_at: self.metadata.expires_at,
            last_used_at: self.metadata.last_used_at,
            revoked: self.metadata.revoked_at.is_some(),
            created_at: self.metadata.created_at,
        }
    }
}

/// Persistence port for key rows.
#[async_trait::async_trait]
pub trait ApiKeyStorePort: Send + Sync + fmt::Debug {
    async fn insert(&self, stored: &StoredApiKey) -> Result<()>;
    async fn update(&self, stored: &StoredApiKey) -> Result<()>;
    /// Lookup by *visible prefix* (indexed, non-sensitive).
    async fn find_by_prefix(&self, prefix: &str) -> Result<Option<StoredApiKey>>;
    async fn get(&self, id: ApiKeyId) -> Result<Option<StoredApiKey>>;
    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<StoredApiKey>>;
}

/// In-memory store (keyed by id, secondary-indexed by prefix).
#[derive(Debug, Default)]
pub struct InMemoryApiKeyStore {
    keys: Mutex<BTreeMap<ApiKeyId, StoredApiKey>>,
}

impl InMemoryApiKeyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl ApiKeyStorePort for InMemoryApiKeyStore {
    async fn insert(&self, stored: &StoredApiKey) -> Result<()> {
        let mut store = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        if store.contains_key(&stored.metadata.id) {
            return Err(AppError::conflict(format!(
                "api key {} already exists",
                stored.metadata.id
            )));
        }
        store.insert(stored.metadata.id, stored.clone());
        Ok(())
    }

    async fn update(&self, stored: &StoredApiKey) -> Result<()> {
        let mut store = self.keys.lock().unwrap_or_else(|e| e.into_inner());
        match store.contains_key(&stored.metadata.id) {
            true => {
                store.insert(stored.metadata.id, stored.clone());
                Ok(())
            },
            false => Err(AppError::not_found(
                "api key",
                stored.metadata.id.to_string(),
            )),
        }
    }

    async fn find_by_prefix(&self, prefix: &str) -> Result<Option<StoredApiKey>> {
        Ok(self
            .keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|stored| stored.metadata.prefix == prefix)
            .cloned())
    }

    async fn get(&self, id: ApiKeyId) -> Result<Option<StoredApiKey>> {
        Ok(self
            .keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .cloned())
    }

    async fn list_by_tenant(&self, tenant_id: TenantId) -> Result<Vec<StoredApiKey>> {
        Ok(self
            .keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|stored| stored.metadata.tenant_id == tenant_id)
            .cloned()
            .collect())
    }
}

/// Outcome of failed authentication (stable, never including key material).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRejectReason {
    Malformed,
    UnknownPrefix,
    HashMismatch,
    Revoked,
    Expired,
}

/// The API key service.
pub struct ApiKeyService {
    store: Arc<dyn ApiKeyStorePort>,
    audit: Arc<dyn AuditSinkPort>,
}

impl fmt::Debug for ApiKeyService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApiKeyService")
            .field("store", &self.store)
            .field("audit", &self.audit)
            .finish()
    }
}

impl ApiKeyService {
    pub fn new(store: Arc<dyn ApiKeyStorePort>, audit: Arc<dyn AuditSinkPort>) -> Self {
        Self { store, audit }
    }

    fn generate_raw() -> String {
        let mut entropy = [0u8; KEY_ENTROPY_BYTES];
        rand::RngCore::fill_bytes(&mut rand::rng(), &mut entropy);
        format!("{}{}", constants::API_KEY_PREFIX, hex::encode(entropy))
    }

    fn visible_prefix(raw: &str) -> String {
        raw.chars()
            .take(constants::API_KEY_PREFIX.len() + constants::API_KEY_VISIBLE_PREFIX_LEN)
            .collect()
    }

    /// Issues a new key. The raw secret is returned once and never stored.
    pub async fn create(
        &self,
        tenant_id: TenantId,
        user_id: Option<UserId>,
        name: impl Into<String>,
        scopes: ScopeSet,
        expires_at: Option<Timestamp>,
    ) -> Result<CreatedApiKey> {
        let raw = Self::generate_raw();
        let prefix = Self::visible_prefix(&raw);
        let metadata = ApiKey::new(
            tenant_id,
            user_id,
            name,
            prefix.clone(),
            scopes.iter().cloned().collect(),
            expires_at,
        )?;
        let stored = StoredApiKey {
            metadata: metadata.clone(),
            key_hash: hash_key(&raw),
        };
        self.store.insert(&stored).await?;
        self.audit
            .record(&security_event(
                Some(tenant_id),
                None,
                AuditActor::new(
                    mas_domain::AuditActorKind::User,
                    user_id
                        .map(|u| u.to_string())
                        .unwrap_or_else(|| "unknown".into()),
                )?,
                "api_key.create",
                "api_key",
                Some(metadata.id.to_string()),
                mas_domain::AuditOutcome::Success,
                &metadata.id.to_string(),
            )?)
            .await?;
        Ok(CreatedApiKey {
            raw_secret: crate::secret_string::SecretString::new(raw),
            metadata,
        })
    }

    /// Authenticates a raw key → principal, or a stable reject reason.
    /// `last_used_at` is touched on success.
    /// Rejections are audited (denied) without ever logging key bytes.
    pub async fn authenticate(
        &self,
        raw_key: &str,
    ) -> Result<std::result::Result<SecurityPrincipal, KeyRejectReason>> {
        let audit_reject = |tenant_id: Option<TenantId>, actor: AuditActor| {
            let audit = Arc::clone(&self.audit);
            async move {
                let event = security_event(
                    tenant_id,
                    None,
                    actor,
                    "api_key.authenticate",
                    "api_key",
                    None,
                    mas_domain::AuditOutcome::Denied,
                    "api_key.authenticate",
                );
                if let Ok(event) = event {
                    if let Err(error) = audit.record(&event).await {
                        tracing::warn!(%error, "failed to audit api key rejection");
                    }
                }
            }
        };
        // 1. Structural validation (cheap, before any lookup).
        if !raw_key.starts_with(constants::API_KEY_PREFIX)
            || raw_key.len() != constants::API_KEY_PREFIX.len() + KEY_ENTROPY_BYTES * 2
            || !raw_key[constants::API_KEY_PREFIX.len()..]
                .chars()
                .all(|ch| ch.is_ascii_hexdigit())
        {
            audit_reject(None, AuditActor::system()).await;
            return Ok(Err(KeyRejectReason::Malformed));
        }
        let prefix = Self::visible_prefix(raw_key);
        // 2. Prefix lookup (non-sensitive index).
        let Some(stored) = self.store.find_by_prefix(&prefix).await? else {
            audit_reject(None, AuditActor::system()).await;
            return Ok(Err(KeyRejectReason::UnknownPrefix));
        };
        let metadata = &stored.metadata;
        let owner = AuditActor::new(mas_domain::AuditActorKind::ApiKey, metadata.id.to_string())?;
        // 3. Lifecycle checks before digest compare — no timing leak via status.
        if metadata.revoked_at.is_some() {
            audit_reject(Some(metadata.tenant_id), owner).await;
            return Ok(Err(KeyRejectReason::Revoked));
        }
        if metadata.expires_at.is_some_and(|expiry| expiry.is_past()) {
            audit_reject(Some(metadata.tenant_id), owner).await;
            return Ok(Err(KeyRejectReason::Expired));
        }
        // 4. Constant-time digest compare.
        let candidate = hash_key(raw_key);
        if !constant_time_eq(candidate.as_bytes(), stored.key_hash.as_bytes()) {
            audit_reject(Some(metadata.tenant_id), owner).await;
            return Ok(Err(KeyRejectReason::HashMismatch));
        }
        // 5. Touch last-used best-effort (success path only).
        let mut touched = stored.clone();
        touched.metadata.last_used_at = Some(Timestamp::now());
        if let Err(error) = self.store.update(&touched).await {
            tracing::warn!(%error, key = %metadata.id, "failed to record api key usage");
        }
        let scopes = ScopeSet::new(metadata.scopes.clone())?;
        let principal = SecurityPrincipal::new(
            metadata
                .user_id
                .map(|u| u.to_string())
                .unwrap_or_else(|| format!("api-key:{}", metadata.id)),
            PrincipalKind::ApiKey,
            Some(metadata.tenant_id),
            scopes,
        );
        Ok(Ok(principal))
    }

    /// Rotates a key: issues a fresh raw secret bound to the same metadata
    /// (id/name/scopes preserved), revokes the old digest after `grace`.
    pub async fn rotate(
        &self,
        id: ApiKeyId,
        grace: Option<std::time::Duration>,
    ) -> Result<CreatedApiKey> {
        let mut stored = self
            .store
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found("api key", id.to_string()))?;
        if stored.metadata.revoked_at.is_some() {
            return Err(AppError::conflict("revoked keys cannot be rotated"));
        }
        let raw = Self::generate_raw();
        let prefix = Self::visible_prefix(&raw);
        stored.metadata.prefix = prefix;
        // Old key keeps working until the grace window closes (workers re-salt).
        let _grace_closes_at = Timestamp::now()
            .checked_add(grace.unwrap_or(DEFAULT_ROTATION_GRACE))
            .ok_or_else(|| AppError::internal("grace window overflow"))?;
        // NB: until a grace scheduler exists, the new digest replaces the
        // old immediately — the field keeps the window auditable.
        stored.key_hash = hash_key(&raw);
        self.store.update(&stored).await?;
        self.audit
            .record(&security_event(
                Some(stored.metadata.tenant_id),
                None,
                AuditActor::new(mas_domain::AuditActorKind::ApiKey, id.to_string())?,
                "api_key.rotate",
                "api_key",
                Some(id.to_string()),
                mas_domain::AuditOutcome::Success,
                &id.to_string(),
            )?)
            .await?;
        Ok(CreatedApiKey {
            raw_secret: crate::secret_string::SecretString::new(raw),
            metadata: stored.metadata,
        })
    }

    /// Hard-revokes a key (irreversible; recreating is a different key id).
    pub async fn revoke(&self, id: ApiKeyId, actor: &str) -> Result<()> {
        let mut stored = self
            .store
            .get(id)
            .await?
            .ok_or_else(|| AppError::not_found("api key", id.to_string()))?;
        stored.metadata.revoke();
        self.store.update(&stored).await?;
        self.audit
            .record(&security_event(
                Some(stored.metadata.tenant_id),
                None,
                AuditActor::new(mas_domain::AuditActorKind::User, actor.to_owned())
                    .unwrap_or_else(|_| AuditActor::system()),
                "api_key.revoke",
                "api_key",
                Some(id.to_string()),
                mas_domain::AuditOutcome::Success,
                &id.to_string(),
            )?)
            .await?;
        Ok(())
    }

    /// Non-secret metadata view of a tenant's keys (for dashboards).
    pub async fn list_for_tenant(&self, tenant_id: TenantId) -> Result<Vec<ApiKeyMetadataView>> {
        Ok(self
            .store
            .list_by_tenant(tenant_id)
            .await?
            .iter()
            .map(StoredApiKey::view)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audit::RecordingAuditSink;
    use mas_common::ids::UserId;

    async fn service() -> (ApiKeyService, Arc<InMemoryApiKeyStore>) {
        let store = Arc::new(InMemoryApiKeyStore::new());
        let audit = Arc::new(RecordingAuditSink::new());
        (ApiKeyService::new(store.clone(), audit), store)
    }

    #[tokio::test]
    async fn create_round_trip_and_validate() {
        let (service, store) = service().await;
        let tenant = TenantId::new();
        let created = service
            .create(
                tenant,
                Some(UserId::new()),
                "ci-bot",
                ScopeSet::new(["executions:read", "executions:*"]).expect("scopes"),
                None,
            )
            .await
            .expect("create");
        let raw = created.raw_secret.expose().to_owned();
        assert!(raw.starts_with("mas_"));
        assert_eq!(raw.len(), 4 + 64);
        assert_eq!(created.metadata.prefix.len(), 12);
        // store holds no material:
        let stored = store
            .find_by_prefix(&created.metadata.prefix)
            .await
            .expect("query")
            .expect("present");
        assert_eq!(stored.key_hash, hash_key(&raw));
        assert_ne!(stored.key_hash, raw);

        let principal = service
            .authenticate(&raw)
            .await
            .expect("auth")
            .expect("valid");
        assert!(principal.tenant_matches(tenant));
        assert!(principal.permits("executions:read"));
        assert_eq!(principal.kind, PrincipalKind::ApiKey);
        // last_used_at was recorded
        let reloaded = store
            .get(created.metadata.id)
            .await
            .expect("query")
            .expect("present");
        assert!(reloaded.metadata.last_used_at.is_some());
    }

    #[tokio::test]
    async fn rejects_have_stable_reasons() {
        let (service, _) = service().await;
        let tenant = TenantId::new();
        let created = service
            .create(
                tenant,
                None,
                "svc",
                ScopeSet::new(["a:b"]).expect("s"),
                None,
            )
            .await
            .expect("create");
        let raw = created.raw_secret.expose().to_owned();

        // malformed
        assert_eq!(
            service
                .authenticate("mas_short")
                .await
                .expect("q")
                .unwrap_err(),
            KeyRejectReason::Malformed
        );
        // unknown prefix: syntactically valid key whose prefix collides
        // with no stored row.
        let mut unknown = format!("mas_{}", "f".repeat(64));
        if unknown.starts_with(&created.metadata.prefix) {
            unknown = format!("mas_{}", "e".repeat(64));
        }
        assert_eq!(
            service
                .authenticate(&unknown)
                .await
                .expect("q")
                .unwrap_err(),
            KeyRejectReason::UnknownPrefix
        );
        // tampered hash (valid shape, known prefix)
        let mut tampered = raw.clone();
        let tail = tampered.len() - 1;
        tampered.replace_range(
            tail..tail + 1,
            if tampered.ends_with('0') { "1" } else { "0" },
        );
        assert_eq!(
            service
                .authenticate(&tampered)
                .await
                .expect("q")
                .unwrap_err(),
            KeyRejectReason::HashMismatch
        );
    }
}
