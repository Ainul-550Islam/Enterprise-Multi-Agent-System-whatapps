//! Tenant-bound API-key use cases over the canonical security key service.

use std::sync::Arc;
use std::time::Duration;

use mas_common::error::AppError;
use mas_common::ids::ApiKeyId;
use mas_common::result::Result;
use mas_contracts::api_key::{ApiKeyMetadataDto, CreatedApiKeyDto};
use mas_domain::MembershipRole;
use mas_security::{ApiKeyService, ApiKeyStorePort, ScopeSet};

use crate::context::ServiceContext;

/// Tenant-scoped wrapper; the underlying security service owns generation,
/// hashing, constant-time verification, rotation and security audit events.
#[derive(Debug)]
pub struct ApiKeyApplicationService {
    keys: Arc<ApiKeyService>,
    store: Arc<dyn ApiKeyStorePort>,
}

impl ApiKeyApplicationService {
    /// Wires the one canonical security service and its matching persistence port.
    pub fn new(keys: Arc<ApiKeyService>, store: Arc<dyn ApiKeyStorePort>) -> Self {
        Self { keys, store }
    }

    /// Issues a key under the caller's resolved role and scope.
    pub async fn create(
        &self,
        ctx: &ServiceContext,
        name: &str,
        scopes: &[String],
        expires_at: Option<mas_common::timestamps::Timestamp>,
    ) -> Result<CreatedApiKeyDto> {
        let tenant = ctx.require_tenant()?;
        let organization = ctx.require_organization()?;
        let role: MembershipRole = ctx.require_resolved_role()?;
        let scopes = ScopeSet::new(scopes.iter().cloned())?;
        let user_id = (ctx.actor.kind == mas_domain::AuditActorKind::User)
            .then(|| mas_common::ids::UserId::parse_str(&ctx.actor.id).ok())
            .flatten();
        let created = self
            .keys
            .create(
                tenant,
                organization,
                user_id,
                role,
                name,
                scopes,
                expires_at,
            )
            .await?;
        Ok(CreatedApiKeyDto {
            metadata: metadata_dto(created.metadata.as_metadata_view()),
            key: created.raw_secret.expose().to_owned(),
        })
    }

    /// Lists safe key metadata for only the resolved tenant.
    pub async fn list(&self, ctx: &ServiceContext) -> Result<Vec<ApiKeyMetadataDto>> {
        let tenant = ctx.require_tenant()?;
        self.keys
            .list_for_tenant(tenant)
            .await
            .map(|keys| keys.into_iter().map(metadata_dto).collect())
    }

    /// Irreversibly revokes a key after hiding foreign-tenant existence.
    pub async fn revoke(&self, ctx: &ServiceContext, id: ApiKeyId) -> Result<()> {
        let tenant = ctx.require_tenant()?;
        self.owned_key(tenant, id).await?;
        self.keys.revoke(id, &ctx.actor.id).await
    }

    /// Rotates the secret, returning replacement plaintext once.
    pub async fn rotate(
        &self,
        ctx: &ServiceContext,
        id: ApiKeyId,
        grace_seconds: Option<u64>,
    ) -> Result<CreatedApiKeyDto> {
        let tenant = ctx.require_tenant()?;
        self.owned_key(tenant, id).await?;
        let grace = grace_seconds.map(|seconds| Duration::from_secs(seconds.min(86_400)));
        let created = self.keys.rotate(id, grace).await?;
        Ok(CreatedApiKeyDto {
            metadata: metadata_dto(created.metadata.as_metadata_view()),
            key: created.raw_secret.expose().to_owned(),
        })
    }

    async fn owned_key(&self, tenant: mas_common::ids::TenantId, id: ApiKeyId) -> Result<()> {
        match self.store.get(id).await? {
            Some(key) if key.metadata.tenant_id == tenant => Ok(()),
            _ => Err(AppError::not_found("api key", id.to_string())),
        }
    }
}

fn metadata_dto(metadata: mas_domain::ApiKeyMetadataView) -> ApiKeyMetadataDto {
    ApiKeyMetadataDto {
        id: metadata.id,
        name: metadata.name,
        prefix: metadata.prefix,
        scopes: metadata.scopes,
        expires_at: metadata.expires_at,
        last_used_at: metadata.last_used_at,
        revoked: metadata.revoked,
        created_at: metadata.created_at,
    }
}
