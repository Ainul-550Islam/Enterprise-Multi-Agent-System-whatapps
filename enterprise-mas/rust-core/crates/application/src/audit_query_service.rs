//! PURPOSE: Read-only, tenant-scoped audit query use case with bounded filters, opaque keyset cursors, and a safe DTO projection.
//!
//! Owning layer: `mas-application`.
//! Production responsibility: validate compliance queries, authorize audit reads through the canonical matrix, page append-only events, and expose only redacted audit fields.
//! Data ownership: audit rows remain owned by the persistence adapter; this service owns query semantics and cursor integrity.
//! MUST NOT: mutate audit events, read outside the resolved tenant, expose credentials or raw payloads, or trust UI permission hints.

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use mas_common::{error::AppError, ids::TenantId, result::Result};
use mas_contracts::audit::{AuditEventDto, AuditPageDto, AuditQuery};
use mas_domain::AuditEvent;
use mas_security::{
    Action, AuthorizationRequest, AuthorizationService, ResourceKind, SecurityPrincipal,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fmt, sync::Mutex};

/// Database-independent filter passed to the persistence read port.
#[derive(Debug, Clone, Default)]
pub struct AuditReadFilter {
    pub actor: Option<String>,
    pub action: Option<String>,
    pub resource: Option<String>,
    pub outcome: Option<String>,
    pub from: Option<mas_common::timestamps::Timestamp>,
    pub to: Option<mas_common::timestamps::Timestamp>,
}

/// Stable descending keyset position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPosition {
    pub occurred_at_ms: i64,
    pub id: String,
}

/// Read-only audit persistence boundary. Implementations must apply tenant and all filters in SQL and return at most `limit` rows.
#[async_trait]
pub trait AuditReadPort: Send + Sync + fmt::Debug {
    async fn read_page(
        &self,
        tenant: TenantId,
        filter: &AuditReadFilter,
        after: Option<&AuditPosition>,
        limit: u16,
    ) -> Result<Vec<AuditEvent>>;
}

/// Read-only audit query service.
#[derive(Debug)]
pub struct AuditQueryService {
    port: Box<dyn AuditReadPort>,
    cursor_key: Vec<u8>,
    max_window_seconds: i64,
}

impl AuditQueryService {
    /// Creates a service with a deployment-owned cursor signing key (minimum 32 bytes).
    pub fn new(port: Box<dyn AuditReadPort>, cursor_key: impl Into<Vec<u8>>) -> Result<Self> {
        let cursor_key = cursor_key.into();
        if cursor_key.len() < 32 {
            return Err(AppError::validation(
                "audit cursor signing key must contain at least 32 bytes",
            ));
        }
        Ok(Self {
            port,
            cursor_key,
            max_window_seconds: 366 * 24 * 60 * 60,
        })
    }

    /// Reads one authorized page and returns redacted DTOs.
    pub async fn query(
        &self,
        principal: &SecurityPrincipal,
        tenant: TenantId,
        organization: mas_common::ids::OrganizationId,
        query: &AuditQuery,
    ) -> Result<AuditPageDto> {
        query.validate()?;
        AuthorizationService::new()
            .authorize(
                principal,
                &AuthorizationRequest {
                    resource: ResourceKind::Audit,
                    action: Action::ViewAudit,
                    tenant_id: tenant,
                    organization_id: organization,
                    project_id: None,
                },
            )
            .into_result()?;
        if let (Some(from), Some(to)) = (query.from, query.to) {
            let span = to.duration_since(&from).ok_or_else(|| {
                AppError::invalid_field("from", "invalid_range", "invalid audit time range")
            })?;
            if span.as_secs() > self.max_window_seconds as u64 {
                return Err(AppError::invalid_field(
                    "from",
                    "window_too_large",
                    "audit query window exceeds the allowed maximum",
                ));
            }
        }
        let after = query
            .cursor
            .as_deref()
            .map(|cursor| self.decode_cursor(cursor))
            .transpose()?;
        let limit = query.limit.unwrap_or(50).clamp(1, 200);
        let filter = AuditReadFilter {
            actor: query.actor.clone(),
            action: query.action.clone(),
            resource: query.resource.clone(),
            outcome: query.outcome.clone(),
            from: query.from,
            to: query.to,
        };
        let mut events = self
            .port
            .read_page(tenant, &filter, after.as_ref(), limit + 1)
            .await?;
        let has_more = events.len() > usize::from(limit);
        if has_more {
            events.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            events
                .last()
                .map(|event| {
                    self.encode_cursor(&AuditPosition {
                        occurred_at_ms: event.occurred_at.to_unix_ms(),
                        id: event.id.to_string(),
                    })
                })
                .transpose()?
        } else {
            None
        };
        Ok(AuditPageDto {
            items: events.into_iter().map(redacted_dto).collect(),
            next_cursor,
        })
    }

    fn encode_cursor(&self, position: &AuditPosition) -> Result<String> {
        let payload = serde_json::to_vec(position)
            .map_err(|_| AppError::internal("could not encode audit cursor"))?;
        let mut signed = payload.clone();
        signed.extend_from_slice(&self.mac(&payload));
        Ok(URL_SAFE_NO_PAD.encode(signed))
    }

    fn decode_cursor(&self, cursor: &str) -> Result<AuditPosition> {
        let bytes = URL_SAFE_NO_PAD.decode(cursor).map_err(|_| {
            AppError::invalid_field("cursor", "invalid_cursor", "audit cursor is invalid")
        })?;
        if bytes.len() < 33 || bytes.len() > 1024 {
            return Err(AppError::invalid_field(
                "cursor",
                "invalid_cursor",
                "audit cursor is invalid",
            ));
        }
        let split = bytes.len() - 32;
        let (payload, signature) = bytes.split_at(split);
        let expected = self.mac(payload);
        let mismatch = signature
            .iter()
            .zip(expected.iter())
            .fold(0_u8, |acc, (a, b)| acc | (a ^ b));
        if mismatch != 0 {
            return Err(AppError::invalid_field(
                "cursor",
                "invalid_cursor",
                "audit cursor is invalid",
            ));
        }
        let decoded: AuditPosition = serde_json::from_slice(payload).map_err(|_| {
            AppError::invalid_field("cursor", "invalid_cursor", "audit cursor is invalid")
        })?;
        if decoded.id.is_empty() || decoded.id.len() > 64 {
            return Err(AppError::invalid_field(
                "cursor",
                "invalid_cursor",
                "audit cursor is invalid",
            ));
        }
        Ok(decoded)
    }

    fn mac(&self, payload: &[u8]) -> [u8; 32] {
        const BLOCK: usize = 64;
        let mut key = [0_u8; BLOCK];
        if self.cursor_key.len() > BLOCK {
            let digest = Sha256::digest(&self.cursor_key);
            key[..digest.len()].copy_from_slice(&digest);
        } else {
            key[..self.cursor_key.len()].copy_from_slice(&self.cursor_key);
        }
        let mut inner_pad = [0x36_u8; BLOCK];
        let mut outer_pad = [0x5c_u8; BLOCK];
        for index in 0..BLOCK {
            inner_pad[index] ^= key[index];
            outer_pad[index] ^= key[index];
        }
        let mut inner = Sha256::new();
        inner.update(inner_pad);
        inner.update(b"mas-audit-cursor-v1\0");
        inner.update(payload);
        let mut outer = Sha256::new();
        outer.update(outer_pad);
        outer.update(inner.finalize());
        outer.finalize().into()
    }
}

fn redacted_dto(event: AuditEvent) -> AuditEventDto {
    let mut metadata = serde_json::Value::Object(event.metadata);
    mas_common::redaction::redact_json_value(&mut metadata);
    let metadata = metadata.as_object().cloned().unwrap_or_default();
    AuditEventDto {
        id: event.id,
        actor_kind: event.actor.kind.to_string(),
        actor_id: event.actor.id,
        action: event.action,
        resource_type: event.resource_type,
        resource_id: event.resource_id,
        outcome: event.outcome.to_string(),
        correlation_id: event.correlation_id,
        metadata,
        occurred_at: event.occurred_at,
    }
}

/// In-memory adapter for isolated service tests; production uses PostgreSQL.
#[derive(Debug, Default)]
pub struct InMemoryAuditReadPort {
    events: Mutex<Vec<AuditEvent>>,
}

impl InMemoryAuditReadPort {
    #[must_use]
    pub fn new(events: Vec<AuditEvent>) -> Self {
        Self {
            events: Mutex::new(events),
        }
    }
    pub fn push(&self, event: AuditEvent) {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(event);
    }
}

#[async_trait]
impl AuditReadPort for InMemoryAuditReadPort {
    async fn read_page(
        &self,
        tenant: TenantId,
        filter: &AuditReadFilter,
        after: Option<&AuditPosition>,
        limit: u16,
    ) -> Result<Vec<AuditEvent>> {
        let mut events: Vec<_> = self
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|event| event.tenant_id == Some(tenant))
            .filter(|event| filter.actor.as_ref().is_none_or(|v| &event.actor.id == v))
            .filter(|event| {
                filter
                    .action
                    .as_ref()
                    .is_none_or(|v| event.action.starts_with(v))
            })
            .filter(|event| {
                filter
                    .resource
                    .as_ref()
                    .is_none_or(|v| &event.resource_type == v)
            })
            .filter(|event| {
                filter
                    .outcome
                    .as_ref()
                    .is_none_or(|v| event.outcome.to_string() == *v)
            })
            .filter(|event| filter.from.is_none_or(|v| !event.occurred_at.is_before(&v)))
            .filter(|event| filter.to.is_none_or(|v| event.occurred_at.is_before(&v)))
            .filter(|event| {
                after.is_none_or(|p| {
                    event.occurred_at.to_unix_ms() < p.occurred_at_ms
                        || (event.occurred_at.to_unix_ms() == p.occurred_at_ms
                            && event.id.to_string() < p.id)
                })
            })
            .cloned()
            .collect();
        events.sort_by(|a, b| {
            b.occurred_at
                .cmp(&a.occurred_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        events.truncate(usize::from(limit));
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mas_domain::{AuditActor, AuditActorKind, AuditOutcome};

    fn svc() -> AuditQueryService {
        AuditQueryService::new(Box::new(InMemoryAuditReadPort::default()), vec![7; 32])
            .expect("service")
    }

    #[test]
    fn cursor_is_opaque_and_tamper_checked() {
        let service = svc();
        let cursor = service
            .encode_cursor(&AuditPosition {
                occurred_at_ms: 42,
                id: "event-1".into(),
            })
            .expect("cursor");
        assert_eq!(
            service
                .decode_cursor(&cursor)
                .expect("valid")
                .occurred_at_ms,
            42
        );
        let mut altered = cursor.into_bytes();
        altered[4] = if altered[4] == b'A' { b'B' } else { b'A' };
        assert!(service
            .decode_cursor(std::str::from_utf8(&altered).expect("base64 ascii"))
            .is_err());
    }

    #[tokio::test]
    async fn in_memory_reader_hides_other_tenants() {
        let tenant = TenantId::new();
        let mut event = AuditEvent::new(
            Some(TenantId::new()),
            None,
            AuditActor::new(AuditActorKind::User, "u").expect("actor"),
            "policy.publish",
            "policy",
            None,
            AuditOutcome::Success,
            "corr",
        )
        .expect("event");
        event.tenant_id = Some(TenantId::new());
        let port = InMemoryAuditReadPort::new(vec![event]);
        let result = port
            .read_page(tenant, &AuditReadFilter::default(), None, 10)
            .await
            .expect("read");
        assert!(result.is_empty());
    }
}
