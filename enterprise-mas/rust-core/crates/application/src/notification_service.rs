//! PURPOSE: Tenant-recipient notification lifecycle use cases and keyset pages.
//!
//! Owning layer: `mas-application`.
//!
//! Production role: create represented-channel notifications with durable
//! tenant/recipient/channel deduplication, expose a stable recipient feed, and
//! apply read/archive lifecycle transitions with mandatory audit records.
//!
//! Security boundary: every read/write requires a resolved tenant context;
//! recipient-visible records are hidden from other users unless a proven
//! Owner/Admin role performs an authorized tenant administration action. Bodies
//! are already pre-rendered/redacted before this service sees them.
//!
//! Data ownership: the store owns notification rows, uniqueness, and durable
//! ordering. This service owns context checks, lifecycle sequencing, cursors,
//! and audit semantics.
//!
//! Prohibited responsibilities: template rendering, sending email/Slack/
//! webhook traffic, storing secrets, accepting arbitrary channels, using offset
//! pagination, or treating duplicate producer retries as new notifications.

use std::fmt;

use async_trait::async_trait;
use base64::Engine as _;
use mas_common::error::AppError;
use mas_common::ids::{TenantId, UserId};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::{AuditActorKind, AuditOutcome, Notification, NotificationChannel};

use crate::audit::{self, AuditSinkPort};
use crate::context::ServiceContext;

/// Input to create one pre-rendered notification.
#[derive(Debug, Clone)]
pub struct CreateNotification {
    pub recipient_id: UserId,
    pub channel: NotificationChannel,
    pub deduplication_key: String,
    pub destination: String,
    pub subject: String,
    pub body: String,
}

/// Stable keyset cursor over `created_at DESC, id DESC`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotificationCursor {
    pub created_at: Timestamp,
    pub id: uuid::Uuid,
}

/// Bounded recipient feed page.
#[derive(Debug, Clone)]
pub struct NotificationPage {
    pub items: Vec<Notification>,
    pub next_cursor: Option<String>,
}

/// Durable notification repository required by the lifecycle service.
#[async_trait]
pub trait NotificationLifecycleStorePort: fmt::Debug + Send + Sync {
    /// Insert a notification or return the existing one under the canonical
    /// tenant/recipient/channel/deduplication unique constraint.
    async fn create_or_get(&self, notification: &Notification) -> Result<Notification>;
    /// Tenant-scoped get; foreign rows must be invisible.
    async fn get(&self, tenant: TenantId, id: uuid::Uuid) -> Result<Option<Notification>>;
    /// Saves a lifecycle transition for an existing tenant-scoped record.
    async fn save(&self, notification: &Notification) -> Result<()>;
    /// Keyset page ordered stably newest-first.
    async fn list_page(
        &self,
        tenant: TenantId,
        recipient: UserId,
        include_archived: bool,
        unread_only: bool,
        after: Option<NotificationCursor>,
        limit: u16,
    ) -> Result<Vec<Notification>>;
}

/// Notification application service.
#[derive(Debug)]
pub struct NotificationService {
    store: Box<dyn NotificationLifecycleStorePort>,
    audit: Box<dyn AuditSinkPort>,
}

impl NotificationService {
    /// Wires lifecycle operations to a durable store and mandatory audit sink.
    pub fn new(
        store: Box<dyn NotificationLifecycleStorePort>,
        audit: Box<dyn AuditSinkPort>,
    ) -> Self {
        Self { store, audit }
    }

    /// Creates or returns a durable deduplicated notification.
    pub async fn emit(
        &self,
        ctx: &ServiceContext,
        input: CreateNotification,
    ) -> Result<Notification> {
        let tenant = ctx.require_tenant()?;
        self.require_producer(ctx)?;
        let notification = Notification::new(
            tenant,
            input.recipient_id,
            input.channel,
            input.deduplication_key,
            input.destination,
            input.subject,
            input.body,
        )?;
        let stored = self.store.create_or_get(&notification).await?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "notification.emit",
            "notification",
            Some(stored.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(stored)
    }

    /// Returns a stable recipient-only keyset page.
    pub async fn list_for_recipient(
        &self,
        ctx: &ServiceContext,
        recipient: UserId,
        include_archived: bool,
        unread_only: bool,
        cursor: Option<&str>,
        limit: u16,
    ) -> Result<NotificationPage> {
        let tenant = ctx.require_tenant()?;
        self.require_recipient_or_administrator(ctx, recipient)?;
        if !(1..=200).contains(&limit) {
            return Err(AppError::invalid_field(
                "limit",
                "out_of_range",
                "notification page limit must be between 1 and 200",
            ));
        }
        let cursor = cursor.map(decode_cursor).transpose()?;
        // Ask for one extra row to derive a next cursor without offset scans.
        let fetch_limit = limit.saturating_add(1);
        let mut items = self
            .store
            .list_page(
                tenant,
                recipient,
                include_archived,
                unread_only,
                cursor,
                fetch_limit,
            )
            .await?;
        let has_more = items.len() > usize::from(limit);
        if has_more {
            items.truncate(usize::from(limit));
        }
        let next_cursor = if has_more {
            items.last().map(|item| {
                encode_cursor(NotificationCursor {
                    created_at: item.created_at,
                    id: item.id,
                })
            })
        } else {
            None
        };
        Ok(NotificationPage { items, next_cursor })
    }

    /// Idempotently marks one recipient-visible notification read.
    pub async fn mark_read(
        &self,
        ctx: &ServiceContext,
        recipient: UserId,
        notification_id: uuid::Uuid,
    ) -> Result<Notification> {
        let mut notification = self
            .scoped_recipient_notification(ctx, recipient, notification_id)
            .await?;
        notification.mark_read();
        self.store.save(&notification).await?;
        // The store may merge a concurrent archive/read transition. Reloading
        // returns the canonical durable lifecycle rather than the caller's
        // stale pre-update snapshot.
        let stored = self
            .store
            .get(ctx.require_tenant()?, notification.id)
            .await?
            .ok_or_else(|| AppError::not_found("notification", notification.id.to_string()))?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "notification.read",
            "notification",
            Some(stored.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(stored)
    }

    /// Idempotently archives a recipient-visible notification without deleting
    /// audit/delivery evidence.
    pub async fn archive(
        &self,
        ctx: &ServiceContext,
        recipient: UserId,
        notification_id: uuid::Uuid,
    ) -> Result<Notification> {
        let mut notification = self
            .scoped_recipient_notification(ctx, recipient, notification_id)
            .await?;
        notification.archive();
        self.store.save(&notification).await?;
        let stored = self
            .store
            .get(ctx.require_tenant()?, notification.id)
            .await?
            .ok_or_else(|| AppError::not_found("notification", notification.id.to_string()))?;
        audit::record_mutation(
            ctx,
            self.audit.as_ref(),
            "notification.archive",
            "notification",
            Some(stored.id.to_string()),
            AuditOutcome::Success,
        )
        .await?;
        Ok(stored)
    }

    async fn scoped_recipient_notification(
        &self,
        ctx: &ServiceContext,
        recipient: UserId,
        notification_id: uuid::Uuid,
    ) -> Result<Notification> {
        let tenant = ctx.require_tenant()?;
        self.require_recipient_or_administrator(ctx, recipient)?;
        self.store
            .get(tenant, notification_id)
            .await?
            .filter(|notification| notification.recipient_id == recipient)
            .ok_or_else(|| AppError::not_found("notification", notification_id.to_string()))
    }

    fn require_producer(&self, ctx: &ServiceContext) -> Result<()> {
        // Background system/service producers use authenticated internal paths;
        // a human/API-key caller needs a proven Developer-or-higher membership.
        if !matches!(
            ctx.actor.kind,
            AuditActorKind::System | AuditActorKind::Service
        ) {
            let role = ctx.require_resolved_role()?;
            if role.rank() < mas_domain::MembershipRole::Developer.rank() {
                return Err(AppError::forbidden(
                    "developer, admin, or owner role is required to emit notifications",
                ));
            }
        }
        Ok(())
    }

    fn require_recipient_or_administrator(
        &self,
        ctx: &ServiceContext,
        recipient: UserId,
    ) -> Result<()> {
        if ctx
            .resolved_role
            .is_some_and(|role| role.rank() >= mas_domain::MembershipRole::Admin.rank())
        {
            return Ok(());
        }
        if ctx.actor.kind != AuditActorKind::User {
            return Err(AppError::forbidden(
                "only the recipient or a tenant administrator may access notifications",
            ));
        }
        let actor = ctx.actor.id.parse::<UserId>().map_err(|_| {
            AppError::forbidden("authenticated user identity is not a canonical user id")
        })?;
        if actor != recipient {
            return Err(AppError::not_found(
                "notification_recipient",
                recipient.to_string(),
            ));
        }
        Ok(())
    }
}

/// Encodes an opaque bounded cursor. It contains no secrets and is checked by
/// tenant/recipient filters in the store before it can affect a result set.
#[must_use]
pub fn encode_cursor(cursor: NotificationCursor) -> String {
    let raw = format!("{}:{}", cursor.created_at.to_unix_ms(), cursor.id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw)
}

/// Parses a cursor strictly; malformed cursors are validation failures rather
/// than silently restarting pagination (which can duplicate user actions).
pub fn decode_cursor(raw: &str) -> Result<NotificationCursor> {
    if raw.is_empty() || raw.len() > 512 {
        return Err(AppError::invalid_field(
            "cursor",
            "invalid_format",
            "notification cursor must be a bounded URL-safe token",
        ));
    }
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .map_err(|_| {
            AppError::invalid_field("cursor", "invalid_format", "invalid cursor encoding")
        })?;
    let decoded = std::str::from_utf8(&decoded)
        .map_err(|_| AppError::invalid_field("cursor", "invalid_format", "cursor is not UTF-8"))?;
    let (milliseconds, id) = decoded.split_once(':').ok_or_else(|| {
        AppError::invalid_field("cursor", "invalid_format", "cursor shape is invalid")
    })?;
    let milliseconds = milliseconds.parse::<i64>().map_err(|_| {
        AppError::invalid_field("cursor", "invalid_format", "cursor timestamp is invalid")
    })?;
    let created_at = Timestamp::from_unix_ms(milliseconds).map_err(|_| {
        AppError::invalid_field(
            "cursor",
            "invalid_format",
            "cursor timestamp is out of range",
        )
    })?;
    let id = uuid::Uuid::parse_str(id)
        .map_err(|_| AppError::invalid_field("cursor", "invalid_format", "cursor id is invalid"))?;
    Ok(NotificationCursor { created_at, id })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trip_is_stable_and_rejects_malformed_values() {
        let cursor = NotificationCursor {
            created_at: Timestamp::from_unix_seconds(1_700_000_000).expect("timestamp"),
            id: uuid::Uuid::now_v7(),
        };
        assert_eq!(
            decode_cursor(&encode_cursor(cursor)).expect("cursor"),
            cursor
        );
        assert!(decode_cursor("not-a-cursor").is_err());
    }
}
