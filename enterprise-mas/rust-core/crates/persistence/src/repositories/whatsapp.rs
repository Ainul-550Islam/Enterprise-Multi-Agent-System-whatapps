//! PURPOSE: The WhatsApp channel and message-log repository.
//!
//! Owning layer: `mas-persistence`.
//!
//! Production responsibility: record tenant-owned channel configuration and
//! every message that entered or left a channel, and answer the one pre-scope
//! question the public webhook receiver asks -- "which tenant owns this
//! `phone_number_id`?" -- through the `mas_resolve_whatsapp_tenant` SECURITY
//! DEFINER function (migration 0055). Everything else runs under the request
//! scope with `tenant_id` bound in SQL.
//!
//! Security boundary: the pre-scope lookup is confined to one function that
//! exposes exactly one value (the 0015 pattern). The message log's `payload`
//! column is untrusted evidence; it is stored verbatim and never re-parsed
//! into authoritative state.
//!
//! Data ownership: provider identifiers (`phone_number_id`, `wamid`) are
//! opaque handles. `verify_token_ref` is a secret *reference* -- the module
//! never resolves or stores the secret itself.
//!
//! It must NOT:
//!   * expose an unscoped list of channels or messages;
//!   * record the same provider message id twice for a channel (the partial
//!     unique index is the control, and a duplicate is treated as a no-op
//!     replay rather than an error);
//!   * let one `phone_number_id` be claimed twice anywhere (migration 0055's
//!     global unique is the control).

use mas_common::error::AppError;
use mas_common::ids::{
    OrganizationId, TenantId, WhatsAppChannelId, WhatsAppConversationId, WhatsAppMessageId,
    WhatsAppTemplateId,
};
use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_domain::whatsapp::{
    WhatsAppChannel, WhatsAppChannelStatus, WhatsAppConversation, WhatsAppDirection,
    WhatsAppMessage, WhatsAppMessageStatus, WhatsAppMessageType, WhatsAppStats, WhatsAppTemplate,
};
use sqlx::PgPool;
use sqlx::Row;

use crate::error::map_sqlx;
use crate::scoped_request;

/// Postgres implementation of the WhatsApp boundary.
#[derive(Debug, Clone)]
pub struct PostgresWhatsAppStore {
    pool: PgPool,
}

impl PostgresWhatsAppStore {
    /// Builds a store over a pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Resolves which tenant (and organization) owns a `phone_number_id` --
    /// **pre-scope**.
    ///
    /// Runs the migration-0055 SECURITY DEFINER function, the only unscoped
    /// read of `whatsapp_channels`. `None` means no platform channel claims
    /// this phone number; the webhook receiver must then refuse the delivery.
    /// The organization is returned alongside the tenant so the caller can
    /// open an authoritative scope.
    pub async fn resolve_owner_by_phone_number(
        &self,
        phone_number_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>> {
        let row: Option<(uuid::Uuid, uuid::Uuid)> = sqlx::query_as(
            "SELECT tenant_id, organization_id FROM mas_resolve_whatsapp_tenant($1)",
        )
        .bind(phone_number_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(row.map(|(tenant, org)| (TenantId::from_uuid(tenant), OrganizationId::from_uuid(org))))
    }

    /// Registers a channel for the scope's tenant.
    ///
    /// # Errors
    ///
    /// [`AppError::conflict`] when the `phone_number_id` is already claimed
    /// by any tenant -- an ambiguous phone number would let one webhook
    /// address two tenants.
    pub async fn create_channel(&self, channel: &WhatsAppChannel) -> Result<WhatsAppChannel> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != channel.tenant_id {
            return Err(AppError::forbidden(
                "whatsapp channel create outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        sqlx::query(
            "INSERT INTO whatsapp_channels \
             (id, tenant_id, display_name, phone_number_id, waba_id, api_version, status, verify_token_ref) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(channel.id.as_uuid())
        .bind(channel.tenant_id.as_uuid())
        .bind(&channel.display_name)
        .bind(&channel.phone_number_id)
        .bind(&channel.waba_id)
        .bind(&channel.api_version)
        .bind(channel.status.as_str())
        .bind(&channel.verify_token_ref)
        .execute(executor.connection().as_mut())
        .await
        .map_err(map_channel_conflict_or_sqlx)?;
        drop(executor);
        self.get_channel(channel.tenant_id, channel.id)
            .await?
            .ok_or_else(|| AppError::database("whatsapp channel insert did not round-trip"))
    }

    /// Tenant-scoped get by row id.
    pub async fn get_channel(
        &self,
        tenant: TenantId,
        id: WhatsAppChannelId,
    ) -> Result<Option<WhatsAppChannel>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp channel read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, display_name, phone_number_id, waba_id, api_version, status, \
                    verify_token_ref, created_at, updated_at \
             FROM whatsapp_channels WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_channel(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider handle.
    pub async fn get_channel_by_phone_number(
        &self,
        tenant: TenantId,
        phone_number_id: &str,
    ) -> Result<Option<WhatsAppChannel>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp channel read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, display_name, phone_number_id, waba_id, api_version, status, \
                    verify_token_ref, created_at, updated_at \
             FROM whatsapp_channels WHERE tenant_id = $1 AND phone_number_id = $2",
        )
        .bind(tenant.as_uuid())
        .bind(phone_number_id)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_channel(&row)).transpose()
    }

    /// The scope's tenant's channels, newest first.
    pub async fn list_channels(&self, tenant: TenantId) -> Result<Vec<WhatsAppChannel>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp channel list outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, display_name, phone_number_id, waba_id, api_version, status, \
                    verify_token_ref, created_at, updated_at \
             FROM whatsapp_channels WHERE tenant_id = $1 \
             ORDER BY created_at DESC, id DESC",
        )
        .bind(tenant.as_uuid())
        .fetch_all(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(row_to_channel).collect()
    }

    /// Records a channel lifecycle transition.
    pub async fn set_channel_status(
        &self,
        tenant: TenantId,
        id: WhatsAppChannelId,
        status: WhatsAppChannelStatus,
    ) -> Result<Option<WhatsAppChannel>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp channel update outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "UPDATE whatsapp_channels SET status = $3 \
             WHERE tenant_id = $1 AND id = $2 \
             RETURNING id, tenant_id, display_name, phone_number_id, waba_id, api_version, status, \
                       verify_token_ref, created_at, updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .bind(status.as_str())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_channel(&row)).transpose()
    }

    /// Records a message, treating a repeated provider message id as the
    /// replay it is: the existing row is returned and nothing is inserted.
    ///
    /// Returns `(message, recorded)` -- `recorded` is `false` when the row was
    /// already present.
    pub async fn record_message(
        &self,
        message: &WhatsAppMessage,
    ) -> Result<(WhatsAppMessage, bool)> {
        let scope = scoped_request::current_scope()?;
        if scope.tenant_id() != message.tenant_id {
            return Err(AppError::forbidden(
                "whatsapp message write outside the request scope's tenant",
            ));
        }
        if message.wamid.is_some() {
            if let Some(existing) = self
                .get_message_by_wamid(
                    message.tenant_id,
                    message.channel_id,
                    message.wamid.as_deref().unwrap_or_default(),
                )
                .await?
            {
                return Ok((existing, false));
            }
        }
        let mut executor = scoped_request::connection().await?;
        sqlx::query(
            "INSERT INTO whatsapp_messages \
             (id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, message_type, \
              body_text, status, payload, occurred_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
        )
        .bind(message.id.as_uuid())
        .bind(message.tenant_id.as_uuid())
        .bind(message.channel_id.as_uuid())
        .bind(message.direction.as_str())
        .bind(&message.wamid)
        .bind(&message.from_addr)
        .bind(&message.to_addr)
        .bind(message.message_type.as_str())
        .bind(&message.body_text)
        .bind(message.status.as_str())
        .bind(&message.payload)
        .bind(message.occurred_at.into_datetime())
        .execute(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        drop(executor);
        let recorded = self
            .get_message(message.tenant_id, message.id)
            .await?
            .ok_or_else(|| AppError::database("whatsapp message insert did not round-trip"))?;
        Ok((recorded, true))
    }

    /// Tenant-scoped get by row id.
    pub async fn get_message(
        &self,
        tenant: TenantId,
        id: WhatsAppMessageId,
    ) -> Result<Option<WhatsAppMessage>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp message read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(message_select())
            .bind(tenant.as_uuid())
            .bind(id.as_uuid())
            .fetch_optional(executor.connection().as_mut())
            .await
            .map_err(map_sqlx)?;
        row.map(|row| row_to_message(&row)).transpose()
    }

    /// Tenant-scoped lookup by provider message id.
    pub async fn get_message_by_wamid(
        &self,
        tenant: TenantId,
        channel_id: WhatsAppChannelId,
        wamid: &str,
    ) -> Result<Option<WhatsAppMessage>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp message read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, message_type, \
                    body_text, status, payload, occurred_at, created_at, updated_at \
             FROM whatsapp_messages \
             WHERE tenant_id = $1 AND channel_id = $2 AND wamid = $3",
        )
        .bind(tenant.as_uuid())
        .bind(channel_id.as_uuid())
        .bind(wamid)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_message(&row)).transpose()
    }

    /// The scope's tenant's messages on a channel, newest first.
    pub async fn list_messages(
        &self,
        tenant: TenantId,
        channel_id: WhatsAppChannelId,
        limit: i64,
    ) -> Result<Vec<WhatsAppMessage>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp message list outside the request scope's tenant",
            ));
        }
        let limit = limit.clamp(1, 200);
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, message_type, \
                    body_text, status, payload, occurred_at, created_at, updated_at \
             FROM whatsapp_messages \
             WHERE tenant_id = $1 AND channel_id = $2 \
             ORDER BY occurred_at DESC, id DESC LIMIT $3",
        )
        .bind(tenant.as_uuid())
        .bind(channel_id.as_uuid())
        .bind(limit)
        .fetch_all(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(row_to_message).collect()
    }

    /// Conversation for one contact on one channel, newest first.
    pub async fn get_conversation(
        &self,
        tenant: TenantId,
        channel_id: WhatsAppChannelId,
        contact: &str,
    ) -> Result<Vec<WhatsAppMessage>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp conversation read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, message_type, \
                    body_text, status, payload, occurred_at, created_at, updated_at \
             FROM whatsapp_messages \
             WHERE tenant_id = $1 AND channel_id = $2 \
               AND (from_addr = $3 OR to_addr = $3) \
             ORDER BY occurred_at DESC, id DESC LIMIT 200",
        )
        .bind(tenant.as_uuid())
        .bind(channel_id.as_uuid())
        .bind(contact)
        .fetch_all(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(row_to_message).collect()
    }

    /// Records a delivery-status update for an outbound message.
    pub async fn update_message_status(
        &self,
        tenant: TenantId,
        wamid: &str,
        status: WhatsAppMessageStatus,
    ) -> Result<Option<WhatsAppMessage>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp status update outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "UPDATE whatsapp_messages SET status = $3, updated_at = now() \
             WHERE tenant_id = $1 AND wamid = $2 \
             RETURNING id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, \
                       message_type, body_text, status, payload, occurred_at, created_at, updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(wamid)
        .bind(status.as_str())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_message(&row)).transpose()
    }

    /// Upserts the inbox conversation for an inbound contact.
    pub async fn upsert_conversation(
        &self,
        tenant: TenantId,
        channel_id: WhatsAppChannelId,
        contact: &str,
        inbound_at: Timestamp,
        opt_out: Option<bool>,
    ) -> Result<WhatsAppConversation> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp conversation write outside the request scope's tenant",
            ));
        }
        let window = inbound_at
            .into_datetime()
            .checked_add_signed(chrono::Duration::hours(24))
            .unwrap_or(inbound_at.into_datetime());
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "INSERT INTO whatsapp_conversations \
             (id, tenant_id, channel_id, contact, last_inbound_at, window_expires_at, opt_out) \
             VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, false)) \
             ON CONFLICT (tenant_id, channel_id, contact) DO UPDATE SET \
                last_inbound_at = EXCLUDED.last_inbound_at, \
                window_expires_at = EXCLUDED.window_expires_at, \
                opt_out = COALESCE($7, whatsapp_conversations.opt_out), \
                updated_at = now() \
             RETURNING id, tenant_id, channel_id, contact, handoff, opt_out, \
                       window_expires_at, last_inbound_at, created_at, updated_at",
        )
        .bind(uuid::Uuid::now_v7())
        .bind(tenant.as_uuid())
        .bind(channel_id.as_uuid())
        .bind(contact)
        .bind(inbound_at.into_datetime())
        .bind(window)
        .bind(opt_out)
        .fetch_one(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row_to_conversation(&row)
    }

    /// Lists conversations on a channel, newest first.
    pub async fn list_conversations(
        &self,
        tenant: TenantId,
        channel_id: Option<WhatsAppChannelId>,
        limit: i64,
    ) -> Result<Vec<WhatsAppConversation>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp conversation list outside the request scope's tenant",
            ));
        }
        let limit = limit.clamp(1, 200);
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, channel_id, contact, handoff, opt_out, \
                    window_expires_at, last_inbound_at, created_at, updated_at \
             FROM whatsapp_conversations \
             WHERE tenant_id = $1 AND ($2::uuid IS NULL OR channel_id = $2) \
             ORDER BY updated_at DESC, id DESC LIMIT $3",
        )
        .bind(tenant.as_uuid())
        .bind(channel_id.map(|id| *id.as_uuid()))
        .bind(limit)
        .fetch_all(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(row_to_conversation).collect()
    }

    /// One conversation by id.
    pub async fn get_conversation_by_id(
        &self,
        tenant: TenantId,
        id: WhatsAppConversationId,
    ) -> Result<Option<WhatsAppConversation>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp conversation read outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT id, tenant_id, channel_id, contact, handoff, opt_out, \
                    window_expires_at, last_inbound_at, created_at, updated_at \
             FROM whatsapp_conversations WHERE tenant_id = $1 AND id = $2",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_conversation(&row)).transpose()
    }

    /// Sets the human-handoff flag.
    pub async fn set_handoff(
        &self,
        tenant: TenantId,
        id: WhatsAppConversationId,
        handoff: bool,
    ) -> Result<Option<WhatsAppConversation>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp handoff outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "UPDATE whatsapp_conversations SET handoff = $3, updated_at = now() \
             WHERE tenant_id = $1 AND id = $2 \
             RETURNING id, tenant_id, channel_id, contact, handoff, opt_out, \
                       window_expires_at, last_inbound_at, created_at, updated_at",
        )
        .bind(tenant.as_uuid())
        .bind(id.as_uuid())
        .bind(handoff)
        .fetch_optional(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row.map(|row| row_to_conversation(&row)).transpose()
    }

    /// Lists templates for a channel.
    pub async fn list_templates(
        &self,
        tenant: TenantId,
        channel_id: WhatsAppChannelId,
    ) -> Result<Vec<WhatsAppTemplate>> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp template list outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let rows = sqlx::query(
            "SELECT id, tenant_id, channel_id, name, language, status, body, created_at, updated_at \
             FROM whatsapp_templates WHERE tenant_id = $1 AND channel_id = $2 \
             ORDER BY name ASC, language ASC",
        )
        .bind(tenant.as_uuid())
        .bind(channel_id.as_uuid())
        .fetch_all(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(row_to_template).collect()
    }

    /// Inserts or updates a template row.
    pub async fn upsert_template(&self, template: &WhatsAppTemplate) -> Result<WhatsAppTemplate> {
        if scoped_request::current_scope()?.tenant_id() != template.tenant_id {
            return Err(AppError::forbidden(
                "whatsapp template write outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "INSERT INTO whatsapp_templates \
             (id, tenant_id, channel_id, name, language, status, body) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (tenant_id, channel_id, name, language) DO UPDATE SET \
                status = EXCLUDED.status, \
                body = EXCLUDED.body, \
                updated_at = now() \
             RETURNING id, tenant_id, channel_id, name, language, status, body, created_at, updated_at",
        )
        .bind(template.id.as_uuid())
        .bind(template.tenant_id.as_uuid())
        .bind(template.channel_id.as_uuid())
        .bind(&template.name)
        .bind(&template.language)
        .bind(&template.status)
        .bind(&template.body)
        .fetch_one(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        row_to_template(&row)
    }

    /// Aggregate counters for the tenant inbox.
    pub async fn stats(&self, tenant: TenantId) -> Result<WhatsAppStats> {
        if scoped_request::current_scope()?.tenant_id() != tenant {
            return Err(AppError::forbidden(
                "whatsapp stats outside the request scope's tenant",
            ));
        }
        let mut executor = scoped_request::connection().await?;
        let row = sqlx::query(
            "SELECT \
                (SELECT count(*) FROM whatsapp_channels WHERE tenant_id = $1) AS channels, \
                (SELECT count(*) FROM whatsapp_conversations WHERE tenant_id = $1) AS conversations, \
                (SELECT count(*) FROM whatsapp_messages WHERE tenant_id = $1 AND direction = 'inbound') AS inbound, \
                (SELECT count(*) FROM whatsapp_messages WHERE tenant_id = $1 AND direction = 'outbound') AS outbound, \
                (SELECT count(*) FROM whatsapp_messages WHERE tenant_id = $1 AND status = 'failed') AS failed",
        )
        .bind(tenant.as_uuid())
        .fetch_one(executor.connection().as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(WhatsAppStats {
            channels: row.try_get::<i64, _>("channels").map_err(map_sqlx)?,
            conversations: row.try_get::<i64, _>("conversations").map_err(map_sqlx)?,
            inbound: row.try_get::<i64, _>("inbound").map_err(map_sqlx)?,
            outbound: row.try_get::<i64, _>("outbound").map_err(map_sqlx)?,
            failed: row.try_get::<i64, _>("failed").map_err(map_sqlx)?,
        })
    }
}

fn message_select() -> &'static str {
    "SELECT id, tenant_id, channel_id, direction, wamid, from_addr, to_addr, message_type, \
     body_text, status, payload, occurred_at, created_at, updated_at \
     FROM whatsapp_messages WHERE tenant_id = $1 AND id = $2"
}

fn map_channel_conflict_or_sqlx(error: sqlx::Error) -> AppError {
    if crate::error::is_unique_violation(&error) {
        AppError::conflict("the phone number is already claimed by a WhatsApp channel")
    } else {
        map_sqlx(error)
    }
}

fn row_to_channel(row: &sqlx::postgres::PgRow) -> Result<WhatsAppChannel> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let status_raw: String = row.try_get("status").map_err(map_sqlx)?;
    let status: WhatsAppChannelStatus = status_raw
        .parse()
        .map_err(|_| AppError::database("whatsapp_channels.status drifted from the enum"))?;
    Ok(WhatsAppChannel {
        id: WhatsAppChannelId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        display_name: row.try_get("display_name").map_err(map_sqlx)?,
        phone_number_id: row.try_get("phone_number_id").map_err(map_sqlx)?,
        waba_id: row.try_get("waba_id").map_err(map_sqlx)?,
        api_version: row.try_get("api_version").map_err(map_sqlx)?,
        status,
        verify_token_ref: row.try_get("verify_token_ref").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

fn row_to_message(row: &sqlx::postgres::PgRow) -> Result<WhatsAppMessage> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let channel: uuid::Uuid = row.try_get("channel_id").map_err(map_sqlx)?;
    let direction_raw: String = row.try_get("direction").map_err(map_sqlx)?;
    let direction: WhatsAppDirection = direction_raw
        .parse()
        .map_err(|_| AppError::database("whatsapp_messages.direction drifted from the enum"))?;
    let type_raw: String = row.try_get("message_type").map_err(map_sqlx)?;
    let message_type: WhatsAppMessageType = type_raw
        .parse()
        .map_err(|_| AppError::database("whatsapp_messages.message_type drifted from the enum"))?;
    let status_raw: String = row.try_get("status").map_err(map_sqlx)?;
    let status: WhatsAppMessageStatus = status_raw
        .parse()
        .map_err(|_| AppError::database("whatsapp_messages.status drifted from the enum"))?;
    Ok(WhatsAppMessage {
        id: WhatsAppMessageId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        channel_id: WhatsAppChannelId::from_uuid(channel),
        direction,
        wamid: row.try_get("wamid").map_err(map_sqlx)?,
        from_addr: row.try_get("from_addr").map_err(map_sqlx)?,
        to_addr: row.try_get("to_addr").map_err(map_sqlx)?,
        message_type,
        body_text: row.try_get("body_text").map_err(map_sqlx)?,
        status,
        payload: row.try_get("payload").map_err(map_sqlx)?,
        occurred_at: Timestamp::from_datetime(row.try_get("occurred_at").map_err(map_sqlx)?),
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

use mas_application::whatsapp_service::WhatsAppStorePort;

#[async_trait::async_trait]
impl WhatsAppStorePort for PostgresWhatsAppStore {
    async fn create_channel(&self, channel: &WhatsAppChannel) -> Result<WhatsAppChannel> {
        PostgresWhatsAppStore::create_channel(self, channel).await
    }

    async fn get_channel(
        &self,
        tenant_id: TenantId,
        channel_id: mas_common::ids::WhatsAppChannelId,
    ) -> Result<Option<WhatsAppChannel>> {
        PostgresWhatsAppStore::get_channel(self, tenant_id, channel_id).await
    }

    async fn get_channel_by_phone_number(
        &self,
        tenant_id: TenantId,
        phone_number_id: &str,
    ) -> Result<Option<WhatsAppChannel>> {
        PostgresWhatsAppStore::get_channel_by_phone_number(self, tenant_id, phone_number_id).await
    }

    async fn list_channels(&self, tenant_id: TenantId) -> Result<Vec<WhatsAppChannel>> {
        PostgresWhatsAppStore::list_channels(self, tenant_id).await
    }

    async fn resolve_owner_by_phone_number(
        &self,
        phone_number_id: &str,
    ) -> Result<Option<(TenantId, OrganizationId)>> {
        PostgresWhatsAppStore::resolve_owner_by_phone_number(self, phone_number_id).await
    }

    async fn record_message(&self, message: &WhatsAppMessage) -> Result<WhatsAppMessage> {
        PostgresWhatsAppStore::record_message(self, message)
            .await
            .map(|(recorded, _created)| recorded)
    }

    async fn list_messages(
        &self,
        tenant_id: TenantId,
        channel_id: mas_common::ids::WhatsAppChannelId,
        limit: i64,
    ) -> Result<Vec<WhatsAppMessage>> {
        PostgresWhatsAppStore::list_messages(self, tenant_id, channel_id, limit).await
    }

    async fn get_conversation(
        &self,
        tenant_id: TenantId,
        channel_id: mas_common::ids::WhatsAppChannelId,
        contact: &str,
    ) -> Result<Vec<WhatsAppMessage>> {
        PostgresWhatsAppStore::get_conversation(self, tenant_id, channel_id, contact).await
    }

    async fn update_message_status(
        &self,
        tenant_id: TenantId,
        wamid: &str,
        status: mas_domain::whatsapp::WhatsAppMessageStatus,
    ) -> Result<Option<WhatsAppMessage>> {
        PostgresWhatsAppStore::update_message_status(self, tenant_id, wamid, status).await
    }

    async fn upsert_conversation(
        &self,
        tenant_id: TenantId,
        channel_id: mas_common::ids::WhatsAppChannelId,
        contact: &str,
        inbound_at: Timestamp,
        opt_out: Option<bool>,
    ) -> Result<WhatsAppConversation> {
        PostgresWhatsAppStore::upsert_conversation(
            self, tenant_id, channel_id, contact, inbound_at, opt_out,
        )
        .await
    }

    async fn get_conversation_by_id(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
    ) -> Result<Option<WhatsAppConversation>> {
        PostgresWhatsAppStore::get_conversation_by_id(self, tenant_id, conversation_id).await
    }

    async fn list_conversations(
        &self,
        tenant_id: TenantId,
        channel_id: Option<mas_common::ids::WhatsAppChannelId>,
        limit: i64,
    ) -> Result<Vec<WhatsAppConversation>> {
        PostgresWhatsAppStore::list_conversations(self, tenant_id, channel_id, limit).await
    }

    async fn set_handoff(
        &self,
        tenant_id: TenantId,
        conversation_id: WhatsAppConversationId,
        handoff: bool,
    ) -> Result<Option<WhatsAppConversation>> {
        PostgresWhatsAppStore::set_handoff(self, tenant_id, conversation_id, handoff).await
    }

    async fn list_templates(
        &self,
        tenant_id: TenantId,
        channel_id: mas_common::ids::WhatsAppChannelId,
    ) -> Result<Vec<WhatsAppTemplate>> {
        PostgresWhatsAppStore::list_templates(self, tenant_id, channel_id).await
    }

    async fn upsert_template(&self, template: &WhatsAppTemplate) -> Result<WhatsAppTemplate> {
        PostgresWhatsAppStore::upsert_template(self, template).await
    }

    async fn stats(&self, tenant_id: TenantId) -> Result<WhatsAppStats> {
        PostgresWhatsAppStore::stats(self, tenant_id).await
    }
}

fn row_to_conversation(row: &sqlx::postgres::PgRow) -> Result<WhatsAppConversation> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let channel: uuid::Uuid = row.try_get("channel_id").map_err(map_sqlx)?;
    Ok(WhatsAppConversation {
        id: WhatsAppConversationId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        channel_id: WhatsAppChannelId::from_uuid(channel),
        contact: row.try_get("contact").map_err(map_sqlx)?,
        handoff: row.try_get("handoff").map_err(map_sqlx)?,
        opt_out: row.try_get("opt_out").map_err(map_sqlx)?,
        window_expires_at: row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("window_expires_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        last_inbound_at: row
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_inbound_at")
            .map_err(map_sqlx)?
            .map(Timestamp::from_datetime),
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}

fn row_to_template(row: &sqlx::postgres::PgRow) -> Result<WhatsAppTemplate> {
    let tenant: uuid::Uuid = row.try_get("tenant_id").map_err(map_sqlx)?;
    let id: uuid::Uuid = row.try_get("id").map_err(map_sqlx)?;
    let channel: uuid::Uuid = row.try_get("channel_id").map_err(map_sqlx)?;
    Ok(WhatsAppTemplate {
        id: WhatsAppTemplateId::from_uuid(id),
        tenant_id: TenantId::from_uuid(tenant),
        channel_id: WhatsAppChannelId::from_uuid(channel),
        name: row.try_get("name").map_err(map_sqlx)?,
        language: row.try_get("language").map_err(map_sqlx)?,
        status: row.try_get("status").map_err(map_sqlx)?,
        body: row.try_get("body").map_err(map_sqlx)?,
        created_at: Timestamp::from_datetime(row.try_get("created_at").map_err(map_sqlx)?),
        updated_at: Timestamp::from_datetime(row.try_get("updated_at").map_err(map_sqlx)?),
    })
}
