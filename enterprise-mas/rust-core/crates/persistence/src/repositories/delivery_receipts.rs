//! PostgreSQL implementation of the worker delivery-receipt port.
//!
//! Receipt allocation is intentionally separate from the task lifecycle. A
//! task attempt answers "may this task run?"; a receipt answers "which broker
//! delivery is this worker settling?". Keeping both durable prevents a local
//! broker sequence or a process restart from becoming an authority.

use mas_common::result::Result;
use mas_common::timestamps::Timestamp;
use mas_worker::delivery_receipts::{
    DeliveryReceipt, DeliveryReceiptPort, DeliveryReceiptSettlement, DeliveryReservationRequest,
};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::error::map_sqlx;

/// PostgreSQL-backed, cross-replica delivery receipts.
#[derive(Debug, Clone)]
pub struct PostgresDeliveryReceiptStore {
    pool: PgPool,
}

impl PostgresDeliveryReceiptStore {
    /// Binds receipt operations to the worker-plane pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl DeliveryReceiptPort for PostgresDeliveryReceiptStore {
    async fn reserve(
        &self,
        request: DeliveryReservationRequest,
    ) -> Result<Option<DeliveryReceipt>> {
        request.validate()?;
        let delivery_count = i32::try_from(request.delivery_count).map_err(|_| {
            mas_common::error::AppError::validation(
                "delivery receipt delivery_count must fit PostgreSQL integer storage",
            )
        })?;
        let mut transaction = self.pool.begin().await.map_err(map_sqlx)?;
        // An expired receipt is no longer an owner. Marking it preserves the
        // audit history while releasing the partial unique index for the
        // redelivery that follows a crashed worker.
        sqlx::query(
            "UPDATE broker_delivery_receipts
             SET status = 'expired', settled_at = now()
             WHERE task_id = $1
               AND status IN ('reserved', 'in_flight')
               AND lease_expires_at <= now()",
        )
        .bind(request.task_id)
        .execute(&mut *transaction)
        .await
        .map_err(map_sqlx)?;

        let row = sqlx::query(
            "INSERT INTO broker_delivery_receipts
                 (consumer_name, task_id, tenant_id, worker_id, delivery_count, status, lease_expires_at)
             VALUES ($1, $2, $3, $4, $5, 'reserved', $6)
             ON CONFLICT (task_id) WHERE status IN ('reserved', 'in_flight') DO NOTHING
             RETURNING receipt_sequence",
        )
        .bind(&request.consumer_name)
        .bind(request.task_id)
        .bind(request.tenant_id)
        .bind(&request.worker_id)
        .bind(delivery_count)
        .bind(request.lease_expires_at.into_datetime())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(map_sqlx)?;
        transaction.commit().await.map_err(map_sqlx)?;
        row.map(|row| {
            row.try_get::<i64, _>("receipt_sequence")
                .map(|sequence| DeliveryReceipt { sequence })
                .map_err(map_sqlx)
        })
        .transpose()
    }

    async fn attach_attempt(&self, receipt: DeliveryReceipt, attempt_id: Uuid) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE broker_delivery_receipts AS receipt
             SET attempt_id = $2, status = 'in_flight'
             FROM task_attempts AS attempt
             JOIN tasks AS task
               ON task.id = attempt.task_id
              AND task.tenant_id = attempt.tenant_id
             WHERE receipt.receipt_sequence = $1
               AND receipt.status IN ('reserved', 'in_flight')
               AND receipt.lease_expires_at > now()
               AND attempt.id = $2
               AND attempt.task_id = receipt.task_id
               AND attempt.tenant_id = receipt.tenant_id
               AND attempt.status = 'running'
               AND attempt.lease_expires > now()
               AND task.status = 'running'
               AND task.locked_until > now()
               AND task.last_attempt_id = attempt.id
               AND (receipt.attempt_id IS NULL OR receipt.attempt_id = $2)",
        )
        .bind(receipt.sequence)
        .bind(attempt_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    async fn renew(&self, receipt: DeliveryReceipt, lease_expires_at: Timestamp) -> Result<bool> {
        if !lease_expires_at.is_after(&Timestamp::now()) {
            return Ok(false);
        }
        let result = sqlx::query(
            "UPDATE broker_delivery_receipts
             SET lease_expires_at = $2
             WHERE receipt_sequence = $1
               AND status IN ('reserved', 'in_flight')
               AND lease_expires_at > now()",
        )
        .bind(receipt.sequence)
        .bind(lease_expires_at.into_datetime())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }

    async fn settle(
        &self,
        receipt: DeliveryReceipt,
        settlement: DeliveryReceiptSettlement,
    ) -> Result<bool> {
        let result = sqlx::query(
            "UPDATE broker_delivery_receipts
             SET status = $2, settled_at = now()
             WHERE receipt_sequence = $1
               AND status IN ('reserved', 'in_flight')
               AND lease_expires_at > now()",
        )
        .bind(receipt.sequence)
        .bind(settlement.as_str())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(result.rows_affected() == 1)
    }
}
