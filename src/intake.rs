//! Typed transactional command intake.

use uuid::Uuid;

use crate::reports::{OperationReportRow, ReportError};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRunRequested, ChannelDigestRunTrigger, ChannelDigestSubscriptionSetRequested,
    OutputLanguage, SubscriptionDesiredState,
};
use ratatoskr_operation_contracts::OperationStatus;
use sha2::{Digest as _, Sha256};

/// Replay-safe intake result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntakeOutcome {
    /// One new domain effect and outcome were committed.
    Applied,
    /// Transport or semantic identity was already durable.
    Replayed,
}

/// Safe command validation or storage failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum IntakeError {
    /// Payload is not a canonical current contract.
    #[error("invalid channel digest command")]
    Invalid,
    /// Atomic storage operation failed.
    #[error("command intake is unavailable")]
    Storage,
}

impl From<ReportError> for IntakeError {
    fn from(error: ReportError) -> Self {
        match error {
            ReportError::Invalid => Self::Invalid,
            ReportError::Storage => Self::Storage,
        }
    }
}

/// Typed inbox/domain/outbox transaction boundary.
#[derive(Debug, Clone)]
pub struct CommandIntake {
    pool: sqlx::PgPool,
}

impl CommandIntake {
    /// Creates intake over the owned finite pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Accepts one subscription command envelope payload.
    ///
    /// A subscription refused by the active-subscription limit is a durable, terminal outcome:
    /// the inbox row is recorded as failed, the operation reports `failed`, and the call returns
    /// success so the message is acknowledged instead of redelivered forever.
    ///
    /// # Errors
    ///
    /// Returns a finite validation or storage class.
    pub async fn accept_subscription(
        &self,
        message_id: Uuid,
        payload: &[u8],
    ) -> Result<IntakeOutcome, IntakeError> {
        let command: ChannelDigestSubscriptionSetRequested =
            serde_json::from_slice(payload).map_err(|_| IntakeError::Invalid)?;
        command
            .validate_for_publish()
            .map_err(|_| IntakeError::Invalid)?;
        let owner_id = command.owner.user_id().0;
        let semantic_key = command.idempotency_key.as_str();
        let payload_sha256 = format!("{:x}", Sha256::digest(payload));
        let mut transaction = self.pool.begin().await.map_err(|_| IntakeError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'channel_digest.subscription.set_requested.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(semantic_key)
        .bind(&payload_sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| IntakeError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        let enabled = command.desired_state == SubscriptionDesiredState::Active;
        let applied: Result<(Uuid, String, bool), sqlx::Error> = sqlx::query_as(
            "select subscription_id, first_activated_at::text, enabled from channel_digests.set_subscription($1, $2, $3, $4, $5, now())",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(owner_id)
        .bind(command.channel_username.as_str())
        .bind(enabled)
        .fetch_one(&mut *transaction)
        .await;
        if let Err(error) = applied {
            if !is_limit_error(&error) {
                return Err(IntakeError::Storage);
            }
            transaction
                .rollback()
                .await
                .map_err(|_| IntakeError::Storage)?;
            return self
                .reject_subscription_limit(message_id, &command, &payload_sha256)
                .await;
        }
        OperationReportRow::new(
            command.operation_id.0,
            owner_id,
            OperationStatus::Succeeded,
            "applied",
        )?
        .caused_by(format!("command:{message_id}"))
        .enqueue(&mut transaction)
        .await?;
        sqlx::query(
            "update channel_digests.inbox_messages set state = 'completed', completed_at = now() where message_id = $1",
        )
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| IntakeError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }

    /// Records a limit refusal in a new transaction, because the refused one is aborted.
    async fn reject_subscription_limit(
        &self,
        message_id: Uuid,
        command: &ChannelDigestSubscriptionSetRequested,
        payload_sha256: &str,
    ) -> Result<IntakeOutcome, IntakeError> {
        let mut transaction = self.pool.begin().await.map_err(|_| IntakeError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state, completed_at, safe_failure_class) values ($1, 'channel_digest.subscription.set_requested.v1', $2, $3, 'failed', now(), 'subscription_limit') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(command.idempotency_key.as_str())
        .bind(payload_sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| IntakeError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        OperationReportRow::new(
            command.operation_id.0,
            command.owner.user_id().0,
            OperationStatus::Failed,
            "rejected",
        )?
        .caused_by(format!("command:{message_id}"))
        .with_error(
            "channel_digest.subscription_limit_reached",
            "At most 20 channels can be active at the same time.",
            false,
        )?
        .enqueue(&mut transaction)
        .await?;
        transaction
            .commit()
            .await
            .map_err(|_| IntakeError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }

    /// Accepts one run command envelope payload while preserving Platform's selected run identity.
    ///
    /// # Errors
    ///
    /// Returns a finite validation or storage class.
    pub async fn accept_run(
        &self,
        message_id: Uuid,
        payload: &[u8],
    ) -> Result<IntakeOutcome, IntakeError> {
        let command: ChannelDigestRunRequested =
            serde_json::from_slice(payload).map_err(|_| IntakeError::Invalid)?;
        command
            .validate_for_publish()
            .map_err(|_| IntakeError::Invalid)?;
        let owner_id = command.owner.user_id().0;
        let semantic_key = command.idempotency_key.as_str();
        let payload_sha256 = format!("{:x}", Sha256::digest(payload));
        let on_demand = matches!(command.trigger, ChannelDigestRunTrigger::OnDemand { .. });
        let mut transaction = self.pool.begin().await.map_err(|_| IntakeError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'channel_digest.run.requested.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(semantic_key)
        .bind(payload_sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| IntakeError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        let run_id = command.digest_run_id.as_uuid();
        let selected: (Uuid,) = sqlx::query_as(
            "select channel_digests.create_digest_run($1, $2, $3, $4, $5, $6::timestamptz, $7::timestamptz)",
        )
        .bind(run_id)
        .bind(owner_id)
        .bind(command.operation_id.0)
        .bind(if on_demand { "on_demand" } else { "scheduled" })
        .bind(semantic_key)
        .bind(command.window.start_at.to_string())
        .bind(command.window.end_at.to_string())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        if selected.0 != run_id {
            return Err(IntakeError::Invalid);
        }
        let output_language = match command.output_language {
            OutputLanguage::Ru => "ru",
            OutputLanguage::En => "en",
        };
        sqlx::query(
            "update channel_digests.digest_runs set output_language = $1 where run_id = $2 and owner_id = $3",
        )
        .bind(output_language)
        .bind(run_id)
        .bind(owner_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        if on_demand {
            OperationReportRow::new(
                command.operation_id.0,
                owner_id,
                OperationStatus::Running,
                "acquiring",
            )?
            .caused_by(format!("command:{message_id}"))
            .enqueue(&mut transaction)
            .await?;
        }
        sqlx::query(
            "update channel_digests.inbox_messages set state = 'completed', completed_at = now() where message_id = $1",
        )
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| IntakeError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }
}

/// Whether the storage error is the active-subscription limit raised by `set_subscription`.
fn is_limit_error(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref()
        == Some("P0001")
}
