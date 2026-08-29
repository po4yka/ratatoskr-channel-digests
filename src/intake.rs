//! Typed transactional command intake.

use uuid::Uuid;

use ratatoskr_channel_digest_contracts::{
    ChannelDigestRunRequested, ChannelDigestRunTrigger, ChannelDigestSubscriptionSetRequested,
    OutputLanguage, SubscriptionDesiredState,
};
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
        let enabled = command.desired_state == SubscriptionDesiredState::Active;
        let _subscription: (Uuid, String, bool) = sqlx::query_as(
            "select subscription_id, first_activated_at::text, enabled from channel_digests.set_subscription($1, $2, $3, $4, $5, now())",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(owner_id)
        .bind(command.channel_username.as_str())
        .bind(enabled)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
        let outcome = serde_json::json!({
            "operation_id": command.operation_id,
            "status": "completed"
        });
        sqlx::query(
            "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, operation_id, payload) values ($1, 'platform.operation.reported.v1', $2, $3, $4, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(semantic_key)
        .bind(owner_id)
        .bind(command.operation_id.0)
        .bind(outcome)
        .execute(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
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
        let trigger = match command.trigger {
            ChannelDigestRunTrigger::OnDemand { .. } => "on_demand",
            ChannelDigestRunTrigger::Scheduled { .. } => "scheduled",
        };
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
            "select channel_digests.create_digest_run($1, $2, $3, $4, $5::timestamptz, $6::timestamptz)",
        )
        .bind(run_id)
        .bind(owner_id)
        .bind(trigger)
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
        let outcome = serde_json::json!({
            "operation_id": command.operation_id,
            "status": "running"
        });
        sqlx::query(
            "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, operation_id, payload) values ($1, 'platform.operation.reported.v1', $2, $3, $4, $5)",
        )
        .bind(Uuid::now_v7())
        .bind(semantic_key)
        .bind(owner_id)
        .bind(command.operation_id.0)
        .bind(outcome)
        .execute(&mut *transaction)
        .await
        .map_err(|_| IntakeError::Storage)?;
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
