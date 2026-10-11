//! Typed transactional command intake.

use uuid::Uuid;

use crate::reports::{OperationReportRow, ReportError};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRunRequested, ChannelDigestRunTrigger, ChannelDigestSubscriptionSetRequested,
    OutputLanguage, SubscriptionDesiredState,
};
use ratatoskr_identifiers::{OperationId, TenantRef};
use ratatoskr_operation_contracts::OperationStatus;
use sha2::{Digest as _, Sha256};

const SUBSCRIPTION_SUBJECT: &str = "channel_digest.subscription.set_requested.v1";
const RUN_SUBJECT: &str = "channel_digest.run.requested.v1";

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

/// The command a payload that did not decode claimed to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommandKind {
    /// `channel_digest.subscription.set_requested.v1`.
    Subscription,
    /// `channel_digest.run.requested.v1`.
    Run,
}

impl CommandKind {
    fn subject(self) -> &'static str {
        match self {
            Self::Subscription => SUBSCRIPTION_SUBJECT,
            Self::Run => RUN_SUBJECT,
        }
    }
}

/// A command refused for good, recorded as a failed inbox row and a failed operation report.
struct Rejection<'a> {
    message_id: Uuid,
    subject: &'static str,
    semantic_key: &'a str,
    payload_sha256: &'a str,
    operation_id: Uuid,
    owner_id: Uuid,
    reason: RejectionReason,
}

/// Closed reason a command is refused for good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RejectionReason {
    /// The owner already has 20 active subscriptions.
    SubscriptionLimit,
    /// The command is attributable but violates the contract or an invariant.
    CommandInvalid,
}

impl RejectionReason {
    fn class(self) -> &'static str {
        match self {
            Self::SubscriptionLimit => "subscription_limit",
            Self::CommandInvalid => "command_invalid",
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::SubscriptionLimit => "channel_digest.subscription_limit_reached",
            Self::CommandInvalid => "channel_digest.command_invalid",
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::SubscriptionLimit => "At most 20 channels can be active at the same time.",
            Self::CommandInvalid => "The command was rejected because it is not valid.",
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
        let owner_id = command.owner.user_id().0;
        let semantic_key = command.idempotency_key.as_str();
        let payload_sha256 = format!("{:x}", Sha256::digest(payload));
        if command.validate_for_publish().is_err() {
            return self
                .reject(&Rejection {
                    message_id,
                    subject: SUBSCRIPTION_SUBJECT,
                    semantic_key,
                    payload_sha256: &payload_sha256,
                    operation_id: command.operation_id.0,
                    owner_id,
                    reason: RejectionReason::CommandInvalid,
                })
                .await;
        }
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
                .reject(&Rejection {
                    message_id,
                    subject: SUBSCRIPTION_SUBJECT,
                    semantic_key,
                    payload_sha256: &payload_sha256,
                    operation_id: command.operation_id.0,
                    owner_id,
                    reason: RejectionReason::SubscriptionLimit,
                })
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

    /// Refuses a payload that did not decode into its typed command, when it is attributable.
    ///
    /// The payload is attributable when it names an `operation_id` and an `owner` that equals the
    /// envelope tenant the producer was authenticated for. It is recorded as a failed inbox row,
    /// the operation reports `failed`, and the call returns success so the message is
    /// acknowledged. Nothing else about the payload is read or stored.
    ///
    /// # Errors
    ///
    /// Returns [`IntakeError::Invalid`] when the payload cannot be attributed, and a storage class
    /// when the rejection cannot be made durable.
    pub async fn reject_unreadable(
        &self,
        message_id: Uuid,
        kind: CommandKind,
        tenant: &TenantRef,
        payload: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<IntakeOutcome, IntakeError> {
        let operation_id = payload
            .get("operation_id")
            .and_then(|value| serde_json::from_value::<OperationId>(value.clone()).ok())
            .ok_or(IntakeError::Invalid)?;
        let owner = payload
            .get("owner")
            .and_then(|value| serde_json::from_value::<TenantRef>(value.clone()).ok())
            .ok_or(IntakeError::Invalid)?;
        if &owner != tenant {
            return Err(IntakeError::Invalid);
        }
        let digest = serde_json::to_vec(payload).map_err(|_| IntakeError::Invalid)?;
        self.reject(&Rejection {
            message_id,
            subject: kind.subject(),
            semantic_key: &format!("invalid:{message_id}"),
            payload_sha256: &format!("{:x}", Sha256::digest(digest)),
            operation_id: operation_id.0,
            owner_id: owner.user_id().0,
            reason: RejectionReason::CommandInvalid,
        })
        .await
    }

    /// Records a refusal in its own transaction, because a refused one may be aborted.
    async fn reject(&self, rejection: &Rejection<'_>) -> Result<IntakeOutcome, IntakeError> {
        let mut transaction = self.pool.begin().await.map_err(|_| IntakeError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state, completed_at, safe_failure_class) values ($1, $2, $3, $4, 'failed', now(), $5) on conflict do nothing returning message_id",
        )
        .bind(rejection.message_id)
        .bind(rejection.subject)
        .bind(rejection.semantic_key)
        .bind(rejection.payload_sha256)
        .bind(rejection.reason.class())
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
            rejection.operation_id,
            rejection.owner_id,
            OperationStatus::Failed,
            "rejected",
        )?
        .caused_by(format!("command:{}", rejection.message_id))
        .with_error(rejection.reason.code(), rejection.reason.message(), false)?
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
        let owner_id = command.owner.user_id().0;
        let semantic_key = command.idempotency_key.as_str();
        let payload_sha256 = format!("{:x}", Sha256::digest(payload));
        let invalid = Rejection {
            message_id,
            subject: RUN_SUBJECT,
            semantic_key,
            payload_sha256: &payload_sha256,
            operation_id: command.operation_id.0,
            owner_id,
            reason: RejectionReason::CommandInvalid,
        };
        if command.validate_for_publish().is_err() {
            return self.reject(&invalid).await;
        }
        let on_demand = matches!(command.trigger, ChannelDigestRunTrigger::OnDemand { .. });
        let mut transaction = self.pool.begin().await.map_err(|_| IntakeError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'channel_digest.run.requested.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
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
            transaction
                .rollback()
                .await
                .map_err(|_| IntakeError::Storage)?;
            return self.reject(&invalid).await;
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
