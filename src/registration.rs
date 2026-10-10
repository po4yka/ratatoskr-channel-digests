//! Registration of this service's daily digest schedule with Platform.
//!
//! The registration is one outbox command, `platform.schedule.registration_requested.v1`, queued
//! at worker start. Platform upserts on `(service_name, name)`, so the command is sent again only
//! when the configured owner, cron expression or enablement differs from what was queued last.

use ratatoskr_channel_digest_contracts::ChannelDigestScheduleOccurrenceRequested;
use ratatoskr_event_envelope::CommandPayload as _;
use ratatoskr_identifiers::{Extensions, UserId};
use ratatoskr_operation_contracts::{
    OperationKind, PlatformScheduleRegistrationRequested, ScheduleCronExpression,
    ScheduleRegistrationLabel,
};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::ScheduleConfig;

/// Name this service registers under, equal to the envelope producer.
const SERVICE_NAME: &str = "ratatoskr-channel-digests";

/// Name of the one schedule this service owns.
const SCHEDULE_NAME: &str = "daily-digest";

/// Operation kind Platform mints for each occurrence of the schedule.
const OPERATION_KIND: &str = "channel_digest.schedule.occurrence";

/// What a registration attempt did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationOutcome {
    /// A registration command was queued for this configuration.
    Enqueued,
    /// The last queued registration already carries this configuration.
    Unchanged,
    /// No schedule owner is configured, so nothing is registered.
    Skipped,
}

/// Safe registration failure.
#[derive(Debug, thiserror::Error)]
#[error("channel digest schedule registration is unavailable")]
pub struct RegistrationError;

/// Queues the schedule registration for the configured owner, cron expression and enablement.
///
/// # Errors
///
/// Returns a safe storage class when the registration cannot be made durable.
pub async fn enqueue_schedule_registration(
    pool: &sqlx::PgPool,
    schedule: Option<&ScheduleConfig>,
) -> Result<RegistrationOutcome, RegistrationError> {
    let Some(schedule) = schedule else {
        tracing::info!(
            class = "schedule_owner_absent",
            "no schedule owner is configured; nothing is registered with Platform"
        );
        return Ok(RegistrationOutcome::Skipped);
    };
    let payload = serde_json::to_value(registration(schedule)?).map_err(|_| RegistrationError)?;
    let semantic_key = format!(
        "{:x}",
        Sha256::digest(format!(
            "{}|{}|{}",
            schedule.owner_user_id, schedule.cron_expression, schedule.enabled
        ))
    );
    let mut transaction = pool.begin().await.map_err(|_| RegistrationError)?;
    let latest: Option<(String,)> = sqlx::query_as(
        "select semantic_key from channel_digests.outbox_messages where subject = 'platform.schedule.registration_requested.v1' order by created_at desc, outbox_id desc limit 1",
    )
    .fetch_optional(&mut *transaction)
    .await
    .map_err(|_| RegistrationError)?;
    if latest.is_some_and(|(key,)| key == semantic_key) {
        return Ok(RegistrationOutcome::Unchanged);
    }
    sqlx::query(
        "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, payload) values ($1, 'platform.schedule.registration_requested.v1', $2, $3, $4) on conflict (subject, semantic_key) do update set outbox_id = excluded.outbox_id, created_at = now(), published_at = null, attempts = 0, next_attempt_at = now(), safe_failure_class = null, payload = excluded.payload",
    )
    .bind(Uuid::now_v7())
    .bind(semantic_key)
    .bind(schedule.owner_user_id)
    .bind(payload)
    .execute(&mut *transaction)
    .await
    .map_err(|_| RegistrationError)?;
    transaction.commit().await.map_err(|_| RegistrationError)?;
    Ok(RegistrationOutcome::Enqueued)
}

fn registration(
    schedule: &ScheduleConfig,
) -> Result<PlatformScheduleRegistrationRequested, RegistrationError> {
    Ok(PlatformScheduleRegistrationRequested {
        service_name: ScheduleRegistrationLabel::parse(SERVICE_NAME)
            .map_err(|_| RegistrationError)?,
        name: ScheduleRegistrationLabel::parse(SCHEDULE_NAME).map_err(|_| RegistrationError)?,
        owner_user_id: UserId(schedule.owner_user_id),
        cron_expression: ScheduleCronExpression::parse(&schedule.cron_expression)
            .map_err(|_| RegistrationError)?,
        command_type: ChannelDigestScheduleOccurrenceRequested::command_type(),
        operation_kind: OperationKind::parse(OPERATION_KIND).map_err(|_| RegistrationError)?,
        payload: serde_json::Map::new(),
        enabled: schedule.enabled,
        extensions: Extensions::new(),
    })
}
