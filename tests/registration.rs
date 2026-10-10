//! Schedule registration queued at worker start (XR-021 CONTRACTS.md S08).

use std::time::Duration;

use ratatoskr_channel_digests::{
    Database, RegistrationOutcome, ScheduleConfig, enqueue_schedule_registration,
};
use ratatoskr_operation_contracts::PlatformScheduleRegistrationRequested;
use uuid::Uuid;

const REGISTRATION: &str = "platform.schedule.registration_requested.v1";

#[tokio::test]
async fn registration_row_is_enqueued_once_per_distinct_configuration()
-> Result<(), Box<dyn std::error::Error>> {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    let daily = ScheduleConfig {
        owner_user_id: owner,
        cron_expression: "0 6 * * *".to_owned(),
        enabled: true,
    };
    let later = ScheduleConfig {
        cron_expression: "30 18 * * *".to_owned(),
        ..daily.clone()
    };

    assert_eq!(
        enqueue_schedule_registration(database.pool(), Some(&daily)).await?,
        RegistrationOutcome::Enqueued
    );
    assert_eq!(
        enqueue_schedule_registration(database.pool(), Some(&daily)).await?,
        RegistrationOutcome::Unchanged,
        "an unchanged configuration is not sent again"
    );
    assert_eq!(rows(database.pool()).await?.len(), 1);

    let (payload, owner_id): (serde_json::Value, Uuid) = rows(database.pool()).await?.remove(0);
    assert_eq!(owner_id, owner);
    let registration: PlatformScheduleRegistrationRequested = serde_json::from_value(payload)?;
    assert_eq!(
        registration.service_name.as_str(),
        "ratatoskr-channel-digests"
    );
    assert_eq!(registration.name.as_str(), "daily-digest");
    assert_eq!(registration.owner_user_id.0, owner);
    assert_eq!(registration.cron_expression.as_str(), "0 6 * * *");
    assert_eq!(
        registration.command_type.to_wire(),
        "channel_digest.schedule.occurrence_requested.v1"
    );
    assert_eq!(
        registration.operation_kind.as_str(),
        "channel_digest.schedule.occurrence"
    );
    assert!(registration.payload.is_empty());
    assert!(registration.enabled);

    assert_eq!(
        enqueue_schedule_registration(database.pool(), Some(&later)).await?,
        RegistrationOutcome::Enqueued,
        "a changed cron expression registers again"
    );
    assert_eq!(rows(database.pool()).await?.len(), 2);

    sqlx::query(
        "update channel_digests.outbox_messages set published_at = now() where subject = $1",
    )
    .bind(REGISTRATION)
    .execute(database.pool())
    .await?;
    assert_eq!(
        enqueue_schedule_registration(database.pool(), Some(&daily)).await?,
        RegistrationOutcome::Enqueued,
        "returning to an earlier configuration must register it again, Platform holds the later one"
    );
    let pending: (i64, i64) = sqlx::query_as(
        "select count(*), count(*) filter (where published_at is null) from channel_digests.outbox_messages where subject = $1",
    )
    .bind(REGISTRATION)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        pending,
        (2, 1),
        "the earlier row is queued again, not duplicated"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn registration_is_skipped_without_an_owner() -> Result<(), Box<dyn std::error::Error>> {
    let database = fresh_database().await?;
    assert_eq!(
        enqueue_schedule_registration(database.pool(), None).await?,
        RegistrationOutcome::Skipped
    );
    assert!(rows(database.pool()).await?.is_empty());
    database.close().await;
    Ok(())
}

async fn fresh_database() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    sqlx::query(
        "truncate channel_digests.digest_results, channel_digests.digest_manifests,
                  channel_digests.post_revisions, channel_digests.subscriptions,
                  channel_digests.channels, channel_digests.digest_runs,
                  channel_digests.inbox_messages, channel_digests.outbox_messages,
                  channel_digests.leases cascade",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

async fn rows(pool: &sqlx::PgPool) -> Result<Vec<(serde_json::Value, Uuid)>, sqlx::Error> {
    sqlx::query_as(
        "select payload, owner_id from channel_digests.outbox_messages where subject = $1 order by created_at, outbox_id",
    )
    .bind(REGISTRATION)
    .fetch_all(pool)
    .await
}
