//! Authoritative occurrence fan-out acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{
    Database, DigestCoordinator, IntakeOutcome, SubscriptionRepository,
};
use uuid::Uuid;

#[tokio::test]
async fn occurrences_fan_out_once_through_the_run_engine() -> Result<(), Box<dyn std::error::Error>>
{
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
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    let active_owner = Uuid::now_v7();
    let inactive_owner = Uuid::now_v7();
    subscriptions
        .set(
            active_owner,
            "schedule_active",
            true,
            "1999-12-29T10:00:00Z",
        )
        .await?;
    subscriptions
        .set(
            inactive_owner,
            "schedule_inactive",
            false,
            "1999-12-29T10:00:00Z",
        )
        .await?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    let occurrence = format!("schedule-occurrence:{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&serde_json::json!({"occurrence_ref": occurrence}))?;
    let message_id = Uuid::now_v7();
    assert_eq!(
        coordinator
            .accept_occurrence(
                message_id,
                &payload,
                &occurrence,
                "1999-12-30T10:00:00Z",
                "1999-12-31T10:00:00Z",
            )
            .await?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        coordinator
            .accept_occurrence(
                message_id,
                &payload,
                &occurrence,
                "1999-12-30T10:00:00Z",
                "1999-12-31T10:00:00Z",
            )
            .await?,
        IntakeOutcome::Replayed
    );
    let counts: (i64, i64) = sqlx::query_as(
        "select count(*), count(*) filter (where owner_id = $1) from channel_digests.digest_runs where idempotency_key = $2",
    ).bind(inactive_owner).bind(&occurrence).fetch_one(database.pool()).await?;
    assert_eq!(counts, (1, 0));
    let deliveries: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.outbox_messages where subject like 'telegram.%'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(deliveries.0, 0);
    database.close().await;
    Ok(())
}
