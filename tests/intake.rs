//! Contract validation and atomic replay acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{CommandIntake, Database, IntakeOutcome};
use uuid::Uuid;

#[tokio::test]
async fn typed_commands_are_deduplicated_and_atomic() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let intake = CommandIntake::new(database.pool().clone());
    let owner = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let semantic = format!("telegram.subscribe.{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&serde_json::json!({
        "operation_id": operation,
        "owner": format!("user:{owner}"),
        "idempotency_key": semantic,
        "channel_username": "example_intake",
        "desired_state": "active"
    }))?;
    let transport = Uuid::now_v7();
    assert_eq!(
        intake.accept_subscription(transport, &payload).await?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        intake.accept_subscription(transport, &payload).await?,
        IntakeOutcome::Replayed
    );
    assert_eq!(
        intake.accept_subscription(Uuid::now_v7(), &payload).await?,
        IntakeOutcome::Replayed
    );

    let counts: (i64, i64, i64) = sqlx::query_as(
        "select (select count(*) from channel_digests.inbox_messages where semantic_key = $1), (select count(*) from channel_digests.outbox_messages where semantic_key = $1), (select count(*) from channel_digests.subscriptions where owner_id = $2)",
    )
    .bind(&semantic)
    .bind(owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(counts, (1, 1, 1));

    let mut extended: serde_json::Value = serde_json::from_slice(&payload)?;
    extended
        .as_object_mut()
        .ok_or("object")?
        .insert("credential".into(), serde_json::json!("must-not-pass"));
    assert!(
        intake
            .accept_subscription(Uuid::now_v7(), &serde_json::to_vec(&extended)?)
            .await
            .is_err()
    );
    database.close().await;
    Ok(())
}
