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
        "select (select count(*) from channel_digests.inbox_messages where semantic_key = $1), (select count(*) from channel_digests.outbox_messages where semantic_key = $3), (select count(*) from channel_digests.subscriptions where owner_id = $2)",
    )
    .bind(&semantic)
    .bind(owner)
    .bind(format!("operation:{operation}:succeeded"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(counts, (1, 1, 1));

    // A producer-authored extension is not a valid command. It names its operation and owner, so it
    // is rejected as a failed operation and acknowledged; nothing of the payload is stored.
    let rejected_operation = Uuid::now_v7();
    let mut extended: serde_json::Value = serde_json::from_slice(&payload)?;
    let object = extended.as_object_mut().ok_or("object")?;
    object.insert("credential".into(), serde_json::json!("must-not-pass"));
    object.insert("operation_id".into(), serde_json::json!(rejected_operation));
    object.insert(
        "idempotency_key".into(),
        serde_json::json!(format!("telegram.subscribe.{}", Uuid::now_v7())),
    );
    object.insert(
        "channel_username".into(),
        serde_json::json!("rejected_intake"),
    );
    assert_eq!(
        intake
            .accept_subscription(Uuid::now_v7(), &serde_json::to_vec(&extended)?)
            .await?,
        IntakeOutcome::Applied
    );
    let rejected: (i64, i64, i64) = sqlx::query_as(
        "select (select count(*) from channel_digests.outbox_messages where semantic_key = $1 and payload->'error'->>'code' = 'channel_digest.command_invalid' and payload::text not like '%must-not-pass%'),
                (select count(*) from channel_digests.subscriptions where owner_id = $2),
                (select count(*) from channel_digests.inbox_messages where state = 'failed' and safe_failure_class = 'command_invalid' and semantic_key like 'telegram.subscribe.%')",
    )
    .bind(format!("operation:{rejected_operation}:failed"))
    .bind(owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        (rejected.0, rejected.1),
        (1, 1),
        "one failed report, no new subscription"
    );
    assert!(rejected.2 >= 1);
    database.close().await;
    Ok(())
}
