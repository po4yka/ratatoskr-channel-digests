//! Worker envelope admission and replay acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{Database, DeliveryDisposition, WorkerMessageHandler};
use uuid::Uuid;

#[tokio::test]
async fn exact_envelopes_drive_one_durable_effect() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone());
    let owner = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let idempotency_key = format!("platform-subscribe-{}", Uuid::now_v7());
    let envelope = serde_json::to_vec(&serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.subscription.set_requested.v1",
        "issued_at": "2026-08-29T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("channel-digest-subscription:{}", Uuid::now_v7()),
        "correlation_id": format!("operation:{operation_id}"),
        "tenant_id": format!("user:{owner}"),
        "schema_version": 1,
        "payload": {
            "operation_id": operation_id,
            "owner": format!("user:{owner}"),
            "idempotency_key": idempotency_key,
            "channel_username": "exact_worker_channel",
            "desired_state": "active"
        }
    }))?;

    assert_eq!(
        handler
            .handle(
                "cmd.channel_digest.subscription.set_requested.v1",
                &envelope,
            )
            .await,
        DeliveryDisposition::Ack
    );
    assert_eq!(
        handler
            .handle(
                "cmd.channel_digest.subscription.set_requested.v1",
                &envelope,
            )
            .await,
        DeliveryDisposition::Ack
    );
    let counts: (i64, i64, i64) = sqlx::query_as(
        "select (select count(*) from channel_digests.inbox_messages where message_id = $1),
                (select count(*) from channel_digests.subscriptions where owner_id = $2),
                (select count(*) from channel_digests.outbox_messages where semantic_key = $3)",
    )
    .bind(command_id)
    .bind(owner)
    .bind(&idempotency_key)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(counts, (1, 1, 1));

    let mut foreign = serde_json::from_slice::<serde_json::Value>(&envelope)?;
    foreign["producer"] = serde_json::json!("foreign-service");
    assert_eq!(
        handler
            .handle(
                "cmd.channel_digest.subscription.set_requested.v1",
                &serde_json::to_vec(&foreign)?,
            )
            .await,
        DeliveryDisposition::Term
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn run_envelope_preserves_selected_identity_and_replays()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone());
    let owner = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let run_id = Uuid::now_v7();
    let idempotency_key = format!("telegram.digest.{}", Uuid::now_v7());
    let envelope = serde_json::to_vec(&serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.run.requested.v1",
        "issued_at": "2026-08-29T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("channel-digest-run:{run_id}"),
        "correlation_id": format!("operation:{operation_id}"),
        "tenant_id": format!("user:{owner}"),
        "schema_version": 1,
        "payload": {
            "operation_id": operation_id,
            "owner": format!("user:{owner}"),
            "digest_run_id": run_id,
            "idempotency_key": idempotency_key,
            "window": {
                "start_at": "2026-08-28T10:00:00Z",
                "end_at": "2026-08-29T10:00:00Z"
            },
            "output_language": "ru",
            "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
        }
    }))?;
    for _ in 0..2 {
        assert_eq!(
            handler
                .handle("cmd.channel_digest.run.requested.v1", &envelope)
                .await,
            DeliveryDisposition::Ack
        );
    }
    let durable: (Uuid, i64, i64) = sqlx::query_as(
        "select r.run_id,
                (select count(*) from channel_digests.inbox_messages where message_id = $1),
                (select count(*) from channel_digests.outbox_messages where semantic_key = $2)
         from channel_digests.digest_runs r where r.owner_id = $3 and r.idempotency_key = $2",
    )
    .bind(command_id)
    .bind(&idempotency_key)
    .bind(owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(durable, (run_id, 1, 1));
    database.close().await;
    Ok(())
}
