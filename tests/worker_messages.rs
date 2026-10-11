//! Worker envelope admission and replay acceptance.

use std::time::Duration;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{
    Database, DeliveryDisposition, SubscriptionRepository, WorkerMessageHandler,
};
use uuid::Uuid;

#[tokio::test]
async fn exact_envelopes_drive_one_durable_effect() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);
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
    .bind(format!("operation:{operation_id}:succeeded"))
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
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);
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
                (select count(*) from channel_digests.outbox_messages where semantic_key = $4)
         from channel_digests.digest_runs r where r.owner_id = $3 and r.idempotency_key = $2",
    )
    .bind(command_id)
    .bind(&idempotency_key)
    .bind(owner)
    .bind(format!("operation:{operation_id}:running"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(durable, (run_id, 1, 1));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn schedule_occurrence_envelope_fans_out_once_to_active_owners()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(
            owner,
            "scheduled_worker_channel",
            true,
            "2026-08-19T10:00:00Z",
        )
        .await?;
    let command_id = Uuid::now_v7();
    let occurrence_id = Uuid::now_v7();
    let envelope = serde_json::to_vec(&serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.schedule.occurrence_requested.v1",
        "issued_at": "2026-08-21T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("schedule-occurrence:{occurrence_id}"),
        "correlation_id": format!("operation:{}", Uuid::now_v7()),
        "tenant_id": format!("user:{}", Uuid::now_v7()),
        "schema_version": 1,
        "payload": {
            "schedule_ref": format!("schedule:{}", Uuid::now_v7()),
            "occurrence_ref": format!("schedule-occurrence:{occurrence_id}"),
            "previous_due_at": "2026-08-20T10:00:00Z",
            "due_at": "2026-08-21T10:00:00Z"
        }
    }))?;
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);

    for _ in 0..2 {
        assert_eq!(
            handler
                .handle(
                    "cmd.channel_digest.schedule.occurrence_requested.v1",
                    &envelope,
                )
                .await,
            DeliveryDisposition::Ack
        );
    }
    let counts: (i64, i64) = sqlx::query_as(
        "select (select count(*) from channel_digests.digest_runs where owner_id = $1 and idempotency_key = $2),
                (select count(*) from channel_digests.inbox_messages where message_id = $3)",
    )
    .bind(owner)
    .bind(format!("schedule-occurrence:{occurrence_id}"))
    .bind(command_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(counts, (1, 1));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn schedule_occurrence_accepts_a_platform_contract_envelope_and_terminates_its_operation()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let subscriber = Uuid::now_v7();
    let schedule_owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(
            subscriber,
            "occurrence_contract_channel",
            true,
            "2026-08-19T10:00:00Z",
        )
        .await?;
    let occurrence_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let envelope = serde_json::json!({
        "command_id": occurrence_id,
        "command_type": "channel_digest.schedule.occurrence_requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("schedule-occurrence:{occurrence_id}"),
        "correlation_id": format!("operation:{operation_id}"),
        "tenant_id": format!("user:{schedule_owner}"),
        "schema_version": 1,
        "payload": {
            "schedule_ref": format!("schedule:{}", Uuid::now_v7()),
            "occurrence_ref": format!("schedule-occurrence:{occurrence_id}"),
            "previous_due_at": "2026-08-20T10:00:00Z",
            "due_at": "2026-08-21T10:00:00Z"
        }
    });
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);
    let subject = "cmd.channel_digest.schedule.occurrence_requested.v1";

    for _ in 0..2 {
        assert_eq!(
            handler
                .handle(subject, &serde_json::to_vec(&envelope)?)
                .await,
            DeliveryDisposition::Ack
        );
    }

    let reports: (i64, i64) = sqlx::query_as(
        "select count(*), count(*) filter (where owner_id = $2 and payload->>'status' = 'succeeded' and payload->>'stage' = 'fanned_out')
         from channel_digests.outbox_messages
         where subject = 'platform.operation.reported.v1' and operation_id = $1",
    )
    .bind(operation_id)
    .bind(schedule_owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        reports,
        (1, 1),
        "one succeeded report on the occurrence operation, owned by the schedule owner"
    );
    let run_operation: (Uuid,) = sqlx::query_as(
        "select operation_id from channel_digests.digest_runs where owner_id = $1 and idempotency_key = $2",
    )
    .bind(subscriber)
    .bind(format!("schedule-occurrence:{occurrence_id}"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        run_operation.0, operation_id,
        "fanned-out runs carry the occurrence operation"
    );
    let mut without_tenant = envelope.clone();
    without_tenant
        .as_object_mut()
        .ok_or("envelope object")?
        .remove("tenant_id");
    let mut foreign_correlation = envelope.clone();
    foreign_correlation["correlation_id"] = serde_json::json!(format!("event:{}", Uuid::now_v7()));
    for malformed in [without_tenant, foreign_correlation] {
        assert_eq!(
            handler
                .handle(subject, &serde_json::to_vec(&malformed)?)
                .await,
            DeliveryDisposition::Term,
            "an occurrence without its operation or owner must be terminated"
        );
    }
    database.close().await;
    Ok(())
}

const RUN_SUBJECT: &str = "cmd.channel_digest.run.requested.v1";
const SUBSCRIPTION_SUBJECT: &str = "cmd.channel_digest.subscription.set_requested.v1";

fn run_envelope(owner: Uuid, operation: Uuid, command_id: Uuid) -> serde_json::Value {
    let run_id = Uuid::now_v7();
    serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.run.requested.v1",
        "issued_at": "2026-08-29T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("channel-digest-run:{run_id}"),
        "correlation_id": format!("operation:{operation}"),
        "tenant_id": format!("user:{owner}"),
        "schema_version": 1,
        "payload": {
            "operation_id": operation,
            "owner": format!("user:{owner}"),
            "digest_run_id": run_id,
            "idempotency_key": format!("operation.{operation}"),
            "window": {"start_at": "2026-08-28T10:00:00Z", "end_at": "2026-08-29T10:00:00Z"},
            "output_language": "ru",
            "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
        }
    })
}

fn subscription_envelope(owner: Uuid, operation: Uuid, command_id: Uuid) -> serde_json::Value {
    serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.subscription.set_requested.v1",
        "issued_at": "2026-08-29T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("operation:{operation}"),
        "correlation_id": format!("operation:{operation}"),
        "tenant_id": format!("user:{owner}"),
        "schema_version": 1,
        "payload": {
            "operation_id": operation,
            "owner": format!("user:{owner}"),
            "idempotency_key": format!("operation.{operation}"),
            "channel_username": "rejected_channel",
            "desired_state": "active"
        }
    })
}

async fn rejection_state(
    database: &Database,
    operation: Uuid,
    command_id: Uuid,
) -> Result<(i64, Vec<(String, String, bool)>, Option<String>), Box<dyn std::error::Error>> {
    let reports: Vec<(String, String, bool)> = sqlx::query_as(
        "select semantic_key, payload->'error'->>'code', (payload->'error'->>'retryable')::boolean
         from channel_digests.outbox_messages
         where subject = 'platform.operation.reported.v1' and operation_id = $1
         order by created_at, outbox_id",
    )
    .bind(operation)
    .fetch_all(database.pool())
    .await?;
    let runs: (i64,) =
        sqlx::query_as("select count(*) from channel_digests.digest_runs where operation_id = $1")
            .bind(operation)
            .fetch_one(database.pool())
            .await?;
    let inbox: Option<(String,)> =
        sqlx::query_as("select state from channel_digests.inbox_messages where message_id = $1")
            .bind(command_id)
            .fetch_optional(database.pool())
            .await?;
    Ok((runs.0, reports, inbox.map(|row| row.0)))
}

#[tokio::test]
async fn a_run_command_that_fails_validation_after_decoding_reports_failed()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);

    // The payload decodes into the typed command, then fails `validate_for_publish` because a
    // producer-authored extension field is present.
    let owner = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let mut extension = run_envelope(owner, operation, command_id);
    extension["payload"]["surprise"] = serde_json::json!(1);
    // The payload does not decode into the typed command (the window is longer than the contract
    // allows) but it still names its operation, and its owner matches the envelope tenant.
    let long_owner = Uuid::now_v7();
    let long_operation = Uuid::now_v7();
    let long_command = Uuid::now_v7();
    let mut too_long = run_envelope(long_owner, long_operation, long_command);
    too_long["payload"]["window"]["start_at"] = serde_json::json!("2026-08-01T10:00:00Z");

    for (envelope, operation, command_id) in [
        (extension, operation, command_id),
        (too_long, long_operation, long_command),
    ] {
        for _ in 0..2 {
            assert_eq!(
                handler
                    .handle(RUN_SUBJECT, &serde_json::to_vec(&envelope)?)
                    .await,
                DeliveryDisposition::Ack,
                "an attributable invalid command is acknowledged, not terminated"
            );
        }
        let (runs, reports, inbox) = rejection_state(&database, operation, command_id).await?;
        assert_eq!(runs, 0, "an invalid command creates no run");
        assert_eq!(
            reports,
            [(
                format!("operation:{operation}:failed"),
                "channel_digest.command_invalid".to_owned(),
                false
            )],
            "exactly one non-retryable failed report"
        );
        assert_eq!(inbox.as_deref(), Some("failed"));
    }
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_subscription_command_that_fails_validation_reports_failed()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);

    let owner = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let command_id = Uuid::now_v7();
    let mut extension = subscription_envelope(owner, operation, command_id);
    extension["payload"]["surprise"] = serde_json::json!(1);
    let bad_owner = Uuid::now_v7();
    let bad_operation = Uuid::now_v7();
    let bad_command = Uuid::now_v7();
    let mut bad_name = subscription_envelope(bad_owner, bad_operation, bad_command);
    bad_name["payload"]["channel_username"] = serde_json::json!("Not A Channel");

    for (envelope, operation, command_id) in [
        (extension, operation, command_id),
        (bad_name, bad_operation, bad_command),
    ] {
        assert_eq!(
            handler
                .handle(SUBSCRIPTION_SUBJECT, &serde_json::to_vec(&envelope)?)
                .await,
            DeliveryDisposition::Ack
        );
        let (_, reports, inbox) = rejection_state(&database, operation, command_id).await?;
        assert_eq!(
            reports,
            [(
                format!("operation:{operation}:failed"),
                "channel_digest.command_invalid".to_owned(),
                false
            )]
        );
        assert_eq!(inbox.as_deref(), Some("failed"));
    }
    let stored: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.subscriptions where owner_id in ($1, $2)",
    )
    .bind(owner)
    .bind(bad_owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, 0, "a rejected subscription must not exist");
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_command_that_cannot_be_attributed_is_still_terminated_without_a_report()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::Ru);
    let base = run_envelope(Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let operation = Uuid::now_v7();

    let mut no_operation = base.clone();
    no_operation["payload"]
        .as_object_mut()
        .ok_or("payload object")?
        .remove("operation_id");
    let mut garbage = base.clone();
    garbage["payload"] = serde_json::json!({"garbage": true});
    let mut foreign_producer = run_envelope(Uuid::now_v7(), operation, Uuid::now_v7());
    foreign_producer["producer"] = serde_json::json!("foreign-service");
    foreign_producer["payload"]["surprise"] = serde_json::json!(1);
    let mut foreign_owner = run_envelope(Uuid::now_v7(), operation, Uuid::now_v7());
    foreign_owner["payload"]["owner"] = serde_json::json!(format!("user:{}", Uuid::now_v7()));
    foreign_owner["payload"]["surprise"] = serde_json::json!(1);
    let mut no_tenant = run_envelope(Uuid::now_v7(), operation, Uuid::now_v7());
    no_tenant["payload"]["surprise"] = serde_json::json!(1);
    no_tenant
        .as_object_mut()
        .ok_or("envelope object")?
        .remove("tenant_id");
    let mut wrong_type = run_envelope(Uuid::now_v7(), operation, Uuid::now_v7());
    wrong_type["command_type"] = serde_json::json!("channel_digest.subscription.set_requested.v1");
    wrong_type["payload"]["surprise"] = serde_json::json!(1);

    for envelope in [
        no_operation,
        garbage,
        foreign_producer,
        foreign_owner,
        no_tenant,
        wrong_type,
    ] {
        assert_eq!(
            handler
                .handle(RUN_SUBJECT, &serde_json::to_vec(&envelope)?)
                .await,
            DeliveryDisposition::Term
        );
    }
    assert_eq!(
        handler.handle(RUN_SUBJECT, b"{not json").await,
        DeliveryDisposition::Term
    );
    let reports: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.outbox_messages where operation_id = $1",
    )
    .bind(operation)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(reports.0, 0, "an unattributable command produces no report");
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn the_worker_handler_passes_its_schedule_language_to_the_fan_out()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "handler_language", true, "2026-08-19T10:00:00Z")
        .await?;
    let occurrence_id = Uuid::now_v7();
    let envelope = serde_json::json!({
        "command_id": occurrence_id,
        "command_type": "channel_digest.schedule.occurrence_requested.v1",
        "issued_at": "2026-08-21T10:00:01Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("schedule-occurrence:{occurrence_id}"),
        "correlation_id": format!("operation:{}", Uuid::now_v7()),
        "tenant_id": format!("user:{}", Uuid::now_v7()),
        "schema_version": 1,
        "payload": {
            "schedule_ref": format!("schedule:{}", Uuid::now_v7()),
            "occurrence_ref": format!("schedule-occurrence:{occurrence_id}"),
            "previous_due_at": "2026-08-20T10:00:00Z",
            "due_at": "2026-08-21T10:00:00Z"
        }
    });
    let handler = WorkerMessageHandler::new(database.pool().clone(), OutputLanguage::En);
    assert_eq!(
        handler
            .handle(
                "cmd.channel_digest.schedule.occurrence_requested.v1",
                &serde_json::to_vec(&envelope)?,
            )
            .await,
        DeliveryDisposition::Ack
    );
    let stored: (String,) = sqlx::query_as(
        "select output_language from channel_digests.digest_runs where owner_id = $1 and idempotency_key = $2",
    )
    .bind(owner)
    .bind(format!("schedule-occurrence:{occurrence_id}"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(stored.0, "en");
    database.close().await;
    Ok(())
}
