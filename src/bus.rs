//! Exact `JetStream` delivery boundary for the channel-digest worker.

use std::time::Duration;

use async_nats::jetstream;
use futures_util::StreamExt as _;

use ratatoskr_channel_digest_contracts::{
    ChannelDigestRunRequested, ChannelDigestScheduleOccurrenceRequested,
    ChannelDigestSubscriptionSetRequested, KnowledgeChannelDigestRecapCompleted,
    KnowledgeChannelDigestRecapFailed,
};
use ratatoskr_event_envelope::{
    CommandEnvelope, CommandPayload as _, EventEnvelope, EventPayload as _,
};
use ratatoskr_identifiers::WireTimestamp;
use tokio::sync::watch;
use uuid::Uuid;

use crate::runtime::WorkerReadiness;
use crate::{CommandIntake, CoordinatorError, DigestCoordinator, IntakeError};

const SUBSCRIPTION_SUBJECT: &str = "cmd.channel_digest.subscription.set_requested.v1";
const RUN_SUBJECT: &str = "cmd.channel_digest.run.requested.v1";
const SCHEDULE_SUBJECT: &str = "cmd.channel_digest.schedule.occurrence_requested.v1";
const PLATFORM_PRODUCER: &str = "ratatoskr-platform";
const COMPLETED_SUBJECT: &str = "evt.knowledge.channel_digest_recap.completed.v1";
const FAILED_SUBJECT: &str = "evt.knowledge.channel_digest_recap.failed.v1";
const KNOWLEDGE_PRODUCER: &str = "ratatoskr-knowledge";
const COMMAND_STREAM: &str = "ratatoskr_commands";
const EVENT_STREAM: &str = "ratatoskr_events";
const SUBSCRIPTION_DURABLE: &str = "ratatoskr_channel_digest_subscriptions";
const RUN_DURABLE: &str = "ratatoskr_channel_digest_runs";
const SCHEDULE_DURABLE: &str = "ratatoskr_channel_digest_schedule_occurrences";
const COMPLETED_DURABLE: &str = "ratatoskr_channel_digest_recap_completed";
const FAILED_DURABLE: &str = "ratatoskr_channel_digest_recap_failed";

#[derive(Debug, thiserror::Error)]
#[error("channel digest bus dependency is unavailable")]
pub(crate) struct BusRuntimeError;

/// Provider acknowledgement selected after durable message handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryDisposition {
    /// The message is durable or an idempotent replay.
    Ack,
    /// The message is malformed, foreign, or permanently invalid.
    Term,
    /// A transient dependency failure requires bounded redelivery.
    Nak,
}

/// Typed worker message handler over the owned database.
#[derive(Debug, Clone)]
pub struct WorkerMessageHandler {
    pool: sqlx::PgPool,
}

impl WorkerMessageHandler {
    /// Creates a handler over one finite pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Validates and applies one exact transport subject and envelope.
    pub async fn handle(&self, subject: &str, bytes: &[u8]) -> DeliveryDisposition {
        match subject {
            SUBSCRIPTION_SUBJECT | RUN_SUBJECT | SCHEDULE_SUBJECT => {
                self.handle_command(subject, bytes).await
            }
            COMPLETED_SUBJECT | FAILED_SUBJECT => self.handle_event(subject, bytes).await,
            _ => DeliveryDisposition::Term,
        }
    }

    async fn handle_command(&self, subject: &str, bytes: &[u8]) -> DeliveryDisposition {
        let Ok(envelope) = CommandEnvelope::from_json(bytes) else {
            return DeliveryDisposition::Term;
        };
        if envelope.producer.as_str() != PLATFORM_PRODUCER {
            return DeliveryDisposition::Term;
        }
        if subject == SCHEDULE_SUBJECT {
            if envelope.command_type.to_wire()
                != ChannelDigestScheduleOccurrenceRequested::COMMAND_TYPE
            {
                return DeliveryDisposition::Term;
            }
            let Ok(command) = envelope.payload_as::<ChannelDigestScheduleOccurrenceRequested>()
            else {
                return DeliveryDisposition::Term;
            };
            if command.validate_for_publish().is_err()
                || envelope.aggregate_id.to_string() != command.occurrence_ref.as_str()
            {
                return DeliveryDisposition::Term;
            }
            let Ok(payload) = serde_json::to_vec(&command) else {
                return DeliveryDisposition::Term;
            };
            return match DigestCoordinator::new(self.pool.clone())
                .accept_occurrence(
                    envelope.command_id.0,
                    &payload,
                    command.occurrence_ref.as_str(),
                    &command.previous_due_at.to_string(),
                    &command.due_at.to_string(),
                )
                .await
            {
                Ok(_) => DeliveryDisposition::Ack,
                Err(CoordinatorError::Invalid) => DeliveryDisposition::Term,
                Err(CoordinatorError::Storage) => DeliveryDisposition::Nak,
            };
        }
        let result = match subject {
            SUBSCRIPTION_SUBJECT => {
                if envelope.command_type.to_wire()
                    != ChannelDigestSubscriptionSetRequested::COMMAND_TYPE
                {
                    return DeliveryDisposition::Term;
                }
                let Ok(command) = envelope.payload_as::<ChannelDigestSubscriptionSetRequested>()
                else {
                    return DeliveryDisposition::Term;
                };
                if envelope.tenant_id.as_ref() != Some(&command.owner) {
                    return DeliveryDisposition::Term;
                }
                let Ok(payload) = serde_json::to_vec(&command) else {
                    return DeliveryDisposition::Term;
                };
                CommandIntake::new(self.pool.clone())
                    .accept_subscription(envelope.command_id.0, &payload)
                    .await
            }
            RUN_SUBJECT => {
                if envelope.command_type.to_wire() != ChannelDigestRunRequested::COMMAND_TYPE {
                    return DeliveryDisposition::Term;
                }
                let Ok(command) = envelope.payload_as::<ChannelDigestRunRequested>() else {
                    return DeliveryDisposition::Term;
                };
                if envelope.tenant_id.as_ref() != Some(&command.owner) {
                    return DeliveryDisposition::Term;
                }
                let Ok(payload) = serde_json::to_vec(&command) else {
                    return DeliveryDisposition::Term;
                };
                CommandIntake::new(self.pool.clone())
                    .accept_run(envelope.command_id.0, &payload)
                    .await
            }
            _ => return DeliveryDisposition::Term,
        };
        match result {
            Ok(_) => DeliveryDisposition::Ack,
            Err(IntakeError::Invalid) => DeliveryDisposition::Term,
            Err(IntakeError::Storage) => DeliveryDisposition::Nak,
        }
    }

    async fn handle_event(&self, subject: &str, bytes: &[u8]) -> DeliveryDisposition {
        let Ok(envelope) = EventEnvelope::from_json(bytes) else {
            return DeliveryDisposition::Term;
        };
        if envelope.producer.as_str() != KNOWLEDGE_PRODUCER {
            return DeliveryDisposition::Term;
        }
        let coordinator = DigestCoordinator::new(self.pool.clone());
        let result = match subject {
            COMPLETED_SUBJECT => {
                if envelope.event_type.to_wire() != KnowledgeChannelDigestRecapCompleted::EVENT_TYPE
                {
                    return DeliveryDisposition::Term;
                }
                let Ok(fact) = envelope.payload_as::<KnowledgeChannelDigestRecapCompleted>() else {
                    return DeliveryDisposition::Term;
                };
                if envelope.tenant_id.as_ref() != Some(&fact.owner) {
                    return DeliveryDisposition::Term;
                }
                let Ok(payload) = serde_json::to_vec(&fact) else {
                    return DeliveryDisposition::Term;
                };
                coordinator
                    .settle_completion(envelope.event_id.0, &payload)
                    .await
            }
            FAILED_SUBJECT => {
                if envelope.event_type.to_wire() != KnowledgeChannelDigestRecapFailed::EVENT_TYPE {
                    return DeliveryDisposition::Term;
                }
                let Ok(fact) = envelope.payload_as::<KnowledgeChannelDigestRecapFailed>() else {
                    return DeliveryDisposition::Term;
                };
                if envelope.tenant_id.as_ref() != Some(&fact.owner) {
                    return DeliveryDisposition::Term;
                }
                let Ok(payload) = serde_json::to_vec(&fact) else {
                    return DeliveryDisposition::Term;
                };
                coordinator
                    .settle_failure(envelope.event_id.0, &payload)
                    .await
            }
            _ => return DeliveryDisposition::Term,
        };
        match result {
            Ok(_) => DeliveryDisposition::Ack,
            Err(CoordinatorError::Invalid) => DeliveryDisposition::Term,
            Err(CoordinatorError::Storage) => DeliveryDisposition::Nak,
        }
    }
}

pub(crate) async fn supervise_bus(
    endpoint: String,
    pool: sqlx::PgPool,
    readiness: WorkerReadiness,
    mut drain: watch::Receiver<bool>,
) {
    while !*drain.borrow() {
        let result = consume_once(&endpoint, &pool, &readiness, &mut drain).await;
        readiness.set_bus(false);
        if *drain.borrow() {
            return;
        }
        tracing::warn!(
            class = if result.is_err() {
                "bus_unavailable"
            } else {
                "consumer_stopped"
            },
            "channel digest worker is not ready"
        );
        tokio::select! {
            biased;
            _ = drain.changed() => {}
            () = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}

async fn consume_once(
    endpoint: &str,
    pool: &sqlx::PgPool,
    readiness: &WorkerReadiness,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), BusRuntimeError> {
    let client = async_nats::connect(endpoint)
        .await
        .map_err(|_| BusRuntimeError)?;
    let context = jetstream::new(client);
    let subscriptions = exact_consumer(
        &context,
        COMMAND_STREAM,
        SUBSCRIPTION_DURABLE,
        SUBSCRIPTION_SUBJECT,
    )
    .await?;
    let runs = exact_consumer(&context, COMMAND_STREAM, RUN_DURABLE, RUN_SUBJECT).await?;
    let schedules =
        exact_consumer(&context, COMMAND_STREAM, SCHEDULE_DURABLE, SCHEDULE_SUBJECT).await?;
    let completed =
        exact_consumer(&context, EVENT_STREAM, COMPLETED_DURABLE, COMPLETED_SUBJECT).await?;
    let failed = exact_consumer(&context, EVENT_STREAM, FAILED_DURABLE, FAILED_SUBJECT).await?;
    let mut subscription_messages = subscriptions
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusRuntimeError)?;
    let mut run_messages = runs
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusRuntimeError)?;
    let mut schedule_messages = schedules
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusRuntimeError)?;
    let mut completion_messages = completed
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusRuntimeError)?;
    let mut failure_messages = failed
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusRuntimeError)?;
    let handler = WorkerMessageHandler::new(pool.clone());
    publish_outbox(pool, &context).await?;
    readiness.set_bus(true);
    let mut outbox_tick = tokio::time::interval(Duration::from_secs(1));
    outbox_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    outbox_tick.tick().await;
    loop {
        tokio::select! {
            biased;
            _ = drain.changed() => return Ok(()),
            _ = outbox_tick.tick() => publish_outbox(pool, &context).await?,
            next = subscription_messages.next() => {
                process_delivery(next, &handler, &context, pool).await?;
            }
            next = run_messages.next() => {
                process_delivery(next, &handler, &context, pool).await?;
            }
            next = schedule_messages.next() => {
                process_delivery(next, &handler, &context, pool).await?;
            }
            next = completion_messages.next() => {
                process_delivery(next, &handler, &context, pool).await?;
            }
            next = failure_messages.next() => {
                process_delivery(next, &handler, &context, pool).await?;
            }
        }
    }
}

async fn exact_consumer(
    context: &jetstream::Context,
    stream: &str,
    durable: &str,
    subject: &str,
) -> Result<jetstream::consumer::PullConsumer, BusRuntimeError> {
    let consumer: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream(durable, stream)
        .await
        .map_err(|_| BusRuntimeError)?;
    let config = &consumer.cached_info().config;
    if config.durable_name.as_deref() != Some(durable)
        || config.filter_subject != subject
        || config.ack_policy != jetstream::consumer::AckPolicy::Explicit
        || config.deliver_subject.is_some()
        || config.deliver_policy != jetstream::consumer::DeliverPolicy::All
    {
        return Err(BusRuntimeError);
    }
    Ok(consumer)
}

async fn process_delivery(
    next: Option<Result<jetstream::Message, jetstream::consumer::pull::MessagesError>>,
    handler: &WorkerMessageHandler,
    context: &jetstream::Context,
    pool: &sqlx::PgPool,
) -> Result<(), BusRuntimeError> {
    let message = next.ok_or(BusRuntimeError)?.map_err(|_| BusRuntimeError)?;
    let disposition = handler
        .handle(message.subject.as_str(), message.payload.as_ref())
        .await;
    publish_outbox(pool, context).await?;
    let ack = match disposition {
        DeliveryDisposition::Ack => jetstream::AckKind::Ack,
        DeliveryDisposition::Term => jetstream::AckKind::Term,
        DeliveryDisposition::Nak => jetstream::AckKind::Nak(Some(Duration::from_secs(2))),
    };
    message.ack_with(ack).await.map_err(|_| BusRuntimeError)
}

async fn publish_outbox(
    pool: &sqlx::PgPool,
    context: &jetstream::Context,
) -> Result<(), BusRuntimeError> {
    let rows: Vec<(Uuid, String, Uuid, Uuid, serde_json::Value)> = sqlx::query_as(
        "select outbox_id, subject, owner_id, operation_id, payload
         from channel_digests.outbox_messages
         where published_at is null and next_attempt_at <= now()
         order by created_at limit 32",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| BusRuntimeError)?;
    for (outbox_id, subject, owner_id, operation_id, payload) in rows {
        let is_command = subject == "knowledge.channel_digest_recap.requested.v1";
        let aggregate_id = payload
            .get("digest_run_id")
            .and_then(serde_json::Value::as_str)
            .map_or_else(
                || format!("operation:{operation_id}"),
                |run_id| format!("channel-digest-run:{run_id}"),
            );
        let envelope = if is_command {
            serde_json::json!({
                "command_id": outbox_id,
                "command_type": subject,
                "issued_at": WireTimestamp::now(),
                "producer": "ratatoskr-channel-digests",
                "aggregate_id": aggregate_id,
                "correlation_id": format!("operation:{operation_id}"),
                "tenant_id": format!("user:{owner_id}"),
                "schema_version": 1,
                "payload": payload
            })
        } else {
            serde_json::json!({
                "event_id": outbox_id,
                "event_type": subject,
                "occurred_at": WireTimestamp::now(),
                "producer": "ratatoskr-channel-digests",
                "aggregate_id": aggregate_id,
                "correlation_id": format!("operation:{operation_id}"),
                "tenant_id": format!("user:{owner_id}"),
                "schema_version": 1,
                "payload": payload
            })
        };
        let bytes = serde_json::to_vec(&envelope).map_err(|_| BusRuntimeError)?;
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("Nats-Msg-Id", outbox_id.to_string());
        context
            .publish_with_headers(
                format!("{}.{subject}", if is_command { "cmd" } else { "evt" }),
                headers,
                bytes.into(),
            )
            .await
            .map_err(|_| BusRuntimeError)?
            .await
            .map_err(|_| BusRuntimeError)?;
        sqlx::query(
            "update channel_digests.outbox_messages set published_at = now(), attempts = attempts + 1 where outbox_id = $1 and published_at is null",
        )
        .bind(outbox_id)
        .execute(pool)
        .await
        .map_err(|_| BusRuntimeError)?;
    }
    Ok(())
}
