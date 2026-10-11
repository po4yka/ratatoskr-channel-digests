//! Exact `JetStream` delivery boundary for the channel-digest worker.

use std::path::Path;
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

use crate::config::BusConfig;
use crate::envelopes::{OutboundMessage, OutboxRow, wrap_outbox_row};
use crate::runtime::WorkerReadiness;
use crate::{
    CommandIntake, CommandKind, CoordinatorError, DigestCoordinator, IntakeError, OccurrenceRequest,
};

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

/// Safe bus failure vocabulary. It never carries payloads, subjects of foreign tenants, or
/// credentials.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BusError {
    /// Connection, topology, or transport failed.
    #[error("channel digest bus dependency is unavailable")]
    Unavailable,
    /// A publish was not acknowledged. A permission denial looks exactly like this to a client.
    #[error(
        "the bus did not acknowledge a published message; check the NATS server log for a Publish Violation"
    )]
    Unacknowledged,
    /// An outbox row cannot be wrapped into a contract envelope.
    #[error("an outbox row cannot be wrapped into a contract envelope")]
    Unwrappable,
}

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
            let Some((operation_id, owner_id)) = occurrence_authority(&envelope) else {
                return DeliveryDisposition::Term;
            };
            return match DigestCoordinator::new(self.pool.clone())
                .accept_occurrence(&OccurrenceRequest {
                    message_id: envelope.command_id.0,
                    payload: &payload,
                    occurrence_key: command.occurrence_ref.as_str(),
                    previous_due_at: &command.previous_due_at.to_string(),
                    due_at: &command.due_at.to_string(),
                    operation_id,
                    owner_id,
                })
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
                    return self
                        .reject_unreadable(&envelope, CommandKind::Subscription)
                        .await;
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
                    return self.reject_unreadable(&envelope, CommandKind::Run).await;
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
    /// Reports a command whose payload does not decode but names its operation and owner.
    ///
    /// Only a payload whose owner equals the envelope tenant is attributable; anything else stays
    /// terminated without a report.
    async fn reject_unreadable(
        &self,
        envelope: &CommandEnvelope,
        kind: CommandKind,
    ) -> DeliveryDisposition {
        let Some(tenant) = envelope.tenant_id else {
            return DeliveryDisposition::Term;
        };
        match CommandIntake::new(self.pool.clone())
            .reject_unreadable(envelope.command_id.0, kind, &tenant, &envelope.payload)
            .await
        {
            Ok(_) => DeliveryDisposition::Ack,
            Err(IntakeError::Invalid) => DeliveryDisposition::Term,
            Err(IntakeError::Storage) => DeliveryDisposition::Nak,
        }
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

/// The occurrence operation and its owner, both carried by the Platform envelope.
fn occurrence_authority(envelope: &CommandEnvelope) -> Option<(Uuid, Uuid)> {
    let correlation = &envelope.correlation_id;
    let operation = if correlation.kind().as_str() == "operation" {
        correlation.as_uuid()?
    } else {
        return None;
    };
    Some((operation, envelope.tenant_id?.user_id().0))
}

/// Expected acknowledgement wait of every durable Edge provisions for this service.
const ACK_WAIT: Duration = Duration::from_secs(30);

/// Connects to the broker, authenticating with an nkey seed when a path is given.
///
/// The seed is read from the file at connection time, never logged, and dropped after the
/// connection options are built.
///
/// # Errors
///
/// Returns [`BusError::Unavailable`] when the seed cannot be read or the broker refuses the
/// connection.
pub async fn connect(
    endpoint: &str,
    nkey_seed_path: Option<&Path>,
) -> Result<async_nats::Client, BusError> {
    let options = match nkey_seed_path {
        Some(path) => {
            let seed = tokio::fs::read_to_string(path)
                .await
                .map_err(|_| BusError::Unavailable)?;
            async_nats::ConnectOptions::with_nkey(seed.trim().to_owned())
        }
        None => async_nats::ConnectOptions::new(),
    };
    options
        .connect(endpoint)
        .await
        .map_err(|_| BusError::Unavailable)
}

/// Publishes one wrapped message and waits for the broker's acknowledgement.
///
/// # Errors
///
/// Returns [`BusError::Unavailable`] when the publish cannot be sent and
/// [`BusError::Unacknowledged`] when no acknowledgement arrives, which is also what a denied
/// publish looks like to the client.
pub async fn publish_message(
    context: &jetstream::Context,
    message: &OutboundMessage,
) -> Result<(), BusError> {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert("Nats-Msg-Id", message.message_id.to_string());
    context
        .publish_with_headers(
            message.subject.clone(),
            headers,
            message.bytes.clone().into(),
        )
        .await
        .map_err(|_| BusError::Unavailable)?
        .await
        .map_err(|_| BusError::Unacknowledged)?;
    Ok(())
}

/// The five Edge-provisioned durables this worker consumes, each verified against its spec.
pub struct ConsumerSet {
    subscriptions: jetstream::consumer::PullConsumer,
    runs: jetstream::consumer::PullConsumer,
    schedules: jetstream::consumer::PullConsumer,
    completed: jetstream::consumer::PullConsumer,
    failed: jetstream::consumer::PullConsumer,
}

impl std::fmt::Debug for ConsumerSet {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConsumerSet")
            .finish_non_exhaustive()
    }
}

/// Fetches every durable this worker consumes and verifies its filter, ack policy and ack wait.
///
/// Consumers are verified, never created: Edge provisions them.
///
/// # Errors
///
/// Returns [`BusError::Unavailable`] when a durable is absent, unreadable, or differs from its
/// spec.
pub async fn verify_consumers(context: &jetstream::Context) -> Result<ConsumerSet, BusError> {
    Ok(ConsumerSet {
        subscriptions: exact_consumer(
            context,
            COMMAND_STREAM,
            SUBSCRIPTION_DURABLE,
            SUBSCRIPTION_SUBJECT,
        )
        .await?,
        runs: exact_consumer(context, COMMAND_STREAM, RUN_DURABLE, RUN_SUBJECT).await?,
        schedules: exact_consumer(context, COMMAND_STREAM, SCHEDULE_DURABLE, SCHEDULE_SUBJECT)
            .await?,
        completed: exact_consumer(context, EVENT_STREAM, COMPLETED_DURABLE, COMPLETED_SUBJECT)
            .await?,
        failed: exact_consumer(context, EVENT_STREAM, FAILED_DURABLE, FAILED_SUBJECT).await?,
    })
}

async fn exact_consumer(
    context: &jetstream::Context,
    stream: &str,
    durable: &str,
    subject: &str,
) -> Result<jetstream::consumer::PullConsumer, BusError> {
    let consumer: jetstream::consumer::PullConsumer = context
        .get_consumer_from_stream(durable, stream)
        .await
        .map_err(|_| BusError::Unavailable)?;
    let config = &consumer.cached_info().config;
    if config.durable_name.as_deref() != Some(durable)
        || config.filter_subject != subject
        || config.ack_policy != jetstream::consumer::AckPolicy::Explicit
        || config.ack_wait != ACK_WAIT
        || config.deliver_subject.is_some()
        || config.deliver_policy != jetstream::consumer::DeliverPolicy::All
    {
        return Err(BusError::Unavailable);
    }
    Ok(consumer)
}

pub(crate) async fn supervise_bus(
    bus: BusConfig,
    pool: sqlx::PgPool,
    readiness: WorkerReadiness,
    mut drain: watch::Receiver<bool>,
) {
    while !*drain.borrow() {
        let result = Box::pin(consume_once(&bus, &pool, &readiness, &mut drain)).await;
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
    bus: &BusConfig,
    pool: &sqlx::PgPool,
    readiness: &WorkerReadiness,
    drain: &mut watch::Receiver<bool>,
) -> Result<(), BusError> {
    let client = connect(&bus.endpoint, bus.nkey_seed_path.as_deref()).await?;
    let context = jetstream::new(client);
    let consumers = verify_consumers(&context).await?;
    let mut subscription_messages = batches(&consumers.subscriptions).await?;
    let mut run_messages = batches(&consumers.runs).await?;
    let mut schedule_messages = batches(&consumers.schedules).await?;
    let mut completion_messages = batches(&consumers.completed).await?;
    let mut failure_messages = batches(&consumers.failed).await?;
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

async fn batches(
    consumer: &jetstream::consumer::PullConsumer,
) -> Result<jetstream::consumer::pull::Stream, BusError> {
    consumer
        .stream()
        .max_messages_per_batch(16)
        .messages()
        .await
        .map_err(|_| BusError::Unavailable)
}

async fn process_delivery(
    next: Option<Result<jetstream::Message, jetstream::consumer::pull::MessagesError>>,
    handler: &WorkerMessageHandler,
    context: &jetstream::Context,
    pool: &sqlx::PgPool,
) -> Result<(), BusError> {
    let message = next
        .ok_or(BusError::Unavailable)?
        .map_err(|_| BusError::Unavailable)?;
    let disposition = handler
        .handle(message.subject.as_str(), message.payload.as_ref())
        .await;
    publish_outbox(pool, context).await?;
    let ack = match disposition {
        DeliveryDisposition::Ack => jetstream::AckKind::Ack,
        DeliveryDisposition::Term => jetstream::AckKind::Term,
        DeliveryDisposition::Nak => jetstream::AckKind::Nak(Some(Duration::from_secs(2))),
    };
    message
        .ack_with(ack)
        .await
        .map_err(|_| BusError::Unavailable)
}

type StoredOutboxRow = (
    Uuid,
    String,
    Uuid,
    Option<Uuid>,
    Option<String>,
    String,
    serde_json::Value,
);

/// Publishes every due outbox row. A row the broker does not acknowledge is backed off and the
/// remaining rows still go out, so one refused row never starves the others.
async fn publish_outbox(pool: &sqlx::PgPool, context: &jetstream::Context) -> Result<(), BusError> {
    let rows: Vec<StoredOutboxRow> = sqlx::query_as(
        "select outbox_id, subject, owner_id, operation_id, causation_ref,
                to_char(created_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),
                payload
         from channel_digests.outbox_messages
         where published_at is null and next_attempt_at <= now()
         order by created_at, outbox_id limit 32",
    )
    .fetch_all(pool)
    .await
    .map_err(|_| BusError::Unavailable)?;
    let mut refused = false;
    for (outbox_id, subject, owner_id, operation_id, causation_ref, created_at, payload) in rows {
        let row = OutboxRow {
            outbox_id,
            subject,
            owner_id,
            operation_id,
            causation_ref,
            created_at: created_at
                .parse::<jiff::Timestamp>()
                .map(WireTimestamp::from_jiff)
                .map_err(|_| BusError::Unwrappable)?,
            payload,
        };
        let message = wrap_outbox_row(&row)?;
        if publish_message(context, &message).await.is_ok() {
            sqlx::query(
                "update channel_digests.outbox_messages set published_at = now(), attempts = attempts + 1 where outbox_id = $1 and published_at is null",
            )
            .bind(outbox_id)
            .execute(pool)
            .await
            .map_err(|_| BusError::Unavailable)?;
        } else {
            refused = true;
            sqlx::query(
                "update channel_digests.outbox_messages set attempts = attempts + 1, safe_failure_class = 'publish_unacknowledged', next_attempt_at = now() + least(300, power(2, least(attempts, 8))) * interval '1 second' where outbox_id = $1 and published_at is null",
            )
            .bind(outbox_id)
            .execute(pool)
            .await
            .map_err(|_| BusError::Unavailable)?;
        }
    }
    if refused {
        Err(BusError::Unacknowledged)
    } else {
        Ok(())
    }
}
