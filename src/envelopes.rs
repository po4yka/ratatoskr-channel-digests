//! Contract envelopes for the outbox rows this service publishes (XR-021 CONTRACTS.md S01, S02).
//!
//! Outbox rows keep the bare typed payload. The envelope is built here, at publish time, from the
//! row alone: the row identity is the envelope identity and the row creation instant is its
//! instant, so republishing a row produces identical bytes.

use ratatoskr_channel_digest_contracts::KnowledgeChannelDigestRecapRequested;
use ratatoskr_event_envelope::{
    CommandEnvelope, CommandPayload, EnvelopeSchemaVersion, EventEnvelope, EventPayload,
    ProducerName,
};
use ratatoskr_identifiers::{
    CommandId, EntityRef, EventId, Extensions, TenantRef, UserId, WireTimestamp,
};
use ratatoskr_operation_contracts::{OperationReported, PlatformScheduleRegistrationRequested};
use uuid::Uuid;

use crate::bus::BusError;
use crate::reports::REPORT_SUBJECT;

/// Producer name carried by every envelope this service publishes.
const PRODUCER: &str = "ratatoskr-channel-digests";

/// Outbox subjects that are commands. Everything else the outbox may hold is an event.
const COMMAND_SUBJECTS: [&str; 2] = [
    KnowledgeChannelDigestRecapRequested::COMMAND_TYPE,
    PlatformScheduleRegistrationRequested::COMMAND_TYPE,
];

/// One unpublished outbox row, as stored.
#[derive(Debug, Clone, PartialEq)]
pub struct OutboxRow {
    /// Row identity, reused as the envelope identity and the `Nats-Msg-Id`.
    pub outbox_id: Uuid,
    /// Contract type name of the stored payload, without the `cmd.` or `evt.` class.
    pub subject: String,
    /// Platform user the message belongs to.
    pub owner_id: Uuid,
    /// Operation the message correlates to, when it belongs to one.
    pub operation_id: Option<Uuid>,
    /// Reference of the inbound command or event that caused this row.
    pub causation_ref: Option<String>,
    /// Instant the row was created, reused as the envelope instant so a republish is identical.
    pub created_at: WireTimestamp,
    /// Bare typed payload.
    pub payload: serde_json::Value,
}

/// A contract envelope ready for the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    /// Full subject including the `cmd.` or `evt.` class.
    pub subject: String,
    /// Deduplication identity, the outbox row identity.
    pub message_id: Uuid,
    /// Serialized contract envelope.
    pub bytes: Vec<u8>,
}

/// Wraps an outbox row into the contract envelope of its type.
///
/// The subject classifies the row: the explicit command set publishes `cmd.` envelopes and the
/// operation report publishes an `evt.` envelope. The payload must decode as its typed contract.
///
/// # Errors
///
/// Returns [`BusError::Unwrappable`] for an unknown subject or an invalid payload. Such a row is a
/// programming error, never skipped silently.
pub fn wrap_outbox_row(row: &OutboxRow) -> Result<OutboundMessage, BusError> {
    let subject = row.subject.as_str();
    let (class, bytes) = if subject == REPORT_SUBJECT {
        ("evt", report_envelope(row)?)
    } else if COMMAND_SUBJECTS.contains(&subject) {
        let bytes = if subject == KnowledgeChannelDigestRecapRequested::COMMAND_TYPE {
            recap_request_envelope(row)?
        } else {
            registration_envelope(row)?
        };
        ("cmd", bytes)
    } else {
        return Err(BusError::Unwrappable);
    };
    Ok(OutboundMessage {
        subject: format!("{class}.{subject}"),
        message_id: row.outbox_id,
        bytes,
    })
}

fn typed_payload<P: serde::de::DeserializeOwned>(row: &OutboxRow) -> Result<P, BusError> {
    serde_json::from_value(row.payload.clone()).map_err(|_| BusError::Unwrappable)
}

fn entity(raw: &str) -> Result<EntityRef, BusError> {
    EntityRef::parse(raw).map_err(|_| BusError::Unwrappable)
}

fn causation(row: &OutboxRow) -> Result<Option<EntityRef>, BusError> {
    row.causation_ref.as_deref().map(entity).transpose()
}

fn producer() -> Result<ProducerName, BusError> {
    ProducerName::parse(PRODUCER).map_err(|_| BusError::Unwrappable)
}

fn command_envelope<P: CommandPayload>(
    row: &OutboxRow,
    payload: &P,
    aggregate_id: &str,
    correlation_id: &str,
) -> Result<Vec<u8>, BusError> {
    let mut envelope = CommandEnvelope {
        command_id: CommandId(row.outbox_id),
        command_type: P::command_type(),
        issued_at: row.created_at,
        producer: producer()?,
        aggregate_id: entity(aggregate_id)?,
        correlation_id: entity(correlation_id)?,
        causation_id: causation(row)?,
        tenant_id: Some(TenantRef::of_user(UserId(row.owner_id))),
        schema_version: EnvelopeSchemaVersion::CURRENT,
        payload: serde_json::Map::new(),
        extensions: Extensions::new(),
    };
    envelope
        .set_payload(payload)
        .map_err(|_| BusError::Unwrappable)?;
    serde_json::to_vec(&envelope).map_err(|_| BusError::Unwrappable)
}

fn recap_request_envelope(row: &OutboxRow) -> Result<Vec<u8>, BusError> {
    let request: KnowledgeChannelDigestRecapRequested = typed_payload(row)?;
    request
        .validate_for_publish()
        .map_err(|_| BusError::Unwrappable)?;
    command_envelope(
        row,
        &request,
        &format!("channel-digest-run:{}", request.digest_run_id),
        &format!("operation:{}", request.operation_id),
    )
}

fn registration_envelope(row: &OutboxRow) -> Result<Vec<u8>, BusError> {
    let registration: PlatformScheduleRegistrationRequested = typed_payload(row)?;
    if registration.service_name.as_str() != PRODUCER {
        return Err(BusError::Unwrappable);
    }
    command_envelope(
        row,
        &registration,
        &format!(
            "schedule-registration:{}.{}",
            registration.service_name.as_str(),
            registration.name.as_str()
        ),
        &format!("command:{}", row.outbox_id),
    )
}

fn report_envelope(row: &OutboxRow) -> Result<Vec<u8>, BusError> {
    let report: OperationReported = typed_payload(row)?;
    report.validate().map_err(|_| BusError::Unwrappable)?;
    let operation = format!("operation:{}", report.operation_id);
    let mut envelope = EventEnvelope {
        event_id: EventId(row.outbox_id),
        event_type: OperationReported::event_type(),
        occurred_at: row.created_at,
        producer: producer()?,
        aggregate_id: entity(&operation)?,
        correlation_id: entity(&operation)?,
        causation_id: causation(row)?,
        tenant_id: Some(TenantRef::of_user(UserId(row.owner_id))),
        schema_version: EnvelopeSchemaVersion::CURRENT,
        payload: serde_json::Map::new(),
        extensions: Extensions::new(),
    };
    envelope
        .set_payload(&report)
        .map_err(|_| BusError::Unwrappable)?;
    serde_json::to_vec(&envelope).map_err(|_| BusError::Unwrappable)
}

#[cfg(test)]
mod tests {
    use ratatoskr_operation_contracts::OperationStatus;
    use serde_json::json;

    use super::*;

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn row(
        subject: &str,
        operation_id: Option<Uuid>,
        causation_ref: Option<&str>,
        owner_id: Uuid,
        payload: serde_json::Value,
    ) -> Result<OutboxRow, Box<dyn std::error::Error>> {
        Ok(OutboxRow {
            outbox_id: Uuid::now_v7(),
            subject: subject.to_owned(),
            owner_id,
            operation_id,
            causation_ref: causation_ref.map(str::to_owned),
            created_at: WireTimestamp::parse("2026-08-29T10:00:00.25Z")?,
            payload,
        })
    }

    fn registration_payload(owner: Uuid, service: &str) -> serde_json::Value {
        json!({
            "service_name": service,
            "name": "daily-digest",
            "owner_user_id": owner,
            "cron_expression": "0 6 * * *",
            "command_type": "channel_digest.schedule.occurrence_requested.v1",
            "operation_kind": "channel_digest.schedule.occurrence",
            "payload": {},
            "enabled": true
        })
    }

    #[test]
    fn outbox_rows_wrap_into_contract_envelopes() -> TestResult {
        report_row_wraps_into_an_event_envelope()?;
        recap_request_row_wraps_into_a_command_envelope()?;
        registration_row_wraps_into_a_self_correlated_command()?;
        unwrappable_rows_are_refused()
    }

    fn report_row_wraps_into_an_event_envelope() -> TestResult {
        let (owner, operation) = (Uuid::now_v7(), Uuid::now_v7());
        let causation = format!("command:{}", Uuid::now_v7());
        let report = row(
            "platform.operation.reported.v1",
            Some(operation),
            Some(&causation),
            owner,
            json!({
                "operation_id": operation,
                "status": "failed",
                "stage": "rejected",
                "error": {
                    "code": "channel_digest.subscription_limit_reached",
                    "message": "At most 20 channels can be active at the same time.",
                    "retryable": false
                }
            }),
        )?;
        let message = wrap_outbox_row(&report)?;
        assert_eq!(message.subject, "evt.platform.operation.reported.v1");
        assert_eq!(message.message_id, report.outbox_id);
        let envelope = EventEnvelope::from_json(&message.bytes)?;
        assert_eq!(envelope.event_id.0, report.outbox_id);
        assert_eq!(envelope.producer.as_str(), "ratatoskr-channel-digests");
        assert_eq!(envelope.occurred_at, report.created_at);
        assert_eq!(
            envelope.aggregate_id.to_wire(),
            format!("operation:{operation}")
        );
        assert_eq!(
            envelope.correlation_id.to_wire(),
            format!("operation:{operation}")
        );
        assert_eq!(
            envelope.causation_id.as_ref().map(EntityRef::to_wire),
            Some(causation)
        );
        assert_eq!(envelope.tenant_id, Some(TenantRef::of_user(UserId(owner))));
        assert_eq!(
            envelope.payload_as::<OperationReported>()?.status,
            OperationStatus::Failed
        );
        assert_eq!(
            wrap_outbox_row(&report)?.bytes,
            message.bytes,
            "a republished row is byte-identical"
        );
        Ok(())
    }

    fn recap_request_row_wraps_into_a_command_envelope() -> TestResult {
        let (owner, operation, run) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
        let recap = row(
            "knowledge.channel_digest_recap.requested.v1",
            Some(operation),
            None,
            owner,
            json!({
                "operation_id": operation,
                "owner": format!("user:{owner}"),
                "digest_run_id": run,
                "window": {"start_at": "2026-08-28T10:00:00Z", "end_at": "2026-08-29T10:00:00Z"},
                "output_language": "ru",
                "source_count": 1,
                "channel_count": 1,
                "manifest_ref": format!("channel-digest-manifest:{}", Uuid::now_v7()),
                "manifest_digest": {"algorithm": "sha256", "hex": "11".repeat(32)},
                "analysis_family": "channel_digest_recap",
                "analysis_contract": "channel_digest_recap.v1"
            }),
        )?;
        let message = wrap_outbox_row(&recap)?;
        assert_eq!(
            message.subject,
            "cmd.knowledge.channel_digest_recap.requested.v1"
        );
        let envelope = CommandEnvelope::from_json(&message.bytes)?;
        assert_eq!(envelope.command_id.0, recap.outbox_id);
        assert_eq!(
            envelope.aggregate_id.to_wire(),
            format!("channel-digest-run:{run}")
        );
        assert_eq!(
            envelope.correlation_id.to_wire(),
            format!("operation:{operation}")
        );
        assert_eq!(envelope.causation_id, None);
        envelope.payload_as::<KnowledgeChannelDigestRecapRequested>()?;
        Ok(())
    }

    fn registration_row_wraps_into_a_self_correlated_command() -> TestResult {
        let owner = Uuid::now_v7();
        let registration = row(
            "platform.schedule.registration_requested.v1",
            None,
            None,
            owner,
            registration_payload(owner, "ratatoskr-channel-digests"),
        )?;
        let message = wrap_outbox_row(&registration)?;
        assert_eq!(
            message.subject,
            "cmd.platform.schedule.registration_requested.v1"
        );
        let envelope = CommandEnvelope::from_json(&message.bytes)?;
        assert_eq!(envelope.producer.as_str(), "ratatoskr-channel-digests");
        assert_eq!(
            envelope.correlation_id.to_wire(),
            format!("command:{}", registration.outbox_id),
            "a registration belongs to no operation and correlates to itself"
        );
        let payload = envelope.payload_as::<PlatformScheduleRegistrationRequested>()?;
        assert_eq!(
            payload.service_name.as_str(),
            envelope.producer.as_str(),
            "Platform requires the producer to be the registering service"
        );
        Ok(())
    }

    fn unwrappable_rows_are_refused() -> TestResult {
        let (owner, operation) = (Uuid::now_v7(), Uuid::now_v7());
        let foreign = row(
            "platform.schedule.registration_requested.v1",
            None,
            None,
            owner,
            registration_payload(owner, "ratatoskr-github"),
        )?;
        assert!(matches!(
            wrap_outbox_row(&foreign),
            Err(BusError::Unwrappable)
        ));
        let unknown = row("channel_digest.unknown.v1", None, None, owner, json!({}))?;
        assert!(matches!(
            wrap_outbox_row(&unknown),
            Err(BusError::Unwrappable)
        ));
        let invalid_report = row(
            "platform.operation.reported.v1",
            Some(operation),
            None,
            owner,
            json!({"operation_id": operation, "status": "failed"}),
        )?;
        assert!(
            matches!(wrap_outbox_row(&invalid_report), Err(BusError::Unwrappable)),
            "a failed report without an error is never published"
        );
        Ok(())
    }
}
