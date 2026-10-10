//! Typed `platform.operation.reported.v1` rows queued in the transaction of the state they report.
//!
//! Every report is an [`OperationReported`] that passes its own validation before it is stored.
//! The outbox keeps the bare typed payload; the bus wraps it into the contract envelope at
//! publish time.

use ratatoskr_error_contracts::{ErrorCode, ErrorEnvelope, WarningEnvelope};
use ratatoskr_identifiers::{EntityRef, Extensions, OperationId, SafeMessage};
use ratatoskr_operation_contracts::{
    OperationReported, OperationResultKind, OperationResultRef, OperationStage, OperationStatus,
};
use sqlx::PgConnection;
use uuid::Uuid;

/// Outbox subject of every operation report.
pub(crate) const REPORT_SUBJECT: &str = "platform.operation.reported.v1";

/// Result kind of the digest result a completed run points at.
const RESULT_KIND: &str = "channel_digest.result";

/// Safe report construction or storage failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReportError {
    /// The report is not a valid contract value.
    #[error("operation report is invalid")]
    Invalid,
    /// Atomic storage operation failed.
    #[error("operation report storage is unavailable")]
    Storage,
}

/// One validated report waiting to be queued in the caller's transaction.
#[derive(Debug)]
pub(crate) struct OperationReportRow {
    operation_id: Uuid,
    owner_id: Uuid,
    causation: Option<String>,
    report: OperationReported,
}

impl OperationReportRow {
    /// Starts a report of `status` in the display `stage` for one operation.
    pub(crate) fn new(
        operation_id: Uuid,
        owner_id: Uuid,
        status: OperationStatus,
        stage: &str,
    ) -> Result<Self, ReportError> {
        Ok(Self {
            operation_id,
            owner_id,
            causation: None,
            report: OperationReported {
                operation_id: OperationId(operation_id),
                status,
                stage: Some(OperationStage::parse(stage).map_err(|_| ReportError::Invalid)?),
                progress_percent: None,
                results: Vec::new(),
                error: None,
                warnings: Vec::new(),
                extensions: Extensions::new(),
            },
        })
    }

    /// Records the inbound command or event that caused this report.
    #[must_use]
    pub(crate) fn caused_by(mut self, reference: String) -> Self {
        self.causation = Some(reference);
        self
    }

    /// Adds a reference to a produced digest result.
    pub(crate) fn with_result(mut self, target: &str) -> Result<Self, ReportError> {
        self.report.results.push(OperationResultRef {
            result_kind: OperationResultKind::parse(RESULT_KIND)
                .map_err(|_| ReportError::Invalid)?,
            target: EntityRef::parse(target).map_err(|_| ReportError::Invalid)?,
            blob: None,
            ai_archive_import_summary: None,
            extensions: Extensions::new(),
        });
        Ok(self)
    }

    /// Sets the terminal error.
    pub(crate) fn with_error(
        mut self,
        code: &str,
        message: &str,
        retryable: bool,
    ) -> Result<Self, ReportError> {
        self.report.error = Some(ErrorEnvelope::new(
            ErrorCode::parse(code).map_err(|_| ReportError::Invalid)?,
            SafeMessage::parse(message).map_err(|_| ReportError::Invalid)?,
            retryable,
        ));
        Ok(self)
    }

    /// Adds a non-terminal warning.
    pub(crate) fn with_warning(mut self, code: &str, message: &str) -> Result<Self, ReportError> {
        self.report.warnings.push(WarningEnvelope {
            code: ErrorCode::parse(code).map_err(|_| ReportError::Invalid)?,
            message: SafeMessage::parse(message).map_err(|_| ReportError::Invalid)?,
            field_path: None,
            extensions: Extensions::new(),
        });
        Ok(self)
    }

    /// Validates the report and queues it once per operation and status.
    pub(crate) async fn enqueue(self, connection: &mut PgConnection) -> Result<(), ReportError> {
        self.report.validate().map_err(|_| ReportError::Invalid)?;
        let payload = serde_json::to_value(&self.report).map_err(|_| ReportError::Invalid)?;
        sqlx::query(
            "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, operation_id, causation_ref, payload) values ($1, $2, $3, $4, $5, $6, $7) on conflict (subject, semantic_key) do nothing",
        )
        .bind(Uuid::now_v7())
        .bind(REPORT_SUBJECT)
        .bind(format!(
            "operation:{}:{}",
            self.operation_id, self.report.status
        ))
        .bind(self.owner_id)
        .bind(self.operation_id)
        .bind(self.causation)
        .bind(payload)
        .execute(connection)
        .await
        .map_err(|_| ReportError::Storage)?;
        Ok(())
    }
}

/// Returns the Platform operation that owns a run, or `None` for a scheduled run.
///
/// Scheduled digest runs are owned by no Platform operation and emit no report.
pub(crate) async fn on_demand_operation(
    connection: &mut PgConnection,
    run_id: Uuid,
    owner_id: Uuid,
) -> Result<Option<Uuid>, ReportError> {
    let row: Option<(Uuid,)> = sqlx::query_as(
        "select operation_id from channel_digests.digest_runs where run_id = $1 and owner_id = $2 and trigger = 'on_demand'",
    )
    .bind(run_id)
    .bind(owner_id)
    .fetch_optional(connection)
    .await
    .map_err(|_| ReportError::Storage)?;
    Ok(row.map(|value| value.0))
}

/// Queues the terminal report of a settled Knowledge completion, when the run is owned.
pub(crate) async fn report_completion(
    connection: &mut PgConnection,
    run: (Uuid, Uuid),
    event_id: Uuid,
    result_ref: &str,
    omitted_count: u16,
) -> Result<(), ReportError> {
    let (run_id, owner_id) = run;
    let Some(operation) = on_demand_operation(connection, run_id, owner_id).await? else {
        return Ok(());
    };
    let status = if omitted_count == 0 {
        OperationStatus::Succeeded
    } else {
        OperationStatus::PartiallySucceeded
    };
    let mut row = OperationReportRow::new(operation, owner_id, status, "completed")?
        .caused_by(format!("event:{event_id}"))
        .with_result(result_ref)?;
    if omitted_count > 0 {
        row = row.with_warning(
            "channel_digest.context_omitted",
            "Some selected posts did not fit the recap context and were left out.",
        )?;
    }
    row.enqueue(connection).await
}

/// Queues the failed report of a Knowledge failure fact, when the run is owned.
pub(crate) async fn report_recap_failure(
    connection: &mut PgConnection,
    run: (Uuid, Uuid),
    event_id: Uuid,
    (failure_class, retryable): (&str, bool),
) -> Result<(), ReportError> {
    let (run_id, owner_id) = run;
    let Some(operation) = on_demand_operation(connection, run_id, owner_id).await? else {
        return Ok(());
    };
    OperationReportRow::new(operation, owner_id, OperationStatus::Failed, "recap")?
        .caused_by(format!("event:{event_id}"))
        .with_error(
            &format!("channel_digest.recap.{failure_class}"),
            "The recap could not be produced.",
            retryable,
        )?
        .enqueue(connection)
        .await
}
