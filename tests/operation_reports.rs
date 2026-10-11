//! Typed terminal operation reports on every path (XR-021 CONTRACTS.md S08, report points a to i).

use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{
    CommandIntake, Database, DigestCoordinator, IntakeOutcome, ObservedRevision, OccurrenceRequest,
    ProviderError, ProviderPage, ProviderPost, PublicChannelProvider, PublicChannelUsername,
    RevisionRepository, RunExecutor, SubscriptionRepository,
};
use ratatoskr_operation_contracts::{OperationReported, OperationStage, OperationStatus};
use serde_json::{Value, json};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WINDOW_START: &str = "2026-08-28T10:00:00Z";
const WINDOW_END: &str = "2026-08-29T10:00:00Z";
const POST_AT: &str = "2026-08-29T09:00:00Z";

#[tokio::test]
async fn subscription_set_reports_succeeded_not_completed() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    let operation = Uuid::now_v7();
    let message = Uuid::now_v7();
    let intake = CommandIntake::new(database.pool().clone());

    let command = subscription_command(owner, operation, "report_applied", "active");
    assert_eq!(
        intake.accept_subscription(message, &command).await?,
        IntakeOutcome::Applied
    );

    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(
        statuses(&reports),
        ["succeeded"],
        "applied is a terminal success"
    );
    let report = typed(&reports, 0)?;
    assert_eq!(
        report.stage.as_ref().map(OperationStage::as_str),
        Some("applied")
    );
    assert!(report.results.is_empty());
    assert!(report.error.is_none());
    assert!(report.warnings.is_empty());
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn twenty_first_active_subscription_fails_the_operation_and_acks() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    for index in 0..20 {
        subscriptions
            .set(
                owner,
                &format!("limit_channel_{index:02}"),
                true,
                WINDOW_START,
            )
            .await?;
    }
    let operation = Uuid::now_v7();
    let message = Uuid::now_v7();
    let command = subscription_command(owner, operation, "limit_channel_21", "active");
    let intake = CommandIntake::new(database.pool().clone());

    let outcome = intake.accept_subscription(message, &command).await;
    assert!(
        outcome.is_ok(),
        "the limit refusal is a durable outcome that must be acknowledged, got {outcome:?}"
    );

    let inbox: (String, Option<String>) = sqlx::query_as(
        "select state, safe_failure_class from channel_digests.inbox_messages where message_id = $1",
    )
    .bind(message)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        inbox,
        ("failed".to_owned(), Some("subscription_limit".to_owned()))
    );
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["failed"]);
    let report = typed(&reports, 0)?;
    let error = report
        .error
        .as_ref()
        .ok_or("the failed report carries an error")?;
    assert_eq!(
        error.code.as_str(),
        "channel_digest.subscription_limit_reached"
    );
    assert!(!error.retryable);
    let stored: (i64,) =
        sqlx::query_as("select count(*) from channel_digests.subscriptions where owner_id = $1")
            .bind(owner)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(stored.0, 20, "the refused subscription must not exist");

    let replay = intake.accept_subscription(message, &command).await?;
    assert_eq!(replay, IntakeOutcome::Replayed);
    assert_eq!(raw_reports(database.pool(), operation).await?.len(), 1);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn zero_source_run_reports_succeeded() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_empty").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let executor = RunExecutor::new(database.pool().clone(), PageProvider::new(Vec::new()));

    assert!(executor.execute_one().await?);

    let state = run_state(database.pool(), run_id).await?;
    assert_eq!(state.0, "completed");
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["running", "succeeded"]);
    let running = typed(&reports, 0)?;
    assert_eq!(
        running.stage.as_ref().map(OperationStage::as_str),
        Some("acquiring")
    );
    let finished = typed(&reports, 1)?;
    assert!(finished.results.is_empty(), "no source means no result");
    assert!(finished.error.is_none());
    let manifests: (i64,) =
        sqlx::query_as("select count(*) from channel_digests.digest_manifests where run_id = $1")
            .bind(run_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(manifests.0, 0, "an empty selection is not a manifest");
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn completion_with_omitted_sources_reports_partially_succeeded_with_warning_and_result_ref()
-> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_partial").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    execute_with_posts(&database, 2).await?;
    let manifest_digest = manifest_digest(database.pool(), run_id).await?;
    let result_id = Uuid::now_v7();
    let fact = completion(
        owner,
        operation,
        run_id,
        result_id,
        &manifest_digest,
        (2, 1, 1),
    );

    let outcome = DigestCoordinator::new(database.pool().clone())
        .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&fact)?)
        .await?;
    assert_eq!(outcome, IntakeOutcome::Applied);

    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["running", "partially_succeeded"]);
    let report = typed(&reports, 1)?;
    assert_eq!(report.status, OperationStatus::PartiallySucceeded);
    assert_eq!(report.warnings.len(), 1);
    assert_eq!(
        report.warnings[0].code.as_str(),
        "channel_digest.context_omitted"
    );
    assert!(report.error.is_none());
    assert_eq!(report.results.len(), 1);
    assert_eq!(
        report.results[0].result_kind.as_str(),
        "channel_digest.result"
    );
    assert_eq!(
        report.results[0].target.to_wire(),
        format!("channel-digest-result:{result_id}")
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn completion_without_omitted_sources_reports_succeeded_with_the_result_ref() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_complete").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    execute_with_posts(&database, 1).await?;
    let manifest_digest = manifest_digest(database.pool(), run_id).await?;
    let result_id = Uuid::now_v7();
    let fact = completion(
        owner,
        operation,
        run_id,
        result_id,
        &manifest_digest,
        (1, 1, 0),
    );

    DigestCoordinator::new(database.pool().clone())
        .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&fact)?)
        .await?;

    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["running", "succeeded"]);
    let report = typed(&reports, 1)?;
    assert!(report.warnings.is_empty());
    assert_eq!(report.results.len(), 1);
    assert_eq!(
        report.results[0].target.to_wire(),
        format!("channel-digest-result:{result_id}")
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn knowledge_failure_reports_failed_with_mapped_code() -> TestResult {
    let database = fresh_database().await?;
    for (failure_code, retryable) in [("provider_timeout", true), ("invalid_output", false)] {
        reset(database.pool()).await?;
        let owner = Uuid::now_v7();
        subscribe(&database, owner, "report_failure").await?;
        let operation = Uuid::now_v7();
        let run_id = accept_run(&database, owner, operation).await?;
        execute_with_posts(&database, 1).await?;
        let manifest_digest = manifest_digest(database.pool(), run_id).await?;
        let fact = json!({
            "owner": format!("user:{owner}"),
            "operation_id": operation,
            "digest_run_id": run_id,
            "manifest_digest": {"algorithm": "sha256", "hex": manifest_digest},
            "failure_code": failure_code,
            "failed_at": "2026-08-29T10:02:00Z"
        });

        let outcome = DigestCoordinator::new(database.pool().clone())
            .settle_failure(Uuid::now_v7(), &serde_json::to_vec(&fact)?)
            .await?;
        assert_eq!(outcome, IntakeOutcome::Applied);

        let reports = raw_reports(database.pool(), operation).await?;
        assert_eq!(statuses(&reports), ["running", "failed"], "{failure_code}");
        let report = typed(&reports, 1)?;
        let error = report
            .error
            .as_ref()
            .ok_or("the failed report carries an error")?;
        assert_eq!(
            error.code.as_str(),
            format!("channel_digest.recap.{failure_code}")
        );
        assert_eq!(error.retryable, retryable, "{failure_code}");
        assert!(report.results.is_empty());
    }
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn acquisition_failure_reports_failed_in_the_same_transaction_as_the_state_change()
-> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_unavailable").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let executor = RunExecutor::new(database.pool().clone(), UnavailableProvider);

    // Fault injection: the report insert is refused, so a failure state that commits without
    // its report would be visible afterwards.
    install_report_fault(database.pool()).await?;
    let refused = executor.execute_one().await;
    let during = run_state(database.pool(), run_id).await;
    let reports_during = raw_reports(database.pool(), operation).await;
    remove_report_fault(database.pool()).await?;
    assert!(refused.is_err(), "the refused report must fail the step");
    assert_eq!(
        during?.0, "acquiring",
        "the failed state must not commit without its report"
    );
    assert_eq!(statuses(&reports_during?), ["running"]);

    assert!(executor.execute_one().await?);
    let state = run_state(database.pool(), run_id).await?;
    assert_eq!(
        state,
        ("failed".to_owned(), Some("provider_unavailable".to_owned()))
    );
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["running", "failed"]);
    let error = typed(&reports, 1)?
        .error
        .ok_or("the failed report carries an error")?;
    assert_eq!(error.code.as_str(), "channel_digest.provider_unavailable");
    assert!(error.retryable);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn manifest_limit_fails_the_run_instead_of_retrying_forever() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_oversize").await?;
    let channel: (Uuid,) = sqlx::query_as(
        "select channel_id from channel_digests.channels where username = 'report_oversize'",
    )
    .fetch_one(database.pool())
    .await?;
    // Acquisition refuses such a body, so the revision is stored directly: the manifest bound is
    // a defence in depth for storage that was not written by acquisition.
    let oversized = "x".repeat(16_385);
    RevisionRepository::new(database.pool().clone())
        .append(&ObservedRevision {
            channel_id: channel.0,
            provider_message_id: 9,
            body: &oversized,
            canonical_link: "https://t.me/report_oversize/9",
            published_at: POST_AT,
            observed_at: POST_AT,
        })
        .await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let executor = RunExecutor::new(database.pool().clone(), PageProvider::new(Vec::new()));

    assert!(executor.execute_one().await?);

    let state = run_state(database.pool(), run_id).await?;
    assert_eq!(
        state,
        ("failed".to_owned(), Some("manifest_invalid".to_owned()))
    );
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(statuses(&reports), ["running", "failed"]);
    let error = typed(&reports, 1)?
        .error
        .ok_or("the failed report carries an error")?;
    assert_eq!(error.code.as_str(), "channel_digest.manifest_invalid");
    assert!(!error.retryable);
    assert!(
        !executor.execute_one().await?,
        "a failed run must not be selected again"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn occurrence_reports_succeeded_on_its_own_operation_and_scheduled_runs_report_nothing()
-> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "report_scheduled").await?;
    let schedule_owner = Uuid::now_v7();
    let occurrence_operation = Uuid::now_v7();
    let occurrence = format!("schedule-occurrence:{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&json!({"occurrence_ref": occurrence}))?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    assert_eq!(
        coordinator
            .accept_occurrence(&OccurrenceRequest {
                message_id: Uuid::now_v7(),
                payload: &payload,
                occurrence_key: &occurrence,
                previous_due_at: "2026-08-28T10:00:00Z",
                due_at: WINDOW_END,
                operation_id: occurrence_operation,
                owner_id: schedule_owner,
                output_language: OutputLanguage::Ru,
            })
            .await?,
        IntakeOutcome::Applied
    );

    let reports = raw_reports(database.pool(), occurrence_operation).await?;
    assert_eq!(statuses(&reports), ["succeeded"]);
    let report = typed(&reports, 0)?;
    assert_eq!(
        report.stage.as_ref().map(OperationStage::as_str),
        Some("fanned_out")
    );
    let report_owner: (Uuid,) = sqlx::query_as(
        "select owner_id from channel_digests.outbox_messages where operation_id = $1",
    )
    .bind(occurrence_operation)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(report_owner.0, schedule_owner);

    execute_with_posts(&database, 1).await?;
    let run: (Uuid, String) = sqlx::query_as(
        "select run_id, state from channel_digests.digest_runs where owner_id = $1 and trigger = 'scheduled'",
    )
    .bind(owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(run.1, "waiting_recap");
    let manifest_digest = manifest_digest(database.pool(), run.0).await?;
    let fact = completion(
        owner,
        occurrence_operation,
        run.0,
        Uuid::now_v7(),
        &manifest_digest,
        (1, 1, 0),
    );
    coordinator
        .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&fact)?)
        .await?;
    assert_eq!(
        run_state(database.pool(), run.0).await?.0,
        "completed",
        "the scheduled run still settles"
    );
    assert_eq!(
        raw_reports(database.pool(), occurrence_operation)
            .await?
            .len(),
        1,
        "a scheduled run is owned by no Platform operation and reports nothing"
    );
    database.close().await;
    Ok(())
}

async fn fresh_database() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset(database.pool()).await?;
    Ok(database)
}

async fn reset(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "truncate channel_digests.digest_results, channel_digests.digest_manifests,
                  channel_digests.post_revisions, channel_digests.subscriptions,
                  channel_digests.channels, channel_digests.digest_runs,
                  channel_digests.inbox_messages, channel_digests.outbox_messages,
                  channel_digests.leases cascade",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn subscribe(database: &Database, owner: Uuid, username: &str) -> TestResult {
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, username, true, "2026-08-27T09:00:00Z")
        .await?;
    Ok(())
}

fn subscription_command(owner: Uuid, operation: Uuid, username: &str, state: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "operation_id": operation,
        "owner": format!("user:{owner}"),
        "idempotency_key": format!("operation.{operation}"),
        "channel_username": username,
        "desired_state": state
    }))
    .unwrap_or_default()
}

async fn accept_run(
    database: &Database,
    owner: Uuid,
    operation: Uuid,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let run_id = Uuid::now_v7();
    let command = serde_json::to_vec(&json!({
        "operation_id": operation,
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("operation.{operation}"),
        "window": {"start_at": WINDOW_START, "end_at": WINDOW_END},
        "output_language": "ru",
        "trigger": {"kind": "on_demand", "accepted_at": WINDOW_END}
    }))?;
    CommandIntake::new(database.pool().clone())
        .accept_run(Uuid::now_v7(), &command)
        .await?;
    Ok(run_id)
}

async fn execute_with_posts(database: &Database, count: i64) -> TestResult {
    let posts = (0..count)
        .map(|index| ProviderPost {
            message_id: 100 + index,
            body: format!("synthetic report post {index}"),
            published_at: POST_AT.to_owned(),
            deleted: false,
        })
        .collect();
    let executor = RunExecutor::new(database.pool().clone(), PageProvider::new(posts));
    assert!(executor.execute_one().await?);
    Ok(())
}

async fn manifest_digest(pool: &sqlx::PgPool, run_id: Uuid) -> Result<String, sqlx::Error> {
    let row: (String,) =
        sqlx::query_as("select sha256 from channel_digests.digest_manifests where run_id = $1")
            .bind(run_id)
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

async fn run_state(
    pool: &sqlx::PgPool,
    run_id: Uuid,
) -> Result<(String, Option<String>), sqlx::Error> {
    sqlx::query_as(
        "select state, safe_failure_class from channel_digests.digest_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await
}

async fn raw_reports(pool: &sqlx::PgPool, operation: Uuid) -> Result<Vec<Value>, sqlx::Error> {
    let rows: Vec<(Value,)> = sqlx::query_as(
        "select payload from channel_digests.outbox_messages
         where subject = 'platform.operation.reported.v1' and operation_id = $1
         order by created_at, outbox_id",
    )
    .bind(operation)
    .fetch_all(pool)
    .await?;
    Ok(rows.into_iter().map(|row| row.0).collect())
}

fn statuses(reports: &[Value]) -> Vec<&str> {
    reports
        .iter()
        .map(|report| report.get("status").and_then(Value::as_str).unwrap_or("?"))
        .collect()
}

fn typed(reports: &[Value], index: usize) -> Result<OperationReported, Box<dyn std::error::Error>> {
    let value = reports.get(index).ok_or("the expected report is absent")?;
    let report: OperationReported = serde_json::from_value(value.clone())?;
    report.validate()?;
    Ok(report)
}

fn completion(
    owner: Uuid,
    operation: Uuid,
    run_id: Uuid,
    result_id: Uuid,
    manifest_digest: &str,
    (selected, included, omitted): (u16, u16, u16),
) -> Value {
    json!({
        "owner": format!("user:{owner}"),
        "operation_id": operation,
        "digest_run_id": run_id,
        "manifest_digest": {"algorithm": "sha256", "hex": manifest_digest},
        "analysis_ref": format!("analysis:{}", Uuid::now_v7()),
        "digest_result_id": result_id,
        "result_ref": format!("channel-digest-result:{result_id}"),
        "result_digest": {"algorithm": "sha256", "hex": "22".repeat(32)},
        "completed_at": "2026-08-29T10:01:00Z",
        "coverage": {
            "selected_count": selected,
            "included_count": included,
            "omitted_count": omitted,
            "channel_count": 1
        }
    })
}

async fn install_report_fault(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "create or replace function channel_digests.refuse_report_inserts() returns trigger
         language plpgsql as $$
         begin raise exception 'report insert refused by fault injection'; end;
         $$;
         drop trigger if exists refuse_report_inserts on channel_digests.outbox_messages;
         create trigger refuse_report_inserts before insert on channel_digests.outbox_messages
         for each row when (new.subject = 'platform.operation.reported.v1')
         execute function channel_digests.refuse_report_inserts();",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn remove_report_fault(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::raw_sql(
        "drop trigger if exists refuse_report_inserts on channel_digests.outbox_messages;
         drop function if exists channel_digests.refuse_report_inserts();",
    )
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug)]
struct PageProvider(Mutex<Option<ProviderPage>>);

impl PageProvider {
    fn new(posts: Vec<ProviderPost>) -> Self {
        Self(Mutex::new(Some(ProviderPage {
            posts,
            next_before_message_id: None,
        })))
    }
}

impl PublicChannelProvider for PageProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        _channel: &Self::Channel,
        _before_message_id: Option<i64>,
        _limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        self.0
            .lock()
            .map_err(|_| ProviderError::Unavailable)?
            .take()
            .ok_or(ProviderError::Unavailable)
    }
}

#[derive(Debug)]
struct UnavailableProvider;

impl PublicChannelProvider for UnavailableProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        _username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        Err(ProviderError::Unavailable)
    }

    async fn fetch_public_posts(
        &self,
        _channel: &Self::Channel,
        _before_message_id: Option<i64>,
        _limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        Err(ProviderError::Unavailable)
    }
}
