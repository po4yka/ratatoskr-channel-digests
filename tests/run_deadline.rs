//! Run deadline reaper (XR-021 CONTRACTS.md R2-05 a): no operation stays `running` for a day.

use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{
    CommandIntake, CoordinatorError, Database, DigestCoordinator, IntakeOutcome, OccurrenceRequest,
    ProviderError, ProviderPage, ProviderPost, PublicChannelProvider, PublicChannelUsername,
    Reaper, RunExecutor, SubscriptionRepository,
};
use ratatoskr_operation_contracts::{OperationReported, OperationStage};
use serde_json::{Value, json};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WINDOW_START: &str = "2026-08-28T10:00:00Z";
const WINDOW_END: &str = "2026-08-29T10:00:00Z";
const POST_AT: &str = "2026-08-29T09:00:00Z";
const DEADLINE: Duration = Duration::from_mins(30);
const OVERDUE_SECONDS: f64 = 3_600.0;
/// The instant every reap in this file observes; run ages are stored relative to it, never to real time.
const CLOCK: &str = "2026-08-29T12:00:00Z";

#[tokio::test]
async fn overdue_on_demand_runs_fail_once_each_with_a_retryable_report() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "deadline_recap").await?;
    let waiting = Case::accept(&database, owner).await?;
    execute_with_posts(&database, 1).await?;
    assert_eq!(
        run_state(database.pool(), waiting.run).await?.0,
        "waiting_recap"
    );
    let acquiring = Case::accept(&database, owner).await?;
    set_state(database.pool(), acquiring.run, "acquiring").await?;
    let accepted = Case::accept(&database, owner).await?;
    let young = Case::accept(&database, owner).await?;
    touch(database.pool(), young.run).await?;
    for case in [&waiting, &acquiring, &accepted] {
        age(database.pool(), case.run).await?;
    }
    let reaper = Reaper::new(database.pool().clone(), DEADLINE);

    assert_eq!(reaper.reap_once(clock()?).await?, 3);

    for case in [&waiting, &acquiring, &accepted] {
        assert_eq!(
            run_state(database.pool(), case.run).await?,
            ("failed".to_owned(), Some("deadline_exceeded".to_owned()))
        );
        let reports = raw_reports(database.pool(), case.operation).await?;
        assert_eq!(statuses(&reports), ["running", "failed"]);
        let report = typed(&reports, 1)?;
        let error = report
            .error
            .as_ref()
            .ok_or("the failed report has an error")?;
        assert_eq!(error.code.as_str(), "channel_digest.run_deadline_exceeded");
        assert!(error.retryable);
        assert!(report.results.is_empty());
        assert_eq!(
            report.stage.as_ref().map(OperationStage::as_str),
            Some("deadline")
        );
        assert_eq!(failed_keys(database.pool(), case.operation).await?, 1);
    }
    assert_eq!(
        run_state(database.pool(), young.run).await?.0,
        "accepted",
        "a run younger than the deadline is untouched"
    );
    assert_eq!(
        statuses(&raw_reports(database.pool(), young.operation).await?),
        ["running"]
    );

    assert_eq!(
        reaper.reap_once(clock()?).await?,
        0,
        "a terminal run is never reaped again"
    );
    assert_eq!(failed_keys(database.pool(), waiting.operation).await?, 1);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn the_injected_clock_decides_what_is_overdue() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    let case = Case::accept(&database, owner).await?;
    touch(database.pool(), case.run).await?;
    let reaper = Reaper::new(database.pool().clone(), DEADLINE);

    assert_eq!(reaper.reap_once(clock()?).await?, 0);
    assert_eq!(run_state(database.pool(), case.run).await?.0, "accepted");

    let later = clock()? + jiff::SignedDuration::from_secs(1_801);
    assert_eq!(reaper.reap_once(later).await?, 1);
    assert_eq!(run_state(database.pool(), case.run).await?.0, "failed");
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_scheduled_run_fails_without_a_report() -> TestResult {
    let database = fresh_database().await?;
    let subscriber = Uuid::now_v7();
    subscribe(&database, subscriber, "deadline_scheduled").await?;
    let occurrence_operation = Uuid::now_v7();
    let occurrence = format!("schedule-occurrence:{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&json!({"occurrence_ref": occurrence}))?;
    let outcome = DigestCoordinator::new(database.pool().clone())
        .accept_occurrence(&OccurrenceRequest {
            message_id: Uuid::now_v7(),
            payload: &payload,
            occurrence_key: &occurrence,
            previous_due_at: WINDOW_START,
            due_at: WINDOW_END,
            operation_id: occurrence_operation,
            owner_id: Uuid::now_v7(),
            output_language: OutputLanguage::Ru,
        })
        .await?;
    assert_eq!(outcome, IntakeOutcome::Applied);
    let run: (Uuid,) = sqlx::query_as(
        "select run_id from channel_digests.digest_runs where owner_id = $1 and trigger = 'scheduled'",
    )
    .bind(subscriber)
    .fetch_one(database.pool())
    .await?;
    age(database.pool(), run.0).await?;

    let reaped = Reaper::new(database.pool().clone(), DEADLINE)
        .reap_once(clock()?)
        .await?;

    assert_eq!(reaped, 1);
    assert_eq!(
        run_state(database.pool(), run.0).await?,
        ("failed".to_owned(), Some("deadline_exceeded".to_owned()))
    );
    assert_eq!(
        statuses(&raw_reports(database.pool(), occurrence_operation).await?),
        ["succeeded"],
        "only the occurrence report exists; a scheduled run owns no Platform operation"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_run_whose_failed_report_already_exists_gets_no_second_one() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    let case = Case::accept(&database, owner).await?;
    sqlx::query(
        "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, operation_id, payload) values ($1, 'platform.operation.reported.v1', $2, $3, $4, $5)",
    )
    .bind(Uuid::now_v7())
    .bind(format!("operation:{}:failed", case.operation))
    .bind(owner)
    .bind(case.operation)
    .bind(json!({
        "operation_id": case.operation,
        "status": "failed",
        "stage": "acquiring",
        "error": {
            "code": "channel_digest.provider_unavailable",
            "message": "The channel posts could not be read from Telegram.",
            "retryable": true
        }
    }))
    .execute(database.pool())
    .await?;
    age(database.pool(), case.run).await?;

    let reaped = Reaper::new(database.pool().clone(), DEADLINE)
        .reap_once(clock()?)
        .await?;

    assert_eq!(reaped, 1);
    assert_eq!(run_state(database.pool(), case.run).await?.0, "failed");
    let reports = raw_reports(database.pool(), case.operation).await?;
    assert_eq!(statuses(&reports), ["running", "failed"]);
    let code = reports
        .iter()
        .find_map(|report| report.pointer("/error/code").and_then(Value::as_str));
    assert_eq!(
        code,
        Some("channel_digest.provider_unavailable"),
        "the report that was already queued is kept"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_late_knowledge_fact_for_a_reaped_run_changes_nothing() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "deadline_late").await?;
    let case = Case::accept(&database, owner).await?;
    execute_with_posts(&database, 1).await?;
    let digest = manifest_digest(database.pool(), case.run).await?;
    age(database.pool(), case.run).await?;
    let reaped = Reaper::new(database.pool().clone(), DEADLINE)
        .reap_once(clock()?)
        .await?;
    assert_eq!(reaped, 1);
    let coordinator = DigestCoordinator::new(database.pool().clone());

    let completion = json!({
        "owner": format!("user:{owner}"),
        "operation_id": case.operation,
        "digest_run_id": case.run,
        "manifest_digest": {"algorithm": "sha256", "hex": digest},
        "analysis_ref": format!("analysis:{}", Uuid::now_v7()),
        "digest_result_id": Uuid::now_v7(),
        "result_ref": format!("channel-digest-result:{}", Uuid::now_v7()),
        "result_digest": {"algorithm": "sha256", "hex": "22".repeat(32)},
        "completed_at": "2026-08-29T10:01:00Z",
        "coverage": {
            "selected_count": 1, "included_count": 1, "omitted_count": 0, "channel_count": 1
        }
    });
    let late_completion = coordinator
        .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&completion)?)
        .await;
    assert!(
        matches!(late_completion, Err(CoordinatorError::Invalid)),
        "a completion for a failed run is refused, got {late_completion:?}"
    );

    let failure = json!({
        "owner": format!("user:{owner}"),
        "operation_id": case.operation,
        "digest_run_id": case.run,
        "manifest_digest": {"algorithm": "sha256", "hex": digest},
        "failure_code": "provider_timeout",
        "failed_at": "2026-08-29T10:02:00Z"
    });
    let late_failure = coordinator
        .settle_failure(Uuid::now_v7(), &serde_json::to_vec(&failure)?)
        .await?;
    assert_eq!(late_failure, IntakeOutcome::Replayed);

    assert_eq!(
        run_state(database.pool(), case.run).await?,
        ("failed".to_owned(), Some("deadline_exceeded".to_owned()))
    );
    let results: (i64,) =
        sqlx::query_as("select count(*) from channel_digests.digest_results where run_id = $1")
            .bind(case.run)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(results.0, 0);
    let reports = raw_reports(database.pool(), case.operation).await?;
    assert_eq!(statuses(&reports), ["running", "failed"]);
    assert_eq!(
        typed(&reports, 1)?
            .error
            .map(|error| error.code.as_str().to_owned()),
        Some("channel_digest.run_deadline_exceeded".to_owned())
    );
    database.close().await;
    Ok(())
}

struct Case {
    run: Uuid,
    operation: Uuid,
}

impl Case {
    async fn accept(database: &Database, owner: Uuid) -> Result<Self, Box<dyn std::error::Error>> {
        let run = Uuid::now_v7();
        let operation = Uuid::now_v7();
        let command = serde_json::to_vec(&json!({
            "operation_id": operation,
            "owner": format!("user:{owner}"),
            "digest_run_id": run,
            "idempotency_key": format!("operation.{operation}"),
            "window": {"start_at": WINDOW_START, "end_at": WINDOW_END},
            "output_language": "ru",
            "trigger": {"kind": "on_demand", "accepted_at": WINDOW_END}
        }))?;
        CommandIntake::new(database.pool().clone())
            .accept_run(Uuid::now_v7(), &command)
            .await?;
        Ok(Self { run, operation })
    }
}

async fn fresh_database() -> Result<Database, Box<dyn std::error::Error>> {
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
    Ok(database)
}

async fn subscribe(database: &Database, owner: Uuid, username: &str) -> TestResult {
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, username, true, "2026-08-27T09:00:00Z")
        .await?;
    Ok(())
}

async fn execute_with_posts(database: &Database, count: i64) -> TestResult {
    let posts = (0..count)
        .map(|index| ProviderPost {
            message_id: 100 + index,
            body: format!("synthetic deadline post {index}"),
            published_at: POST_AT.to_owned(),
            deleted: false,
        })
        .collect();
    let executor = RunExecutor::new(database.pool().clone(), PageProvider::new(posts));
    assert!(executor.execute_one().await?);
    Ok(())
}

fn clock() -> Result<jiff::Timestamp, jiff::Error> {
    CLOCK.parse()
}

async fn age(pool: &sqlx::PgPool, run: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update channel_digests.digest_runs set updated_at = $3::timestamptz - make_interval(secs => $2) where run_id = $1",
    )
    .bind(run)
    .bind(OVERDUE_SECONDS)
    .bind(CLOCK)
    .execute(pool)
    .await?;
    Ok(())
}

async fn touch(pool: &sqlx::PgPool, run: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "update channel_digests.digest_runs set updated_at = $2::timestamptz where run_id = $1",
    )
    .bind(run)
    .bind(CLOCK)
    .execute(pool)
    .await?;
    Ok(())
}

async fn set_state(pool: &sqlx::PgPool, run: Uuid, state: &str) -> Result<(), sqlx::Error> {
    sqlx::query("update channel_digests.digest_runs set state = $2 where run_id = $1")
        .bind(run)
        .bind(state)
        .execute(pool)
        .await?;
    Ok(())
}

async fn manifest_digest(pool: &sqlx::PgPool, run: Uuid) -> Result<String, sqlx::Error> {
    let row: (String,) =
        sqlx::query_as("select sha256 from channel_digests.digest_manifests where run_id = $1")
            .bind(run)
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

async fn run_state(
    pool: &sqlx::PgPool,
    run: Uuid,
) -> Result<(String, Option<String>), sqlx::Error> {
    sqlx::query_as(
        "select state, safe_failure_class from channel_digests.digest_runs where run_id = $1",
    )
    .bind(run)
    .fetch_one(pool)
    .await
}

async fn failed_keys(pool: &sqlx::PgPool, operation: Uuid) -> Result<i64, sqlx::Error> {
    let row: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.outbox_messages where subject = 'platform.operation.reported.v1' and semantic_key = $1",
    )
    .bind(format!("operation:{operation}:failed"))
    .fetch_one(pool)
    .await?;
    Ok(row.0)
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
