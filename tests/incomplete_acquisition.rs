//! An acquisition that is incomplete is reported as incomplete, never as an empty success
//! (XR-021 CONTRACTS.md R3-10 rules 1 to 3).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{
    CommandIntake, Database, DigestCoordinator, OccurrenceRequest, ProviderError, ProviderPage,
    ProviderPost, PublicChannelProvider, PublicChannelUsername, RunExecutor,
    SubscriptionRepository,
};
use ratatoskr_operation_contracts::OperationReported;
use serde_json::{Value, json};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WINDOW_START: &str = "2026-08-28T10:00:00Z";
const WINDOW_END: &str = "2026-08-29T10:00:00Z";
const POST_AT: &str = "2026-08-29T09:00:00Z";
const OLDER_THAN_WINDOW: &str = "2026-08-27T09:00:00Z";
/// More pages than the executor's page limit of twenty, so a walk that never stops runs dry only
/// after it has exhausted the limit.
const DEEP_HISTORY_PAGES: usize = 40;

#[tokio::test]
async fn one_unavailable_channel_and_no_posts_fails_the_run_instead_of_completing_empty()
-> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "incomplete_alpha").await?;
    subscribe(&database, owner, "incomplete_bravo").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let provider = ScriptedProvider::new([
        ("incomplete_alpha", vec![Step::Unavailable]),
        ("incomplete_bravo", vec![Step::Page(page(&[], None))]),
    ]);

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(
        run_state(database.pool(), run_id).await?,
        ("failed".to_owned(), Some("provider_unavailable".to_owned()))
    );
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(
        terminal_statuses(&reports),
        ["failed"],
        "an incomplete empty window is not a success"
    );
    assert!(
        reports
            .iter()
            .all(|report| { report.get("stage").and_then(Value::as_str) != Some("no_sources") }),
        "no no_sources report may exist"
    );
    let error = typed(&reports, reports.len() - 1)?
        .error
        .ok_or("the failed report carries an error")?;
    assert_eq!(error.code.as_str(), "channel_digest.provider_unavailable");
    assert!(error.retryable);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_channel_that_fails_mid_pagination_marks_the_run_incomplete() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "incomplete_midway").await?;
    let run_id = accept_run(&database, owner, Uuid::now_v7()).await?;
    let provider = ScriptedProvider::new([(
        "incomplete_midway",
        vec![
            Step::Page(page(&[(100, "first page post")], Some(50))),
            Step::Unavailable,
        ],
    )]);

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(run_state(database.pool(), run_id).await?.0, "waiting_recap");
    assert!(
        incomplete_flag(database.pool(), run_id).await?,
        "a truncated page walk must be recorded"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn an_incomplete_acquisition_with_sources_settles_partially_succeeded() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "incomplete_sources").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    execute_midway(&database, "incomplete_sources", 2).await?;
    let result_id = Uuid::now_v7();

    settle(&database, owner, operation, run_id, result_id, (2, 2, 0)).await?;

    assert_eq!(
        result_outcome(database.pool(), run_id).await?,
        "partial",
        "missing channel coverage makes the result partial"
    );
    assert_eq!(run_state(database.pool(), run_id).await?.0, "partial");
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["partially_succeeded"]);
    let report = typed(&reports, reports.len() - 1)?;
    assert_eq!(
        warning_codes(&report),
        ["channel_digest.acquisition_incomplete"]
    );
    assert_eq!(report.results.len(), 1);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn an_incomplete_acquisition_with_omitted_sources_carries_both_warnings() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "incomplete_both").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    execute_midway(&database, "incomplete_both", 2).await?;

    settle(
        &database,
        owner,
        operation,
        run_id,
        Uuid::now_v7(),
        (2, 1, 1),
    )
    .await?;

    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["partially_succeeded"]);
    let report = typed(&reports, reports.len() - 1)?;
    let mut codes = warning_codes(&report);
    codes.sort_unstable();
    assert_eq!(
        codes,
        [
            "channel_digest.acquisition_incomplete",
            "channel_digest.context_omitted"
        ]
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_complete_acquisition_still_settles_succeeded() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "complete_channel").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let provider = ScriptedProvider::new([(
        "complete_channel",
        vec![Step::Page(page(&[(100, "only page post")], None))],
    )]);
    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );
    assert!(!incomplete_flag(database.pool(), run_id).await?);

    settle(
        &database,
        owner,
        operation,
        run_id,
        Uuid::now_v7(),
        (1, 1, 0),
    )
    .await?;

    assert_eq!(result_outcome(database.pool(), run_id).await?, "completed");
    assert_eq!(run_state(database.pool(), run_id).await?.0, "completed");
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["succeeded"]);
    assert!(typed(&reports, reports.len() - 1)?.warnings.is_empty());
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn an_incomplete_scheduled_run_settles_partial_without_a_report() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "incomplete_scheduled").await?;
    let occurrence_operation = Uuid::now_v7();
    let occurrence = format!("schedule-occurrence:{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&json!({"occurrence_ref": occurrence}))?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    coordinator
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
    execute_midway(&database, "incomplete_scheduled", 1).await?;
    let run: (Uuid,) = sqlx::query_as(
        "select run_id from channel_digests.digest_runs where owner_id = $1 and trigger = 'scheduled'",
    )
    .bind(owner)
    .fetch_one(database.pool())
    .await?;

    settle(
        &database,
        owner,
        occurrence_operation,
        run.0,
        Uuid::now_v7(),
        (1, 1, 0),
    )
    .await?;

    assert_eq!(result_outcome(database.pool(), run.0).await?, "partial");
    assert_eq!(run_state(database.pool(), run.0).await?.0, "partial");
    assert_eq!(
        raw_reports(database.pool(), occurrence_operation)
            .await?
            .len(),
        1,
        "a scheduled run reports nothing beyond the occurrence fan-out"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn deep_history_with_the_window_satisfied_on_page_one_is_not_partial() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "deep_history").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let mut steps = vec![Step::Page(page_with_dates(
        &[
            (1_000, "in the window", POST_AT),
            (999, "before it", OLDER_THAN_WINDOW),
        ],
        Some(999),
    ))];
    steps.extend(older_pages(998));
    let provider = ScriptedProvider::new([("deep_history", steps)]);
    let calls = provider.calls();

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the first page already reaches before the window"
    );
    assert!(
        !incomplete_flag(database.pool(), run_id).await?,
        "a window read to its start is complete however deep the history is"
    );
    assert_eq!(
        acquisition_state(database.pool(), run_id).await?,
        "completed"
    );
    assert_eq!(run_state(database.pool(), run_id).await?.0, "waiting_recap");
    settle(
        &database,
        owner,
        operation,
        run_id,
        Uuid::now_v7(),
        (1, 1, 0),
    )
    .await?;
    assert_eq!(result_outcome(database.pool(), run_id).await?, "completed");
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["succeeded"]);
    assert!(typed(&reports, reports.len() - 1)?.warnings.is_empty());
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_quiet_channel_with_deep_history_completes_as_an_empty_success() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "quiet_deep").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let mut steps = vec![Step::Page(page_with_dates(
        &[
            (500, "long ago", OLDER_THAN_WINDOW),
            (499, "longer ago", OLDER_THAN_WINDOW),
        ],
        Some(499),
    ))];
    steps.extend(older_pages(498));
    let provider = ScriptedProvider::new([("quiet_deep", steps)]);
    let calls = provider.calls();

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(
        run_state(database.pool(), run_id).await?,
        ("completed".to_owned(), None),
        "no post in the window is an empty success, not an unavailable provider"
    );
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["succeeded"]);
    assert_eq!(
        reports
            .last()
            .and_then(|report| report.get("stage"))
            .and_then(Value::as_str),
        Some("no_sources")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_window_longer_than_max_pages_full_pages_is_still_partial() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "long_window").await?;
    let operation = Uuid::now_v7();
    let run_id = accept_run(&database, owner, operation).await?;
    let steps: Vec<Step> = (0..DEEP_HISTORY_PAGES)
        .map(|index| {
            let id = 1_000 - i64::try_from(index).unwrap_or(0);
            Step::Page(page_with_dates(
                &[(id, "inside the window", POST_AT)],
                Some(id),
            ))
        })
        .collect();
    let provider = ScriptedProvider::new([("long_window", steps)]);
    let calls = provider.calls();

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(
        calls.load(Ordering::SeqCst),
        20,
        "the page limit bounds the walk"
    );
    assert!(incomplete_flag(database.pool(), run_id).await?);
    assert_eq!(
        acquisition_state(database.pool(), run_id).await?,
        "page_limit"
    );
    settle(
        &database,
        owner,
        operation,
        run_id,
        Uuid::now_v7(),
        (20, 20, 0),
    )
    .await?;
    assert_eq!(result_outcome(database.pool(), run_id).await?, "partial");
    let reports = raw_reports(database.pool(), operation).await?;
    assert_eq!(terminal_statuses(&reports), ["partially_succeeded"]);
    assert_eq!(
        warning_codes(&typed(&reports, reports.len() - 1)?),
        ["channel_digest.acquisition_incomplete"]
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_mid_walk_provider_failure_is_still_partial() -> TestResult {
    let database = fresh_database().await?;
    let owner = Uuid::now_v7();
    subscribe(&database, owner, "midwalk_failure").await?;
    let run_id = accept_run(&database, owner, Uuid::now_v7()).await?;
    let provider = ScriptedProvider::new([(
        "midwalk_failure",
        vec![
            Step::Page(page(&[(100, "in the window")], Some(50))),
            Step::Unavailable,
        ],
    )]);
    let calls = provider.calls();

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(incomplete_flag(database.pool(), run_id).await?);
    assert_eq!(
        acquisition_state(database.pool(), run_id).await?,
        "provider_unavailable"
    );
    database.close().await;
    Ok(())
}

/// Runs the one pending run with a channel that returns `posts` posts and then fails.
async fn execute_midway(database: &Database, username: &str, posts: i64) -> TestResult {
    let entries: Vec<(i64, String)> = (0..posts)
        .map(|index| (100 + index, format!("midway post {index}")))
        .collect();
    let borrowed: Vec<(i64, &str)> = entries
        .iter()
        .map(|(id, body)| (*id, body.as_str()))
        .collect();
    let provider = ScriptedProvider::new([(
        username,
        vec![Step::Page(page(&borrowed, Some(50))), Step::Unavailable],
    )]);
    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );
    Ok(())
}

async fn settle(
    database: &Database,
    owner: Uuid,
    operation: Uuid,
    run_id: Uuid,
    result_id: Uuid,
    (selected, included, omitted): (u16, u16, u16),
) -> TestResult {
    let digest: (String,) =
        sqlx::query_as("select sha256 from channel_digests.digest_manifests where run_id = $1")
            .bind(run_id)
            .fetch_one(database.pool())
            .await?;
    let fact = json!({
        "owner": format!("user:{owner}"),
        "operation_id": operation,
        "digest_run_id": run_id,
        "manifest_digest": {"algorithm": "sha256", "hex": digest.0},
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
    });
    DigestCoordinator::new(database.pool().clone())
        .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&fact)?)
        .await?;
    Ok(())
}

fn page(posts: &[(i64, &str)], next: Option<i64>) -> ProviderPage {
    ProviderPage {
        posts: posts
            .iter()
            .map(|(message_id, body)| ProviderPost {
                message_id: *message_id,
                body: (*body).to_owned(),
                published_at: POST_AT.to_owned(),
                deleted: false,
            })
            .collect(),
        next_before_message_id: next,
    }
}

/// A page whose posts carry their own publication times.
fn page_with_dates(posts: &[(i64, &str, &str)], next: Option<i64>) -> ProviderPage {
    ProviderPage {
        posts: posts
            .iter()
            .map(|(message_id, body, published_at)| ProviderPost {
                message_id: *message_id,
                body: (*body).to_owned(),
                published_at: (*published_at).to_owned(),
                deleted: false,
            })
            .collect(),
        next_before_message_id: next,
    }
}

/// Pages of posts that all predate the window, each with a cursor: a history with no end in sight.
fn older_pages(first_id: i64) -> Vec<Step> {
    (0..DEEP_HISTORY_PAGES)
        .map(|index| {
            let id = first_id - i64::try_from(index).unwrap_or(0);
            Step::Page(page_with_dates(
                &[(id, "ancient post", OLDER_THAN_WINDOW)],
                Some(id),
            ))
        })
        .collect()
}

/// The state of the acquisition checkpoint a run left behind.
async fn acquisition_state(pool: &sqlx::PgPool, run_id: Uuid) -> Result<String, sqlx::Error> {
    let row: (String,) = sqlx::query_as(
        "select checkpoint->>'state' from channel_digests.leases
         where resource_kind like 'acquisition:%' and resource_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
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

async fn incomplete_flag(pool: &sqlx::PgPool, run_id: Uuid) -> Result<bool, sqlx::Error> {
    let row: (bool,) = sqlx::query_as(
        "select acquisition_incomplete from channel_digests.digest_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

async fn result_outcome(pool: &sqlx::PgPool, run_id: Uuid) -> Result<String, sqlx::Error> {
    let row: (String,) =
        sqlx::query_as("select outcome from channel_digests.digest_results where run_id = $1")
            .bind(run_id)
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

/// Statuses other than the non-terminal `queued` and `running` progress reports.
fn terminal_statuses(reports: &[Value]) -> Vec<&str> {
    reports
        .iter()
        .filter_map(|report| report.get("status").and_then(Value::as_str))
        .filter(|status| !matches!(*status, "queued" | "running"))
        .collect()
}

fn typed(reports: &[Value], index: usize) -> Result<OperationReported, Box<dyn std::error::Error>> {
    let value = reports.get(index).ok_or("the expected report is absent")?;
    let report: OperationReported = serde_json::from_value(value.clone())?;
    report.validate()?;
    Ok(report)
}

fn warning_codes(report: &OperationReported) -> Vec<&str> {
    report
        .warnings
        .iter()
        .map(|warning| warning.code.as_str())
        .collect()
}

/// One scripted provider answer.
#[derive(Debug)]
enum Step {
    Page(ProviderPage),
    Unavailable,
}

/// A provider whose answers are scripted per channel, one step per page request.
#[derive(Debug)]
struct ScriptedProvider {
    scripts: Mutex<HashMap<String, VecDeque<Step>>>,
    calls: Arc<AtomicUsize>,
}

impl ScriptedProvider {
    fn new<const N: usize>(scripts: [(&str, Vec<Step>); N]) -> Self {
        Self {
            scripts: Mutex::new(
                scripts
                    .into_iter()
                    .map(|(username, steps)| (username.to_owned(), steps.into()))
                    .collect(),
            ),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The count of page requests served, readable after the provider moved into an executor.
    fn calls(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.calls)
    }
}

impl PublicChannelProvider for ScriptedProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        channel: &Self::Channel,
        _before_message_id: Option<i64>,
        _limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let step = self
            .scripts
            .lock()
            .map_err(|_| ProviderError::Unavailable)?
            .get_mut(channel)
            .and_then(VecDeque::pop_front);
        match step {
            Some(Step::Page(page)) => Ok(page),
            Some(Step::Unavailable) | None => Err(ProviderError::Unavailable),
        }
    }
}
