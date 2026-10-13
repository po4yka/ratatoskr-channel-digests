//! End-to-end durable run execution with a synthetic provider.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatoskr_channel_digest_contracts::OutputLanguage;
use ratatoskr_channel_digests::{
    CommandIntake, Database, DigestCoordinator, OccurrenceRequest, ProviderError, ProviderPage,
    ProviderPost, PublicChannelProvider, PublicChannelUsername, RunExecutor,
    SubscriptionRepository,
};
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// The publication instant of message id 0: message `n` was published `n` minutes later, so ids
/// and times rise together exactly as a Telegram channel's do.
const FIRST_POST_AT: &str = "2026-08-28T10:00:00Z";

#[tokio::test]
async fn accepted_run_acquires_and_commits_one_recap_request()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset_state(database.pool()).await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "executor_channel", true, "2026-08-28T09:00:00Z")
        .await?;
    let run_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let command = serde_json::to_vec(&serde_json::json!({
        "operation_id": operation_id,
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("executor-{run_id}"),
        "window": {
            "start_at": "2026-08-28T10:00:00Z",
            "end_at": "2026-08-29T10:00:00Z"
        },
        "output_language": "ru",
        "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
    }))?;
    CommandIntake::new(database.pool().clone())
        .accept_run(Uuid::now_v7(), &command)
        .await?;
    let executor = RunExecutor::new(
        database.pool().clone(),
        FakeProvider(Mutex::new(Some(ProviderPage {
            posts: vec![ProviderPost {
                message_id: 17,
                body: "bounded synthetic post".to_owned(),
                published_at: "2026-08-29T09:00:00Z".to_owned(),
                deleted: false,
            }],
            next_before_message_id: None,
        }))),
    );
    assert!(executor.execute_one().await?);
    let durable: (String, i64, i64) = sqlx::query_as(
        "select r.state,
                (select count(*) from channel_digests.digest_manifests where run_id = r.run_id),
                (select count(*) from channel_digests.outbox_messages where subject = 'knowledge.channel_digest_recap.requested.v1' and semantic_key = r.run_id::text)
         from channel_digests.digest_runs r where r.run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(durable, ("waiting_recap".into(), 1, 1));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn flood_wait_keeps_the_run_restartable_without_a_manifest()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset_state(database.pool()).await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "waiting_channel", true, "2026-08-28T09:00:00Z")
        .await?;
    let run_id = Uuid::now_v7();
    let operation_id = Uuid::now_v7();
    let command = serde_json::to_vec(&serde_json::json!({
        "operation_id": operation_id,
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("waiting-{run_id}"),
        "window": {
            "start_at": "2026-08-28T10:00:00Z",
            "end_at": "2026-08-29T10:00:00Z"
        },
        "output_language": "ru",
        "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
    }))?;
    CommandIntake::new(database.pool().clone())
        .accept_run(Uuid::now_v7(), &command)
        .await?;

    let executor = RunExecutor::new(database.pool().clone(), WaitProvider);
    assert!(executor.execute_one().await?);
    let durable: (String, i64) = sqlx::query_as(
        "select r.state,
                (select count(*) from channel_digests.digest_manifests where run_id = r.run_id)
         from channel_digests.digest_runs r where r.run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(durable, ("acquiring".into(), 0));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn deferred_run_does_not_block_the_next_owner() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset_state(database.pool()).await?;
    let waiting_owner = Uuid::now_v7();
    let ready_owner = Uuid::now_v7();
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    subscriptions
        .set(
            waiting_owner,
            "waiting_channel",
            true,
            "2026-08-28T09:00:00Z",
        )
        .await?;
    subscriptions
        .set(ready_owner, "ready_channel", true, "2026-08-28T09:00:00Z")
        .await?;
    let waiting_run = accept_run(database.pool(), waiting_owner, "first").await?;
    let ready_run = accept_run(database.pool(), ready_owner, "second").await?;
    let executor = RunExecutor::new(database.pool().clone(), SelectiveProvider);

    assert!(executor.execute_one().await?);
    assert!(executor.execute_one().await?);
    let states: (String, String) = sqlx::query_as(
        "select
            (select state from channel_digests.digest_runs where run_id = $1),
            (select state from channel_digests.digest_runs where run_id = $2)",
    )
    .bind(waiting_run)
    .bind(ready_run)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(states, ("acquiring".into(), "waiting_recap".into()));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn scheduled_run_from_an_occurrence_executes_and_commits_a_manifest()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset_state(database.pool()).await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "scheduled_executor", true, "2026-08-19T10:00:00Z")
        .await?;
    let occurrence_operation = Uuid::now_v7();
    let occurrence = format!("schedule-occurrence:{}", Uuid::now_v7());
    let payload = serde_json::to_vec(&serde_json::json!({"occurrence_ref": occurrence}))?;
    DigestCoordinator::new(database.pool().clone())
        .accept_occurrence(&OccurrenceRequest {
            message_id: Uuid::now_v7(),
            payload: &payload,
            occurrence_key: &occurrence,
            previous_due_at: "2026-08-20T10:00:00Z",
            due_at: "2026-08-21T10:00:00Z",
            operation_id: occurrence_operation,
            owner_id: Uuid::now_v7(),
            output_language: OutputLanguage::Ru,
        })
        .await?;
    let executor = RunExecutor::new(
        database.pool().clone(),
        FakeProvider(Mutex::new(Some(ProviderPage {
            posts: vec![ProviderPost {
                message_id: 31,
                body: "scheduled synthetic post".to_owned(),
                published_at: "2026-08-21T09:00:00Z".to_owned(),
                deleted: false,
            }],
            next_before_message_id: None,
        }))),
    );

    assert!(
        executor.execute_one().await?,
        "a scheduled run must be selected and executed"
    );
    let durable: (Uuid, String, i64, Option<Uuid>) = sqlx::query_as(
        "select r.run_id, r.state,
                (select count(*) from channel_digests.digest_manifests where run_id = r.run_id),
                (select operation_id from channel_digests.outbox_messages
                  where subject = 'knowledge.channel_digest_recap.requested.v1'
                    and semantic_key = r.run_id::text)
         from channel_digests.digest_runs r where r.owner_id = $1 and r.trigger = 'scheduled'",
    )
    .bind(owner)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        (durable.1.as_str(), durable.2, durable.3),
        ("waiting_recap", 1, Some(occurrence_operation))
    );
    let request: (serde_json::Value,) = sqlx::query_as(
        "select payload from channel_digests.outbox_messages
         where subject = 'knowledge.channel_digest_recap.requested.v1' and semantic_key = $1",
    )
    .bind(durable.0.to_string())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        request.0.get("operation_id"),
        Some(&serde_json::json!(occurrence_operation))
    );
    database.close().await;
    Ok(())
}

async fn reset_state(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "truncate channel_digests.digest_results, channel_digests.digest_manifest_sources,
                  channel_digests.digest_manifests,
                  channel_digests.post_revisions, channel_digests.subscriptions,
                  channel_digests.channels, channel_digests.digest_runs,
                  channel_digests.inbox_messages, channel_digests.outbox_messages,
                  channel_digests.leases cascade",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn accept_run(
    pool: &sqlx::PgPool,
    owner: Uuid,
    key: &str,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let run_id = Uuid::now_v7();
    let command = serde_json::to_vec(&serde_json::json!({
        "operation_id": Uuid::now_v7(),
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("{key}-{run_id}"),
        "window": {
            "start_at": "2026-08-28T10:00:00Z",
            "end_at": "2026-08-29T10:00:00Z"
        },
        "output_language": "ru",
        "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
    }))?;
    CommandIntake::new(pool.clone())
        .accept_run(Uuid::now_v7(), &command)
        .await?;
    Ok(run_id)
}

#[derive(Debug)]
struct FakeProvider(Mutex<Option<ProviderPage>>);

impl PublicChannelProvider for FakeProvider {
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
struct WaitProvider;

impl PublicChannelProvider for WaitProvider {
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
        Err(ProviderError::FloodWait(Duration::from_mins(1)))
    }
}

#[derive(Debug)]
struct SelectiveProvider;

impl PublicChannelProvider for SelectiveProvider {
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
        if channel == "waiting_channel" {
            Err(ProviderError::FloodWait(Duration::from_mins(1)))
        } else {
            Ok(ProviderPage {
                posts: vec![ProviderPost {
                    message_id: 18,
                    body: "next owner remains runnable".to_owned(),
                    published_at: "2026-08-29T09:00:00Z".to_owned(),
                    deleted: false,
                }],
                next_before_message_id: None,
            })
        }
    }
}

#[tokio::test]
async fn a_window_with_101_posts_keeps_the_newest_100_and_marks_the_run_incomplete() -> TestResult {
    let outcome = run_over_posts(101, None).await?;

    assert_eq!(outcome.message_ids, (2..=101).collect::<Vec<i64>>());
    assert!(
        outcome.incomplete,
        "one dropped post is a digest that does not cover the window"
    );
    assert_eq!(outcome.state, "waiting_recap");
    Ok(())
}

#[tokio::test]
async fn a_window_with_250_posts_keeps_the_newest_100_in_ascending_order() -> TestResult {
    let outcome = run_over_posts(250, None).await?;

    assert_eq!(outcome.message_ids, (151..=250).collect::<Vec<i64>>());
    assert!(outcome.incomplete);
    Ok(())
}

#[tokio::test]
async fn a_window_with_exactly_100_posts_is_complete() -> TestResult {
    let outcome = run_over_posts(100, None).await?;

    assert_eq!(outcome.message_ids, (1..=100).collect::<Vec<i64>>());
    assert!(!outcome.incomplete, "the cap itself is not a truncation");
    Ok(())
}

#[tokio::test]
async fn a_smaller_configured_source_count_is_honoured() -> TestResult {
    let outcome = run_over_posts(25, Some(10)).await?;

    assert_eq!(outcome.message_ids, (16..=25).collect::<Vec<i64>>());
    assert!(outcome.incomplete);
    Ok(())
}

#[tokio::test]
async fn the_truncation_warning_carries_both_counts_and_no_content() -> TestResult {
    let logs = LogCapture::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish(),
    );

    run_over_posts(25, Some(10)).await?;

    let text = logs.text();
    let line = text
        .lines()
        .find(|line| line.contains("WARN") && line.contains("selected=10"))
        .ok_or("no WARN line carries the selected count")?;
    assert!(line.contains("eligible=25"), "the line is {line}");
    assert!(
        !text.contains("synthetic post"),
        "a log line must never carry post content"
    );
    Ok(())
}

/// What one run left behind after selection.
struct SelectionOutcome {
    message_ids: Vec<i64>,
    incomplete: bool,
    state: String,
}

/// Runs one fresh run over a channel that holds `count` in-window posts, optionally capped.
async fn run_over_posts(
    count: i64,
    cap: Option<usize>,
) -> Result<SelectionOutcome, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    reset_state(database.pool()).await?;
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "capped_channel", true, "2026-08-27T09:00:00Z")
        .await?;
    let run_id = accept_run(database.pool(), owner, "capped").await?;
    let mut executor = RunExecutor::new(database.pool().clone(), PagedProvider::new(count));
    if let Some(cap) = cap {
        executor = executor.with_source_count(cap);
    }
    assert!(executor.execute_one().await?);
    let (state, incomplete): (String, bool) = sqlx::query_as(
        "select state, acquisition_incomplete from channel_digests.digest_runs where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    let manifest: (String,) = sqlx::query_as(
        "select canonical_text from channel_digests.digest_manifests where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    let parsed: serde_json::Value = serde_json::from_str(&manifest.0)?;
    let message_ids = parsed
        .get("sources")
        .and_then(serde_json::Value::as_array)
        .ok_or("the manifest has no sources")?
        .iter()
        .map(|source| {
            source
                .get("message_id")
                .and_then(serde_json::Value::as_str)
                .ok_or("a source has no message id")?
                .parse::<i64>()
                .map_err(|_| "a message id is not a number")
        })
        .collect::<Result<Vec<i64>, _>>()?;
    database.close().await;
    Ok(SelectionOutcome {
        message_ids,
        incomplete,
        state,
    })
}

/// A channel whose posts are ids `1..=count`, served newest first by cursor like a real channel.
#[derive(Debug)]
struct PagedProvider(i64);

impl PagedProvider {
    fn new(count: i64) -> Self {
        Self(count)
    }
}

impl PublicChannelProvider for PagedProvider {
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
        before_message_id: Option<i64>,
        limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        let newest = before_message_id
            .map_or(self.0, |before| before - 1)
            .min(self.0);
        let first =
            (newest - i64::try_from(limit).map_err(|_| ProviderError::Unavailable)? + 1).max(1);
        let posts: Vec<ProviderPost> = (first..=newest)
            .rev()
            .map(|message_id| ProviderPost {
                message_id,
                body: format!("synthetic post {message_id}"),
                published_at: post_time(message_id),
                deleted: false,
            })
            .collect();
        let next_before_message_id = (posts.len() == limit)
            .then(|| posts.last().map(|post| post.message_id))
            .flatten();
        Ok(ProviderPage {
            posts,
            next_before_message_id,
        })
    }
}

/// The instant message `message_id` was published, a minute after the previous one.
fn post_time(message_id: i64) -> String {
    let base: jiff::Timestamp = FIRST_POST_AT.parse().unwrap_or(jiff::Timestamp::UNIX_EPOCH);
    base.checked_add(jiff::SignedDuration::from_mins(message_id))
        .map_or_else(|_| FIRST_POST_AT.to_owned(), |instant| instant.to_string())
}

/// A shared in-memory log sink.
#[derive(Debug, Clone, Default)]
struct LogCapture(Arc<Mutex<Vec<u8>>>);

impl LogCapture {
    fn text(&self) -> String {
        self.0
            .lock()
            .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
            .unwrap_or_default()
    }
}

impl std::io::Write for LogCapture {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("log sink poisoned"))?
            .write_all(buffer)?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogCapture {
    type Writer = LogCapture;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
