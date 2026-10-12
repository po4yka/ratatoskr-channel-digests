//! Stored post bodies are minimized on a timer while provenance stays (XR-021 CONTRACTS.md R5-07
//! rule 3).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use ratatoskr_channel_digests::{
    Database, RetentionCounter, RetentionSweeper, metrics_router, supervise_retention,
};
use tokio::sync::watch;
use tower::ServiceExt as _;
use tracing_subscriber::fmt::MakeWriter;
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const BODY_DAYS: u32 = 30;
const MARKER: &str = "unique-retention-payload";

#[tokio::test]
async fn a_sweep_nulls_only_old_bodies_and_keeps_provenance() -> TestResult {
    let (database, channel) = seeded().await?;
    let now = fixed_now()?;
    let old = insert(&database, channel, 1, now, 40).await?;
    let young = insert(&database, channel, 2, now, 10).await?;
    let counter = RetentionCounter::new();
    let sweeper = RetentionSweeper::new(database.pool().clone(), BODY_DAYS, counter.clone());

    assert_eq!(sweeper.sweep_once(now).await?, 1);

    let kept: (bool, String, bool, bool) = row(&database, old).await?;
    assert_eq!(
        kept,
        (true, "ab".repeat(32), true, true),
        "the old body is gone but its digest, publication time and minimization instant stay"
    );
    let untouched: (bool, String, bool, bool) = row(&database, young).await?;
    assert_eq!(
        untouched,
        (false, "ab".repeat(32), false, true),
        "a younger body is left alone"
    );
    assert_eq!(counter.get(), 1);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn one_pass_minimizes_at_most_5000_rows() -> TestResult {
    let (database, channel) = seeded().await?;
    let now = fixed_now()?;
    let published = now.checked_sub(jiff::SignedDuration::from_hours(24 * 40))?;
    sqlx::query(
        "insert into channel_digests.post_revisions
            (revision_id, channel_id, provider_message_id, content_sha256, body, published_at,
             observed_at, canonical_link)
         select gen_random_uuid(), $1, series, lpad(to_hex(series), 64, '0'), 'old body',
                $2::timestamptz, $2::timestamptz, 'https://t.me/retention_batch/' || series
         from generate_series(1, 5001) as series",
    )
    .bind(channel)
    .bind(published.to_string())
    .execute(database.pool())
    .await?;
    let counter = RetentionCounter::new();
    let sweeper = RetentionSweeper::new(database.pool().clone(), BODY_DAYS, counter.clone());

    assert_eq!(sweeper.sweep_once(now).await?, 5_000);
    assert_eq!(sweeper.sweep_once(now).await?, 1);
    assert_eq!(sweeper.sweep_once(now).await?, 0);

    assert_eq!(counter.get(), 5_001);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn the_supervisor_sweeps_on_its_own_timer_and_logs_only_a_count() -> TestResult {
    let logs = LogCapture::default();
    let _guard = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish(),
    );
    let (database, channel) = seeded().await?;
    let now = fixed_now()?;
    let old = insert(&database, channel, 1, now, 40).await?;
    let counter = RetentionCounter::new();
    let (drain_tx, drain_rx) = watch::channel(false);
    // No provider and no bus exist here: the sweep must not depend on either.
    let supervisor = tokio::spawn(supervise_retention(
        RetentionSweeper::new(database.pool().clone(), BODY_DAYS, counter.clone()),
        Duration::from_millis(50),
        drain_rx,
    ));

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while counter.get() == 0 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    assert_eq!(counter.get(), 1, "the timer must have swept the old body");
    assert!(
        row(&database, old).await?.0,
        "the old body is null after the timer swept"
    );
    let _sent = drain_tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), supervisor).await??;
    let text = logs.text();
    assert!(
        text.lines().any(|line| line.contains("count=1")),
        "the sweep logs the number of minimized bodies: {text}"
    );
    assert!(!text.contains(MARKER), "a log line must never carry a body");
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn the_counter_is_served_in_the_prometheus_text_format() -> TestResult {
    let counter = RetentionCounter::new();
    counter.add(7);
    let response = metrics_router(counter)
        .oneshot(Request::builder().uri("/metrics").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 4_096).await?;
    let text = String::from_utf8(bytes.to_vec())?;
    assert!(
        text.lines()
            .any(|line| line == "channel_digests_bodies_minimized_total 7"),
        "{text}"
    );
    Ok(())
}

/// A fixed instant, so no test depends on the wall clock. Rows are aged relative to it, and the
/// supervisor test runs later than it, so a row 40 days older is past the 30 day window either way.
fn fixed_now() -> Result<jiff::Timestamp, jiff::Error> {
    "2026-06-01T00:00:00Z".parse()
}

/// A database with one channel and nothing else.
async fn seeded() -> Result<(Database, Uuid), Box<dyn std::error::Error>> {
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
    let channel = Uuid::now_v7();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel)
        .bind("retention_batch")
        .execute(database.pool())
        .await?;
    Ok((database, channel))
}

/// Stores one revision published `age_days` before `now`, carrying the marker in its body.
async fn insert(
    database: &Database,
    channel: Uuid,
    message_id: i64,
    now: jiff::Timestamp,
    age_days: i64,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let published = now.checked_sub(jiff::SignedDuration::from_hours(24 * age_days))?;
    let revision = Uuid::now_v7();
    sqlx::query(
        "insert into channel_digests.post_revisions
            (revision_id, channel_id, provider_message_id, content_sha256, body, published_at,
             observed_at, canonical_link)
         values ($1, $2, $3, $4, $5, $6::timestamptz, $6::timestamptz, 'https://t.me/retention_batch/1')",
    )
    .bind(revision)
    .bind(channel)
    .bind(message_id)
    .bind("ab".repeat(32))
    .bind(MARKER)
    .bind(published.to_string())
    .execute(database.pool())
    .await?;
    Ok(revision)
}

/// Whether the body is gone, the digest, whether the instant was recorded, and publication stays.
async fn row(
    database: &Database,
    revision: Uuid,
) -> Result<(bool, String, bool, bool), sqlx::Error> {
    sqlx::query_as(
        "select body is null, content_sha256, minimized_at is not null, published_at is not null
         from channel_digests.post_revisions where revision_id = $1",
    )
    .bind(revision)
    .fetch_one(database.pool())
    .await
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
