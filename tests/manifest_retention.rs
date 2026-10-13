//! A manifest of a settled run loses its text on the same cutoff as post bodies, keyed by the run
//! window, and keeps everything else (XR-021 CONTRACTS.md R6-02 rule 2).

use std::time::Duration;

use ratatoskr_channel_digests::{Database, RetentionCounter, RetentionSweeper};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const BODY_DAYS: u32 = 30;
const TEXT: &str = "{\"fixture\":true}";

/// What a minimized manifest must keep.
type Kept = (String, i32, i32, Uuid, Uuid, String);

#[tokio::test]
async fn a_settled_run_older_than_the_cutoff_loses_its_text_and_keeps_its_linkage() -> TestResult {
    let database = connect().await?;
    let now = fixed_now()?;
    let counter = RetentionCounter::new();
    let sweeper = RetentionSweeper::new(database.pool().clone(), BODY_DAYS, counter.clone());
    let mut seeded = Vec::new();
    for state in ["completed", "partial", "failed"] {
        let manifest = seed(&database, state, now, 40).await?;
        let before = kept(&database, manifest).await?;
        seeded.push((manifest, before));
    }
    let young = seed(&database, "completed", now, 10).await?;
    let waiting = seed(&database, "waiting_recap", now, 40).await?;

    let outcome = sweeper.sweep_once(now).await?;

    assert_eq!(outcome.manifests, 3);
    assert_eq!(counter.manifests(), 3);
    for (manifest, before) in seeded {
        assert_eq!(
            text_state(&database, manifest).await?,
            (true, true),
            "the text is gone and the instant is recorded"
        );
        assert_eq!(
            kept(&database, manifest).await?,
            before,
            "digest, counts, owner, run and creation time are unchanged"
        );
        let linked: (i64,) = sqlx::query_as(
            "select count(*) from channel_digests.digest_results r
             join channel_digests.digest_manifests m on m.manifest_id = r.manifest_id
             where m.manifest_id = $1",
        )
        .bind(manifest)
        .fetch_one(database.pool())
        .await?;
        assert_eq!(linked.0, 1, "the result still joins its manifest");
    }
    assert_eq!(
        text_state(&database, young).await?,
        (false, false),
        "a run whose window is younger than the cutoff keeps its text"
    );
    assert_eq!(
        text_state(&database, waiting).await?,
        (false, false),
        "a run that still waits for its recap keeps its text however old its window is"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn one_pass_minimizes_at_most_5000_manifests() -> TestResult {
    let database = connect().await?;
    let now = fixed_now()?;
    let window = now.checked_sub(jiff::SignedDuration::from_hours(24 * 41))?;
    sqlx::query(
        "with runs as (
             insert into channel_digests.digest_runs
                 (run_id, owner_id, operation_id, trigger, idempotency_key, window_start,
                  window_end, state)
             select gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'on_demand',
                    'batch-' || series, $1::timestamptz, $1::timestamptz + interval '1 day',
                    'completed'
             from generate_series(1, 5001) as series
             returning run_id, owner_id
         )
         insert into channel_digests.digest_manifests
             (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_text)
         select gen_random_uuid(), run_id, owner_id, repeat('ab', 32), 1, 1, $2 from runs",
    )
    .bind(window.to_string())
    .bind(TEXT)
    .execute(database.pool())
    .await?;
    let counter = RetentionCounter::new();
    let sweeper = RetentionSweeper::new(database.pool().clone(), BODY_DAYS, counter.clone());

    assert_eq!(sweeper.sweep_once(now).await?.manifests, 5_000);
    assert_eq!(sweeper.sweep_once(now).await?.manifests, 1);
    assert_eq!(sweeper.sweep_once(now).await?.manifests, 0);

    assert_eq!(counter.manifests(), 5_001);
    database.close().await;
    Ok(())
}

/// A fixed instant, so no test depends on the wall clock.
fn fixed_now() -> Result<jiff::Timestamp, jiff::Error> {
    "2026-06-01T00:00:00Z".parse()
}

async fn connect() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    sqlx::query(
        "truncate channel_digests.digest_results, channel_digests.digest_manifest_sources,
                  channel_digests.digest_manifests,
                  channel_digests.post_revisions, channel_digests.subscriptions,
                  channel_digests.channels, channel_digests.digest_runs,
                  channel_digests.inbox_messages, channel_digests.outbox_messages,
                  channel_digests.leases cascade",
    )
    .execute(database.pool())
    .await?;
    Ok(database)
}

/// Seeds a run in `state` whose window began `age_days` before `now`, with a manifest holding its
/// text and, for a settled run, the result that points at the manifest.
async fn seed(
    database: &Database,
    state: &str,
    now: jiff::Timestamp,
    age_days: i64,
) -> Result<Uuid, Box<dyn std::error::Error>> {
    let window = now.checked_sub(jiff::SignedDuration::from_hours(24 * age_days))?;
    let (run, manifest, owner) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    sqlx::query(
        "insert into channel_digests.digest_runs
            (run_id, owner_id, operation_id, trigger, idempotency_key, window_start, window_end,
             state)
         values ($1, $2, $3, 'on_demand', $4, $5::timestamptz,
                 $5::timestamptz + interval '1 day', $6)",
    )
    .bind(run)
    .bind(owner)
    .bind(Uuid::now_v7())
    .bind(format!("retention-{run}"))
    .bind(window.to_string())
    .bind(state)
    .execute(database.pool())
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_manifests
            (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_text)
         values ($1, $2, $3, $4, 2, 1, $5)",
    )
    .bind(manifest)
    .bind(run)
    .bind(owner)
    .bind("cd".repeat(32))
    .bind(TEXT)
    .execute(database.pool())
    .await?;
    if matches!(state, "completed" | "partial" | "failed") {
        let failed = state == "failed";
        sqlx::query(
            "insert into channel_digests.digest_results
                (result_id, run_id, manifest_id, owner_id, outcome, recap_id, result_digest_hex,
                 safe_failure_class)
             values ($1, $2, $3, $4, $5, $6, $7, $8)",
        )
        .bind(Uuid::now_v7())
        .bind(run)
        .bind(manifest)
        .bind(owner)
        .bind(state)
        .bind((!failed).then(Uuid::now_v7))
        .bind((!failed).then(|| "ef".repeat(32)))
        .bind(failed.then_some("provider_unavailable"))
        .execute(database.pool())
        .await?;
    }
    Ok(manifest)
}

/// Everything a minimized manifest must keep, as stored.
async fn kept(database: &Database, manifest: Uuid) -> Result<Kept, sqlx::Error> {
    sqlx::query_as(
        "select sha256, source_count, channel_count, owner_id, run_id, created_at::text
         from channel_digests.digest_manifests where manifest_id = $1",
    )
    .bind(manifest)
    .fetch_one(database.pool())
    .await
}

/// Whether the text is gone and whether the minimization instant is recorded.
async fn text_state(database: &Database, manifest: Uuid) -> Result<(bool, bool), sqlx::Error> {
    sqlx::query_as(
        "select canonical_text is null, minimized_at is not null
         from channel_digests.digest_manifests where manifest_id = $1",
    )
    .bind(manifest)
    .fetch_one(database.pool())
    .await
}
