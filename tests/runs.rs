//! Deterministic run, window, lease, and terminal-state acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::Database;
use uuid::Uuid;

#[tokio::test]
async fn windows_replay_leases_and_terminal_state_are_deterministic()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    let first_id = Uuid::now_v7();
    let first = create_run(database.pool(), first_id, owner).await?;
    let replay = create_run(database.pool(), Uuid::now_v7(), owner).await?;
    assert_eq!(first, replay);
    assert_eq!(first, first_id);

    let on_demand: (String, String) = sqlx::query_as(
        "select to_char(start_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), to_char(end_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from channel_digests.normalized_window(false, '2026-08-20T10:00:00Z', null, '2026-08-21T10:00:00Z')",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        on_demand,
        ("2026-08-20T10:00:00Z".into(), "2026-08-21T10:00:00Z".into())
    );
    let capped: (String,) = sqlx::query_as(
        "select to_char(start_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') from channel_digests.normalized_window(true, '2026-08-01T10:00:00Z', '2026-08-02T10:00:00Z', '2026-08-21T10:00:00Z')",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(capped.0, "2026-08-14T10:00:00Z");

    let holder = Uuid::now_v7();
    let acquired: (bool,) = sqlx::query_as(
        "select channel_digests.acquire_lease('run', $1, $2, '2026-08-21T10:00:00Z', '2026-08-21T10:01:00Z')",
    )
    .bind(first)
    .bind(holder)
    .fetch_one(database.pool())
    .await?;
    assert!(acquired.0);
    let blocked: (bool,) = sqlx::query_as(
        "select channel_digests.acquire_lease('run', $1, $2, '2026-08-21T10:00:30Z', '2026-08-21T10:01:30Z')",
    )
    .bind(first)
    .bind(Uuid::now_v7())
    .fetch_one(database.pool())
    .await?;
    assert!(!blocked.0);

    assert!(transition(database.pool(), first, "accepted", "completed").await?);
    assert!(!transition(database.pool(), first, "accepted", "failed").await?);
    let state: (String,) =
        sqlx::query_as("select state from channel_digests.digest_runs where run_id = $1")
            .bind(first)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(state.0, "completed");
    database.close().await;
    Ok(())
}

async fn create_run(pool: &sqlx::PgPool, run_id: Uuid, owner: Uuid) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as(
        "select channel_digests.create_digest_run($1, $2, 'on_demand', 'same-key', '2026-08-20T10:00:00Z', '2026-08-21T10:00:00Z')",
    )
    .bind(run_id)
    .bind(owner)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

async fn transition(
    pool: &sqlx::PgPool,
    run_id: Uuid,
    expected: &str,
    target: &str,
) -> Result<bool, sqlx::Error> {
    let row: (bool,) = sqlx::query_as("select channel_digests.transition_run($1, $2, $3, null)")
        .bind(run_id)
        .bind(expected)
        .bind(target)
        .fetch_one(pool)
        .await?;
    Ok(row.0)
}
