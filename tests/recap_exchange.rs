//! Manifest commit and body-free recap request acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{
    Database, DigestCoordinator, IntakeOutcome, ManifestBuilder, ManifestSource,
};
use uuid::Uuid;

#[tokio::test]
async fn non_empty_manifest_publishes_one_body_free_recap_request()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    let run_id = create_run(database.pool(), owner, "recap-key").await?;
    let manifest_id = Uuid::now_v7();
    let marker = format!("private-source-{}", Uuid::now_v7());
    let manifest = ManifestBuilder::build(
        run_id,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        vec![ManifestSource {
            revision_id: Uuid::now_v7(),
            channel_username: "example_channel".into(),
            message_id: 42,
            content_sha256: "11".repeat(32),
            published_at: "2026-08-20T12:00:00Z".into(),
            canonical_link: "https://t.me/example_channel/42".into(),
            body: marker.clone(),
        }],
    )?;
    let request = serde_json::to_vec(&serde_json::json!({
        "operation_id": Uuid::now_v7(),
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "window": {"start_at": "2026-08-20T10:00:00Z", "end_at": "2026-08-21T10:00:00Z"},
        "output_language": "ru",
        "source_count": 1,
        "channel_count": 1,
        "manifest_ref": format!("channel-digest-manifest:{manifest_id}"),
        "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    assert_eq!(
        coordinator
            .commit_manifest(manifest_id, owner, &manifest, Some(&request))
            .await?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        coordinator
            .commit_manifest(manifest_id, owner, &manifest, Some(&request))
            .await?,
        IntakeOutcome::Replayed
    );
    let outbox: (i64, i64) = sqlx::query_as(
        "select count(*), count(*) filter (where payload::text like $1) from channel_digests.outbox_messages where subject = 'knowledge.channel_digest_recap.requested.v1' and semantic_key = $2",
    )
    .bind(format!("%{marker}%"))
    .bind(run_id.to_string())
    .fetch_one(database.pool())
    .await?;
    assert_eq!(outbox, (1, 0));

    let empty_run = create_run(database.pool(), owner, "empty-key").await?;
    let empty = ManifestBuilder::build(
        empty_run,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        Vec::new(),
    )?;
    assert_eq!(
        coordinator
            .commit_manifest(Uuid::now_v7(), owner, &empty, None)
            .await?,
        IntakeOutcome::Applied
    );
    let state: (String,) =
        sqlx::query_as("select state from channel_digests.digest_runs where run_id = $1")
            .bind(empty_run)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(state.0, "completed");
    database.close().await;
    Ok(())
}

async fn create_run(pool: &sqlx::PgPool, owner: Uuid, key: &str) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as(
        "select channel_digests.create_digest_run($1, $2, 'on_demand', $3, '2026-08-20T10:00:00Z', '2026-08-21T10:00:00Z')",
    ).bind(Uuid::now_v7()).bind(owner).bind(key).fetch_one(pool).await?;
    Ok(row.0)
}
