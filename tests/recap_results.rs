//! Knowledge terminal-fact verification acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{
    Database, DigestCoordinator, IntakeOutcome, ManifestBuilder, ManifestSource,
};
use uuid::Uuid;

#[tokio::test]
async fn only_consistent_terminal_facts_settle_a_run() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    let run_id = create_run(database.pool(), owner).await?;
    let manifest_id = Uuid::now_v7();
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
            body: "synthetic".into(),
        }],
    )?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    let request = recap_request(owner, run_id, manifest_id, &manifest.sha256);
    coordinator
        .commit_manifest(
            manifest_id,
            owner,
            &manifest,
            Some(&serde_json::to_vec(&request)?),
        )
        .await
        .map_err(|error| format!("manifest commit failed: {error:?}"))?;

    let result_id = Uuid::now_v7();
    let mut invalid_completion = completion(owner, run_id, result_id, &manifest.sha256);
    invalid_completion.as_object_mut().ok_or("object")?.insert(
        "owner".into(),
        serde_json::json!(format!("user:{}", Uuid::now_v7())),
    );
    assert!(
        coordinator
            .settle_completion(Uuid::now_v7(), &serde_json::to_vec(&invalid_completion)?)
            .await
            .is_err()
    );
    let waiting: (String,) =
        sqlx::query_as("select state from channel_digests.digest_runs where run_id = $1")
            .bind(run_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(waiting.0, "waiting_recap");

    let valid = completion(owner, run_id, result_id, &manifest.sha256);
    let message_id = Uuid::now_v7();
    assert_eq!(
        coordinator
            .settle_completion(message_id, &serde_json::to_vec(&valid)?)
            .await
            .map_err(|error| format!("valid completion failed: {error:?}"))?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        coordinator
            .settle_completion(message_id, &serde_json::to_vec(&valid)?)
            .await?,
        IntakeOutcome::Replayed
    );
    let terminal: (String, i64) = sqlx::query_as(
        "select r.state, count(result_id) from channel_digests.digest_runs r left join channel_digests.digest_results d using (run_id) where r.run_id = $1 group by r.state",
    ).bind(run_id).fetch_one(database.pool()).await?;
    assert_eq!(terminal, ("completed".into(), 1));
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn consistent_failure_is_terminal_and_replay_safe() -> Result<(), Box<dyn std::error::Error>>
{
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    let run_id = create_run(database.pool(), owner).await?;
    let manifest_id = Uuid::now_v7();
    let manifest = ManifestBuilder::build(
        run_id,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        vec![ManifestSource {
            revision_id: Uuid::now_v7(),
            channel_username: "failure_channel".into(),
            message_id: 7,
            content_sha256: "33".repeat(32),
            published_at: "2026-08-20T12:00:00Z".into(),
            canonical_link: "https://t.me/failure_channel/7".into(),
            body: "synthetic failure source".into(),
        }],
    )?;
    let coordinator = DigestCoordinator::new(database.pool().clone());
    coordinator
        .commit_manifest(
            manifest_id,
            owner,
            &manifest,
            Some(&serde_json::to_vec(&recap_request(
                owner,
                run_id,
                manifest_id,
                &manifest.sha256,
            ))?),
        )
        .await?;
    let failure = serde_json::json!({
        "owner": format!("user:{owner}"),
        "operation_id": Uuid::now_v7(),
        "digest_run_id": run_id,
        "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
        "failure_code": "provider_timeout",
        "failed_at": "2026-08-21T10:02:00Z"
    });
    let message_id = Uuid::now_v7();
    assert_eq!(
        coordinator
            .settle_failure(message_id, &serde_json::to_vec(&failure)?)
            .await?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        coordinator
            .settle_failure(message_id, &serde_json::to_vec(&failure)?)
            .await?,
        IntakeOutcome::Replayed
    );
    let terminal: (String, String, i64) = sqlx::query_as(
        "select r.state, d.safe_failure_class, count(d.result_id)
         from channel_digests.digest_runs r join channel_digests.digest_results d using (run_id)
         where r.run_id = $1 group by r.state, d.safe_failure_class",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(terminal, ("failed".into(), "provider_timeout".into(), 1));
    database.close().await;
    Ok(())
}

fn recap_request(owner: Uuid, run: Uuid, manifest: Uuid, digest: &str) -> serde_json::Value {
    serde_json::json!({
        "operation_id": Uuid::now_v7(), "owner": format!("user:{owner}"), "digest_run_id": run,
        "window": {"start_at": "2026-08-20T10:00:00Z", "end_at": "2026-08-21T10:00:00Z"},
        "output_language": "ru", "source_count": 1, "channel_count": 1,
        "manifest_ref": format!("channel-digest-manifest:{manifest}"),
        "manifest_digest": {"algorithm": "sha256", "hex": digest},
        "analysis_family": "channel_digest_recap", "analysis_contract": "channel_digest_recap.v1"
    })
}

fn completion(owner: Uuid, run: Uuid, result: Uuid, digest: &str) -> serde_json::Value {
    serde_json::json!({
        "owner": format!("user:{owner}"), "operation_id": Uuid::now_v7(), "digest_run_id": run,
        "manifest_digest": {"algorithm": "sha256", "hex": digest},
        "analysis_ref": format!("analysis:{}", Uuid::now_v7()), "digest_result_id": result,
        "result_ref": format!("channel-digest-result:{result}"),
        "result_digest": {"algorithm": "sha256", "hex": "22".repeat(32)},
        "completed_at": "2026-08-21T10:01:00Z",
        "coverage": {"selected_count": 1, "included_count": 1, "omitted_count": 0, "channel_count": 1}
    })
}

async fn create_run(pool: &sqlx::PgPool, owner: Uuid) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as(
        "select channel_digests.create_digest_run($1, $2, 'on_demand', $3, '2026-08-20T10:00:00Z', '2026-08-21T10:00:00Z')",
    ).bind(Uuid::now_v7()).bind(owner).bind(format!("result-{}", Uuid::now_v7())).fetch_one(pool).await?;
    Ok(row.0)
}
