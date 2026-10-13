//! A manifest records the messages it holds, in the transaction that commits it (XR-021
//! CONTRACTS.md R6-02 rule 1).

use std::time::Duration;

use ratatoskr_channel_digest_contracts::sha256_hex;
use ratatoskr_channel_digests::{
    CoordinatorError, Database, DigestCoordinator, IntakeOutcome, ManifestBuilder, ManifestSource,
};
use uuid::Uuid;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const WINDOW_START: &str = "2026-08-20T10:00:00Z";
const WINDOW_END: &str = "2026-08-21T10:00:00Z";

#[test]
fn the_builder_returns_each_message_once_whatever_its_revisions() -> TestResult {
    let (alpha, bravo) = (Uuid::now_v7(), Uuid::now_v7());
    let mut edited = source(17, "alpha", alpha);
    edited.revision_index = 2;
    edited.body = "edited".to_owned();
    edited.content_sha256 = sha256_hex(edited.body.as_bytes());
    let sources = [
        source(17, "alpha", alpha),
        edited,
        source(4, "bravo", bravo),
    ];

    let manifest = ManifestBuilder::build(
        Uuid::now_v7(),
        Uuid::now_v7(),
        Uuid::now_v7(),
        WINDOW_START,
        WINDOW_END,
        &sources,
    )?;

    let mut expected = vec![(alpha, 17), (bravo, 4)];
    expected.sort_unstable();
    assert_eq!(manifest.source_count, 3);
    assert_eq!(
        manifest.messages, expected,
        "two revisions of one message are one message"
    );
    Ok(())
}

#[tokio::test]
async fn the_commit_writes_the_messages_once_and_a_replay_writes_none() -> TestResult {
    let database = connect().await?;
    let owner = Uuid::now_v7();
    let run_id = create_run(database.pool(), owner).await?;
    let (alpha, bravo) = (Uuid::now_v7(), Uuid::now_v7());
    let manifest = built(owner, run_id, &[(alpha, 17), (alpha, 18), (bravo, 4)])?;
    let request = recap_request(&manifest)?;
    let coordinator = DigestCoordinator::new(database.pool().clone());

    assert_eq!(
        coordinator.commit_manifest(&manifest, &request).await?,
        IntakeOutcome::Applied
    );
    assert_eq!(
        coordinator.commit_manifest(&manifest, &request).await?,
        IntakeOutcome::Replayed
    );

    let mut stored = linked(&database, manifest.manifest_id).await?;
    stored.sort_unstable();
    let mut expected = vec![(alpha, 17), (alpha, 18), (bravo, 4)];
    expected.sort_unstable();
    assert_eq!(
        stored, expected,
        "exactly the messages the manifest holds, once"
    );
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_commit_that_fails_leaves_no_message_behind() -> TestResult {
    let database = connect().await?;
    let owner = Uuid::now_v7();
    let run_id = create_run(database.pool(), owner).await?;
    // A settled run cannot move to waiting_recap, so the commit fails after its inserts.
    sqlx::query("update channel_digests.digest_runs set state = 'failed' where run_id = $1")
        .bind(run_id)
        .execute(database.pool())
        .await?;
    let manifest = built(owner, run_id, &[(Uuid::now_v7(), 9)])?;
    let request = recap_request(&manifest)?;
    let coordinator = DigestCoordinator::new(database.pool().clone());

    let outcome = coordinator.commit_manifest(&manifest, &request).await;

    assert!(
        matches!(outcome, Err(CoordinatorError::Invalid)),
        "{outcome:?}"
    );
    assert!(
        linked(&database, manifest.manifest_id).await?.is_empty(),
        "the link rows roll back with the manifest row"
    );
    let manifests: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.digest_manifests where manifest_id = $1",
    )
    .bind(manifest.manifest_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(manifests.0, 0);
    database.close().await;
    Ok(())
}

fn built(
    owner: Uuid,
    run_id: Uuid,
    messages: &[(Uuid, i64)],
) -> Result<ratatoskr_channel_digests::CanonicalManifest, Box<dyn std::error::Error>> {
    let sources: Vec<ManifestSource> = messages
        .iter()
        .map(|(channel, message)| source(*message, "alpha", *channel))
        .collect();
    Ok(ManifestBuilder::build(
        Uuid::now_v7(),
        owner,
        run_id,
        WINDOW_START,
        WINDOW_END,
        &sources,
    )?)
}

fn source(message_id: i64, username: &str, channel_id: Uuid) -> ManifestSource {
    let body = format!("body {message_id}");
    ManifestSource {
        revision_id: Uuid::now_v7(),
        channel_id,
        channel_username: username.to_owned(),
        channel_display_name: None,
        message_id,
        content_sha256: sha256_hex(body.as_bytes()),
        published_at: "2026-08-20T12:00:00Z".to_owned(),
        canonical_link: format!("https://t.me/{username}/{message_id}"),
        body,
        revision_index: 1,
    }
}

fn recap_request(
    manifest: &ratatoskr_channel_digests::CanonicalManifest,
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&serde_json::json!({
        "operation_id": Uuid::now_v7(),
        "owner": format!("user:{}", manifest.owner_id),
        "digest_run_id": manifest.run_id,
        "window": {"start_at": WINDOW_START, "end_at": WINDOW_END},
        "output_language": "ru",
        "source_count": manifest.source_count,
        "channel_count": manifest.channel_count,
        "manifest_ref": format!("channel-digest-manifest:{}", manifest.manifest_id),
        "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))
}

async fn linked(database: &Database, manifest_id: Uuid) -> Result<Vec<(Uuid, i64)>, sqlx::Error> {
    sqlx::query_as(
        "select channel_id, provider_message_id from channel_digests.digest_manifest_sources
         where manifest_id = $1",
    )
    .bind(manifest_id)
    .fetch_all(database.pool())
    .await
}

async fn create_run(pool: &sqlx::PgPool, owner: Uuid) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) = sqlx::query_as(
        "select channel_digests.create_digest_run($1, $2, $3, 'on_demand', $4, $5::timestamptz, $6::timestamptz)",
    )
    .bind(Uuid::now_v7())
    .bind(owner)
    .bind(Uuid::now_v7())
    .bind(format!("sources-{}", Uuid::now_v7()))
    .bind(WINDOW_START)
    .bind(WINDOW_END)
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}

async fn connect() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    Ok(database)
}
