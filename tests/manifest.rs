//! Canonical manifest determinism acceptance.

use std::time::Duration;

use ratatoskr_channel_digest_contracts::{ChannelDigestManifest, sha256_hex};
use ratatoskr_channel_digests::{
    CommandIntake, Database, ManifestBuilder, ManifestError, ManifestSource, ObservedRevision,
    ProviderError, ProviderPage, PublicChannelProvider, PublicChannelUsername, RevisionRepository,
    RunExecutor, SubscriptionRepository,
};
use uuid::Uuid;

const WINDOW_START: &str = "2026-08-20T10:00:00Z";
const WINDOW_END: &str = "2026-08-21T10:00:00Z";

#[test]
fn canonical_manifest_is_stable_bounded_and_integral() -> Result<(), Box<dyn std::error::Error>> {
    let (manifest_id, owner, run_id) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let (bravo, alpha) = (Uuid::now_v7(), Uuid::now_v7());
    let first = source(1, "bravo", bravo);
    let second = source(2, "alpha", alpha);
    let a = ManifestBuilder::build(
        manifest_id,
        owner,
        run_id,
        WINDOW_START,
        WINDOW_END,
        &[first.clone(), second.clone()],
    )?;
    let b = ManifestBuilder::build(
        manifest_id,
        owner,
        run_id,
        WINDOW_START,
        WINDOW_END,
        &[second, first],
    )?;
    assert_eq!(a.text, b.text, "the same sources in another order");
    assert_eq!(a.sha256, b.sha256);
    assert_eq!(sha256_hex(a.text.as_bytes()), a.sha256);
    assert_eq!((a.source_count, a.channel_count), (2, 2));
    assert!(a.text.contains("https://t.me/alpha/2"));
    let decoded = ChannelDigestManifest::from_canonical_bytes(a.text.as_bytes())?;
    assert_eq!(decoded.sources.len(), 2);

    let too_many: Vec<ManifestSource> = (1..=101).map(|id| source(id, "alpha", alpha)).collect();
    assert!(matches!(
        ManifestBuilder::build(
            manifest_id,
            owner,
            run_id,
            WINDOW_START,
            WINDOW_END,
            &too_many
        ),
        Err(ManifestError::Limit)
    ));
    let mut oversized = source(7, "alpha", alpha);
    oversized.body = "x".repeat(16_385);
    oversized.content_sha256 = sha256_hex(oversized.body.as_bytes());
    assert!(matches!(
        ManifestBuilder::build(
            manifest_id,
            owner,
            run_id,
            WINDOW_START,
            WINDOW_END,
            &[oversized]
        ),
        Err(ManifestError::Limit)
    ));
    assert!(matches!(
        ManifestBuilder::build(manifest_id, owner, run_id, WINDOW_START, WINDOW_END, &[]),
        Err(ManifestError::Invalid)
    ));
    let mut mismatched = source(8, "alpha", alpha);
    mismatched.content_sha256 = "11".repeat(32);
    assert!(
        ManifestBuilder::build(
            manifest_id,
            owner,
            run_id,
            WINDOW_START,
            WINDOW_END,
            &[mismatched]
        )
        .is_err(),
        "a stored digest that does not match the stored body is corruption"
    );
    Ok(())
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

#[tokio::test]
async fn built_manifest_round_trips_through_the_contract_and_matches_the_stored_digest()
-> Result<(), Box<dyn std::error::Error>> {
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
    let owner = Uuid::now_v7();
    let (alpha, bravo) = seed_revisions(&database, owner).await?;
    let run_id = Uuid::now_v7();
    let command = serde_json::to_vec(&serde_json::json!({
        "operation_id": Uuid::now_v7(),
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("manifest-{run_id}"),
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

    let executor = RunExecutor::new(database.pool().clone(), EmptyProvider);
    assert!(executor.execute_one().await?);

    let (manifest_id, stored, stored_sha256): (Uuid, String, String) = sqlx::query_as(
        "select manifest_id, canonical_text, sha256 from channel_digests.digest_manifests where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    let manifest = ChannelDigestManifest::from_canonical_bytes(stored.as_bytes())
        .map_err(|error| format!("stored manifest is not the contract artifact: {error:?}"))?;
    manifest.validate()?;
    assert_eq!(sha256_hex(stored.as_bytes()), stored_sha256);
    assert_eq!(manifest.digest_run_id.as_uuid(), run_id);
    assert_eq!(manifest.owner.to_string(), format!("user:{owner}"));
    assert_eq!(
        manifest.manifest_ref.as_str(),
        format!("channel-digest-manifest:{manifest_id}")
    );
    assert_eq!(manifest.sources.len(), 2);
    let (first, second) = (&manifest.sources[0], &manifest.sources[1]);
    assert_eq!(
        first.channel_ref,
        format!("telegram-public-channel:{bravo}")
    );
    assert_eq!(first.channel_label, "manifest_bravo");
    assert_eq!((first.message_id.as_str(), first.revision), ("4", 1));
    assert_eq!(
        second.channel_ref,
        format!("telegram-public-channel:{alpha}")
    );
    assert_eq!(second.channel_label, "Alpha Daily");
    assert_eq!(second.content, "edited text");
    assert_eq!((second.message_id.as_str(), second.revision), ("17", 2));
    assert_eq!(
        second.public_link.as_deref(),
        Some("https://t.me/manifest_alpha/17")
    );
    assert_eq!(
        second.content_digest.hex.as_str(),
        sha256_hex(b"edited text")
    );
    database.close().await;
    Ok(())
}

/// Two subscribed channels; the first carries two revisions of one message.
async fn seed_revisions(
    database: &Database,
    owner: Uuid,
) -> Result<(Uuid, Uuid), Box<dyn std::error::Error>> {
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    subscriptions
        .set(owner, "manifest_alpha", true, "2026-08-28T09:00:00Z")
        .await?;
    subscriptions
        .set(owner, "manifest_bravo", true, "2026-08-28T09:00:00Z")
        .await?;
    sqlx::query("update channel_digests.channels set display_name = 'Alpha Daily' where username = 'manifest_alpha'")
        .execute(database.pool())
        .await?;
    let alpha = channel_id(database.pool(), "manifest_alpha").await?;
    let bravo = channel_id(database.pool(), "manifest_bravo").await?;
    let revisions = RevisionRepository::new(database.pool().clone());
    for (channel, message_id, body, observed_at) in [
        (alpha, 17, "first draft", "2026-08-29T08:30:00Z"),
        (alpha, 17, "edited text", "2026-08-29T09:30:00Z"),
        (bravo, 4, "bravo post", "2026-08-29T08:30:00Z"),
    ] {
        let username = if channel == alpha {
            "manifest_alpha"
        } else {
            "manifest_bravo"
        };
        let published_at = if channel == alpha {
            "2026-08-29T08:00:00Z"
        } else {
            "2026-08-29T07:00:00Z"
        };
        revisions
            .append(&ObservedRevision {
                channel_id: channel,
                provider_message_id: message_id,
                body,
                canonical_link: &format!("https://t.me/{username}/{message_id}"),
                published_at,
                observed_at,
            })
            .await?;
    }
    Ok((alpha, bravo))
}

async fn channel_id(pool: &sqlx::PgPool, username: &str) -> Result<Uuid, sqlx::Error> {
    let row: (Uuid,) =
        sqlx::query_as("select channel_id from channel_digests.channels where username = $1")
            .bind(username)
            .fetch_one(pool)
            .await?;
    Ok(row.0)
}

#[derive(Debug)]
struct EmptyProvider;

impl PublicChannelProvider for EmptyProvider {
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
        Ok(ProviderPage {
            posts: Vec::new(),
            next_before_message_id: None,
        })
    }
}
