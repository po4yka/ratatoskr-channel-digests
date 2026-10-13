//! Immutable provider-observation acceptance.

use std::time::Duration;

use std::collections::VecDeque;
use std::sync::Mutex;

use ratatoskr_channel_digests::{
    CommandIntake, Database, ObservedRevision, ProviderError, ProviderPage, ProviderPost,
    PublicChannelProvider, PublicChannelUsername, RevisionRepository, RunExecutor,
    SubscriptionRepository,
};
use uuid::Uuid;

#[tokio::test]
async fn provider_edits_append_immutable_revisions_without_leakage()
-> Result<(), Box<dyn std::error::Error>> {
    let database = connect().await?;
    let channel_id = Uuid::now_v7();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel_id)
        .bind(format!("revision_{}", channel_id.simple()))
        .execute(database.pool())
        .await?;
    let marker = format!("private-body-{}", Uuid::now_v7());
    let first = append(database.pool(), channel_id, 42, "00", &marker).await?;
    let duplicate = append(database.pool(), channel_id, 42, "00", &marker).await?;
    let edited = append(database.pool(), channel_id, 42, "11", "edited body").await?;
    assert_eq!(first, duplicate);
    assert_ne!(first, edited);

    let rows: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.post_revisions where channel_id = $1 and provider_message_id = 42",
    )
    .bind(channel_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(rows.0, 2);
    let leaked: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.outbox_messages where payload::text like $1",
    )
    .bind(format!("%{marker}%"))
    .fetch_one(database.pool())
    .await?;
    assert_eq!(leaked.0, 0);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn a_withdrawn_revision_is_never_selected_for_a_manifest()
-> Result<(), Box<dyn std::error::Error>> {
    let database = connect().await?;
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
    let owner = Uuid::now_v7();
    SubscriptionRepository::new(database.pool().clone())
        .set(owner, "selection_filter", true, "2026-08-27T09:00:00Z")
        .await?;
    let (channel_id,): (Uuid,) =
        sqlx::query_as("select channel_id from channel_digests.channels where username = $1")
            .bind("selection_filter")
            .fetch_one(database.pool())
            .await?;
    RevisionRepository::new(database.pool().clone())
        .append(&ObservedRevision {
            channel_id,
            provider_message_id: 5,
            body: "withdrawn but still holding its body",
            canonical_link: "https://t.me/selection_filter/5",
            published_at: "2026-08-28T12:00:00Z",
            observed_at: "2026-08-28T12:01:00Z",
        })
        .await?;
    sqlx::query(
        "update channel_digests.post_revisions set deleted_at = now() where provider_message_id = 5",
    )
    .execute(database.pool())
    .await?;
    let run_id = Uuid::now_v7();
    let command = serde_json::to_vec(&serde_json::json!({
        "operation_id": Uuid::now_v7(),
        "owner": format!("user:{owner}"),
        "digest_run_id": run_id,
        "idempotency_key": format!("selection-{run_id}"),
        "window": {"start_at": "2026-08-28T10:00:00Z", "end_at": "2026-08-29T10:00:00Z"},
        "output_language": "ru",
        "trigger": {"kind": "on_demand", "accepted_at": "2026-08-29T10:00:00Z"}
    }))?;
    CommandIntake::new(database.pool().clone())
        .accept_run(Uuid::now_v7(), &command)
        .await?;
    // One full page proves only ids from 100 up, so message 5 stays outside what the walk proves
    // and only the selection itself can keep it out of the manifest.
    let provider = TwoPages(Mutex::new(VecDeque::from([Some(ProviderPage {
        posts: vec![ProviderPost {
            message_id: 100,
            body: "a live post".to_owned(),
            published_at: "2026-08-28T13:00:00Z".to_owned(),
            deleted: false,
        }],
        next_before_message_id: Some(100),
    })])));

    assert!(
        RunExecutor::new(database.pool().clone(), provider)
            .execute_one()
            .await?
    );

    let manifest: (String,) = sqlx::query_as(
        "select canonical_text from channel_digests.digest_manifests where run_id = $1",
    )
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    let parsed: serde_json::Value = serde_json::from_str(&manifest.0)?;
    let message_ids: Vec<&str> = parsed
        .get("sources")
        .and_then(serde_json::Value::as_array)
        .ok_or("the manifest has no sources")?
        .iter()
        .filter_map(|source| source.get("message_id").and_then(serde_json::Value::as_str))
        .collect();
    assert_eq!(message_ids, ["100"]);
    database.close().await;
    Ok(())
}

/// Serves the queued pages in order and fails once they run out.
#[derive(Debug)]
struct TwoPages(Mutex<VecDeque<Option<ProviderPage>>>);

impl PublicChannelProvider for TwoPages {
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
            .pop_front()
            .flatten()
            .ok_or(ProviderError::Unavailable)
    }
}

async fn connect() -> Result<Database, Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    Ok(database)
}

async fn append(
    pool: &sqlx::PgPool,
    channel_id: Uuid,
    message_id: i64,
    digest_pair: &str,
    body: &str,
) -> Result<Uuid, sqlx::Error> {
    let digest = digest_pair.repeat(32);
    let row: (Uuid,) = sqlx::query_as(
        "select channel_digests.append_revision($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz)",
    )
    .bind(Uuid::now_v7())
    .bind(channel_id)
    .bind(message_id)
    .bind(digest)
    .bind(body)
    .bind("https://t.me/example/42")
    .bind("2026-08-20T10:00:00Z")
    .bind("2026-08-20T10:01:00Z")
    .fetch_one(pool)
    .await?;
    Ok(row.0)
}
