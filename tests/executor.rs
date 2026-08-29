//! End-to-end durable run execution with a synthetic provider.

use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_channel_digests::{
    CommandIntake, Database, ProviderError, ProviderPage, ProviderPost, PublicChannelProvider,
    PublicChannelUsername, RunExecutor, SubscriptionRepository,
};
use uuid::Uuid;

#[tokio::test]
async fn accepted_run_acquires_and_commits_one_recap_request()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
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
