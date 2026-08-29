//! Bounded pagination and immutable-edit recovery acceptance.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use ratatoskr_channel_digests::{
    AcquisitionEngine, AcquisitionRequest, Database, ProviderError, ProviderPage, ProviderPost,
    PublicChannelProvider, PublicChannelUsername, RevisionRepository,
};
use uuid::Uuid;

#[tokio::test]
async fn pagination_edits_partial_failures_and_restart_are_bounded()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let channel_id = Uuid::now_v7();
    let suffix: String = channel_id
        .simple()
        .to_string()
        .chars()
        .rev()
        .take(20)
        .collect();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel_id)
        .bind(format!("acquire_{suffix}"))
        .execute(database.pool())
        .await?;
    let provider = FakeProvider::new([
        page([(9, "old"), (8, "duplicate")], Some(8)),
        page([(9, "edited"), (8, "duplicate")], None),
    ]);
    let engine = AcquisitionEngine::new(
        provider,
        RevisionRepository::new(database.pool().clone()),
        Duration::from_millis(100),
    );
    let report = engine
        .execute(&AcquisitionRequest {
            run_id: Uuid::now_v7(),
            channel_id,
            username: "example_channel",
            window_start: "2026-08-20T00:00:00Z",
            window_end: "2026-08-22T00:00:00Z",
            max_pages: 3,
            page_size: 2,
        })
        .await?;
    assert_eq!(report.pages, 2);
    assert_eq!(
        report.revisions, 3,
        "duplicate converges while edit appends"
    );
    let count: (i64,) =
        sqlx::query_as("select count(*) from channel_digests.post_revisions where channel_id = $1")
            .bind(channel_id)
            .fetch_one(database.pool())
            .await?;
    assert_eq!(count.0, 3);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn restart_resumes_the_persisted_channel_cursor() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let run_id = Uuid::now_v7();
    let channel_id = Uuid::now_v7();
    let suffix: String = channel_id.simple().to_string().chars().take(20).collect();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel_id)
        .bind(format!("resume_{suffix}"))
        .execute(database.pool())
        .await?;

    let first = FakeProvider::new([page([(9, "first")], Some(8))]);
    let engine = AcquisitionEngine::new(
        first,
        RevisionRepository::new(database.pool().clone()),
        Duration::from_millis(100),
    );
    let request = AcquisitionRequest {
        run_id,
        channel_id,
        username: "example_channel",
        window_start: "2026-08-20T00:00:00Z",
        window_end: "2026-08-22T00:00:00Z",
        max_pages: 3,
        page_size: 1,
    };
    let first_report = engine.execute(&request).await?;
    assert!(first_report.partial);

    let resumed = FakeProvider::new([page([(7, "resumed")], None)]);
    let calls = Arc::clone(&resumed.calls);
    let engine = AcquisitionEngine::new(
        resumed,
        RevisionRepository::new(database.pool().clone()),
        Duration::from_millis(100),
    );
    engine.execute(&request).await?;
    assert_eq!(*calls.lock().expect("calls lock"), vec![Some(8)]);
    database.close().await;
    Ok(())
}

#[tokio::test]
async fn flood_wait_is_persisted_and_blocks_early_retry() -> Result<(), Box<dyn std::error::Error>>
{
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let run_id = Uuid::now_v7();
    let channel_id = Uuid::now_v7();
    let suffix: String = channel_id.simple().to_string().chars().take(20).collect();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel_id)
        .bind(format!("wait_{suffix}"))
        .execute(database.pool())
        .await?;
    let request = AcquisitionRequest {
        run_id,
        channel_id,
        username: "example_channel",
        window_start: "2026-08-20T00:00:00Z",
        window_end: "2026-08-22T00:00:00Z",
        max_pages: 3,
        page_size: 1,
    };

    let calls = Arc::new(Mutex::new(0_usize));
    let engine = AcquisitionEngine::new(
        FloodProvider {
            calls: Arc::clone(&calls),
            wait: true,
        },
        RevisionRepository::new(database.pool().clone()),
        Duration::from_millis(100),
    );
    assert!(engine.execute(&request).await.is_err());
    let persisted: (String, bool) = sqlx::query_as(
        "select checkpoint->>'state', expires_at > now() + interval '50 seconds' from channel_digests.leases where resource_kind = $1 and resource_id = $2",
    )
    .bind(format!("acquisition:{channel_id}"))
    .bind(run_id)
    .fetch_one(database.pool())
    .await?;
    assert_eq!(persisted, ("flood_wait".to_owned(), true));

    let engine = AcquisitionEngine::new(
        FloodProvider {
            calls: Arc::clone(&calls),
            wait: false,
        },
        RevisionRepository::new(database.pool().clone()),
        Duration::from_millis(100),
    );
    assert!(engine.execute(&request).await.is_err());
    assert_eq!(*calls.lock().expect("calls lock"), 1);
    database.close().await;
    Ok(())
}

fn page<const N: usize>(items: [(i64, &'static str); N], next: Option<i64>) -> ProviderPage {
    ProviderPage {
        posts: items
            .into_iter()
            .map(|(message_id, body)| ProviderPost {
                message_id,
                body: body.to_owned(),
                published_at: "2026-08-21T10:00:00Z".to_owned(),
                deleted: false,
            })
            .collect(),
        next_before_message_id: next,
    }
}

#[derive(Debug)]
struct FakeProvider {
    pages: Mutex<VecDeque<ProviderPage>>,
    calls: Arc<Mutex<Vec<Option<i64>>>>,
}

impl FakeProvider {
    fn new<const N: usize>(pages: [ProviderPage; N]) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl PublicChannelProvider for FakeProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<String, ProviderError> {
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        _channel: &String,
        before: Option<i64>,
        _limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        self.calls
            .lock()
            .map_err(|_| ProviderError::Unavailable)?
            .push(before);
        self.pages
            .lock()
            .map_err(|_| ProviderError::Unavailable)?
            .pop_front()
            .ok_or(ProviderError::Unavailable)
    }
}

#[derive(Debug)]
struct FloodProvider {
    calls: Arc<Mutex<usize>>,
    wait: bool,
}

impl PublicChannelProvider for FloodProvider {
    type Channel = String;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<String, ProviderError> {
        Ok(username.as_str().to_owned())
    }

    async fn fetch_public_posts(
        &self,
        _channel: &String,
        _before: Option<i64>,
        _limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        *self.calls.lock().map_err(|_| ProviderError::Unavailable)? += 1;
        if self.wait {
            Err(ProviderError::FloodWait(Duration::from_mins(1)))
        } else {
            Ok(page([(7, "too early")], None))
        }
    }
}
