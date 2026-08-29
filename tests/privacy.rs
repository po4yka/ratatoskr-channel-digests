//! Payload minimization and provenance-retention acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::{Database, Maintenance};
use uuid::Uuid;

#[tokio::test]
async fn retention_minimizes_payloads_without_losing_provenance()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let channel = Uuid::now_v7();
    let suffix: String = channel
        .simple()
        .to_string()
        .chars()
        .rev()
        .take(20)
        .collect();
    sqlx::query("insert into channel_digests.channels (channel_id, username) values ($1, $2)")
        .bind(channel)
        .bind(format!("privacy_{suffix}"))
        .execute(database.pool())
        .await?;
    let revision = Uuid::now_v7();
    sqlx::query(
        "insert into channel_digests.post_revisions (revision_id, channel_id, provider_message_id, content_sha256, body, published_at, observed_at, canonical_link) values ($1, $2, 42, $3, $4, '2026-08-01T10:00:00Z', '2026-08-01T10:01:00Z', 'https://t.me/privacy_channel/42')",
    ).bind(revision).bind(channel).bind("33".repeat(32)).bind("unique-private-payload").execute(database.pool()).await?;
    let maintenance = Maintenance::new(database.pool().clone());
    assert!(maintenance.minimize_before("2026-08-20T00:00:00Z").await? >= 1);
    let retained: (bool, String, bool) = sqlx::query_as(
        "select body is null, content_sha256, minimized_at is not null from channel_digests.post_revisions where revision_id = $1",
    ).bind(revision).fetch_one(database.pool()).await?;
    assert_eq!(retained, (true, "33".repeat(32), true));
    let forbidden_columns: (i64,) = sqlx::query_as(
        "select count(*) from information_schema.columns where table_schema = 'channel_digests' and (column_name like '%session%' or column_name like '%credential%')",
    ).fetch_one(database.pool()).await?;
    assert_eq!(forbidden_columns.0, 0);
    database.close().await;
    Ok(())
}
