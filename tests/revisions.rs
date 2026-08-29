//! Immutable provider-observation acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::Database;
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
