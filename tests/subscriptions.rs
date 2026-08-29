//! Owner-scoped subscription transition acceptance.

use std::time::Duration;

use ratatoskr_channel_digests::Database;
use uuid::Uuid;

#[tokio::test]
async fn subscriptions_are_owner_scoped_idempotent_and_limited()
-> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    let owner = Uuid::now_v7();
    let other_owner = Uuid::now_v7();
    let activated_at = "2026-08-20T10:00:00Z";

    let first = set_subscription(
        database.pool(),
        owner,
        "Example_Channel",
        true,
        activated_at,
    )
    .await?;
    let replay = set_subscription(
        database.pool(),
        owner,
        "example_channel",
        true,
        "2026-08-21T10:00:00Z",
    )
    .await?;
    assert_eq!(
        first, replay,
        "enable replay must preserve identity and first activation"
    );
    assert_eq!(first.1, activated_at);

    let foreign: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.subscriptions s join channel_digests.channels c using (channel_id) where s.owner_id = $1 and c.username = $2",
    )
    .bind(other_owner)
    .bind("example_channel")
    .fetch_one(database.pool())
    .await?;
    assert_eq!(foreign.0, 0);

    let disabled = set_subscription(
        database.pool(),
        owner,
        "example_channel",
        false,
        "2026-08-22T10:00:00Z",
    )
    .await?;
    assert!(!disabled.2);
    let enabled = set_subscription(
        database.pool(),
        owner,
        "example_channel",
        true,
        "2026-08-23T10:00:00Z",
    )
    .await?;
    assert!(enabled.2);
    assert_eq!(enabled.1, activated_at);

    for index in 1..20 {
        let username = format!("channel_{index:02}");
        set_subscription(database.pool(), owner, &username, true, activated_at).await?;
    }
    let error = set_subscription(database.pool(), owner, "channel_20", true, activated_at)
        .await
        .expect_err("the twenty-first active subscription must be refused");
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some("P0001")
    );

    database.close().await;
    Ok(())
}

async fn set_subscription(
    pool: &sqlx::PgPool,
    owner: Uuid,
    username: &str,
    enabled: bool,
    activated_at: &str,
) -> Result<(Uuid, String, bool), sqlx::Error> {
    sqlx::query_as(
        "select subscription_id, to_char(first_activated_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), enabled from channel_digests.set_subscription($1, $2, $3, $4, $5, $6::timestamptz)",
    )
    .bind(Uuid::now_v7())
    .bind(Uuid::now_v7())
    .bind(owner)
    .bind(username)
    .bind(enabled)
    .bind(activated_at)
    .fetch_one(pool)
    .await
}
