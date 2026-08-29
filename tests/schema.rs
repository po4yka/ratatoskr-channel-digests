//! Current-schema acceptance against disposable `PostgreSQL`.

use std::time::Duration;

use ratatoskr_channel_digests::Database;

#[tokio::test]
async fn owned_schema_is_complete_idempotent_and_isolated() -> Result<(), Box<dyn std::error::Error>>
{
    let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
    let database = Database::connect(&url, 3, Duration::from_secs(2)).await?;
    database.apply_schema().await?;
    database.apply_schema().await?;

    let rows: Vec<(String,)> = sqlx::query_as(
        "select table_name::text from information_schema.tables \
         where table_schema = 'channel_digests' order by table_name",
    )
    .fetch_all(database.pool())
    .await?;
    let names: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(
        names,
        [
            "channels",
            "digest_manifests",
            "digest_results",
            "digest_runs",
            "inbox_messages",
            "leases",
            "outbox_messages",
            "post_revisions",
            "provider_status",
            "subscriptions",
        ]
    );

    let foreign_keys: (i64,) = sqlx::query_as(
        "select count(*) from pg_constraint c \
         join pg_class t on t.oid = c.conrelid \
         join pg_namespace n on n.oid = t.relnamespace \
         join pg_class target on target.oid = c.confrelid \
         join pg_namespace target_n on target_n.oid = target.relnamespace \
         where c.contype = 'f' and n.nspname = 'channel_digests' \
         and target_n.nspname <> 'channel_digests'",
    )
    .fetch_one(database.pool())
    .await?;
    assert_eq!(
        foreign_keys.0, 0,
        "owned schema must not reference another bounded context"
    );

    database.close().await;
    Ok(())
}
