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

    assert_digest_result_shape(&database).await?;

    database.close().await;
    Ok(())
}

async fn assert_digest_result_shape(database: &Database) -> Result<(), Box<dyn std::error::Error>> {
    let result_columns: Vec<(String, String, String)> = sqlx::query_as(
        "select column_name::text, data_type::text, is_nullable::text \
         from information_schema.columns \
         where table_schema = 'channel_digests' and table_name = 'digest_results' \
         order by ordinal_position",
    )
    .fetch_all(database.pool())
    .await?;
    assert_eq!(
        result_columns,
        [
            ("result_id".to_owned(), "uuid".to_owned(), "NO".to_owned()),
            ("run_id".to_owned(), "uuid".to_owned(), "NO".to_owned()),
            ("manifest_id".to_owned(), "uuid".to_owned(), "NO".to_owned(),),
            ("owner_id".to_owned(), "uuid".to_owned(), "NO".to_owned()),
            ("outcome".to_owned(), "text".to_owned(), "NO".to_owned()),
            ("recap_id".to_owned(), "uuid".to_owned(), "YES".to_owned()),
            (
                "result_digest_hex".to_owned(),
                "text".to_owned(),
                "YES".to_owned(),
            ),
            (
                "citation_count".to_owned(),
                "integer".to_owned(),
                "NO".to_owned(),
            ),
            (
                "safe_failure_class".to_owned(),
                "text".to_owned(),
                "YES".to_owned(),
            ),
            (
                "created_at".to_owned(),
                "timestamp with time zone".to_owned(),
                "NO".to_owned(),
            ),
        ],
        "digest results must retain only immutable linkage, never recap narrative"
    );

    let result_checks: Vec<(String,)> = sqlx::query_as(
        "select lower(pg_get_constraintdef(c.oid)) \
         from pg_constraint c \
         join pg_class t on t.oid = c.conrelid \
         join pg_namespace n on n.oid = t.relnamespace \
         where c.contype = 'c' and n.nspname = 'channel_digests' \
         and t.relname = 'digest_results'",
    )
    .fetch_all(database.pool())
    .await?;
    assert!(
        result_checks.iter().any(|(definition,)| {
            definition.contains("result_digest_hex")
                && definition.contains("[0-9a-f]")
                && (definition.contains("{64}")
                    || (definition.contains("length(result_digest_hex)")
                        && definition.contains("64")))
        }),
        "result digest must be canonical lowercase SHA-256 hex"
    );
    assert!(
        result_checks.iter().any(|(definition,)| {
            definition.contains("outcome")
                && definition.contains("completed")
                && definition.contains("partial")
                && definition.contains("failed")
                && definition.contains("recap_id")
                && definition.contains("result_digest_hex")
                && definition.contains("is not null")
                && definition.contains("is null")
        }),
        "completed/partial results require recap and digest linkage while failed results forbid both"
    );

    Ok(())
}
