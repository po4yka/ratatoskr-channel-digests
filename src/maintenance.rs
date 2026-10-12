//! Privacy-preserving retention maintenance.

/// Safe maintenance failure.
#[derive(Debug, thiserror::Error)]
#[error("retention maintenance is unavailable")]
pub struct MaintenanceError;

/// Owned payload minimization operations.
#[derive(Debug, Clone)]
pub struct Maintenance {
    pool: sqlx::PgPool,
}

impl Maintenance {
    /// Creates maintenance over the owned pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Removes up to `limit` expired body payloads while retaining immutable provenance.
    ///
    /// The oldest publications go first, so a backlog drains over several calls. The digest,
    /// publication time and revision identity stay; only the body is dropped.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class.
    pub async fn minimize_before(&self, before: &str, limit: i64) -> Result<u64, MaintenanceError> {
        let result = sqlx::query(
            "update channel_digests.post_revisions set body = null, minimized_at = now()
             where revision_id in (
                 select revision_id from channel_digests.post_revisions
                 where published_at < $1::timestamptz and body is not null
                 order by published_at, revision_id
                 limit $2
                 for update skip locked
             )",
        )
        .bind(before)
        .bind(limit)
        .execute(&self.pool)
        .await
        .map_err(|_| MaintenanceError)?;
        Ok(result.rows_affected())
    }
}
