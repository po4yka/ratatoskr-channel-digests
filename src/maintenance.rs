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

    /// Removes expired body payloads while retaining immutable provenance.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class.
    pub async fn minimize_before(&self, before: &str) -> Result<u64, MaintenanceError> {
        let result = sqlx::query(
            "update channel_digests.post_revisions set body = null, minimized_at = now() where published_at < $1::timestamptz and body is not null",
        )
        .bind(before)
        .execute(&self.pool)
        .await
        .map_err(|_| MaintenanceError)?;
        Ok(result.rows_affected())
    }
}
