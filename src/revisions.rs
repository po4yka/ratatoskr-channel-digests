//! Immutable normalized post-revision persistence.

use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// One bounded normalized provider observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedRevision<'a> {
    /// Resolved owned channel identity.
    pub channel_id: Uuid,
    /// Provider message identity within that channel.
    pub provider_message_id: i64,
    /// Normalized UTF-8 body held only in owned storage.
    pub body: &'a str,
    /// Redirect-free public message link.
    pub canonical_link: &'a str,
    /// Provider publication instant.
    pub published_at: &'a str,
    /// Acquisition observation instant.
    pub observed_at: &'a str,
}

/// Safe immutable-revision failure.
#[derive(Debug, thiserror::Error)]
#[error("revision storage is unavailable")]
pub struct RevisionError;

/// Typed immutable revision repository.
#[derive(Debug, Clone)]
pub struct RevisionRepository {
    pool: sqlx::PgPool,
}

impl RevisionRepository {
    /// Creates a repository over the service-owned pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    pub(crate) fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Appends changed normalized bytes and converges duplicate observations.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class on any database rejection.
    pub async fn append(&self, observed: &ObservedRevision<'_>) -> Result<Uuid, RevisionError> {
        let digest = format!("{:x}", Sha256::digest(observed.body.as_bytes()));
        let row: (Uuid,) = sqlx::query_as(
            "select channel_digests.append_revision($1, $2, $3, $4, $5, $6, $7::timestamptz, $8::timestamptz)",
        )
        .bind(Uuid::now_v7())
        .bind(observed.channel_id)
        .bind(observed.provider_message_id)
        .bind(digest)
        .bind(observed.body)
        .bind(observed.canonical_link)
        .bind(observed.published_at)
        .bind(observed.observed_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| RevisionError)?;
        Ok(row.0)
    }
}
