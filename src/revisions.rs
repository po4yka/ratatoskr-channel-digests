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

/// The message ids one provider page proves, as a half-open interval of channel-local ids.
///
/// A newest-first page answers for every message id below the cursor it was requested with, from
/// its own lowest id down; a short last page also answers for everything below that.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvedIdRange {
    /// Lowest id the page proves, inclusive. `i64::MIN` means down to the start of the channel.
    pub lowest: i64,
    /// First id above the proved range, exclusive. `None` means no upper limit.
    pub below: Option<i64>,
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

    /// Withdraws the stored messages a page proves are gone, and the ones it marks deleted.
    ///
    /// A stored, non-deleted message whose id lies inside `proved` and is not in `present` is
    /// withdrawn, and so is every stored, non-deleted message in `deleted` wherever it lies. All
    /// revisions of a withdrawn message get `deleted_at`, lose their body and get `minimized_at`
    /// in one statement; content is not kept for a deleted post. Returns the revisions changed.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class on any database rejection.
    pub async fn withdraw_missing(
        &self,
        channel_id: Uuid,
        proved: ProvedIdRange,
        (present, deleted): (&[i64], &[i64]),
    ) -> Result<u64, RevisionError> {
        let result = sqlx::query(
            "update channel_digests.post_revisions
             set deleted_at = now(), body = null, minimized_at = coalesce(minimized_at, now())
             where channel_id = $1 and deleted_at is null
               and ((provider_message_id >= $2
                     and ($3::bigint is null or provider_message_id < $3)
                     and provider_message_id <> all($4::bigint[]))
                    or provider_message_id = any($5::bigint[]))",
        )
        .bind(channel_id)
        .bind(proved.lowest)
        .bind(proved.below)
        .bind(present)
        .bind(deleted)
        .execute(&self.pool)
        .await
        .map_err(|_| RevisionError)?;
        Ok(result.rows_affected())
    }
}
