//! Run deadline reaper: no run stays non-terminal forever.
//!
//! Only the executor, the provider and a Knowledge fact move a run, so a Knowledge outage, a lost
//! provider session or a recap request that never arrived would leave an on-demand operation
//! `running` until Platform's stale reaper. This reaper fails such a run, and reports the owning
//! operation, in one transaction (XR-021 CONTRACTS.md R2-05 a).

use std::time::Duration;

use ratatoskr_operation_contracts::OperationStatus;
use uuid::Uuid;

use crate::reports::{OperationReportRow, ReportError};

/// Stored class of a run the reaper failed.
const FAILURE_CLASS: &str = "deadline_exceeded";

/// Most runs failed by one tick, so a backlog never builds one unbounded transaction.
const BATCH: i64 = 100;

/// Safe reaper failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReaperError {
    /// Atomic storage operation failed.
    #[error("run deadline reaping is unavailable")]
    Storage,
}

impl From<ReportError> for ReaperError {
    fn from(_: ReportError) -> Self {
        Self::Storage
    }
}

/// Fails runs that outlived their deadline and reports the owning operations.
#[derive(Debug, Clone)]
pub struct Reaper {
    pool: sqlx::PgPool,
    deadline: Duration,
}

impl Reaper {
    /// Creates a reaper over the owned pool with the configured deadline.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, deadline: Duration) -> Self {
        Self { pool, deadline }
    }

    /// Fails every run last updated before `now` minus the deadline and returns how many.
    ///
    /// An on-demand run queues one retryable `failed` report in the same transaction; the outbox
    /// key `operation:<id>:failed` keeps a report that is already queued from being duplicated.
    /// A scheduled run owns no Platform operation and fails without a report.
    ///
    /// # Errors
    ///
    /// Returns a safe storage class.
    pub async fn reap_once(&self, now: jiff::Timestamp) -> Result<usize, ReaperError> {
        let mut transaction = self.pool.begin().await.map_err(|_| ReaperError::Storage)?;
        let reaped: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
            "update channel_digests.digest_runs set state = 'failed', safe_failure_class = $1, updated_at = now()
             where run_id in (
                 select run_id from channel_digests.digest_runs
                 where state in ('accepted', 'acquiring', 'waiting_recap')
                   and updated_at < $2::timestamptz - make_interval(secs => $3)
                 order by updated_at, run_id
                 limit $4
                 for update skip locked
             )
             returning owner_id, operation_id, trigger",
        )
        .bind(FAILURE_CLASS)
        .bind(now.to_string())
        .bind(self.deadline.as_secs_f64())
        .bind(BATCH)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| ReaperError::Storage)?;
        for (owner_id, operation_id, trigger) in &reaped {
            if trigger == "on_demand" {
                OperationReportRow::new(
                    *operation_id,
                    *owner_id,
                    OperationStatus::Failed,
                    "deadline",
                )?
                .with_error(
                    "channel_digest.run_deadline_exceeded",
                    "The digest did not finish before its deadline.",
                    true,
                )?
                .enqueue(&mut transaction)
                .await?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| ReaperError::Storage)?;
        Ok(reaped.len())
    }
}
