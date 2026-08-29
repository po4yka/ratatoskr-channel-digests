//! Deterministic digest-run state and lease persistence.

use uuid::Uuid;

/// Finite run trigger vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunTrigger {
    /// User-accepted trailing window.
    OnDemand,
    /// Platform-authoritative schedule occurrence.
    Scheduled,
}

impl RunTrigger {
    fn as_str(self) -> &'static str {
        match self {
            Self::OnDemand => "on_demand",
            Self::Scheduled => "scheduled",
        }
    }
}

/// Finite durable run state vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    /// Command is durable.
    Accepted,
    /// Provider acquisition is active.
    Acquiring,
    /// Immutable manifest awaits Knowledge.
    WaitingRecap,
    /// Full terminal success.
    Completed,
    /// Truthful terminal partial success.
    Partial,
    /// Safe terminal failure.
    Failed,
}

impl RunState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Acquiring => "acquiring",
            Self::WaitingRecap => "waiting_recap",
            Self::Completed => "completed",
            Self::Partial => "partial",
            Self::Failed => "failed",
        }
    }
}

/// Stable run projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestRun {
    /// Stable run identity.
    pub run_id: Uuid,
    /// Internal owner identity.
    pub owner_id: Uuid,
    /// Closed lower bound.
    pub window_start: String,
    /// Open upper bound.
    pub window_end: String,
}

/// Safe run repository failure.
#[derive(Debug, thiserror::Error)]
#[error("digest run storage is unavailable")]
pub struct RunError;

/// Typed deterministic run and lease repository.
#[derive(Debug, Clone)]
pub struct RunRepository {
    pool: sqlx::PgPool,
}

impl RunRepository {
    /// Creates a repository over the owned pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Creates or reuses one natural-key run.
    ///
    /// # Errors
    ///
    /// Returns a safe storage class on database failure.
    pub async fn create(
        &self,
        owner_id: Uuid,
        trigger: RunTrigger,
        idempotency_key: &str,
        start: &str,
        end: &str,
    ) -> Result<Uuid, RunError> {
        let row: (Uuid,) = sqlx::query_as(
            "select channel_digests.create_digest_run($1, $2, $3, $4, $5::timestamptz, $6::timestamptz)",
        )
        .bind(Uuid::now_v7())
        .bind(owner_id)
        .bind(trigger.as_str())
        .bind(idempotency_key)
        .bind(start)
        .bind(end)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| RunError)?;
        Ok(row.0)
    }

    /// Applies one expected-state transition without terminal regression.
    ///
    /// # Errors
    ///
    /// Returns a safe storage class on database failure.
    pub async fn transition(
        &self,
        run_id: Uuid,
        expected: RunState,
        target: RunState,
        failure_class: Option<&str>,
    ) -> Result<bool, RunError> {
        let row: (bool,) = sqlx::query_as("select channel_digests.transition_run($1, $2, $3, $4)")
            .bind(run_id)
            .bind(expected.as_str())
            .bind(target.as_str())
            .bind(failure_class)
            .fetch_one(&self.pool)
            .await
            .map_err(|_| RunError)?;
        Ok(row.0)
    }
}
