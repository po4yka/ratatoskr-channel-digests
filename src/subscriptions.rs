//! Owner-scoped public-channel subscription persistence.

use uuid::Uuid;

/// Minimized owner-authorized subscription projection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// Stable subscription identity.
    pub subscription_id: Uuid,
    /// Canonical lowercase public username.
    pub channel_username: String,
    /// First activation instant retained across state replay.
    pub first_activated_at: String,
    /// Current desired state.
    pub enabled: bool,
}

/// Safe subscription mutation/read failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubscriptionError {
    /// The owner already has twenty active subscriptions.
    #[error("active subscription limit reached")]
    Limit,
    /// Storage rejected or could not execute the operation.
    #[error("subscription storage is unavailable")]
    Storage,
}

/// Typed subscription persistence over the service-owned schema.
#[derive(Debug, Clone)]
pub struct SubscriptionRepository {
    pool: sqlx::PgPool,
}

impl SubscriptionRepository {
    /// Creates an owner-scoped repository.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Converges a requested state while preserving first activation and the active limit.
    ///
    /// # Errors
    ///
    /// Returns [`SubscriptionError::Limit`] at twenty active subscriptions or a safe storage
    /// class for database failure.
    pub async fn set(
        &self,
        owner_id: Uuid,
        username: &str,
        enabled: bool,
        requested_at: &str,
    ) -> Result<Subscription, SubscriptionError> {
        let row: (Uuid, String, bool) = sqlx::query_as(
            "select subscription_id, to_char(first_activated_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), enabled from channel_digests.set_subscription($1, $2, $3, $4, $5, $6::timestamptz)",
        )
        .bind(Uuid::now_v7())
        .bind(Uuid::now_v7())
        .bind(owner_id)
        .bind(username)
        .bind(enabled)
        .bind(requested_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|error| map_error(&error))?;
        Ok(Subscription {
            subscription_id: row.0,
            channel_username: username.to_ascii_lowercase(),
            first_activated_at: row.1,
            enabled: row.2,
        })
    }

    /// Returns one owner-scoped projection; foreign and missing values both yield `None`.
    ///
    /// # Errors
    ///
    /// Returns a safe storage class for database failure.
    pub async fn get(
        &self,
        owner_id: Uuid,
        username: &str,
    ) -> Result<Option<Subscription>, SubscriptionError> {
        let row: Option<(Uuid, String, String, bool)> = sqlx::query_as(
            "select s.subscription_id, c.username, to_char(s.first_activated_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'), s.enabled from channel_digests.subscriptions s join channel_digests.channels c using (channel_id) where s.owner_id = $1 and c.username = lower($2)",
        )
        .bind(owner_id)
        .bind(username)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| SubscriptionError::Storage)?;
        Ok(row.map(|value| Subscription {
            subscription_id: value.0,
            channel_username: value.1,
            first_activated_at: value.2,
            enabled: value.3,
        }))
    }
}

fn map_error(error: &sqlx::Error) -> SubscriptionError {
    if error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .as_deref()
        == Some("P0001")
    {
        SubscriptionError::Limit
    } else {
        SubscriptionError::Storage
    }
}
