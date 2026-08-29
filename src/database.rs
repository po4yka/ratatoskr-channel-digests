//! Finite `PostgreSQL` pool owned by the digest service.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;

/// Cloneable process database handle.
#[derive(Debug, Clone)]
pub struct Database {
    pool: sqlx::PgPool,
}

/// Safe database startup or schema failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DatabaseError {
    /// Connection or pool acquisition failed.
    #[error("channel digest storage is unavailable")]
    Unavailable,
    /// The current schema could not be applied.
    #[error("channel digest schema is unavailable")]
    Schema,
}

impl Database {
    /// Opens one finite lazy-free pool and verifies a connection.
    ///
    /// # Errors
    ///
    /// Returns a safe unavailable class without rendering the database URL.
    pub async fn connect(
        url: &str,
        max_connections: u32,
        acquire_timeout: Duration,
    ) -> Result<Self, DatabaseError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(acquire_timeout)
            .connect(url)
            .await
            .map_err(|_| DatabaseError::Unavailable)?;
        Ok(Self { pool })
    }

    /// Returns the process-owned pool for repository composition.
    #[must_use]
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Applies the one current schema definition.
    ///
    /// # Errors
    ///
    /// Returns a safe schema class for any SQL failure.
    pub async fn apply_schema(&self) -> Result<(), DatabaseError> {
        sqlx::raw_sql(include_str!("../schema.sql"))
            .execute(&self.pool)
            .await
            .map_err(|_| DatabaseError::Schema)?;
        Ok(())
    }

    /// Closes the finite pool.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
