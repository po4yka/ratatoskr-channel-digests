//! Periodic minimization of stored post bodies.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::routing::get;
use tokio::sync::watch;

use crate::{Maintenance, MaintenanceError};

/// Most rows one sweep minimizes.
const SWEEP_BATCH: i64 = 5_000;

/// Name of the counter on the operator plane.
const METRIC: &str = "channel_digests_bodies_minimized_total";

/// Process-wide count of minimized bodies.
#[derive(Debug, Clone, Default)]
pub struct RetentionCounter(Arc<AtomicU64>);

impl RetentionCounter {
    /// Creates a counter at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bodies minimized since the process started.
    #[must_use]
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    /// Adds minimized bodies.
    pub fn add(&self, count: u64) {
        self.0.fetch_add(count, Ordering::AcqRel);
    }

    /// The counter in the Prometheus text format.
    #[must_use]
    pub fn render(&self) -> String {
        format!(
            "# HELP {METRIC} Stored post bodies minimized by the retention sweep.\n# TYPE {METRIC} counter\n{METRIC} {}\n",
            self.get()
        )
    }
}

/// One retention policy over the owned pool.
#[derive(Debug, Clone)]
pub struct RetentionSweeper {
    maintenance: Maintenance,
    body_days: u32,
    counter: RetentionCounter,
}

impl RetentionSweeper {
    /// Creates a sweeper that minimizes bodies older than `body_days`.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, body_days: u32, counter: RetentionCounter) -> Self {
        Self {
            maintenance: Maintenance::new(pool),
            body_days,
            counter,
        }
    }

    /// Minimizes one batch of bodies published before `now` minus the retention.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class.
    pub async fn sweep_once(&self, now: jiff::Timestamp) -> Result<u64, MaintenanceError> {
        let cutoff = now
            .checked_sub(jiff::SignedDuration::from_hours(
                24 * i64::from(self.body_days),
            ))
            .map_err(|_| MaintenanceError)?;
        let minimized = self
            .maintenance
            .minimize_before(&cutoff.to_string(), SWEEP_BATCH)
            .await?;
        self.counter.add(minimized);
        Ok(minimized)
    }
}

/// Sweeps on a timer until the drain signal, whatever state the provider or bus is in.
///
/// It touches neither readiness nor the provider, so an outage elsewhere never stops it, and a
/// failed sweep is logged as a class and tried again on the next tick. It logs only counts.
pub async fn supervise_retention(
    sweeper: RetentionSweeper,
    interval: Duration,
    mut drain: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            _ = drain.changed() => return,
            _ = tick.tick() => match sweeper.sweep_once(jiff::Timestamp::now()).await {
                Ok(0) => {}
                Ok(count) => tracing::info!(count, class = "bodies_minimized", "minimized stored post bodies"),
                Err(_) => tracing::warn!(class = "retention_unavailable"),
            },
        }
    }
}

/// The route that serves the counter on the operator plane.
pub fn metrics_router(counter: RetentionCounter) -> Router {
    Router::new()
        .route(
            "/metrics",
            get(|State(counter): State<RetentionCounter>| async move {
                (
                    [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
                    counter.render(),
                )
            }),
        )
        .with_state(counter)
}
