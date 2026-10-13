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

/// Name of the body counter on the operator plane.
const BODIES_METRIC: &str = "channel_digests_bodies_minimized_total";

/// Name of the manifest counter on the operator plane.
const MANIFESTS_METRIC: &str = "channel_digests_manifests_minimized_total";

/// What one retention sweep minimized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SweepOutcome {
    /// Post revision bodies minimized.
    pub bodies: u64,
    /// Manifest texts minimized.
    pub manifests: u64,
}

/// Process-wide counts of minimized bodies and manifest texts.
#[derive(Debug, Clone, Default)]
pub struct RetentionCounter {
    bodies: Arc<AtomicU64>,
    manifests: Arc<AtomicU64>,
}

impl RetentionCounter {
    /// Creates counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Bodies minimized since the process started.
    #[must_use]
    pub fn bodies(&self) -> u64 {
        self.bodies.load(Ordering::Acquire)
    }

    /// Manifest texts minimized since the process started, by the sweep or by a withdrawal.
    #[must_use]
    pub fn manifests(&self) -> u64 {
        self.manifests.load(Ordering::Acquire)
    }

    /// Adds minimized bodies.
    pub fn add_bodies(&self, count: u64) {
        self.bodies.fetch_add(count, Ordering::AcqRel);
    }

    /// Adds minimized manifest texts.
    pub fn add_manifests(&self, count: u64) {
        self.manifests.fetch_add(count, Ordering::AcqRel);
    }

    /// The counters in the Prometheus text format.
    #[must_use]
    pub fn render(&self) -> String {
        format!(
            "# HELP {BODIES_METRIC} Stored post bodies minimized by the retention sweep.\n# TYPE {BODIES_METRIC} counter\n{BODIES_METRIC} {}\n# HELP {MANIFESTS_METRIC} Manifest texts minimized by the retention sweep or a withdrawal.\n# TYPE {MANIFESTS_METRIC} counter\n{MANIFESTS_METRIC} {}\n",
            self.bodies(),
            self.manifests()
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

    /// Minimizes one batch of bodies published before `now` minus the retention, then one batch of
    /// manifests whose run window began before the same cutoff.
    ///
    /// # Errors
    ///
    /// Returns a content-free storage class.
    pub async fn sweep_once(&self, now: jiff::Timestamp) -> Result<SweepOutcome, MaintenanceError> {
        let cutoff = now
            .checked_sub(jiff::SignedDuration::from_hours(
                24 * i64::from(self.body_days),
            ))
            .map_err(|_| MaintenanceError)?;
        let bodies = self
            .maintenance
            .minimize_before(&cutoff.to_string(), SWEEP_BATCH)
            .await?;
        self.counter.add_bodies(bodies);
        let manifests = self
            .maintenance
            .minimize_manifests_before(&cutoff.to_string(), SWEEP_BATCH)
            .await?;
        self.counter.add_manifests(manifests);
        Ok(SweepOutcome { bodies, manifests })
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
                Ok(SweepOutcome { bodies: 0, manifests: 0 }) => {}
                Ok(SweepOutcome { bodies, manifests }) => tracing::info!(
                    bodies,
                    manifests,
                    class = "retention_minimized",
                    "minimized stored post bodies and manifest texts"
                ),
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
