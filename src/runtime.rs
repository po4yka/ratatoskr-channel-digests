//! Joined API/worker lifecycle and loopback operator plane.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use tokio::sync::watch;

use crate::{Config, Database, MtProtoPublicChannelProvider, RunExecutor, SessionMaterial};

/// Safe runtime composition failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// Storage did not become usable.
    #[error(transparent)]
    Database(#[from] crate::DatabaseError),
    /// A configured listener could not bind or serve.
    #[error("channel digest listener is unavailable")]
    Listener,
    /// Signal handling could not be installed.
    #[error("channel digest shutdown signal is unavailable")]
    Signal,
    /// The API-only Knowledge result reader could not be composed.
    #[error("Knowledge result reader is unavailable")]
    ResultReader,
    /// A joined server task failed or exceeded the shutdown bound.
    #[error("channel digest process did not drain cleanly")]
    Drain,
}

#[derive(Debug, Clone)]
pub(crate) struct Lifecycle {
    ready: Arc<AtomicBool>,
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerReadiness {
    bus: Arc<AtomicBool>,
    provider: Arc<AtomicBool>,
    lifecycle: Lifecycle,
}

impl WorkerReadiness {
    fn new(lifecycle: Lifecycle) -> Self {
        Self {
            bus: Arc::new(AtomicBool::new(false)),
            provider: Arc::new(AtomicBool::new(false)),
            lifecycle,
        }
    }

    pub(crate) fn set_bus(&self, ready: bool) {
        self.bus.store(ready, Ordering::Release);
        self.refresh();
    }

    fn set_provider(&self, ready: bool) {
        self.provider.store(ready, Ordering::Release);
        self.refresh();
    }

    fn refresh(&self) {
        self.lifecycle
            .set_ready(self.bus.load(Ordering::Acquire) && self.provider.load(Ordering::Acquire));
    }
}

impl Lifecycle {
    fn new(ready: bool) -> Self {
        Self {
            ready: Arc::new(AtomicBool::new(ready)),
        }
    }

    pub(crate) fn begin_drain(&self) {
        self.ready.store(false, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub(crate) fn set_ready(&self, ready: bool) {
        self.ready.store(ready, Ordering::Release);
    }
}

/// Runs the API role until a termination signal, joining both listeners.
///
/// # Errors
///
/// Returns a safe storage, listener, signal, or drain failure.
pub async fn run_api(config: Config) -> Result<(), RuntimeError> {
    let result_reader = crate::KnowledgeResultReader::from_config(&config)
        .map_err(|_| RuntimeError::ResultReader)?;
    let database = connect_database(&config).await?;
    database.apply_schema().await?;
    let lifecycle = Lifecycle::new(true);
    let api = tokio::net::TcpListener::bind(config.api.listen_address)
        .await
        .map_err(|_| RuntimeError::Listener)?;
    let operator = tokio::net::TcpListener::bind(config.operator.listen_address)
        .await
        .map_err(|_| RuntimeError::Listener)?;
    serve_pair(
        api,
        crate::api::router(
            database.pool().clone(),
            config.service_secret().to_owned(),
            config.limits.page_size,
            config.limits.request_bytes,
            result_reader,
        ),
        operator,
        operator_router(lifecycle.clone()),
        lifecycle,
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await?;
    database.close().await;
    Ok(())
}

/// Runs the worker operator plane while dependencies remain unavailable.
///
/// # Errors
///
/// Returns a safe storage, listener, signal, or drain failure.
pub async fn run_worker(config: Config, session: SessionMaterial) -> Result<(), RuntimeError> {
    let database = connect_database(&config).await?;
    database.apply_schema().await?;
    let lifecycle = Lifecycle::new(false);
    let readiness = WorkerReadiness::new(lifecycle.clone());
    let operator = tokio::net::TcpListener::bind(config.operator.listen_address)
        .await
        .map_err(|_| RuntimeError::Listener)?;
    let endpoint = config
        .bus
        .as_ref()
        .ok_or(RuntimeError::Listener)?
        .endpoint
        .clone();
    let (drain_tx, drain_rx) = watch::channel(false);
    let server = tokio::spawn(serve(
        operator,
        operator_router(lifecycle.clone()),
        drain_rx.clone(),
    ));
    let worker_pool = database.pool().clone();
    let bus_readiness = readiness.clone();
    let worker = tokio::spawn(async move {
        crate::bus::supervise_bus(endpoint, worker_pool, bus_readiness, drain_rx).await;
        Ok::<(), RuntimeError>(())
    });
    let provider_config = config
        .provider
        .as_ref()
        .ok_or(RuntimeError::Listener)?
        .clone();
    let provider_readiness = readiness;
    let provider_drain = drain_tx.subscribe();
    let provider_pool = database.pool().clone();
    let provider = tokio::spawn(async move {
        supervise_provider(
            provider_config,
            session,
            provider_pool,
            provider_readiness,
            provider_drain,
        )
        .await;
        Ok::<(), RuntimeError>(())
    });
    shutdown_signal().await?;
    lifecycle.begin_drain();
    let _sent = drain_tx.send(true);
    join_servers(
        [server, worker, provider],
        Duration::from_millis(config.limits.shutdown_timeout_ms),
    )
    .await?;
    database.close().await;
    Ok(())
}

async fn supervise_provider(
    config: crate::config::ProviderConfig,
    session: SessionMaterial,
    pool: sqlx::PgPool,
    readiness: WorkerReadiness,
    mut drain: watch::Receiver<bool>,
) {
    while !*drain.borrow() {
        match MtProtoPublicChannelProvider::connect(config.api_id, &session, Duration::from_secs(5))
            .await
        {
            Ok(provider) => {
                readiness.set_provider(true);
                let executor = RunExecutor::new(pool.clone(), &provider);
                let mut execution_tick = tokio::time::interval(Duration::from_secs(1));
                execution_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        biased;
                        _ = drain.changed() => break,
                        _ = execution_tick.tick() => {
                            if executor.execute_one().await.is_err() {
                                tracing::warn!(class = "run_execution_unavailable");
                            }
                        }
                    }
                }
                readiness.set_provider(false);
                drop(provider);
            }
            Err(error) => {
                readiness.set_provider(false);
                tracing::warn!(class = "provider_unavailable", %error);
                tokio::select! {
                    biased;
                    _ = drain.changed() => {}
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
            }
        }
    }
}

async fn connect_database(config: &Config) -> Result<Database, RuntimeError> {
    Database::connect(
        config.database.url(),
        config.database.max_connections,
        Duration::from_millis(config.database.acquire_timeout_ms),
    )
    .await
    .map_err(RuntimeError::Database)
}

fn operator_router(lifecycle: Lifecycle) -> Router {
    Router::new()
        .route("/live", get(|| async { StatusCode::OK }))
        .route("/ready", get(ready))
        .with_state(lifecycle)
        .layer(middleware::from_fn(no_store))
}

async fn ready(State(lifecycle): State<Lifecycle>) -> StatusCode {
    if lifecycle.is_ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn no_store(request: axum::extract::Request, next: Next) -> Response {
    let mut response = next.run(request).await.into_response();
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn serve_pair(
    first_listener: tokio::net::TcpListener,
    first_router: Router,
    second_listener: tokio::net::TcpListener,
    second_router: Router,
    lifecycle: Lifecycle,
    shutdown_bound: Duration,
) -> Result<(), RuntimeError> {
    let (drain_tx, drain_rx) = watch::channel(false);
    let first = tokio::spawn(serve(first_listener, first_router, drain_rx.clone()));
    let second = tokio::spawn(serve(second_listener, second_router, drain_rx));
    shutdown_signal().await?;
    lifecycle.begin_drain();
    let _sent = drain_tx.send(true);
    join_servers([first, second], shutdown_bound).await
}

async fn serve(
    listener: tokio::net::TcpListener,
    router: Router,
    mut drain: watch::Receiver<bool>,
) -> Result<(), RuntimeError> {
    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            while !*drain.borrow() {
                if drain.changed().await.is_err() {
                    break;
                }
            }
        })
        .await
        .map_err(|_| RuntimeError::Listener)
}

async fn join_servers<const N: usize>(
    servers: [tokio::task::JoinHandle<Result<(), RuntimeError>>; N],
    shutdown_bound: Duration,
) -> Result<(), RuntimeError> {
    tokio::time::timeout(shutdown_bound, async move {
        for server in servers {
            server.await.map_err(|_| RuntimeError::Drain)??;
        }
        Ok::<(), RuntimeError>(())
    })
    .await
    .map_err(|_| RuntimeError::Drain)?
}

async fn shutdown_signal() -> Result<(), RuntimeError> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| RuntimeError::Signal)?;
    tokio::select! {
        biased;
        _ = terminate.recv() => Ok(()),
        result = tokio::signal::ctrl_c() => result.map_err(|_| RuntimeError::Signal),
    }
}
