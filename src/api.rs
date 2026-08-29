//! Loopback service-authenticated owner projections.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub(crate) struct ApiState {
    pool: sqlx::PgPool,
    secret: Arc<str>,
    page_limit: usize,
}

#[derive(Debug, Clone, Copy)]
struct AuthorizedOwner(Uuid);

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    #[serde(default = "default_page_size")]
    page_size: usize,
}

fn default_page_size() -> usize {
    50
}

pub(crate) fn router(
    pool: sqlx::PgPool,
    secret: String,
    page_limit: usize,
    body_limit: usize,
) -> Router {
    let state = ApiState {
        pool,
        secret: Arc::from(secret),
        page_limit,
    };
    let protected = Router::new()
        .route("/subscriptions", get(list_subscriptions))
        .route("/manifests/{manifest_id}", get(get_manifest))
        .route("/results/{result_id}", get(get_result))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state);
    Router::new()
        .nest("/v1", protected)
        .layer(middleware::from_fn(no_store))
}

async fn authorize(State(state): State<ApiState>, mut request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if !supplied.is_some_and(|value| constant_time_equal(value.as_bytes(), state.secret.as_bytes()))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let owner = request
        .headers()
        .get("x-ratatoskr-owner-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| Uuid::parse_str(value).ok());
    let Some(owner) = owner else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    request.extensions_mut().insert(AuthorizedOwner(owner));
    next.run(request).await
}

async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn list_subscriptions(
    State(state): State<ApiState>,
    axum::Extension(owner): axum::Extension<AuthorizedOwner>,
    Query(page): Query<PageQuery>,
) -> Response {
    if page.page_size == 0 || page.page_size > state.page_limit {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let rows: Result<Vec<(Uuid, String, bool)>, _> = sqlx::query_as(
        "select s.subscription_id, c.username, s.enabled from channel_digests.subscriptions s join channel_digests.channels c using (channel_id) where s.owner_id = $1 order by c.username limit $2",
    )
    .bind(owner.0)
    .bind(i64::try_from(page.page_size).unwrap_or(i64::MAX))
    .fetch_all(&state.pool)
    .await;
    match rows {
        Ok(rows) => Json(serde_json::json!({
            "subscriptions": rows.into_iter().map(|row| serde_json::json!({
                "subscription_id": row.0,
                "channel_username": row.1,
                "enabled": row.2,
            })).collect::<Vec<_>>()
        }))
        .into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn get_manifest(
    State(state): State<ApiState>,
    axum::Extension(owner): axum::Extension<AuthorizedOwner>,
    Path(manifest_id): Path<Uuid>,
) -> Response {
    let row: Result<Option<(serde_json::Value,)>, _> = sqlx::query_as(
        "select canonical_json from channel_digests.digest_manifests where manifest_id = $1 and owner_id = $2",
    )
    .bind(manifest_id)
    .bind(owner.0)
    .fetch_optional(&state.pool)
    .await;
    scoped_json(row)
}

async fn get_result(
    State(state): State<ApiState>,
    axum::Extension(owner): axum::Extension<AuthorizedOwner>,
    Path(result_id): Path<Uuid>,
) -> Response {
    let row: Result<Option<(serde_json::Value,)>, _> = sqlx::query_as(
        "select jsonb_build_object('result_id', result_id, 'run_id', run_id, 'outcome', outcome, 'recap_id', recap_id, 'citation_count', citation_count, 'safe_failure_class', safe_failure_class) from channel_digests.digest_results where result_id = $1 and owner_id = $2",
    )
    .bind(result_id)
    .bind(owner.0)
    .fetch_optional(&state.pool)
    .await;
    scoped_json(row)
}

fn scoped_json(row: Result<Option<(serde_json::Value,)>, sqlx::Error>) -> Response {
    match row {
        Ok(Some(value)) => Json(value.0).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..left.len().max(right.len()) {
        difference |= usize::from(
            left.get(index).copied().unwrap_or(0) ^ right.get(index).copied().unwrap_or(0),
        );
    }
    difference == 0
}
