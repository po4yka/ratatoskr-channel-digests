//! Loopback service-authenticated owner projections.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse as _, Response};
use axum::routing::get;
use axum::{Extension, Json, Router};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestFailureClass, ChannelDigestOutcome, ChannelDigestRecapDocument,
    ChannelDigestResultId, ChannelDigestResultPage, ChannelDigestResultSummary,
    ChannelDigestResultView, ChannelDigestRunId, ChannelDigestSubscriptionId,
    ChannelDigestSubscriptionPage, ChannelDigestSubscriptionView, ChannelUsername,
    HEADER_DIGEST_RUN_ID, HEADER_MANIFEST_DIGEST, HEADER_OWNER_ID,
};
use ratatoskr_identifiers::{
    ContentDigest, DigestAlgorithm, DigestHex, Extensions, UserId, WireTimestamp,
};
use serde::Deserialize;
use uuid::Uuid;

use crate::{KnowledgeResultReadError, KnowledgeResultReader};

#[derive(Debug, Clone)]
pub(crate) struct ApiState {
    pool: sqlx::PgPool,
    secret: Arc<str>,
    page_limit: usize,
    result_reader: KnowledgeResultReader,
}

#[derive(Debug, Clone, Copy)]
struct AuthorizedOwner(Uuid);

type ResultRow = (
    Uuid,
    Uuid,
    String,
    Option<Uuid>,
    Option<String>,
    i32,
    Option<String>,
);

type SummaryRow = (Uuid, Uuid, String, Option<String>, String);

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
    result_reader: KnowledgeResultReader,
) -> Router {
    let state = ApiState {
        pool,
        secret: Arc::from(secret),
        page_limit,
        result_reader,
    };
    let owner_scoped = Router::new()
        .route("/subscriptions", get(list_subscriptions))
        .route("/manifests/{manifest_id}", get(get_manifest))
        .route("/results", get(list_results))
        .route("/results/{result_id}", get(get_result))
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(middleware::from_fn(require_owner))
        .layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state.clone());
    let readiness = Router::new()
        .route("/ready", get(ready))
        .route_layer(middleware::from_fn_with_state(state.clone(), authorize))
        .with_state(state);
    Router::new()
        .nest("/v1", owner_scoped)
        .merge(readiness)
        .layer(middleware::from_fn(no_store))
}

/// Requires the fixed service bearer. It is the whole authorization of `/ready`.
async fn authorize(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if !supplied.is_some_and(|value| constant_time_equal(value.as_bytes(), state.secret.as_bytes()))
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    next.run(request).await
}

/// Requires the bare user UUID of the owner every `/v1` read is scoped to.
async fn require_owner(mut request: Request, next: Next) -> Response {
    let owner = request
        .headers()
        .get(HEADER_OWNER_ID)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| UserId::parse(value).ok());
    let Some(owner) = owner else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    request.extensions_mut().insert(AuthorizedOwner(owner.0));
    next.run(request).await
}

async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

async fn ready(State(state): State<ApiState>) -> StatusCode {
    match sqlx::query("select 1").execute(&state.pool).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn checked_page_size(state: &ApiState, page: &PageQuery) -> Result<i64, StatusCode> {
    if page.page_size == 0 || page.page_size > state.page_limit {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(i64::try_from(page.page_size).unwrap_or(i64::MAX))
}

async fn list_subscriptions(
    State(state): State<ApiState>,
    Extension(owner): Extension<AuthorizedOwner>,
    Query(page): Query<PageQuery>,
) -> Response {
    let limit = match checked_page_size(&state, &page) {
        Ok(limit) => limit,
        Err(status) => return status.into_response(),
    };
    let rows: Result<Vec<(Uuid, String, bool)>, _> = sqlx::query_as(
        "select s.subscription_id, c.username, s.enabled from channel_digests.subscriptions s join channel_digests.channels c using (channel_id) where s.owner_id = $1 order by s.first_activated_at desc, s.subscription_id desc limit $2",
    )
    .bind(owner.0)
    .bind(limit)
    .fetch_all(&state.pool)
    .await;
    let Ok(rows) = rows else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let views: Option<Vec<ChannelDigestSubscriptionView>> = rows
        .into_iter()
        .map(|(subscription_id, username, enabled)| {
            Some(ChannelDigestSubscriptionView {
                subscription_id: ChannelDigestSubscriptionId::parse(&subscription_id.to_string())
                    .ok()?,
                channel_username: ChannelUsername::parse(&username).ok()?,
                enabled,
                extensions: Extensions::new(),
            })
        })
        .collect();
    match views {
        Some(subscriptions) => Json(ChannelDigestSubscriptionPage {
            subscriptions,
            extensions: Extensions::new(),
        })
        .into_response(),
        None => result_failure(StatusCode::INTERNAL_SERVER_ERROR, "stored_value_invalid")
            .into_response(),
    }
}

/// Serves the stored canonical manifest text verbatim when every claim matches the stored row.
///
/// An absent manifest, another owner's manifest, a missing claim and a mismatching claim are all
/// the same empty 404.
async fn get_manifest(
    State(state): State<ApiState>,
    Extension(owner): Extension<AuthorizedOwner>,
    Path(manifest_id): Path<Uuid>,
    headers: HeaderMap,
) -> Response {
    let run_id = header_text(&headers, HEADER_DIGEST_RUN_ID)
        .and_then(|value| ChannelDigestRunId::parse(value).ok());
    let digest = header_text(&headers, HEADER_MANIFEST_DIGEST)
        .and_then(|value| DigestHex::parse(value).ok());
    let (Some(run_id), Some(digest)) = (run_id, digest) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let row: Result<Option<(String,)>, _> = sqlx::query_as(
        "select canonical_text from channel_digests.digest_manifests where manifest_id = $1 and owner_id = $2 and run_id = $3 and sha256 = $4",
    )
    .bind(manifest_id)
    .bind(owner.0)
    .bind(run_id.as_uuid())
    .bind(digest.as_str())
    .fetch_optional(&state.pool)
    .await;
    match row {
        Ok(Some((text,))) => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            text,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

async fn list_results(
    State(state): State<ApiState>,
    Extension(owner): Extension<AuthorizedOwner>,
    Query(page): Query<PageQuery>,
) -> Response {
    let limit = match checked_page_size(&state, &page) {
        Ok(limit) => limit,
        Err(status) => return status.into_response(),
    };
    let rows: Result<Vec<SummaryRow>, _> = sqlx::query_as(
        "select result_id, run_id, outcome, safe_failure_class, to_char(created_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"') from channel_digests.digest_results where owner_id = $1 order by created_at desc, result_id desc limit $2",
    )
    .bind(owner.0)
    .bind(limit)
    .fetch_all(&state.pool)
    .await;
    let Ok(rows) = rows else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match rows
        .into_iter()
        .map(result_summary)
        .collect::<Option<Vec<_>>>()
    {
        Some(results) => Json(ChannelDigestResultPage {
            results,
            extensions: Extensions::new(),
        })
        .into_response(),
        None => result_failure(StatusCode::INTERNAL_SERVER_ERROR, "stored_value_invalid")
            .into_response(),
    }
}

fn result_summary(
    (result_id, run_id, outcome, failure_class, created_at): SummaryRow,
) -> Option<ChannelDigestResultSummary> {
    Some(ChannelDigestResultSummary {
        result_id: ChannelDigestResultId::parse(&result_id.to_string()).ok()?,
        run_id: ChannelDigestRunId::parse(&run_id.to_string()).ok()?,
        outcome: outcome_of(&outcome)?,
        safe_failure_class: failure_class
            .map(|class| ChannelDigestFailureClass::parse(&class))
            .transpose()
            .ok()?,
        created_at: created_at
            .parse::<jiff::Timestamp>()
            .map(WireTimestamp::from_jiff)
            .ok()?,
        extensions: Extensions::new(),
    })
}

fn outcome_of(stored: &str) -> Option<ChannelDigestOutcome> {
    match stored {
        "completed" => Some(ChannelDigestOutcome::Completed),
        "partial" => Some(ChannelDigestOutcome::Partial),
        "failed" => Some(ChannelDigestOutcome::Failed),
        _ => None,
    }
}

async fn get_result(
    State(state): State<ApiState>,
    Extension(owner): Extension<AuthorizedOwner>,
    Path(result_id): Path<Uuid>,
) -> Response {
    let row: Result<Option<ResultRow>, _> = sqlx::query_as(
        "select result_id, run_id, outcome, recap_id, result_digest_hex, citation_count, safe_failure_class from channel_digests.digest_results where result_id = $1 and owner_id = $2",
    )
    .bind(result_id)
    .bind(owner.0)
    .fetch_optional(&state.pool)
    .await;
    let row = match row {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) => {
            return result_failure(StatusCode::SERVICE_UNAVAILABLE, "storage_unavailable")
                .into_response();
        }
    };
    match result_view(&state.result_reader, row).await {
        Ok(view) => Json(view).into_response(),
        Err(status) => status.into_response(),
    }
}

/// Builds the typed result view; a completed or partial result reads its recap from Knowledge.
async fn result_view(
    reader: &KnowledgeResultReader,
    row: ResultRow,
) -> Result<ChannelDigestResultView, StatusCode> {
    let (result_id, run_id, outcome, recap_id, digest_hex, citations, failure_class) = row;
    let invalid = || result_failure(StatusCode::INTERNAL_SERVER_ERROR, "stored_value_invalid");
    let result_id = ChannelDigestResultId::parse(&result_id.to_string()).map_err(|_| invalid())?;
    let run_id = ChannelDigestRunId::parse(&run_id.to_string()).map_err(|_| invalid())?;
    let outcome = outcome_of(&outcome).ok_or_else(invalid)?;
    if outcome == ChannelDigestOutcome::Failed {
        return Ok(ChannelDigestResultView {
            result_id,
            run_id,
            outcome,
            recap_id: None,
            citation_count: None,
            result_digest: None,
            recap: None,
            safe_failure_class: failure_class
                .map(|class| ChannelDigestFailureClass::parse(&class))
                .transpose()
                .map_err(|_| invalid())?,
            extensions: Extensions::new(),
        });
    }
    let (Some(recap_id), Some(digest_hex)) = (recap_id, digest_hex) else {
        return Err(result_failure(
            StatusCode::BAD_GATEWAY,
            "local_linkage_invalid",
        ));
    };
    let projection = reader
        .read(recap_id, &digest_hex)
        .await
        .map_err(|error| match error {
            KnowledgeResultReadError::Unavailable => {
                result_failure(StatusCode::SERVICE_UNAVAILABLE, "upstream_unavailable")
            }
            KnowledgeResultReadError::Invalid => {
                result_failure(StatusCode::BAD_GATEWAY, "upstream_invalid")
            }
        })?;
    let invalid_upstream = || result_failure(StatusCode::BAD_GATEWAY, "upstream_invalid");
    let members = projection
        .recap
        .as_object()
        .cloned()
        .ok_or_else(invalid_upstream)?;
    Ok(ChannelDigestResultView {
        result_id,
        run_id,
        outcome,
        recap_id: Some(recap_id),
        citation_count: Some(u16::try_from(citations).map_err(|_| invalid())?),
        result_digest: Some(ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(&projection.result_digest_hex).map_err(|_| invalid_upstream())?,
        }),
        recap: Some(ChannelDigestRecapDocument { members }),
        safe_failure_class: None,
        extensions: Extensions::new(),
    })
}

fn result_failure(status: StatusCode, outcome: &'static str) -> StatusCode {
    tracing::warn!(result_read_outcome = outcome, "digest result read failed");
    status
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

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt as _;

    use super::*;
    use crate::{Config, Role};

    #[tokio::test]
    async fn ready_answers_503_when_the_pool_is_closed() -> Result<(), Box<dyn std::error::Error>> {
        let url = std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL")?;
        let pool = sqlx::PgPool::connect(&url).await?;
        pool.close().await;
        let config = Config::from_environment(
            Role::Api,
            [
                (
                    "RATATOSKR__DATABASE__URL",
                    "postgres://fixture.invalid/digests",
                ),
                ("RATATOSKR__AUTH__SERVICE_SECRET", "secret"),
                ("RATATOSKR__KNOWLEDGE__BASE_URL", "http://127.0.0.1:8096"),
                (
                    "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
                    "reader",
                ),
                ("RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS", "100"),
                ("RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS", "200"),
                ("RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES", "65536"),
            ],
        )?;
        let app = router(
            pool,
            "secret".to_owned(),
            100,
            1024,
            KnowledgeResultReader::from_config(&config)?,
        );

        let closed = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(closed.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            closed.headers().get(header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );

        let unauthenticated = app
            .oneshot(Request::builder().uri("/ready").body(Body::empty())?)
            .await?;
        assert_eq!(
            unauthenticated.status(),
            StatusCode::UNAUTHORIZED,
            "authorization is checked before the database is asked"
        );
        Ok(())
    }
}
