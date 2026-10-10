//! Contract views, readiness, claim-checked manifest reads and result listing.

mod common;
mod support;

use common::{
    RECAP_SENTINEL, SERVICE_SECRET, TestResult, assert_no_store, connect_database, request,
    request_with_headers, response_body, seed_results, start_api, status,
};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestFailureClass, ChannelDigestOutcome, ChannelDigestResultPage,
    ChannelDigestResultView, ChannelDigestSubscriptionPage, HEADER_DIGEST_RUN_ID,
    HEADER_MANIFEST_DIGEST, HEADER_OWNER_ID, sha256_hex,
};
use ratatoskr_channel_digests::{ManifestBuilder, ManifestSource, SubscriptionRepository};
use std::collections::HashMap;
use std::net::SocketAddr;
use uuid::Uuid;

#[test]
fn ready_requires_bearer_only() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let api = start_api(HashMap::new())?;

    let ready = request(api.domain, "/ready", Some(SERVICE_SECRET), None)?;
    assert_eq!(
        status(&ready)?,
        200,
        "the bearer alone authorizes readiness"
    );
    assert_no_store(&ready);
    assert!(response_body(&ready)?.is_empty());
    for (secret, owner) in [
        (None, None),
        (Some("wrong-secret"), None),
        (None, Some(Uuid::now_v7().to_string())),
    ] {
        let denied = request(api.domain, "/ready", secret, owner.as_deref())?;
        assert_eq!(status(&denied)?, 401);
        assert_no_store(&denied);
    }
    assert_eq!(
        status(&request(api.domain, "/live", Some(SERVICE_SECRET), None)?)?,
        404,
        "liveness stays on the operator plane"
    );
    runtime.block_on(database.close());
    Ok(())
}

struct SeededManifest {
    owner_id: Uuid,
    manifest_id: Uuid,
    run_id: Uuid,
    text: String,
    sha256: String,
}

async fn seed_manifest(pool: &sqlx::PgPool) -> Result<SeededManifest, Box<dyn std::error::Error>> {
    let (owner_id, manifest_id, run_id) = (Uuid::now_v7(), Uuid::now_v7(), Uuid::now_v7());
    let body = "naive caf\u{e9} \u{2014} exact bytes";
    let manifest = ManifestBuilder::build(
        manifest_id,
        owner_id,
        run_id,
        "2026-08-20T10:00:00Z",
        "2026-08-21T10:00:00Z",
        &[ManifestSource {
            revision_id: Uuid::now_v7(),
            channel_id: Uuid::now_v7(),
            channel_username: "manifest_route".to_owned(),
            channel_display_name: Some("Route \"quoted\"".to_owned()),
            message_id: 5,
            content_sha256: sha256_hex(body.as_bytes()),
            published_at: "2026-08-20T12:00:00Z".to_owned(),
            canonical_link: "https://t.me/manifest_route/5".to_owned(),
            body: body.to_owned(),
            revision_index: 1,
        }],
    )?;
    sqlx::query(
        "insert into channel_digests.digest_runs \
         (run_id, owner_id, operation_id, trigger, idempotency_key, window_start, window_end, state) \
         values ($1, $2, $3, 'on_demand', $4, '2026-08-20T10:00:00Z', '2026-08-21T10:00:00Z', 'waiting_recap')",
    )
    .bind(run_id)
    .bind(owner_id)
    .bind(Uuid::now_v7())
    .bind(format!("manifest-route-{run_id}"))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_manifests \
         (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_text) \
         values ($1, $2, $3, $4, 1, 1, $5)",
    )
    .bind(manifest_id)
    .bind(run_id)
    .bind(owner_id)
    .bind(&manifest.sha256)
    .bind(&manifest.text)
    .execute(pool)
    .await?;
    Ok(SeededManifest {
        owner_id,
        manifest_id,
        run_id,
        text: manifest.text,
        sha256: manifest.sha256,
    })
}

fn manifest_request(
    address: SocketAddr,
    manifest_id: Uuid,
    headers: &[(&str, &str)],
) -> Result<String, Box<dyn std::error::Error>> {
    request_with_headers(address, &format!("/v1/manifests/{manifest_id}"), headers)
}

#[test]
fn manifest_serves_exact_stored_bytes_and_hashes_to_the_digest_header() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let seeded = runtime.block_on(seed_manifest(database.pool()))?;
    let api = start_api(HashMap::new())?;
    let authorization = format!("Bearer {SERVICE_SECRET}");
    let owner = seeded.owner_id.to_string();
    let run = seeded.run_id.to_string();

    let response = manifest_request(
        api.domain,
        seeded.manifest_id,
        &[
            ("Authorization", authorization.as_str()),
            (HEADER_OWNER_ID, owner.as_str()),
            (HEADER_DIGEST_RUN_ID, run.as_str()),
            (HEADER_MANIFEST_DIGEST, seeded.sha256.as_str()),
        ],
    )?;

    assert_eq!(status(&response)?, 200);
    assert_no_store(&response);
    assert!(
        response
            .to_ascii_lowercase()
            .contains("content-type: application/json")
    );
    let body = response_body(&response)?;
    assert_eq!(body, seeded.text, "the stored text is served verbatim");
    assert_eq!(
        sha256_hex(body.as_bytes()),
        seeded.sha256,
        "the body hashes to the digest the caller sent"
    );
    runtime.block_on(database.close());
    Ok(())
}

#[test]
fn manifest_with_wrong_run_id_or_wrong_digest_or_foreign_owner_is_404_with_empty_body() -> TestResult
{
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let seeded = runtime.block_on(seed_manifest(database.pool()))?;
    let api = start_api(HashMap::new())?;
    let authorization = format!("Bearer {SERVICE_SECRET}");
    let owner = seeded.owner_id.to_string();
    let run = seeded.run_id.to_string();
    let foreign_owner = Uuid::now_v7().to_string();
    let other_run = Uuid::now_v7().to_string();
    let other_digest = "0".repeat(64);

    let digest = seeded.sha256.as_str();
    let cases = [
        (
            "wrong run id",
            seeded.manifest_id,
            claims(Some(&owner), Some(&other_run), Some(digest)),
        ),
        (
            "wrong digest",
            seeded.manifest_id,
            claims(Some(&owner), Some(&run), Some(&other_digest)),
        ),
        (
            "foreign owner",
            seeded.manifest_id,
            claims(Some(&foreign_owner), Some(&run), Some(digest)),
        ),
        (
            "missing run id",
            seeded.manifest_id,
            claims(Some(&owner), None, Some(digest)),
        ),
        (
            "missing digest",
            seeded.manifest_id,
            claims(Some(&owner), Some(&run), None),
        ),
        (
            "malformed digest",
            seeded.manifest_id,
            claims(Some(&owner), Some(&run), Some("NOT-HEX")),
        ),
        (
            "unknown manifest",
            Uuid::now_v7(),
            claims(Some(&owner), Some(&run), Some(digest)),
        ),
    ];
    let mut normalized = Vec::new();
    for (name, manifest_id, held) in cases {
        let mut headers = vec![("Authorization", authorization.as_str())];
        headers.extend(held);
        let response = manifest_request(api.domain, manifest_id, &headers)?;
        assert_eq!(status(&response)?, 404, "{name}");
        assert!(
            response_body(&response)?.is_empty(),
            "{name} must have an empty body"
        );
        normalized.push(without_date(&response));
    }
    assert!(
        normalized.windows(2).all(|pair| pair[0] == pair[1]),
        "every refusal is the same response, so none reveals which claim failed"
    );

    let unauthenticated = manifest_request(
        api.domain,
        seeded.manifest_id,
        &claims(Some(&owner), Some(&run), Some(digest)),
    )?;
    assert_eq!(status(&unauthenticated)?, 401);
    let mut headers = claims(Some("not-a-uuid"), Some(&run), Some(digest));
    headers.push(("Authorization", authorization.as_str()));
    let unparsable_owner = manifest_request(api.domain, seeded.manifest_id, &headers)?;
    assert_eq!(status(&unparsable_owner)?, 401);
    runtime.block_on(database.close());
    Ok(())
}

fn claims<'a>(
    owner: Option<&'a str>,
    run: Option<&'a str>,
    digest: Option<&'a str>,
) -> Vec<(&'static str, &'a str)> {
    [
        (HEADER_OWNER_ID, owner),
        (HEADER_DIGEST_RUN_ID, run),
        (HEADER_MANIFEST_DIGEST, digest),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|value| (name, value)))
    .collect()
}

fn without_date(response: &str) -> String {
    response
        .lines()
        .filter(|line| !line.to_ascii_lowercase().starts_with("date:"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn results_list_is_owner_scoped_newest_first_and_content_free() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let seeded = runtime.block_on(seed_results(database.pool()))?;
    let api = start_api(HashMap::new())?;
    let owner = seeded.completed.owner_id.to_string();

    let response = request(
        api.domain,
        "/v1/results?page_size=10",
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&response)?, 200);
    assert_no_store(&response);
    let body = response_body(&response)?;
    let page: ChannelDigestResultPage = serde_json::from_str(body)?;
    let listed: Vec<(Uuid, ChannelDigestOutcome)> = page
        .results
        .iter()
        .map(|summary| (summary.result_id.as_uuid(), summary.outcome))
        .collect();
    assert_eq!(
        listed,
        [
            (seeded.failed.result_id, ChannelDigestOutcome::Failed),
            (seeded.partial.result_id, ChannelDigestOutcome::Partial),
            (seeded.completed.result_id, ChannelDigestOutcome::Completed),
        ],
        "newest first, and only this owner's results"
    );
    assert_eq!(
        page.results[0]
            .safe_failure_class
            .as_ref()
            .map(ChannelDigestFailureClass::as_str),
        Some("provider_timeout")
    );
    assert!(page.results[1].safe_failure_class.is_none());
    assert!(!body.contains(RECAP_SENTINEL));
    assert!(
        !body.contains("recap"),
        "a summary never carries recap content"
    );
    assert_eq!(
        api.knowledge.request_count()?,
        0,
        "a listing never reads Knowledge"
    );

    let limited = request(
        api.domain,
        "/v1/results?page_size=2",
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    let limited: ChannelDigestResultPage = serde_json::from_str(response_body(&limited)?)?;
    assert_eq!(limited.results.len(), 2);

    for bad in ["0", "101", "many"] {
        let rejected = request(
            api.domain,
            &format!("/v1/results?page_size={bad}"),
            Some(SERVICE_SECRET),
            Some(&owner),
        )?;
        assert_eq!(status(&rejected)?, 400, "page_size={bad}");
    }
    let foreign = request(
        api.domain,
        "/v1/results",
        Some(SERVICE_SECRET),
        Some(&seeded.foreign.owner_id.to_string()),
    )?;
    let foreign: ChannelDigestResultPage = serde_json::from_str(response_body(&foreign)?)?;
    assert_eq!(foreign.results.len(), 1);
    assert_eq!(
        foreign.results[0].result_id.as_uuid(),
        seeded.foreign.result_id
    );
    runtime.block_on(database.close());
    Ok(())
}

#[test]
fn result_and_subscription_bodies_parse_as_contract_views() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let seeded = runtime.block_on(seed_results(database.pool()))?;
    let subscriber = Uuid::now_v7();
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    for (username, enabled, at) in [
        ("view_older", true, "2026-08-20T10:00:00Z"),
        ("view_newer", true, "2026-08-22T10:00:00Z"),
        ("view_paused", false, "2026-08-21T10:00:00Z"),
    ] {
        runtime.block_on(subscriptions.set(subscriber, username, enabled, at))?;
    }
    let api = start_api(seeded.knowledge_responses()?)?;

    let response = request(
        api.domain,
        "/v1/subscriptions",
        Some(SERVICE_SECRET),
        Some(&subscriber.to_string()),
    )?;
    assert_eq!(status(&response)?, 200);
    let page: ChannelDigestSubscriptionPage = serde_json::from_str(response_body(&response)?)?;
    let listed: Vec<(&str, bool)> = page
        .subscriptions
        .iter()
        .map(|view| (view.channel_username.as_str(), view.enabled))
        .collect();
    assert_eq!(
        listed,
        [
            ("view_newer", true),
            ("view_paused", false),
            ("view_older", true)
        ],
        "newest first by first activation"
    );

    let owner = seeded.completed.owner_id.to_string();
    let completed = request(
        api.domain,
        &format!("/v1/results/{}", seeded.completed.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    let view: ChannelDigestResultView = serde_json::from_str(response_body(&completed)?)?;
    assert_eq!(view.outcome, ChannelDigestOutcome::Completed);
    assert_eq!(view.recap_id, seeded.completed.analysis_id);
    assert_eq!(view.citation_count, Some(2));
    assert!(view.recap.is_some() && view.result_digest.is_some());
    assert!(view.safe_failure_class.is_none());

    let failed = request(
        api.domain,
        &format!("/v1/results/{}", seeded.failed.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    let view: ChannelDigestResultView = serde_json::from_str(response_body(&failed)?)?;
    assert_eq!(view.outcome, ChannelDigestOutcome::Failed);
    assert_eq!(
        view.safe_failure_class
            .as_ref()
            .map(ChannelDigestFailureClass::as_str),
        Some("provider_timeout")
    );
    assert!(view.recap.is_none() && view.recap_id.is_none() && view.result_digest.is_none());
    runtime.block_on(database.close());
    Ok(())
}
