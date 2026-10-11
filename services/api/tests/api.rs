//! Loopback service-auth and owner-scope acceptance.

mod common;
mod support;

use common::{
    ChildGuard, KNOWLEDGE_AUTHORIZATION, KNOWLEDGE_SECRET, RECAP_SENTINEL, SERVICE_SECRET,
    SeededResult, SeededResults, TestResult, assert_no_recap_storage, assert_no_store,
    connect_database, database_url, insert_seed, json_body, request, reserve, response_body,
    seed_results, start_api_with, status, stop, successful_seed, wait_live,
};
use ratatoskr_channel_digests::{Database, SubscriptionRepository};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::time::Duration;
use support::{FakeResponse, RecordingKnowledge};
use uuid::Uuid;

const UPSTREAM_PRIVATE_BODY: &str = "private-upstream-diagnostic-must-not-escape";

#[test]
fn routes_require_service_and_owner_scope() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = runtime.block_on(Database::connect(
        &database_url(),
        3,
        Duration::from_secs(2),
    ))?;
    runtime.block_on(database.apply_schema())?;
    let seeded = runtime.block_on(seed_results(database.pool()))?;
    runtime.block_on(assert_no_recap_storage(database.pool()))?;

    let mut knowledge =
        RecordingKnowledge::start(seeded.knowledge_responses()?, KNOWLEDGE_AUTHORIZATION)?;
    let domain = reserve()?;
    let operator = reserve()?;
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-api"))
            .env("RATATOSKR__DATABASE__URL", database_url())
            .env("RATATOSKR__AUTH__SERVICE_SECRET", SERVICE_SECRET)
            .env("RATATOSKR__KNOWLEDGE__BASE_URL", knowledge.base_url())
            .env(
                "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
                KNOWLEDGE_SECRET,
            )
            .env("RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS", "50")
            .env("RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS", "100")
            .env("RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES", "65536")
            .env("RATATOSKR__API__LISTEN_ADDRESS", domain.to_string())
            .env("RATATOSKR__OPERATOR__LISTEN_ADDRESS", operator.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?,
    );
    wait_live(operator, &mut child.0)?;

    assert_scoped_before_upstream(domain, &seeded, &knowledge)?;
    assert_completed_and_partial(domain, &seeded, &knowledge)?;
    assert_failed_is_local(domain, &seeded, &knowledge)?;
    assert_upstream_failure_matrix(domain, &seeded.completed, &knowledge)?;
    runtime.block_on(assert_no_recap_storage(database.pool()))?;

    stop(&mut child.0)?;
    knowledge.stop()?;
    runtime.block_on(database.close());
    Ok(())
}

fn assert_scoped_before_upstream(
    domain: SocketAddr,
    seeded: &SeededResults,
    knowledge: &RecordingKnowledge,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = seeded.completed.owner_id.to_string();
    let unauthenticated = request(
        domain,
        &format!("/v1/results/{}", seeded.completed.result_id),
        None,
        Some(&owner),
    )?;
    assert_eq!(status(&unauthenticated)?, 401);

    let authorized = request(
        domain,
        "/v1/subscriptions?page_size=10",
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&authorized)?, 200);
    assert!(
        authorized
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    assert!(!authorized.contains(SERVICE_SECRET));

    let missing = request(
        domain,
        &format!("/v1/results/{}", Uuid::now_v7()),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&missing)?, 404);
    let foreign = request(
        domain,
        &format!("/v1/results/{}", seeded.foreign.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&foreign)?, 404);
    assert_eq!(
        knowledge.request_count()?,
        0,
        "missing and foreign results must be rejected before Knowledge"
    );
    Ok(())
}

fn assert_completed_and_partial(
    domain: SocketAddr,
    seeded: &SeededResults,
    knowledge: &RecordingKnowledge,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = seeded.completed.owner_id.to_string();
    let completed = request(
        domain,
        &format!("/v1/results/{}", seeded.completed.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&completed)?, 200);
    assert_no_store(&completed);
    assert_eq!(
        knowledge.request_count()?,
        1,
        "completed result must perform exactly one Knowledge read"
    );
    assert_knowledge_request(knowledge, 0, &seeded.completed)?;
    assert_eq!(
        json_body(&completed)?,
        seeded.completed.expected_projection()?
    );

    let partial = request(
        domain,
        &format!("/v1/results/{}", seeded.partial.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&partial)?, 200);
    assert_no_store(&partial);
    assert_eq!(
        knowledge.request_count()?,
        2,
        "partial result must perform exactly one Knowledge read"
    );
    assert_knowledge_request(knowledge, 1, &seeded.partial)?;
    assert_eq!(json_body(&partial)?, seeded.partial.expected_projection()?);
    Ok(())
}

fn assert_failed_is_local(
    domain: SocketAddr,
    seeded: &SeededResults,
    knowledge: &RecordingKnowledge,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = seeded.failed.owner_id.to_string();
    let before = knowledge.request_count()?;
    let response = request(
        domain,
        &format!("/v1/results/{}", seeded.failed.result_id),
        Some(SERVICE_SECRET),
        Some(&owner),
    )?;
    assert_eq!(status(&response)?, 200);
    assert_no_store(&response);
    assert_eq!(
        json_body(&response)?,
        json!({
            "result_id": seeded.failed.result_id,
            "run_id": seeded.failed.run_id,
            "outcome": "failed",
            "safe_failure_class": "provider_timeout"
        })
    );
    assert_eq!(
        knowledge.request_count()?,
        before,
        "failed result must not contact Knowledge"
    );
    assert!(!response.contains(RECAP_SENTINEL));
    Ok(())
}

fn assert_upstream_failure_matrix(
    domain: SocketAddr,
    result: &SeededResult,
    knowledge: &RecordingKnowledge,
) -> Result<(), Box<dyn std::error::Error>> {
    let owner = result.owner_id.to_string();
    let analysis_id = result.analysis_id.ok_or("result analysis ID is absent")?;
    let analysis_id_text = analysis_id.to_string();
    let result_id_text = result.result_id.to_string();
    let digest = result
        .result_digest_hex
        .as_deref()
        .ok_or("result digest is absent")?;
    for case in failure_cases(result, &knowledge.base_url())? {
        let releases_hold = matches!(&case.response, FakeResponse::Hold);
        knowledge.respond_with(case.response)?;
        let before = knowledge.request_count()?;
        let response = request(
            domain,
            &format!("/v1/results/{}", result.result_id),
            Some(SERVICE_SECRET),
            Some(&owner),
        );
        if releases_hold {
            knowledge.release_hold()?;
        }
        let response = response?;
        assert_eq!(
            status(&response)?,
            case.expected_status,
            "wrong API status for {}",
            case.name
        );
        assert_no_store(&response);
        assert!(
            response_body(&response)?.is_empty(),
            "{} failure body must be empty",
            case.name
        );
        for sensitive in [
            KNOWLEDGE_SECRET,
            SERVICE_SECRET,
            RECAP_SENTINEL,
            UPSTREAM_PRIVATE_BODY,
            owner.as_str(),
            result_id_text.as_str(),
            analysis_id_text.as_str(),
            digest,
        ] {
            assert!(
                !response.contains(sensitive),
                "{} leaked content",
                case.name
            );
        }
        assert_eq!(
            knowledge.request_count()?,
            before + 1,
            "{} must perform exactly one upstream request",
            case.name
        );
        assert_knowledge_request(knowledge, before, result)?;
    }
    Ok(())
}

struct FailureCase {
    name: &'static str,
    response: FakeResponse,
    expected_status: u16,
}

fn failure_cases(
    result: &SeededResult,
    base_url: &str,
) -> Result<Vec<FailureCase>, Box<dyn std::error::Error>> {
    let valid: Value = serde_json::from_str(&result.knowledge_response()?)?;
    let mut unknown = valid.clone();
    set_field(&mut unknown, "unexpected", json!(UPSTREAM_PRIVATE_BODY))?;
    let mut invalid_digest = valid.clone();
    set_nested_field(
        &mut invalid_digest,
        "result_digest",
        "hex",
        json!("AA".repeat(32)),
    )?;
    let mut analysis_mismatch = valid.clone();
    set_field(&mut analysis_mismatch, "analysis_id", json!(Uuid::now_v7()))?;
    let mut digest_mismatch = valid.clone();
    set_nested_field(
        &mut digest_mismatch,
        "result_digest",
        "hex",
        json!("44".repeat(32)),
    )?;
    let valid_bytes = serde_json::to_vec(&valid)?;
    Ok(vec![
        FailureCase::new(
            "200 wrong content type",
            FakeResponse::body("200 OK", "text/plain", valid_bytes),
            502,
        ),
        FailureCase::status("upstream 401", "401 Unauthorized", 502),
        FailureCase::status("upstream 403", "403 Forbidden", 502),
        FailureCase::status("upstream 404", "404 Not Found", 502),
        FailureCase::new(
            "redirect",
            FakeResponse::redirect(base_url, UPSTREAM_PRIVATE_BODY),
            502,
        ),
        FailureCase::status("upstream 503", "503 Service Unavailable", 503),
        FailureCase::new("disconnect", FakeResponse::Disconnect, 503),
        FailureCase::new("request timeout", FakeResponse::Hold, 503),
        FailureCase::new(
            "oversized body",
            FakeResponse::body("200 OK", "application/json", vec![b'x'; 65_537]),
            502,
        ),
        FailureCase::new(
            "malformed JSON",
            FakeResponse::body("200 OK", "application/json", b"{".to_vec()),
            502,
        ),
        FailureCase::json("unknown envelope field", &unknown, 502)?,
        FailureCase::json("invalid digest", &invalid_digest, 502)?,
        FailureCase::json("analysis mismatch", &analysis_mismatch, 502)?,
        FailureCase::json("digest mismatch", &digest_mismatch, 502)?,
    ])
}

impl FailureCase {
    fn new(name: &'static str, response: FakeResponse, expected_status: u16) -> Self {
        Self {
            name,
            response,
            expected_status,
        }
    }

    fn status(name: &'static str, status: &'static str, expected_status: u16) -> Self {
        Self::new(
            name,
            FakeResponse::body(
                status,
                "application/json",
                UPSTREAM_PRIVATE_BODY.as_bytes().to_vec(),
            ),
            expected_status,
        )
    }

    fn json(
        name: &'static str,
        value: &Value,
        expected_status: u16,
    ) -> Result<Self, serde_json::Error> {
        Ok(Self::new(
            name,
            FakeResponse::body("200 OK", "application/json", serde_json::to_vec(&value)?),
            expected_status,
        ))
    }
}

fn set_field(value: &mut Value, field: &str, replacement: Value) -> Result<(), &'static str> {
    value
        .as_object_mut()
        .ok_or("fixture response must be an object")?
        .insert(field.to_owned(), replacement);
    Ok(())
}

fn set_nested_field(
    value: &mut Value,
    parent: &str,
    field: &str,
    replacement: Value,
) -> Result<(), &'static str> {
    value
        .get_mut(parent)
        .and_then(Value::as_object_mut)
        .ok_or("fixture response field must be an object")?
        .insert(field.to_owned(), replacement);
    Ok(())
}

fn assert_knowledge_request(
    knowledge: &RecordingKnowledge,
    index: usize,
    result: &SeededResult,
) -> Result<(), Box<dyn std::error::Error>> {
    let analysis_id = result.analysis_id.ok_or("result analysis ID is absent")?;
    let requests = knowledge.requests()?;
    let request = requests
        .get(index)
        .ok_or("Knowledge request was not recorded")?;
    assert!(request.starts_with(&format!(
        "GET /internal/channel-digest-results/{analysis_id} HTTP/1.1\r\n"
    )));
    assert!(request.lines().any(|line| {
        line.split_once(':').is_some_and(|(name, value)| {
            name.eq_ignore_ascii_case("authorization") && value.trim() == KNOWLEDGE_AUTHORIZATION
        })
    }));
    assert!(!request.contains(SERVICE_SECRET));
    assert!(
        !request
            .to_ascii_lowercase()
            .contains("x-ratatoskr-owner-id")
    );
    Ok(())
}

#[test]
fn a_page_size_above_the_configured_limit_is_clamped_not_refused() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = connect_database(&runtime)?;
    let owner = Uuid::now_v7();
    let owner_text = owner.to_string();
    let subscriptions = SubscriptionRepository::new(database.pool().clone());
    for index in 0..30 {
        runtime.block_on(insert_seed(
            database.pool(),
            &successful_seed(owner, "completed", "11", 1, "clamp recap"),
        ))?;
        runtime.block_on(subscriptions.set(
            owner,
            &format!("clamp_channel_{index:02}"),
            false,
            "2026-08-27T09:00:00Z",
        ))?;
    }
    let count = |api: &common::RunningApi,
                 kind: &str,
                 query: &str|
     -> Result<(u16, Option<usize>), Box<dyn std::error::Error>> {
        let response = request(
            api.domain,
            &format!("/v1/{kind}{query}"),
            Some(SERVICE_SECRET),
            Some(&owner_text),
        )?;
        let code = status(&response)?;
        if code != 200 {
            return Ok((code, None));
        }
        Ok((code, json_body(&response)?[kind].as_array().map(Vec::len)))
    };

    let capped = start_api_with(HashMap::new(), &[("RATATOSKR__LIMITS__PAGE_SIZE", "25")])?;
    for kind in ["subscriptions", "results"] {
        assert_eq!(
            count(&capped, kind, "?page_size=100")?,
            (200, Some(25)),
            "{kind}"
        );
        assert_eq!(
            count(&capped, kind, "?page_size=25")?,
            (200, Some(25)),
            "{kind}"
        );
        assert_eq!(
            count(&capped, kind, "?page_size=10")?,
            (200, Some(10)),
            "{kind}"
        );
        assert_eq!(
            count(&capped, kind, "")?,
            (200, Some(25)),
            "{kind}: default is clamped too"
        );
        for refused in [
            "?page_size=0",
            "?page_size=101",
            "?page_size=many",
            "?page_size=-1",
        ] {
            assert_eq!(
                count(&capped, kind, refused)?,
                (400, None),
                "{kind}{refused}"
            );
        }
    }
    drop(capped);

    let default = start_api_with(HashMap::new(), &[])?;
    for kind in ["subscriptions", "results"] {
        assert_eq!(
            count(&default, kind, "")?,
            (200, Some(30)),
            "{kind}: default page is 50"
        );
        assert_eq!(
            count(&default, kind, "?page_size=100")?,
            (200, Some(30)),
            "{kind}"
        );
        assert_eq!(
            count(&default, kind, "?page_size=0")?,
            (400, None),
            "{kind}"
        );
    }
    drop(default);
    runtime.block_on(database.close());
    Ok(())
}
