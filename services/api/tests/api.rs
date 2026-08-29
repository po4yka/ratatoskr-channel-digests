//! Loopback service-auth and owner-scope acceptance.

mod support;

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_channel_digests::Database;
use serde_json::{Value, json};
use support::{FakeResponse, RecordingKnowledge};
use uuid::Uuid;

const SERVICE_SECRET: &str = "synthetic-service-secret";
const KNOWLEDGE_SECRET: &str = "synthetic-knowledge-result-secret";
const KNOWLEDGE_AUTHORIZATION: &str = "Bearer synthetic-knowledge-result-secret";
const RECAP_SENTINEL: &str = "private-recap-must-not-be-stored";
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

fn assert_no_store(response: &str) {
    assert!(
        response
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
}

fn json_body(response: &str) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_str(response_body(response)?)?)
}

fn response_body(response: &str) -> Result<&str, Box<dyn std::error::Error>> {
    Ok(response
        .split_once("\r\n\r\n")
        .ok_or("missing HTTP body")?
        .1)
}

struct SeededResult {
    owner_id: Uuid,
    result_id: Uuid,
    run_id: Uuid,
    manifest_id: Uuid,
    outcome: &'static str,
    analysis_id: Option<Uuid>,
    result_digest_hex: Option<String>,
    citation_count: i32,
    safe_failure_class: Option<&'static str>,
    recap: Option<Value>,
}

impl SeededResult {
    fn expected_projection(&self) -> Result<Value, Box<dyn std::error::Error>> {
        let analysis_id = self.analysis_id.ok_or("result analysis ID is absent")?;
        let digest_hex = self
            .result_digest_hex
            .as_deref()
            .ok_or("result digest is absent")?;
        let recap = self.recap.as_ref().ok_or("result recap is absent")?;
        Ok(json!({
            "result_id": self.result_id,
            "run_id": self.run_id,
            "outcome": self.outcome,
            "recap_id": analysis_id,
            "citation_count": self.citation_count,
            "result_digest": {"algorithm": "sha256", "hex": digest_hex},
            "recap": recap
        }))
    }

    fn knowledge_response(&self) -> Result<String, Box<dyn std::error::Error>> {
        let analysis_id = self.analysis_id.ok_or("result analysis ID is absent")?;
        let digest_hex = self
            .result_digest_hex
            .as_deref()
            .ok_or("result digest is absent")?;
        let recap = self.recap.as_ref().ok_or("result recap is absent")?;
        Ok(serde_json::to_string(&json!({
            "analysis_id": analysis_id,
            "result_digest": {"algorithm": "sha256", "hex": digest_hex},
            "recap": recap
        }))?)
    }
}

struct SeededResults {
    completed: SeededResult,
    partial: SeededResult,
    failed: SeededResult,
    foreign: SeededResult,
}

impl SeededResults {
    fn knowledge_responses(&self) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
        let mut responses = HashMap::new();
        for result in [&self.completed, &self.partial] {
            let analysis_id = result.analysis_id.ok_or("result analysis ID is absent")?;
            responses.insert(
                format!("/internal/channel-digest-results/{analysis_id}"),
                result.knowledge_response()?,
            );
        }
        Ok(responses)
    }
}

async fn seed_results(pool: &sqlx::PgPool) -> Result<SeededResults, sqlx::Error> {
    let owner = Uuid::now_v7();
    let completed = successful_seed(owner, "completed", "11", 2, "completed recap");
    let partial = successful_seed(owner, "partial", "22", 1, "partial recap");
    let failed = SeededResult {
        owner_id: owner,
        result_id: Uuid::now_v7(),
        run_id: Uuid::now_v7(),
        manifest_id: Uuid::now_v7(),
        outcome: "failed",
        analysis_id: None,
        result_digest_hex: None,
        citation_count: 0,
        safe_failure_class: Some("provider_timeout"),
        recap: None,
    };
    let foreign = successful_seed(Uuid::now_v7(), "completed", "33", 1, "foreign recap");
    for result in [&completed, &partial, &failed, &foreign] {
        insert_seed(pool, result).await?;
    }
    Ok(SeededResults {
        completed,
        partial,
        failed,
        foreign,
    })
}

fn successful_seed(
    owner_id: Uuid,
    outcome: &'static str,
    digest_pair: &str,
    citation_count: i32,
    label: &str,
) -> SeededResult {
    SeededResult {
        owner_id,
        result_id: Uuid::now_v7(),
        run_id: Uuid::now_v7(),
        manifest_id: Uuid::now_v7(),
        outcome,
        analysis_id: Some(Uuid::now_v7()),
        result_digest_hex: Some(digest_pair.repeat(32)),
        citation_count,
        safe_failure_class: None,
        recap: Some(json!({
            "title": label,
            "summary": RECAP_SENTINEL,
            "citations": [{"ordinal": 1, "label": label}]
        })),
    }
}

async fn insert_seed(pool: &sqlx::PgPool, result: &SeededResult) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into channel_digests.digest_runs \
         (run_id, owner_id, trigger, idempotency_key, window_start, window_end, state) \
         values ($1, $2, 'on_demand', $3, '2026-08-20T10:00:00Z', \
         '2026-08-21T10:00:00Z', $4)",
    )
    .bind(result.run_id)
    .bind(result.owner_id)
    .bind(format!("api-result-{}", result.result_id))
    .bind(result.outcome)
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_manifests \
         (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_json) \
         values ($1, $2, $3, $4, 1, 1, jsonb_build_object('fixture', true))",
    )
    .bind(result.manifest_id)
    .bind(result.run_id)
    .bind(result.owner_id)
    .bind("aa".repeat(32))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_results \
         (result_id, run_id, manifest_id, owner_id, outcome, recap_id, result_digest_hex, \
         citation_count, safe_failure_class) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    )
    .bind(result.result_id)
    .bind(result.run_id)
    .bind(result.manifest_id)
    .bind(result.owner_id)
    .bind(result.outcome)
    .bind(result.analysis_id)
    .bind(result.result_digest_hex.as_deref())
    .bind(result.citation_count)
    .bind(result.safe_failure_class)
    .execute(pool)
    .await?;
    Ok(())
}

async fn assert_no_recap_storage(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    let narrative_columns: (i64,) = sqlx::query_as(
        "select count(*) from information_schema.columns \
         where table_schema = 'channel_digests' and table_name = 'digest_results' \
         and column_name <> 'recap_id' \
         and (column_name like '%recap%' or column_name like '%summary%' \
         or column_name like '%narrative%' or column_name like '%content%')",
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(
        narrative_columns.0, 0,
        "recap narrative column is forbidden"
    );
    let stored_recap: (i64,) = sqlx::query_as(
        "select count(*) from channel_digests.digest_results d \
         where to_jsonb(d)::text like $1",
    )
    .bind(format!("%{RECAP_SENTINEL}%"))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        stored_recap.0, 0,
        "recap narrative must remain in Knowledge"
    );
    Ok(())
}

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }
}

fn reserve() -> Result<SocketAddr, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

fn database_url() -> String {
    std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest".to_owned()
    })
}

fn request(
    address: SocketAddr,
    path: &str,
    secret: Option<&str>,
    owner: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
    )?;
    if let Some(secret) = secret {
        write!(stream, "Authorization: Bearer {secret}\r\n")?;
    }
    if let Some(owner) = owner {
        write!(stream, "X-Ratatoskr-Owner-Id: {owner}\r\n")?;
    }
    write!(stream, "\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn status(response: &str) -> Result<u16, Box<dyn std::error::Error>> {
    Ok(response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing status")?
        .parse()?)
}

fn wait_live(address: SocketAddr, child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("API exited: {status}").into());
        }
        if request(address, "/live", None, None)
            .ok()
            .and_then(|response| status(&response).ok())
            == Some(200)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("API did not become live".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn stop(child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    if !status.success() {
        return Err("signal failed".into());
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("shutdown timeout".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}
