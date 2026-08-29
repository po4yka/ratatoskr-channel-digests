//! Real API-role process lifecycle behavior.

mod support;

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use ratatoskr_channel_digests::Database;
use serde_json::json;
use support::{FakeResponse, RecordingKnowledge};
use uuid::Uuid;

const SERVICE_SECRET: &str = "synthetic-service-secret";
const KNOWLEDGE_SECRET: &str = "synthetic-knowledge-result-secret";
const KNOWLEDGE_AUTHORIZATION: &str = "Bearer synthetic-knowledge-result-secret";

#[test]
fn api_and_worker_roles_are_separate_ready_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Runtime::new()?;
    let database = runtime.block_on(Database::connect(
        &database_url(),
        3,
        Duration::from_secs(2),
    ))?;
    runtime.block_on(database.apply_schema())?;
    let seeded = runtime.block_on(seed_result(database.pool()))?;
    let mut responses = HashMap::new();
    responses.insert(
        format!("/internal/channel-digest-results/{}", seeded.analysis_id),
        serde_json::to_string(&json!({
            "analysis_id": seeded.analysis_id,
            "result_digest": {"algorithm": "sha256", "hex": seeded.result_digest_hex},
            "recap": {"title": "restart-safe recap", "citations": []}
        }))?,
    );
    let mut knowledge = RecordingKnowledge::start(responses, KNOWLEDGE_AUTHORIZATION)?;

    let api = TcpListener::bind("127.0.0.1:0")?;
    let api_address = api.local_addr()?;
    let operator = TcpListener::bind("127.0.0.1:0")?;
    let operator_address = operator.local_addr()?;
    let mut missing_authority = base_configured(api_address, operator_address);
    let status = missing_authority.arg("check-config").status()?;
    assert!(
        !status.success(),
        "API configuration without Knowledge authority must fail closed"
    );
    let mut invalid_authority = configured(api_address, operator_address, "https://example.com");
    let status = invalid_authority.arg("check-config").status()?;
    assert!(
        !status.success(),
        "non-loopback Knowledge authority must fail closed"
    );
    let mut check = configured(api_address, operator_address, &knowledge.base_url());
    let status = check.arg("check-config").status()?;
    assert!(status.success(), "valid API configuration must pass");
    assert!(
        TcpListener::bind(api_address).is_err(),
        "fixture still reserves API port"
    );
    assert!(
        TcpListener::bind(operator_address).is_err(),
        "fixture still reserves operator port"
    );
    drop(api);
    drop(operator);

    let mut child = configured(api_address, operator_address, &knowledge.base_url())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_status(operator_address, "/live", 200, &mut child)?;
    let ready = wait_status(operator_address, "/ready", 200, &mut child)?;
    assert!(
        ready
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    assert_eq!(http_status(api_address, "/live")?.0, 404);
    assert_result(api_address, &seeded, 200)?;
    stop(&mut child)?;

    let mut restarted = configured(api_address, operator_address, &knowledge.base_url())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_status(operator_address, "/ready", 200, &mut restarted)?;
    assert_result(api_address, &seeded, 200)?;
    knowledge.respond_with(FakeResponse::body(
        "503 Service Unavailable",
        "application/json",
        b"{\"private\":\"must not escape\"}".to_vec(),
    ))?;
    assert_result(api_address, &seeded, 503)?;
    assert_eq!(
        http_status(operator_address, "/ready")?.0,
        200,
        "request-scoped Knowledge failure must not make API readiness false"
    );
    stop(&mut restarted)?;
    knowledge.stop()?;
    runtime.block_on(database.close());
    Ok(())
}

fn base_configured(api: SocketAddr, operator: SocketAddr) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-api"));
    command
        .env("RATATOSKR__DATABASE__URL", database_url())
        .env("RATATOSKR__AUTH__SERVICE_SECRET", SERVICE_SECRET)
        .env("RATATOSKR__API__LISTEN_ADDRESS", api.to_string())
        .env("RATATOSKR__OPERATOR__LISTEN_ADDRESS", operator.to_string());
    command
}

fn configured(api: SocketAddr, operator: SocketAddr, knowledge_base_url: &str) -> Command {
    let mut command = base_configured(api, operator);
    command
        .env("RATATOSKR__KNOWLEDGE__BASE_URL", knowledge_base_url)
        .env(
            "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
            KNOWLEDGE_SECRET,
        )
        .env("RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS", "50")
        .env("RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS", "100")
        .env("RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES", "65536");
    command
}

struct SeededResult {
    owner_id: Uuid,
    result_id: Uuid,
    analysis_id: Uuid,
    result_digest_hex: String,
}

async fn seed_result(pool: &sqlx::PgPool) -> Result<SeededResult, sqlx::Error> {
    let seeded = SeededResult {
        owner_id: Uuid::now_v7(),
        result_id: Uuid::now_v7(),
        analysis_id: Uuid::now_v7(),
        result_digest_hex: "44".repeat(32),
    };
    let run_id = Uuid::now_v7();
    let manifest_id = Uuid::now_v7();
    sqlx::query(
        "insert into channel_digests.digest_runs \
         (run_id, owner_id, trigger, idempotency_key, window_start, window_end, state) \
         values ($1, $2, 'on_demand', $3, '2026-08-20T10:00:00Z', \
         '2026-08-21T10:00:00Z', 'completed')",
    )
    .bind(run_id)
    .bind(seeded.owner_id)
    .bind(format!("boot-result-{}", seeded.result_id))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_manifests \
         (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_json) \
         values ($1, $2, $3, $4, 1, 1, jsonb_build_object('fixture', true))",
    )
    .bind(manifest_id)
    .bind(run_id)
    .bind(seeded.owner_id)
    .bind("aa".repeat(32))
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_results \
         (result_id, run_id, manifest_id, owner_id, outcome, recap_id, result_digest_hex, \
         citation_count) values ($1, $2, $3, $4, 'completed', $5, $6, 0)",
    )
    .bind(seeded.result_id)
    .bind(run_id)
    .bind(manifest_id)
    .bind(seeded.owner_id)
    .bind(seeded.analysis_id)
    .bind(&seeded.result_digest_hex)
    .execute(pool)
    .await?;
    Ok(seeded)
}

fn assert_result(
    address: SocketAddr,
    seeded: &SeededResult,
    expected: u16,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = authorized_status(
        address,
        &format!("/v1/results/{}", seeded.result_id),
        &seeded.owner_id.to_string(),
    )?;
    assert_eq!(response.0, expected);
    assert!(
        response
            .1
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    if expected != 200 {
        assert!(response.1.ends_with("\r\n\r\n"));
        assert!(!response.1.contains(KNOWLEDGE_SECRET));
        assert!(!response.1.contains("must not escape"));
    }
    Ok(())
}

fn database_url() -> String {
    std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest".to_owned()
    })
}

fn wait_status(
    address: SocketAddr,
    path: &str,
    expected: u16,
    child: &mut Child,
) -> Result<String, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(format!("process exited before readiness: {status}").into());
        }
        if let Ok((status, response)) = http_status(address, path)
            && status == expected
        {
            return Ok(response);
        }
        if Instant::now() >= deadline {
            return Err(format!("{path} did not reach {expected}").into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn http_status(
    address: SocketAddr,
    path: &str,
) -> Result<(u16, String), Box<dyn std::error::Error>> {
    request_status(address, path, None, None)
}

fn authorized_status(
    address: SocketAddr,
    path: &str,
    owner_id: &str,
) -> Result<(u16, String), Box<dyn std::error::Error>> {
    request_status(address, path, Some(SERVICE_SECRET), Some(owner_id))
}

fn request_status(
    address: SocketAddr,
    path: &str,
    secret: Option<&str>,
    owner_id: Option<&str>,
) -> Result<(u16, String), Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
    )?;
    if let Some(secret) = secret {
        write!(stream, "Authorization: Bearer {secret}\r\n")?;
    }
    if let Some(owner_id) = owner_id {
        write!(stream, "X-Ratatoskr-Owner-Id: {owner_id}\r\n")?;
    }
    write!(stream, "\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing status")?
        .parse()?;
    Ok((status, response))
}

fn stop(child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
    let status = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()?;
    if !status.success() {
        return Err("could not signal API process".into());
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("API process exceeded shutdown bound".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}
