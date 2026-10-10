//! Shared fixtures of the API process tests: process control, raw HTTP and result seeding.

#![allow(
    dead_code,
    reason = "shared integration helper features are exercised by separate test binaries"
)]

use crate::support::RecordingKnowledge;
use ratatoskr_channel_digests::Database;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use uuid::Uuid;

pub(crate) const SERVICE_SECRET: &str = "synthetic-service-secret";

pub(crate) const KNOWLEDGE_SECRET: &str = "synthetic-knowledge-result-secret";

pub(crate) const KNOWLEDGE_AUTHORIZATION: &str = "Bearer synthetic-knowledge-result-secret";

pub(crate) const RECAP_SENTINEL: &str = "private-recap-must-not-be-stored";

pub(crate) type TestResult = Result<(), Box<dyn std::error::Error>>;

/// A running API process with its Knowledge fake.
pub(crate) struct RunningApi {
    pub(crate) _child: ChildGuard,
    pub(crate) domain: SocketAddr,
    pub(crate) knowledge: RecordingKnowledge,
}

pub(crate) fn start_api(
    responses: HashMap<String, String>,
) -> Result<RunningApi, Box<dyn std::error::Error>> {
    let knowledge = RecordingKnowledge::start(responses, KNOWLEDGE_AUTHORIZATION)?;
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
    Ok(RunningApi {
        _child: child,
        domain,
        knowledge,
    })
}

pub(crate) fn connect_database(
    runtime: &tokio::runtime::Runtime,
) -> Result<Database, Box<dyn std::error::Error>> {
    let database = runtime.block_on(Database::connect(
        &database_url(),
        3,
        Duration::from_secs(2),
    ))?;
    runtime.block_on(database.apply_schema())?;
    Ok(database)
}

pub(crate) fn request_with_headers(
    address: SocketAddr,
    path: &str,
    headers: &[(&str, &str)],
) -> Result<String, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n"
    )?;
    for (name, value) in headers {
        write!(stream, "{name}: {value}\r\n")?;
    }
    write!(stream, "\r\n")?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

pub(crate) fn assert_no_store(response: &str) {
    assert!(
        response
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
}

pub(crate) fn json_body(response: &str) -> Result<Value, Box<dyn std::error::Error>> {
    Ok(serde_json::from_str(response_body(response)?)?)
}

pub(crate) fn response_body(response: &str) -> Result<&str, Box<dyn std::error::Error>> {
    Ok(response
        .split_once("\r\n\r\n")
        .ok_or("missing HTTP body")?
        .1)
}

pub(crate) struct SeededResult {
    pub(crate) owner_id: Uuid,
    pub(crate) result_id: Uuid,
    pub(crate) run_id: Uuid,
    pub(crate) manifest_id: Uuid,
    pub(crate) outcome: &'static str,
    pub(crate) analysis_id: Option<Uuid>,
    pub(crate) result_digest_hex: Option<String>,
    pub(crate) citation_count: i32,
    pub(crate) safe_failure_class: Option<&'static str>,
    pub(crate) recap: Option<Value>,
}

impl SeededResult {
    pub(crate) fn expected_projection(&self) -> Result<Value, Box<dyn std::error::Error>> {
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

    pub(crate) fn knowledge_response(&self) -> Result<String, Box<dyn std::error::Error>> {
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

pub(crate) struct SeededResults {
    pub(crate) completed: SeededResult,
    pub(crate) partial: SeededResult,
    pub(crate) failed: SeededResult,
    pub(crate) foreign: SeededResult,
}

impl SeededResults {
    pub(crate) fn knowledge_responses(
        &self,
    ) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
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

pub(crate) async fn seed_results(pool: &sqlx::PgPool) -> Result<SeededResults, sqlx::Error> {
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

pub(crate) fn successful_seed(
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

pub(crate) async fn insert_seed(
    pool: &sqlx::PgPool,
    result: &SeededResult,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "insert into channel_digests.digest_runs \
         (run_id, owner_id, operation_id, trigger, idempotency_key, window_start, window_end, state) \
         values ($1, $2, $5, 'on_demand', $3, '2026-08-20T10:00:00Z', \
         '2026-08-21T10:00:00Z', $4)",
    )
    .bind(result.run_id)
    .bind(result.owner_id)
    .bind(format!("api-result-{}", result.result_id))
    .bind(result.outcome)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    sqlx::query(
        "insert into channel_digests.digest_manifests \
         (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_text) \
         values ($1, $2, $3, $4, 1, 1, '{\"fixture\":true}')",
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

pub(crate) async fn assert_no_recap_storage(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
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

pub(crate) struct ChildGuard(pub(crate) Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }
}

pub(crate) fn reserve() -> Result<SocketAddr, std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    drop(listener);
    Ok(address)
}

pub(crate) fn database_url() -> String {
    std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest".to_owned()
    })
}

pub(crate) fn request(
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

pub(crate) fn status(response: &str) -> Result<u16, Box<dyn std::error::Error>> {
    Ok(response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("missing status")?
        .parse()?)
}

pub(crate) fn wait_live(
    address: SocketAddr,
    child: &mut Child,
) -> Result<(), Box<dyn std::error::Error>> {
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

pub(crate) fn stop(child: &mut Child) -> Result<(), Box<dyn std::error::Error>> {
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
