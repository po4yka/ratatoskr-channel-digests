//! Real worker-role process lifecycle behavior.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt as _;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use chacha20poly1305::aead::{Aead as _, KeyInit as _, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use futures_util::StreamExt as _;

#[test]
fn worker_liveness_is_separate_from_dependency_readiness_and_drains()
-> Result<(), Box<dyn std::error::Error>> {
    let operator = TcpListener::bind("127.0.0.1:0")?;
    let operator_address = operator.local_addr()?;
    let fixture =
        std::env::temp_dir().join(format!("channel-digest-worker-{}", operator_address.port()));
    std::fs::create_dir_all(&fixture)?;
    let session = fixture.join("session.enc");
    let key = fixture.join("session.key");
    let session_key = [7_u8; 32];
    std::fs::write(&key, session_key)?;
    let cipher = XChaCha20Poly1305::new((&session_key).into());
    let nonce = [3_u8; 24];
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: b"synthetic-session",
                aad: b"ratatoskr-channel-digests-session-v1",
            },
        )
        .map_err(|_| "fixture encryption failed")?;
    let mut encrypted = nonce.to_vec();
    encrypted.extend_from_slice(&ciphertext);
    std::fs::write(&session, encrypted)?;
    std::fs::set_permissions(&session, std::fs::Permissions::from_mode(0o600))?;
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600))?;

    let mut check = configured(operator_address, &session, &key);
    assert!(check.arg("check-config").status()?.success());
    drop(operator);
    let mut child = configured(operator_address, &session, &key)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_status(operator_address, "/live", 200, &mut child)?;
    let response = wait_status(operator_address, "/ready", 503, &mut child)?;
    assert!(
        response
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    stop(&mut child)?;
    let _ignored = std::fs::remove_dir_all(fixture);
    Ok(())
}

fn configured(operator: SocketAddr, session: &std::path::Path, key: &std::path::Path) -> Command {
    configured_with_bus(operator, session, key, "nats://127.0.0.1:1")
}

fn configured_with_bus(
    operator: SocketAddr,
    session: &std::path::Path,
    key: &std::path::Path,
    bus_endpoint: &str,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-worker"));
    command
        .env(
            "RATATOSKR__DATABASE__URL",
            std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest".to_owned()
            }),
        )
        .env(
            "RATATOSKR__AUTH__SERVICE_SECRET",
            "synthetic-service-secret",
        )
        .env("RATATOSKR__PROVIDER__API_ID", "12345")
        .env("RATATOSKR__PROVIDER__API_HASH", "synthetic-api-hash")
        .env("RATATOSKR__PROVIDER__SESSION_FILE", session)
        .env("RATATOSKR__PROVIDER__SESSION_KEY_FILE", key)
        .env("RATATOSKR__BUS__ENDPOINT", bus_endpoint)
        .env("RATATOSKR__OPERATOR__LISTEN_ADDRESS", operator.to_string());
    command
}

#[tokio::test]
async fn worker_consumes_only_preprovisioned_topology_while_provider_is_unready()
-> Result<(), Box<dyn std::error::Error>> {
    let nats_url = std::env::var("CHANNEL_DIGEST_TEST_NATS_URL")
        .unwrap_or_else(|_| "nats://127.0.0.1:14224".to_owned());
    provision_topology(&nats_url).await?;
    let client = async_nats::connect(&nats_url).await?;
    let mut reports = client
        .subscribe("evt.platform.operation.reported.v1")
        .await?;
    let operator = TcpListener::bind("127.0.0.1:0")?;
    let operator_address = operator.local_addr()?;
    let fixture = std::env::temp_dir().join(format!(
        "channel-digest-worker-bus-{}",
        operator_address.port()
    ));
    std::fs::create_dir_all(&fixture)?;
    let session = fixture.join("session.enc");
    let key = fixture.join("session.key");
    let session_key = [9_u8; 32];
    std::fs::write(&key, session_key)?;
    let cipher = XChaCha20Poly1305::new((&session_key).into());
    let nonce = [4_u8; 24];
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: b"synthetic-invalid-provider-session",
                aad: b"ratatoskr-channel-digests-session-v1",
            },
        )
        .map_err(|_| "fixture encryption failed")?;
    let mut encrypted = nonce.to_vec();
    encrypted.extend_from_slice(&ciphertext);
    std::fs::write(&session, encrypted)?;
    std::fs::set_permissions(&session, std::fs::Permissions::from_mode(0o600))?;
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600))?;
    drop(operator);
    let mut child = configured_with_bus(operator_address, &session, &key, &nats_url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_status(operator_address, "/live", 200, &mut child)?;
    wait_status(operator_address, "/ready", 503, &mut child)?;

    let command_id = uuid::Uuid::now_v7();
    let operation_id = uuid::Uuid::now_v7();
    let owner = uuid::Uuid::now_v7();
    let envelope = serde_json::to_vec(&serde_json::json!({
        "command_id": command_id,
        "command_type": "channel_digest.subscription.set_requested.v1",
        "issued_at": "2026-08-29T10:00:00Z",
        "producer": "ratatoskr-platform",
        "aggregate_id": format!("channel-digest-subscription:{}", uuid::Uuid::now_v7()),
        "correlation_id": format!("operation:{operation_id}"),
        "tenant_id": format!("user:{owner}"),
        "schema_version": 1,
        "payload": {
            "operation_id": operation_id,
            "owner": format!("user:{owner}"),
            "idempotency_key": format!("worker-bus-{command_id}"),
            "channel_username": "worker_bus_channel",
            "desired_state": "active"
        }
    }))?;
    let context = async_nats::jetstream::new(client);
    context
        .publish(
            "cmd.channel_digest.subscription.set_requested.v1",
            envelope.into(),
        )
        .await?
        .await?;
    let report = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let message = reports
                .next()
                .await
                .ok_or("operation report stream ended")?;
            let value: serde_json::Value =
                serde_json::from_slice(&message.payload).map_err(|_| "invalid report envelope")?;
            if value.pointer("/payload/operation_id") == Some(&serde_json::json!(operation_id)) {
                return Ok::<serde_json::Value, &'static str>(value);
            }
        }
    })
    .await??;
    assert_eq!(
        report.pointer("/payload/status"),
        Some(&serde_json::json!("completed"))
    );
    assert_eq!(
        report.get("tenant_id"),
        Some(&serde_json::json!(format!("user:{owner}")))
    );
    stop(&mut child)?;
    let _ignored = std::fs::remove_dir_all(fixture);
    Ok(())
}

async fn provision_topology(nats_url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let context = async_nats::jetstream::new(async_nats::connect(nats_url).await?);
    let commands = context
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_commands".to_owned(),
            subjects: vec!["cmd.>".to_owned()],
            ..async_nats::jetstream::stream::Config::default()
        })
        .await?;
    for (durable, subject) in [
        (
            "ratatoskr_channel_digest_subscriptions",
            "cmd.channel_digest.subscription.set_requested.v1",
        ),
        (
            "ratatoskr_channel_digest_runs",
            "cmd.channel_digest.run.requested.v1",
        ),
    ] {
        commands
            .get_or_create_consumer(
                durable,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(durable.to_owned()),
                    filter_subject: subject.to_owned(),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..async_nats::jetstream::consumer::pull::Config::default()
                },
            )
            .await?;
    }
    let events = context
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: "ratatoskr_events".to_owned(),
            subjects: vec!["evt.>".to_owned()],
            ..async_nats::jetstream::stream::Config::default()
        })
        .await?;
    for (durable, subject) in [
        (
            "ratatoskr_channel_digest_recap_completed",
            "evt.knowledge.channel_digest_recap.completed.v1",
        ),
        (
            "ratatoskr_channel_digest_recap_failed",
            "evt.knowledge.channel_digest_recap.failed.v1",
        ),
    ] {
        events
            .get_or_create_consumer(
                durable,
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some(durable.to_owned()),
                    filter_subject: subject.to_owned(),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ..async_nats::jetstream::consumer::pull::Config::default()
                },
            )
            .await?;
    }
    Ok(())
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
            return Err(format!("worker exited before status: {status}").into());
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
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )?;
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
        return Err("could not signal worker process".into());
    }
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait()?.is_none() {
        if Instant::now() >= deadline {
            child.kill()?;
            return Err("worker exceeded shutdown bound".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Ok(())
}
