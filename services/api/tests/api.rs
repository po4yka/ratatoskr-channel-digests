//! Loopback service-auth and owner-scope acceptance.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use uuid::Uuid;

#[test]
fn routes_require_service_and_owner_scope() -> Result<(), Box<dyn std::error::Error>> {
    let domain = reserve()?;
    let operator = reserve()?;
    let mut child = Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-api"))
        .env("RATATOSKR__DATABASE__URL", database_url())
        .env(
            "RATATOSKR__AUTH__SERVICE_SECRET",
            "synthetic-service-secret",
        )
        .env("RATATOSKR__API__LISTEN_ADDRESS", domain.to_string())
        .env("RATATOSKR__OPERATOR__LISTEN_ADDRESS", operator.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    wait_live(operator, &mut child)?;

    let unauthenticated = request(domain, "/v1/subscriptions?page_size=10", None, None)?;
    assert_eq!(status(&unauthenticated)?, 401);
    let owner = Uuid::now_v7().to_string();
    let authorized = request(
        domain,
        "/v1/subscriptions?page_size=10",
        Some("synthetic-service-secret"),
        Some(&owner),
    )?;
    assert_eq!(status(&authorized)?, 200);
    assert!(
        authorized
            .to_ascii_lowercase()
            .contains("cache-control: no-store")
    );
    assert!(!authorized.contains("synthetic-service-secret"));

    let missing = request(
        domain,
        &format!("/v1/manifests/{}", Uuid::now_v7()),
        Some("synthetic-service-secret"),
        Some(&owner),
    )?;
    assert_eq!(status(&missing)?, 404);
    stop(&mut child)?;
    Ok(())
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
