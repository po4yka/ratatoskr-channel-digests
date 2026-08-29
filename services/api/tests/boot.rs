//! Real API-role process lifecycle behavior.

use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn api_and_worker_roles_are_separate_ready_and_bounded() -> Result<(), Box<dyn std::error::Error>> {
    let api = TcpListener::bind("127.0.0.1:0")?;
    let api_address = api.local_addr()?;
    let operator = TcpListener::bind("127.0.0.1:0")?;
    let operator_address = operator.local_addr()?;
    let mut check = configured(api_address, operator_address);
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

    let mut child = configured(api_address, operator_address)
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
    stop(&mut child)?;
    Ok(())
}

fn configured(api: SocketAddr, operator: SocketAddr) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-api"));
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
        .env("RATATOSKR__API__LISTEN_ADDRESS", api.to_string())
        .env("RATATOSKR__OPERATOR__LISTEN_ADDRESS", operator.to_string());
    command
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
