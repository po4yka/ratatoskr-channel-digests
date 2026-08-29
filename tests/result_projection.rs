//! Bounded authenticated Knowledge result-reader acceptance.

use std::io;

use ratatoskr_channel_digests::{
    Config, KnowledgeResultProjection, KnowledgeResultReadError, KnowledgeResultReader, Role,
};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

const READER_SECRET: &str = "result-reader-LEAKME";
const API_SERVICE_SECRET: &str = "api-service-LEAKME";
const PRIVATE_BODY: &str = "private-recap-LEAKME";

#[derive(Debug)]
struct ObservedRequest {
    request_line: String,
    authorization: Option<String>,
    accept: Option<String>,
    raw: String,
}

enum FakeReply {
    Json(Value),
    Status(&'static str, Vec<u8>),
    Redirect,
    Hold,
    Disconnect,
}

struct FakeServer {
    base_url: String,
    stop: oneshot::Sender<()>,
    task: JoinHandle<io::Result<Vec<ObservedRequest>>>,
}

impl FakeServer {
    async fn spawn(reply: FakeReply) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let base_url = format!("http://{address}");
        let redirect_location = format!("{base_url}/redirect-trap");
        let (stop, stop_receiver) = oneshot::channel();
        let task = tokio::spawn(serve(listener, reply, redirect_location, stop_receiver));
        Ok(Self {
            base_url,
            stop,
            task,
        })
    }

    async fn finish(self) -> Result<Vec<ObservedRequest>, Box<dyn std::error::Error>> {
        let _ = self.stop.send(());
        Ok(self.task.await??)
    }
}

async fn serve(
    listener: TcpListener,
    reply: FakeReply,
    redirect_location: String,
    mut stop: oneshot::Receiver<()>,
) -> io::Result<Vec<ObservedRequest>> {
    let first = tokio::select! {
        accepted = listener.accept() => Some(accepted?),
        _ = &mut stop => None,
    };
    let Some((mut stream, _)) = first else {
        return Ok(Vec::new());
    };
    let mut requests = vec![read_request(&mut stream).await?];

    match reply {
        FakeReply::Json(value) => {
            write_response(
                &mut stream,
                "200 OK",
                "application/json",
                &serde_json::to_vec(&value).map_err(io::Error::other)?,
                None,
            )
            .await?;
        }
        FakeReply::Status(status, body) => {
            write_response(&mut stream, status, "application/json", &body, None).await?;
        }
        FakeReply::Redirect => {
            write_response(
                &mut stream,
                "302 Found",
                "text/plain",
                b"redirect",
                Some(&redirect_location),
            )
            .await?;
        }
        FakeReply::Hold => {
            drop(stop.await);
            return Ok(requests);
        }
        FakeReply::Disconnect => return Ok(requests),
    }
    drop(stream);

    let second = tokio::select! {
        accepted = listener.accept() => Some(accepted?),
        _ = &mut stop => None,
    };
    if let Some((mut stream, _)) = second {
        requests.push(read_request(&mut stream).await?);
        write_response(
            &mut stream,
            "502 Bad Gateway",
            "text/plain",
            b"unexpected second request",
            None,
        )
        .await?;
    }
    Ok(requests)
}

async fn read_request(stream: &mut TcpStream) -> io::Result<ObservedRequest> {
    let mut bytes = Vec::with_capacity(1_024);
    loop {
        let read = stream.read_buf(&mut bytes).await?;
        if read == 0 || bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if bytes.len() > 16_384 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request headers exceed fake-server limit",
            ));
        }
    }
    let raw = String::from_utf8(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "request is not UTF-8"))?;
    let mut lines = raw.split("\r\n");
    let request_line = lines.next().unwrap_or_default().to_owned();
    let mut authorization = None;
    let mut accept = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("authorization") {
            authorization = Some(value.trim().to_owned());
        } else if name.eq_ignore_ascii_case("accept") {
            accept = Some(value.trim().to_owned());
        }
    }
    Ok(ObservedRequest {
        request_line,
        authorization,
        accept,
        raw,
    })
}

async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    location: Option<&str>,
) -> io::Result<()> {
    let location = location.map_or_else(String::new, |value| format!("Location: {value}\r\n"));
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{location}Connection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await
}

fn reader_config(
    base_url: &str,
    request_timeout_ms: u64,
) -> Result<Config, Box<dyn std::error::Error>> {
    Ok(Config::from_environment(
        Role::Api,
        [
            (
                "RATATOSKR__DATABASE__URL",
                "postgres://fixture.invalid/digests".to_owned(),
            ),
            (
                "RATATOSKR__AUTH__SERVICE_SECRET",
                API_SERVICE_SECRET.to_owned(),
            ),
            ("RATATOSKR__KNOWLEDGE__BASE_URL", base_url.to_owned()),
            (
                "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
                READER_SECRET.to_owned(),
            ),
            ("RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS", "10".to_owned()),
            (
                "RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS",
                request_timeout_ms.to_string(),
            ),
            (
                "RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES",
                "65536".to_owned(),
            ),
        ],
    )?)
}

async fn read_from_fake(
    reply: FakeReply,
    analysis_id: Uuid,
    expected_digest_hex: &str,
    request_timeout_ms: u64,
) -> Result<
    (
        Result<KnowledgeResultProjection, KnowledgeResultReadError>,
        Vec<ObservedRequest>,
    ),
    Box<dyn std::error::Error>,
> {
    let server = FakeServer::spawn(reply).await?;
    let config = reader_config(&server.base_url, request_timeout_ms)?;
    let reader = KnowledgeResultReader::from_config(&config)?;
    let result = reader.read(analysis_id, expected_digest_hex).await;
    let requests = server.finish().await?;
    Ok((result, requests))
}

fn envelope(analysis_id: Uuid, digest_hex: &str) -> Value {
    json!({
        "analysis_id": analysis_id,
        "result_digest": {"algorithm": "sha256", "hex": digest_hex},
        "recap": {"summary": PRIVATE_BODY, "citations": []}
    })
}

async fn verify_failure_matrix(
    analysis_id: Uuid,
    digest_hex: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let analysis_id_text = analysis_id.to_string();
    let mut unknown_field = envelope(analysis_id, digest_hex);
    set_field(&mut unknown_field, "unexpected", json!(PRIVATE_BODY))?;
    let mut invalid_digest = envelope(analysis_id, digest_hex);
    set_nested_field(
        &mut invalid_digest,
        "result_digest",
        "hex",
        json!("AA".repeat(32)),
    )?;
    let mismatched_analysis = envelope(Uuid::now_v7(), digest_hex);
    let mismatched_digest = envelope(analysis_id, &"44".repeat(32));
    let cases = [
        (
            "redirect",
            FakeReply::Redirect,
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "timeout",
            FakeReply::Hold,
            25,
            KnowledgeResultReadError::Unavailable,
        ),
        (
            "oversized body",
            FakeReply::Status("200 OK", vec![b'x'; 65_537]),
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "unknown envelope field",
            FakeReply::Json(unknown_field),
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "invalid digest",
            FakeReply::Json(invalid_digest),
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "analysis mismatch",
            FakeReply::Json(mismatched_analysis),
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "digest mismatch",
            FakeReply::Json(mismatched_digest),
            500,
            KnowledgeResultReadError::Invalid,
        ),
        (
            "upstream unavailable",
            FakeReply::Status("503 Service Unavailable", PRIVATE_BODY.as_bytes().to_vec()),
            500,
            KnowledgeResultReadError::Unavailable,
        ),
        (
            "disconnect",
            FakeReply::Disconnect,
            500,
            KnowledgeResultReadError::Unavailable,
        ),
    ];
    for (name, reply, request_timeout_ms, expected_error) in cases {
        let (result, requests) =
            read_from_fake(reply, analysis_id, digest_hex, request_timeout_ms).await?;
        let error = result
            .err()
            .ok_or_else(|| format!("{name} response unexpectedly succeeded"))?;
        assert_eq!(error, expected_error, "wrong safe class for {name}");
        let diagnostic = error.to_string();
        assert!(!diagnostic.contains(READER_SECRET));
        assert!(!diagnostic.contains(PRIVATE_BODY));
        assert!(!diagnostic.contains(analysis_id_text.as_str()));
        assert_eq!(
            requests.len(),
            1,
            "{name} must not redirect, retry, or issue a second request"
        );
    }
    Ok(())
}

fn set_field(value: &mut Value, field: &str, replacement: Value) -> Result<(), &'static str> {
    value
        .as_object_mut()
        .ok_or("fixture envelope must be an object")?
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
        .ok_or("fixture envelope field must be an object")?
        .insert(field.to_owned(), replacement);
    Ok(())
}

#[tokio::test]
async fn knowledge_reader_is_authenticated_bounded_and_integrity_checked()
-> Result<(), Box<dyn std::error::Error>> {
    let analysis_id = Uuid::now_v7();
    let digest_hex = "22".repeat(32);
    let expected_recap = json!({"summary": PRIVATE_BODY, "citations": []});
    let (success, requests) = read_from_fake(
        FakeReply::Json(envelope(analysis_id, &digest_hex)),
        analysis_id,
        &digest_hex,
        500,
    )
    .await?;
    let projection =
        success.map_err(|error| format!("successful Knowledge result read failed: {error:?}"))?;
    assert_eq!(
        projection,
        KnowledgeResultProjection {
            analysis_id,
            result_digest_hex: digest_hex.clone(),
            recap: expected_recap,
        }
    );
    assert_eq!(
        requests.len(),
        1,
        "one read must perform exactly one request"
    );
    let request = requests.first().ok_or("success request was not recorded")?;
    assert_eq!(
        request.request_line,
        format!("GET /internal/channel-digest-results/{analysis_id} HTTP/1.1")
    );
    assert_eq!(
        request.authorization.as_deref(),
        Some("Bearer result-reader-LEAKME")
    );
    assert_eq!(request.accept.as_deref(), Some("application/json"));
    assert!(!request.raw.contains(API_SERVICE_SECRET));

    verify_failure_matrix(analysis_id, &digest_hex).await?;

    Ok(())
}
