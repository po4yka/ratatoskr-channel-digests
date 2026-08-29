//! Deterministic recording loopback Knowledge fake.

#![allow(
    dead_code,
    reason = "shared integration helper features are exercised by separate test binaries"
)]

use std::collections::HashMap;
use std::io::{Read as _, Write as _};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// One deterministic response selected for the next Knowledge request.
#[derive(Debug, Clone)]
pub(super) enum FakeResponse {
    /// Send one complete HTTP response.
    Body {
        /// HTTP status line suffix.
        status: &'static str,
        /// Exact response media type.
        content_type: &'static str,
        /// Response body bytes.
        body: Vec<u8>,
        /// Optional redirect location.
        location: Option<String>,
    },
    /// Close the accepted connection without a response.
    Disconnect,
    /// Hold the accepted connection until the test releases it.
    Hold,
}

impl FakeResponse {
    /// Builds one fixed body response.
    pub(super) fn body(status: &'static str, content_type: &'static str, body: Vec<u8>) -> Self {
        Self::Body {
            status,
            content_type,
            body,
            location: None,
        }
    }

    /// Builds a same-origin redirect trap.
    pub(super) fn redirect(base_url: &str, private_body: &str) -> Self {
        Self::Body {
            status: "302 Found",
            content_type: "text/plain",
            body: private_body.as_bytes().to_vec(),
            location: Some(format!("{base_url}/redirect-trap")),
        }
    }
}

/// Single-threaded loopback fake that records every non-control request.
#[derive(Debug)]
pub(super) struct RecordingKnowledge {
    address: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    response: Arc<Mutex<Option<FakeResponse>>>,
    hold_release: mpsc::Sender<()>,
    thread: Option<JoinHandle<std::io::Result<()>>>,
}

impl RecordingKnowledge {
    /// Binds the fake before returning and starts its recording thread.
    pub(super) fn start(
        responses: HashMap<String, String>,
        authorization: &'static str,
    ) -> Result<Self, std::io::Error> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let requests = Arc::new(Mutex::new(Vec::new()));
        let server_requests = requests.clone();
        let response = Arc::new(Mutex::new(None));
        let server_response = response.clone();
        let (hold_release, hold_receiver) = mpsc::channel();
        let thread = thread::spawn(move || {
            serve(
                &listener,
                &responses,
                &server_requests,
                &server_response,
                &hold_receiver,
                authorization,
            )
        });
        Ok(Self {
            address,
            requests,
            response,
            hold_release,
            thread: Some(thread),
        })
    }

    /// Returns the bound loopback HTTP origin.
    pub(super) fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    /// Returns the number of recorded domain requests.
    pub(super) fn request_count(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| "Knowledge request recorder is poisoned")?
            .len())
    }

    /// Returns a snapshot of recorded raw request headers.
    pub(super) fn requests(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| "Knowledge request recorder is poisoned")?
            .clone())
    }

    /// Selects a deterministic response for subsequent requests.
    pub(super) fn respond_with(
        &self,
        response: FakeResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        *self
            .response
            .lock()
            .map_err(|_| "Knowledge response mode is poisoned")? = Some(response);
        Ok(())
    }

    /// Releases one held response without timing-based synchronization.
    pub(super) fn release_hold(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.hold_release
            .send(())
            .map_err(|_| "Knowledge hold channel is closed".into())
    }

    /// Stops and joins the fake thread.
    pub(super) fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.release_hold()?;
        signal_stop(self.address)?;
        let thread = self.thread.take().ok_or("Knowledge fake is not running")?;
        thread
            .join()
            .map_err(|_| "Knowledge fake thread panicked")??;
        Ok(())
    }
}

impl Drop for RecordingKnowledge {
    fn drop(&mut self) {
        if let Some(thread) = self.thread.take() {
            let _ = self.hold_release.send(());
            drop(signal_stop(self.address));
            drop(thread.join());
        }
    }
}

fn serve(
    listener: &TcpListener,
    responses: &HashMap<String, String>,
    requests: &Arc<Mutex<Vec<String>>>,
    response: &Arc<Mutex<Option<FakeResponse>>>,
    hold_receiver: &mpsc::Receiver<()>,
    authorization: &str,
) -> std::io::Result<()> {
    loop {
        let (mut stream, _) = listener.accept()?;
        stream.set_read_timeout(Some(Duration::from_secs(2)))?;
        let request = read_headers(&mut stream)?;
        let request_line = request.lines().next().unwrap_or_default();
        if request_line == "GET /__stop HTTP/1.1" {
            return Ok(());
        }
        requests
            .lock()
            .map_err(|_| std::io::Error::other("Knowledge request recorder is poisoned"))?
            .push(request.clone());
        let selected = response
            .lock()
            .map_err(|_| std::io::Error::other("Knowledge response mode is poisoned"))?
            .clone();
        match selected {
            Some(FakeResponse::Disconnect) => continue,
            Some(FakeResponse::Hold) => {
                hold_receiver.recv().map_err(|_| {
                    std::io::Error::other("Knowledge hold channel closed unexpectedly")
                })?;
                continue;
            }
            Some(FakeResponse::Body {
                status,
                content_type,
                body,
                location,
            }) => {
                write_response(
                    &mut stream,
                    status,
                    content_type,
                    &body,
                    location.as_deref(),
                )?;
                continue;
            }
            None => {}
        }
        let authorized = request.lines().any(|line| {
            line.split_once(':').is_some_and(|(name, value)| {
                name.eq_ignore_ascii_case("authorization") && value.trim() == authorization
            })
        });
        let path = request_line.split_whitespace().nth(1).unwrap_or_default();
        let (status, body) = if !authorized {
            ("401 Unauthorized", "{}")
        } else if let Some(body) = responses.get(path) {
            ("200 OK", body.as_str())
        } else {
            ("404 Not Found", "{}")
        };
        write_response(
            &mut stream,
            status,
            "application/json",
            body.as_bytes(),
            None,
        )?;
    }
}

fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    location: Option<&str>,
) -> std::io::Result<()> {
    let location = location.map_or_else(String::new, |value| format!("Location: {value}\r\n"));
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{location}Connection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)?;
    stream.flush()
}

fn read_headers(stream: &mut TcpStream) -> std::io::Result<String> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1_024];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        let bytes = chunk.get(..read).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Knowledge fake reported an invalid read length",
            )
        })?;
        request.extend_from_slice(bytes);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
        if request.len() > 16_384 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Knowledge fake request headers are oversized",
            ));
        }
    }
    String::from_utf8(request).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Knowledge fake request headers are not UTF-8",
        )
    })
}

fn signal_stop(address: SocketAddr) -> std::io::Result<()> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    stream.write_all(b"GET /__stop HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
}
