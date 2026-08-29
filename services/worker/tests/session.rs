//! Encrypted worker-only session boundary acceptance.

use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;

use chacha20poly1305::aead::{Aead as _, KeyInit as _, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ratatoskr_channel_digests::{Config, Role};

#[test]
fn session_files_are_separate_worker_only_and_fail_closed() -> Result<(), Box<dyn std::error::Error>>
{
    let fixture =
        std::env::temp_dir().join(format!("channel-digest-session-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&fixture)?;
    let session = fixture.join("session.enc");
    let key_path = fixture.join("session.key");

    let missing = worker(&session, &key_path).output()?;
    assert!(!missing.status.success(), "missing files must fail closed");

    let key = [7_u8; 32];
    std::fs::write(&key_path, key)?;
    std::fs::write(&session, b"unique-corrupt-session-marker")?;
    private(&key_path)?;
    private(&session)?;
    let corrupt = worker(&session, &key_path).output()?;
    assert!(!corrupt.status.success());
    let diagnostic = String::from_utf8_lossy(&corrupt.stderr);
    assert!(!diagnostic.contains("unique-corrupt-session-marker"));
    assert!(!diagnostic.contains(session.to_string_lossy().as_ref()));

    write_encrypted_session(&session, &key, b"synthetic-grammers-session")?;
    std::fs::set_permissions(&session, std::fs::Permissions::from_mode(0o644))?;
    assert!(
        !worker(&session, &key_path).status()?.success(),
        "world-readable session refused"
    );
    private(&session)?;
    assert!(worker(&session, &key_path).status()?.success());

    let api = Config::from_environment(
        Role::Api,
        [
            ("RATATOSKR__DATABASE__URL", database_url().as_str()),
            ("RATATOSKR__AUTH__SERVICE_SECRET", "synthetic"),
            (
                "RATATOSKR__PROVIDER__SESSION_FILE",
                session.to_str().ok_or("non-UTF8 fixture")?,
            ),
        ],
    );
    assert!(api.is_err(), "API role must reject provider authority");
    let _ignored = std::fs::remove_dir_all(fixture);
    Ok(())
}

fn worker(session: &Path, key: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ratatoskr-channel-digests-worker"));
    command
        .arg("check-config")
        .env("RATATOSKR__DATABASE__URL", database_url())
        .env("RATATOSKR__AUTH__SERVICE_SECRET", "synthetic")
        .env("RATATOSKR__PROVIDER__API_ID", "12345")
        .env("RATATOSKR__PROVIDER__API_HASH", "synthetic")
        .env("RATATOSKR__PROVIDER__SESSION_FILE", session)
        .env("RATATOSKR__PROVIDER__SESSION_KEY_FILE", key)
        .env("RATATOSKR__BUS__ENDPOINT", "nats://127.0.0.1:1");
    command
}

fn database_url() -> String {
    std::env::var("CHANNEL_DIGEST_TEST_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://channel_digest:channel_digest@127.0.0.1:15435/channel_digest".to_owned()
    })
}

fn private(path: &Path) -> std::io::Result<()> {
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

fn write_encrypted_session(
    path: &Path,
    key: &[u8; 32],
    plaintext: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let nonce = [3_u8; 24];
    let cipher = XChaCha20Poly1305::new(key.into());
    let ciphertext = cipher
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: b"ratatoskr-channel-digests-session-v1",
            },
        )
        .map_err(|_| "fixture encryption failed")?;
    let mut bytes = nonce.to_vec();
    bytes.extend_from_slice(&ciphertext);
    std::fs::write(path, bytes)?;
    Ok(())
}
