//! Fail-closed encrypted provider-session boundary.

use std::fmt;
use std::os::unix::fs::MetadataExt as _;
use std::path::Path;

use chacha20poly1305::aead::{Aead as _, KeyInit as _, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use zeroize::Zeroizing;

use crate::config::ProviderConfig;

const NONCE_BYTES: usize = 24;
const KEY_BYTES: usize = 32;
const MAX_SESSION_BYTES: u64 = 1_048_576;
const AAD: &[u8] = b"ratatoskr-channel-digests-session-v1";

/// Decrypted session bytes whose debug representation and drop behavior reveal no material.
pub struct SessionMaterial(Zeroizing<Vec<u8>>);

impl fmt::Debug for SessionMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SessionMaterial([redacted])")
    }
}

impl SessionMaterial {
    /// Reads two private regular files and authenticates the bounded ciphertext.
    ///
    /// # Errors
    ///
    /// Returns only a finite safe class; neither paths nor bytes are formatted.
    pub fn load(config: &ProviderConfig) -> Result<Self, SessionError> {
        let key = read_private_file(&config.session_key_file, KEY_BYTES as u64)?;
        if key.len() != KEY_BYTES {
            return Err(SessionError::Invalid);
        }
        let encrypted = read_private_file(&config.session_file, MAX_SESSION_BYTES)?;
        if encrypted.len() <= NONCE_BYTES {
            return Err(SessionError::Invalid);
        }
        let (nonce, ciphertext) = encrypted.split_at(NONCE_BYTES);
        let cipher = XChaCha20Poly1305::new_from_slice(&key).map_err(|_| SessionError::Invalid)?;
        let plaintext = cipher
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: ciphertext,
                    aad: AAD,
                },
            )
            .map_err(|_| SessionError::Invalid)?;
        if plaintext.is_empty() {
            return Err(SessionError::Invalid);
        }
        Ok(Self(Zeroizing::new(plaintext)))
    }

    /// Borrows authenticated bytes only inside the provider adapter.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Safe session readiness failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    /// File is absent, not private/regular/bounded, or not authenticated ciphertext.
    #[error("provider session is unavailable")]
    Invalid,
}

fn read_private_file(path: &Path, maximum: u64) -> Result<Zeroizing<Vec<u8>>, SessionError> {
    let metadata = std::fs::metadata(path).map_err(|_| SessionError::Invalid)?;
    if !metadata.is_file()
        || metadata.len() == 0
        || metadata.len() > maximum
        || metadata.mode() & 0o077 != 0
        || metadata.mode() & 0o400 == 0
    {
        return Err(SessionError::Invalid);
    }
    std::fs::read(path)
        .map(Zeroizing::new)
        .map_err(|_| SessionError::Invalid)
}
