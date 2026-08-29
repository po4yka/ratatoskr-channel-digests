//! Canonical immutable source-manifest construction.

use sha2::{Digest as _, Sha256};
use uuid::Uuid;

/// One exact immutable source revision in a recap manifest.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ManifestSource {
    /// Immutable revision identity.
    pub revision_id: Uuid,
    /// Canonical public channel username.
    pub channel_username: String,
    /// Provider message identity.
    pub message_id: i64,
    /// Digest of normalized body bytes.
    pub content_sha256: String,
    /// UTC publication instant.
    pub published_at: String,
    /// Redirect-free public link.
    pub canonical_link: String,
    /// Owned normalized content, available only through authenticated manifest access.
    pub body: String,
}

/// Canonical bytes and exact linkage for one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalManifest {
    /// Stable run identity.
    pub run_id: Uuid,
    /// Closed lower bound.
    pub window_start: String,
    /// Open upper bound.
    pub window_end: String,
    /// Exact canonical JSON bytes.
    pub bytes: Vec<u8>,
    /// Lowercase SHA-256 over [`Self::bytes`].
    pub sha256: String,
    /// Number of selected revisions.
    pub source_count: usize,
    /// Number of represented channels.
    pub channel_count: usize,
}

/// Safe manifest construction failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ManifestError {
    /// Selection exceeds the fixed source or channel bound.
    #[error("manifest selection exceeds its bound")]
    Limit,
    /// Window or source linkage is invalid.
    #[error("manifest source is invalid")]
    Invalid,
}

/// Pure canonical manifest builder.
#[derive(Debug, Default, Clone, Copy)]
pub struct ManifestBuilder;

impl ManifestBuilder {
    /// Builds canonical bytes from exact immutable revisions.
    ///
    /// # Errors
    ///
    /// Returns a finite bound or validation class.
    pub fn build(
        run_id: Uuid,
        window_start: &str,
        window_end: &str,
        mut sources: Vec<ManifestSource>,
    ) -> Result<CanonicalManifest, ManifestError> {
        if sources.len() > 100 {
            return Err(ManifestError::Limit);
        }
        let start = window_start
            .parse::<jiff::Timestamp>()
            .map_err(|_| ManifestError::Invalid)?;
        let end = window_end
            .parse::<jiff::Timestamp>()
            .map_err(|_| ManifestError::Invalid)?;
        if start >= end {
            return Err(ManifestError::Invalid);
        }
        for source in &sources {
            let published = source
                .published_at
                .parse::<jiff::Timestamp>()
                .map_err(|_| ManifestError::Invalid)?;
            let expected_link = format!(
                "https://t.me/{}/{}",
                source.channel_username, source.message_id
            );
            if published < start
                || published >= end
                || source.content_sha256.len() != 64
                || !source
                    .content_sha256
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || source.canonical_link != expected_link
            {
                return Err(ManifestError::Invalid);
            }
        }
        sources.sort_by(|left, right| {
            (
                &left.published_at,
                &left.channel_username,
                left.message_id,
                &left.content_sha256,
                left.revision_id,
            )
                .cmp(&(
                    &right.published_at,
                    &right.channel_username,
                    right.message_id,
                    &right.content_sha256,
                    right.revision_id,
                ))
        });
        let source_count = sources.len();
        let channel_count = sources
            .iter()
            .map(|source| source.channel_username.as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        if channel_count > 20 {
            return Err(ManifestError::Limit);
        }
        let bytes = serde_json::to_vec(&serde_json::json!({
            "run_id": run_id,
            "window": {"start_at": window_start, "end_at": window_end},
            "sources": sources,
        }))
        .map_err(|_| ManifestError::Invalid)?;
        let sha256 = format!("{:x}", Sha256::digest(&bytes));
        Ok(CanonicalManifest {
            run_id,
            window_start: window_start.to_owned(),
            window_end: window_end.to_owned(),
            bytes,
            sha256,
            source_count,
            channel_count,
        })
    }
}
