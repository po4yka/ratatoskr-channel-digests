//! Canonical immutable source-manifest construction over the contract artifact.

use std::collections::BTreeSet;

use ratatoskr_channel_digest_contracts::{
    ChannelDigestManifest, ChannelDigestManifestRef, ChannelDigestManifestSchema,
    ChannelDigestManifestSource, ChannelDigestRunId, DigestWindow, MAX_CHANNELS, MAX_CONTENT_BYTES,
    MAX_SOURCES, sha256_hex,
};
use ratatoskr_identifiers::{
    ContentDigest, DigestAlgorithm, DigestHex, TenantRef, UserId, WireTimestamp,
};
use uuid::Uuid;

/// One exact immutable source revision resolved for a recap manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSource {
    /// Immutable revision identity.
    pub revision_id: Uuid,
    /// Owned channel identity.
    pub channel_id: Uuid,
    /// Canonical public channel username.
    pub channel_username: String,
    /// Provider display name of the channel, when known.
    pub channel_display_name: Option<String>,
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
    /// One-based ordinal of this body among all observed revisions of the message.
    pub revision_index: u32,
}

/// Canonical text and exact linkage for one run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalManifest {
    /// Identity the manifest carries as its own reference.
    pub manifest_id: Uuid,
    /// Owning user.
    pub owner_id: Uuid,
    /// Stable run identity.
    pub run_id: Uuid,
    /// Exact canonical manifest text, stored and served unchanged.
    pub text: String,
    /// Lowercase SHA-256 over [`Self::text`].
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
    /// Selection exceeds the fixed source, channel, or content bound.
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
    /// Builds the contract manifest from exact immutable revisions.
    ///
    /// # Errors
    ///
    /// Returns a finite bound or validation class. An empty selection is not a manifest.
    pub fn build(
        manifest_id: Uuid,
        owner_id: Uuid,
        run_id: Uuid,
        window_start: &str,
        window_end: &str,
        sources: &[ManifestSource],
    ) -> Result<CanonicalManifest, ManifestError> {
        if sources.len() > MAX_SOURCES {
            return Err(ManifestError::Limit);
        }
        let channel_count = sources
            .iter()
            .map(|source| source.channel_id)
            .collect::<BTreeSet<_>>()
            .len();
        if channel_count > MAX_CHANNELS {
            return Err(ManifestError::Limit);
        }
        let window = DigestWindow::new(instant(window_start)?, instant(window_end)?)
            .map_err(|_| ManifestError::Invalid)?;
        let mut selected = sources
            .iter()
            .map(contract_source)
            .collect::<Result<Vec<_>, _>>()?;
        selected.sort_by(|left, right| order_key(left).cmp(&order_key(right)));
        let manifest = ChannelDigestManifest {
            schema: ChannelDigestManifestSchema::V1,
            manifest_ref: ChannelDigestManifestRef::parse(&format!(
                "channel-digest-manifest:{manifest_id}"
            ))
            .map_err(|_| ManifestError::Invalid)?,
            owner: TenantRef::of_user(UserId(owner_id)),
            digest_run_id: ChannelDigestRunId::parse(&run_id.to_string())
                .map_err(|_| ManifestError::Invalid)?,
            window,
            sources: selected,
        };
        let bytes = manifest
            .to_canonical_bytes()
            .map_err(|_| ManifestError::Invalid)?;
        let text = String::from_utf8(bytes).map_err(|_| ManifestError::Invalid)?;
        Ok(CanonicalManifest {
            manifest_id,
            owner_id,
            run_id,
            sha256: sha256_hex(text.as_bytes()),
            source_count: manifest.sources.len(),
            channel_count,
            text,
        })
    }
}

fn instant(raw: &str) -> Result<WireTimestamp, ManifestError> {
    WireTimestamp::parse(raw).map_err(|_| ManifestError::Invalid)
}

fn contract_source(source: &ManifestSource) -> Result<ChannelDigestManifestSource, ManifestError> {
    if source.body.len() > MAX_CONTENT_BYTES {
        return Err(ManifestError::Limit);
    }
    let expected_link = format!(
        "https://t.me/{}/{}",
        source.channel_username, source.message_id
    );
    let digest = sha256_hex(source.body.as_bytes());
    if source.canonical_link != expected_link || source.content_sha256 != digest {
        return Err(ManifestError::Invalid);
    }
    let label: String = source
        .channel_display_name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or(&source.channel_username)
        .chars()
        .take(80)
        .collect();
    Ok(ChannelDigestManifestSource {
        revision_ref: format!("channel-post-revision:{}", source.revision_id),
        channel_ref: format!("telegram-public-channel:{}", source.channel_id),
        channel_label: label,
        message_id: source.message_id.to_string(),
        published_at: instant(&source.published_at)?,
        content: source.body.clone(),
        content_digest: ContentDigest {
            algorithm: DigestAlgorithm::Sha256,
            hex: DigestHex::parse(&digest).map_err(|_| ManifestError::Invalid)?,
        },
        public_link: Some(source.canonical_link.clone()),
        revision: source.revision_index,
    })
}

fn order_key(source: &ChannelDigestManifestSource) -> (WireTimestamp, &str, u128, u32) {
    (
        source.published_at,
        source.channel_ref.as_str(),
        source.message_id.parse().unwrap_or(u128::MAX),
        source.revision,
    )
}
