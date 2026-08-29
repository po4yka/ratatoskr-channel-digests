//! Narrow public-channel provider capability.

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use grammers_client::peer::Peer;
use grammers_client::sender::SenderPool;
use grammers_mtsender::InvocationError;
use grammers_session::SessionData;
use grammers_session::storages::MemorySession;
use grammers_session::types::{DcOption, PeerInfo, UpdatesState};

use crate::SessionMaterial;

/// Canonical public username admitted before any provider call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicChannelUsername(String);

impl PublicChannelUsername {
    /// Parses a prospective public username.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError::InvalidChannel`] for invalid locators.
    pub fn parse(candidate: &str) -> Result<Self, ProviderError> {
        let bytes = candidate.as_bytes();
        if !(5..=32).contains(&bytes.len())
            || !bytes.first().is_some_and(u8::is_ascii_alphabetic)
            || bytes
                .iter()
                .any(|byte| !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_'))
        {
            Err(ProviderError::InvalidChannel)
        } else {
            Ok(Self(candidate.to_owned()))
        }
    }

    /// Returns the provider-facing canonical username.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Safe provider failure vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProviderError {
    /// Locator is not a canonical public username.
    #[error("invalid public channel")]
    InvalidChannel,
    /// Account authorization is no longer usable.
    #[error("provider reauthorization required")]
    ReauthorizationRequired,
    /// A bounded provider request failed temporarily.
    #[error("provider temporarily unavailable")]
    Unavailable,
    /// Telegram requires waiting before the next provider request.
    #[error("provider flood wait")]
    FloodWait(Duration),
}

/// Provider surface that can resolve only validated public usernames.
pub trait PublicChannelProvider {
    /// Provider-independent resolved channel identity.
    type Channel: Send + Sync;

    /// Resolves one already-validated public username.
    fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> impl Future<Output = Result<Self::Channel, ProviderError>> + Send;

    /// Fetches one bounded page without exposing dialog or membership operations.
    fn fetch_public_posts(
        &self,
        channel: &Self::Channel,
        before_message_id: Option<i64>,
        limit: usize,
    ) -> impl Future<Output = Result<ProviderPage, ProviderError>> + Send;
}

impl<P> PublicChannelProvider for &P
where
    P: PublicChannelProvider + Sync,
{
    type Channel = P::Channel;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        (*self).resolve_public_channel(username).await
    }

    async fn fetch_public_posts(
        &self,
        channel: &Self::Channel,
        before_message_id: Option<i64>,
        limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        (*self)
            .fetch_public_posts(channel, before_message_id, limit)
            .await
    }
}

/// One normalized provider post observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPost {
    /// Channel-local provider message identity.
    pub message_id: i64,
    /// Normalized provider text.
    pub body: String,
    /// UTC publication instant.
    pub published_at: String,
    /// Whether this observation is an explicit deletion marker.
    pub deleted: bool,
}

/// One bounded provider page and its exclusive continuation cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderPage {
    /// Observations in provider order.
    pub posts: Vec<ProviderPost>,
    /// Next exclusive provider message cursor.
    pub next_before_message_id: Option<i64>,
}

/// Provider-independent public broadcast-channel identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPublicChannel {
    /// Stable Telegram channel identity.
    pub provider_channel_id: i64,
    /// Canonical username used for resolution.
    pub username: String,
    /// Current provider display title, bounded by Telegram.
    pub display_name: String,
    peer_ref: grammers_session::types::PeerRef,
}

/// Production `MTProto` adapter exposing only the narrow public-channel trait.
pub struct MtProtoPublicChannelProvider {
    client: grammers_client::Client,
    runner: tokio::task::JoinHandle<()>,
}

impl fmt::Debug for MtProtoPublicChannelProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MtProtoPublicChannelProvider")
            .field("client", &"[connected]")
            .field("runner", &"[joined]")
            .finish()
    }
}

impl MtProtoPublicChannelProvider {
    /// Builds an in-memory grammers session, starts its sender, and verifies authorization.
    ///
    /// # Errors
    ///
    /// Returns a safe reauthorization or unavailable class for invalid state or network failure.
    pub async fn connect(
        api_id: i32,
        session: &SessionMaterial,
        timeout: Duration,
    ) -> Result<Self, ProviderError> {
        let persisted: PersistedSession = serde_json::from_slice(session.as_bytes())
            .map_err(|_| ProviderError::ReauthorizationRequired)?;
        if !(1..=5).contains(&persisted.home_dc) {
            return Err(ProviderError::ReauthorizationRequired);
        }
        let data = SessionData {
            home_dc: persisted.home_dc,
            dc_options: persisted
                .dc_options
                .into_iter()
                .map(|option| (option.id, option))
                .collect(),
            peer_infos: persisted
                .peer_infos
                .into_iter()
                .map(|peer| (peer.id(), peer))
                .collect(),
            updates_state: persisted.updates_state,
        };
        let pool = SenderPool::new(Arc::new(MemorySession::from(data)), api_id);
        let client = grammers_client::Client::new(pool.handle);
        let runner = tokio::spawn(pool.runner.run());
        let authorized = tokio::time::timeout(timeout, client.is_authorized())
            .await
            .map_err(|_| ProviderError::Unavailable)?
            .map_err(|_| ProviderError::Unavailable)?;
        if !authorized {
            runner.abort();
            return Err(ProviderError::ReauthorizationRequired);
        }
        Ok(Self { client, runner })
    }
}

impl Drop for MtProtoPublicChannelProvider {
    fn drop(&mut self) {
        self.runner.abort();
    }
}

impl PublicChannelProvider for MtProtoPublicChannelProvider {
    type Channel = ResolvedPublicChannel;

    async fn resolve_public_channel(
        &self,
        username: &PublicChannelUsername,
    ) -> Result<Self::Channel, ProviderError> {
        let peer = self
            .client
            .resolve_username(username.as_str())
            .await
            .map_err(classify_invocation_error)?
            .ok_or(ProviderError::InvalidChannel)?;
        match peer {
            Peer::Channel(channel) => Ok(ResolvedPublicChannel {
                provider_channel_id: channel
                    .id()
                    .bare_id()
                    .ok_or(ProviderError::InvalidChannel)?,
                username: username.as_str().to_owned(),
                display_name: channel.title().to_owned(),
                peer_ref: channel
                    .to_ref()
                    .await
                    .map_err(|_| ProviderError::Unavailable)?
                    .ok_or(ProviderError::InvalidChannel)?,
            }),
            Peer::User(_) | Peer::Group(_) => Err(ProviderError::InvalidChannel),
        }
    }

    async fn fetch_public_posts(
        &self,
        channel: &Self::Channel,
        before_message_id: Option<i64>,
        limit: usize,
    ) -> Result<ProviderPage, ProviderError> {
        let offset = before_message_id
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(0);
        let mut iterator = self
            .client
            .iter_messages(channel.peer_ref)
            .offset_id(offset)
            .limit(limit.min(100));
        let mut posts = Vec::new();
        while let Some(message) = iterator.next().await.map_err(classify_invocation_error)? {
            posts.push(ProviderPost {
                message_id: i64::from(message.id()),
                body: message.text().to_owned(),
                published_at: message.date().to_rfc3339(),
                deleted: false,
            });
        }
        let next_before_message_id = (posts.len() == limit)
            .then(|| posts.last().map(|post| post.message_id))
            .flatten();
        Ok(ProviderPage {
            posts,
            next_before_message_id,
        })
    }
}

fn classify_invocation_error(error: InvocationError) -> ProviderError {
    match error {
        InvocationError::Rpc(rpc) if rpc.code == 420 => {
            rpc.value.map_or(ProviderError::Unavailable, |seconds| {
                ProviderError::FloodWait(Duration::from_secs(u64::from(seconds)))
            })
        }
        InvocationError::Rpc(rpc) if rpc.code == 401 => ProviderError::ReauthorizationRequired,
        _ => ProviderError::Unavailable,
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedSession {
    home_dc: i32,
    dc_options: Vec<DcOption>,
    peer_infos: Vec<PeerInfo>,
    #[serde(default)]
    updates_state: UpdatesState,
}
