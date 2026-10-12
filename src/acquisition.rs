//! Bounded restart-safe public-channel acquisition.

use std::collections::HashSet;
use std::time::Duration;

use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::{
    ObservedRevision, ProvedIdRange, ProviderError, ProviderPage, ProviderPost,
    PublicChannelProvider, PublicChannelUsername, RevisionRepository,
};

/// One finite acquisition execution request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquisitionRequest<'a> {
    /// Run being acquired.
    pub run_id: Uuid,
    /// Owned resolved channel identity.
    pub channel_id: Uuid,
    /// Canonical public username.
    pub username: &'a str,
    /// Closed UTC publication lower bound.
    pub window_start: &'a str,
    /// Open UTC publication upper bound.
    pub window_end: &'a str,
    /// Maximum pages.
    pub max_pages: usize,
    /// Maximum posts per page.
    pub page_size: usize,
}

/// Truthful acquisition report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcquisitionReport {
    /// Provider pages completed.
    pub pages: usize,
    /// Immutable observations selected.
    pub revisions: usize,
    /// Whether a safe partial result was retained.
    pub partial: bool,
}

/// Safe acquisition failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AcquisitionError {
    /// A normal retry may be attempted on a later worker tick.
    #[error("channel acquisition is unavailable")]
    Unavailable,
    /// A persisted provider wait prevents an early retry.
    #[error("channel acquisition is deferred")]
    Deferred,
}

/// Finite acquisition engine over an injected provider.
#[derive(Debug)]
pub struct AcquisitionEngine<P> {
    provider: P,
    revisions: RevisionRepository,
    request_timeout: Duration,
}

type StoredCheckpoint = (Option<String>, Option<i64>, i64, i64, bool);

impl<P: PublicChannelProvider> AcquisitionEngine<P> {
    /// Creates a bounded acquisition engine.
    #[must_use]
    pub fn new(provider: P, revisions: RevisionRepository, request_timeout: Duration) -> Self {
        Self {
            provider,
            revisions,
            request_timeout,
        }
    }

    /// Acquires one channel under the supplied page/window bounds.
    ///
    /// # Errors
    ///
    /// Returns a safe class for provider, timeout, or storage failure.
    pub async fn execute(
        &self,
        request: &AcquisitionRequest<'_>,
    ) -> Result<AcquisitionReport, AcquisitionError> {
        let (mut before, mut pages, mut revisions) = self.resume(request).await?;
        let username = PublicChannelUsername::parse(request.username)
            .map_err(|_| AcquisitionError::Unavailable)?;
        let channel = self
            .resolve(request, &username, before, pages, revisions)
            .await?;
        let (start, end) = parse_window(request)?;
        let mut seen = HashSet::new();
        while pages < request.max_pages {
            let fetched = tokio::time::timeout(
                self.request_timeout,
                self.provider
                    .fetch_public_posts(&channel, before, request.page_size.min(100)),
            )
            .await;
            let page = match fetched {
                Ok(Ok(page)) => page,
                Ok(Err(ProviderError::FloodWait(wait))) => {
                    self.checkpoint(request, before, pages, revisions, "flood_wait", Some(wait))
                        .await?;
                    return Err(AcquisitionError::Deferred);
                }
                Ok(Err(_)) | Err(_) if pages > 0 => {
                    self.checkpoint(
                        request,
                        before,
                        pages,
                        revisions,
                        "provider_unavailable",
                        None,
                    )
                    .await?;
                    return Ok(AcquisitionReport {
                        pages,
                        revisions,
                        partial: true,
                    });
                }
                Ok(Err(_)) | Err(_) => return Err(AcquisitionError::Unavailable),
            };
            pages += 1;
            let (stored, reached_window_start) = self
                .store_page(request, &username, (start, end), &page, &mut seen)
                .await?;
            self.withdraw_absent(request.channel_id, before, &page)
                .await?;
            revisions += stored;
            // Pages arrive newest first and ids rise with time, so the first post older than the
            // window proves every later one older: the whole window has been read.
            before = if reached_window_start {
                None
            } else {
                page.next_before_message_id
            };
            self.checkpoint(request, before, pages, revisions, "page_committed", None)
                .await?;
            if before.is_none() {
                break;
            }
        }
        let partial = before.is_some();
        self.checkpoint(
            request,
            before,
            pages,
            revisions,
            if partial { "page_limit" } else { "completed" },
            None,
        )
        .await?;
        Ok(AcquisitionReport {
            pages,
            revisions,
            partial,
        })
    }

    /// Stores the in-window posts of one page and says whether the page reached the window start.
    ///
    /// Pages arrive newest first, so a post published before the window start proves every later
    /// one older. The page is still stored in full: its other posts may lie inside the window.
    async fn store_page(
        &self,
        request: &AcquisitionRequest<'_>,
        username: &PublicChannelUsername,
        (start, end): (jiff::Timestamp, jiff::Timestamp),
        page: &ProviderPage,
        seen: &mut HashSet<(i64, Vec<u8>)>,
    ) -> Result<(usize, bool), AcquisitionError> {
        let mut stored = 0;
        let mut reached_window_start = false;
        for post in &page.posts {
            if post.deleted || post.body.is_empty() || post.body.len() > 16_384 {
                continue;
            }
            let published = post
                .published_at
                .parse::<jiff::Timestamp>()
                .map_err(|_| AcquisitionError::Unavailable)?;
            if published < start {
                reached_window_start = true;
                continue;
            }
            if published >= end {
                continue;
            }
            let digest = Sha256::digest(post.body.as_bytes());
            if seen.insert((post.message_id, digest.to_vec())) {
                let canonical_link =
                    format!("https://t.me/{}/{}", username.as_str(), post.message_id);
                self.revisions
                    .append(&ObservedRevision {
                        channel_id: request.channel_id,
                        provider_message_id: post.message_id,
                        body: &post.body,
                        canonical_link: &canonical_link,
                        published_at: &post.published_at,
                        observed_at: request.window_end,
                    })
                    .await
                    .map_err(|_| AcquisitionError::Unavailable)?;
                stored += 1;
            }
        }
        Ok((stored, reached_window_start))
    }

    /// Withdraws the stored posts this page proves are gone.
    ///
    /// The page answered a request for ids below `requested_before` (no limit for the first page of
    /// a walk), so it proves the ids from its own lowest down to that cursor; a short page, with no
    /// next cursor, reaches the start of the channel. A post without text is still on the page and
    /// so is present; a post the provider marks deleted is withdrawn wherever it lies.
    async fn withdraw_absent(
        &self,
        channel_id: Uuid,
        requested_before: Option<i64>,
        page: &ProviderPage,
    ) -> Result<(), AcquisitionError> {
        let Some(proved) = proved_range(page, requested_before) else {
            return Ok(());
        };
        let (deleted, present): (Vec<i64>, Vec<i64>) = {
            let (gone, live): (Vec<&ProviderPost>, Vec<&ProviderPost>) =
                page.posts.iter().partition(|post| post.deleted);
            (
                gone.iter().map(|post| post.message_id).collect(),
                live.iter().map(|post| post.message_id).collect(),
            )
        };
        self.revisions
            .withdraw_missing(channel_id, proved, (&present, &deleted))
            .await
            .map(|_withdrawn| ())
            .map_err(|_| AcquisitionError::Unavailable)
    }

    async fn resolve(
        &self,
        request: &AcquisitionRequest<'_>,
        username: &PublicChannelUsername,
        before: Option<i64>,
        pages: usize,
        revisions: usize,
    ) -> Result<P::Channel, AcquisitionError> {
        let resolved = tokio::time::timeout(
            self.request_timeout,
            self.provider.resolve_public_channel(username),
        )
        .await;
        match resolved {
            Ok(Ok(channel)) => Ok(channel),
            Ok(Err(ProviderError::FloodWait(wait))) => {
                self.checkpoint(request, before, pages, revisions, "flood_wait", Some(wait))
                    .await?;
                Err(AcquisitionError::Deferred)
            }
            Ok(Err(_)) | Err(_) => Err(AcquisitionError::Unavailable),
        }
    }

    async fn resume(
        &self,
        request: &AcquisitionRequest<'_>,
    ) -> Result<(Option<i64>, usize, usize), AcquisitionError> {
        let checkpoint: Option<StoredCheckpoint> = sqlx::query_as(
            "select checkpoint->>'state',
                    (checkpoint->>'before_message_id')::bigint,
                    coalesce((checkpoint->>'pages')::bigint, 0),
                    coalesce((checkpoint->>'revisions')::bigint, 0),
                    expires_at > now()
             from channel_digests.leases
             where resource_kind = $1 and resource_id = $2",
        )
        .bind(acquisition_kind(request.channel_id))
        .bind(request.run_id)
        .fetch_optional(self.revisions.pool())
        .await
        .map_err(|_| AcquisitionError::Unavailable)?;
        let Some((state, before, pages, revisions, wait_active)) = checkpoint else {
            return Ok((None, 0, 0));
        };
        if state.as_deref() == Some("flood_wait") && wait_active {
            return Err(AcquisitionError::Deferred);
        }
        let pages = usize::try_from(pages).map_err(|_| AcquisitionError::Unavailable)?;
        let revisions = usize::try_from(revisions).map_err(|_| AcquisitionError::Unavailable)?;
        if state.as_deref() == Some("completed") {
            return Ok((None, request.max_pages, revisions));
        }
        Ok((before, pages, revisions))
    }

    async fn checkpoint(
        &self,
        request: &AcquisitionRequest<'_>,
        before: Option<i64>,
        pages: usize,
        revisions: usize,
        state: &str,
        retry_after: Option<Duration>,
    ) -> Result<(), AcquisitionError> {
        let retry_seconds =
            retry_after.map(|wait| i64::try_from(wait.as_secs().clamp(1, 3_600)).unwrap_or(3_600));
        sqlx::query(
            "insert into channel_digests.leases (resource_kind, resource_id, holder_id, acquired_at, expires_at, checkpoint)
             values ($1, $2, $3, now(),
                     case when $8::bigint is null then now() + interval '5 minutes'
                          else now() + ($8 * interval '1 second') end,
                     jsonb_build_object('channel_id', $4::text, 'before_message_id', $5,
                                        'pages', $6, 'revisions', $7, 'state', $9))
             on conflict (resource_kind, resource_id) do update
             set checkpoint = excluded.checkpoint, acquired_at = excluded.acquired_at,
                 expires_at = excluded.expires_at, holder_id = excluded.holder_id",
        )
        .bind(acquisition_kind(request.channel_id))
        .bind(request.run_id)
        .bind(Uuid::now_v7())
        .bind(request.channel_id)
        .bind(before)
        .bind(i64::try_from(pages).map_err(|_| AcquisitionError::Unavailable)?)
        .bind(i64::try_from(revisions).map_err(|_| AcquisitionError::Unavailable)?)
        .bind(retry_seconds)
        .bind(state)
        .execute(self.revisions.pool())
        .await
        .map_err(|_| AcquisitionError::Unavailable)?;
        Ok(())
    }
}

/// The ids a page proves: from its lowest id, or from the channel start for a short last page.
///
/// A page with a next cursor and no post proves nothing, so the range is absent.
fn proved_range(page: &ProviderPage, requested_before: Option<i64>) -> Option<ProvedIdRange> {
    if page.next_before_message_id.is_none() {
        return Some(ProvedIdRange {
            lowest: i64::MIN,
            below: requested_before,
        });
    }
    page.posts
        .iter()
        .map(|post| post.message_id)
        .min()
        .map(|lowest| ProvedIdRange {
            lowest,
            below: requested_before,
        })
}

fn acquisition_kind(channel_id: Uuid) -> String {
    format!("acquisition:{channel_id}")
}

fn parse_window(
    request: &AcquisitionRequest<'_>,
) -> Result<(jiff::Timestamp, jiff::Timestamp), AcquisitionError> {
    let start = request
        .window_start
        .parse()
        .map_err(|_| AcquisitionError::Unavailable)?;
    let end = request
        .window_end
        .parse()
        .map_err(|_| AcquisitionError::Unavailable)?;
    Ok((start, end))
}
