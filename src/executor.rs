//! Durable provider-to-manifest run execution.

use std::time::Duration;

use uuid::Uuid;

use crate::coordinator::RunFailure;
use crate::{
    AcquisitionEngine, AcquisitionError, AcquisitionRequest, CanonicalManifest, DigestCoordinator,
    ManifestBuilder, ManifestSource, PublicChannelProvider, RevisionRepository,
};

/// Safe run execution failure.
#[derive(Debug, thiserror::Error)]
#[error("digest run execution is unavailable")]
pub struct RunExecutionError;

/// Restart-safe executor over one provider connection and owned database.
#[derive(Debug)]
pub struct RunExecutor<P> {
    pool: sqlx::PgPool,
    provider: P,
}

/// One accepted or interrupted run, read from the run table alone.
struct PendingRun {
    run_id: Uuid,
    owner_id: Uuid,
    window_start: String,
    window_end: String,
    language: String,
    state: String,
    operation_id: Uuid,
}

type PendingRow = (Uuid, Uuid, String, String, String, String, Uuid);

type SourceRow = (
    Uuid,
    Uuid,
    String,
    Option<String>,
    i64,
    String,
    String,
    String,
    String,
    i64,
);

/// Outcome of acquiring every subscribed channel for one run.
enum Acquired {
    Ready,
    Deferred,
    Unavailable,
}

impl<P: PublicChannelProvider + Sync> RunExecutor<P> {
    /// Creates one executor.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, provider: P) -> Self {
        Self { pool, provider }
    }

    /// Executes at most one accepted or interrupted run.
    ///
    /// # Errors
    ///
    /// Returns a safe provider, storage, or manifest class.
    pub async fn execute_one(&self) -> Result<bool, RunExecutionError> {
        let Some(run) = self.select_pending().await? else {
            return Ok(false);
        };
        if run.state == "accepted" && !self.begin_acquisition(run.run_id).await? {
            return Ok(true);
        }
        let subscriptions = self.subscribed_channels(run.owner_id).await?;
        let coordinator = DigestCoordinator::new(self.pool.clone());
        match self.acquire(&run, &subscriptions).await {
            Acquired::Deferred => return Ok(true),
            Acquired::Unavailable => {
                coordinator
                    .fail_run(run.run_id, run.owner_id, RunFailure::ProviderUnavailable)
                    .await
                    .map_err(|_| RunExecutionError)?;
                return Ok(true);
            }
            Acquired::Ready => {}
        }
        let sources = self.select_sources(&run).await?;
        self.commit(&coordinator, &run, &sources).await?;
        Ok(true)
    }

    async fn select_pending(&self) -> Result<Option<PendingRun>, RunExecutionError> {
        let pending: Option<PendingRow> = sqlx::query_as(
            "select r.run_id, r.owner_id,
                    to_char(r.window_start at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    to_char(r.window_end at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    r.output_language, r.state, r.operation_id
             from channel_digests.digest_runs r
             where r.state in ('accepted', 'acquiring')
               and not exists (
                   select 1 from channel_digests.leases l
                   where l.resource_id = r.run_id
                     and l.resource_kind like 'acquisition:%'
                     and l.checkpoint->>'state' = 'flood_wait'
                     and l.expires_at > now()
               )
             order by r.created_at, r.run_id limit 1",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| RunExecutionError)?;
        Ok(pending.map(
            |(run_id, owner_id, window_start, window_end, language, state, operation_id)| {
                PendingRun {
                    run_id,
                    owner_id,
                    window_start,
                    window_end,
                    language,
                    state,
                    operation_id,
                }
            },
        ))
    }

    async fn begin_acquisition(&self, run_id: Uuid) -> Result<bool, RunExecutionError> {
        let changed = sqlx::query(
            "update channel_digests.digest_runs set state = 'acquiring', updated_at = now() where run_id = $1 and state = 'accepted'",
        )
        .bind(run_id)
        .execute(&self.pool)
        .await
        .map_err(|_| RunExecutionError)?;
        Ok(changed.rows_affected() == 1)
    }

    async fn subscribed_channels(
        &self,
        owner_id: Uuid,
    ) -> Result<Vec<(Uuid, String)>, RunExecutionError> {
        sqlx::query_as(
            "select c.channel_id, c.username
             from channel_digests.subscriptions s
             join channel_digests.channels c using (channel_id)
             where s.owner_id = $1 and s.enabled
             order by c.username limit 20",
        )
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| RunExecutionError)
    }

    async fn acquire(&self, run: &PendingRun, subscriptions: &[(Uuid, String)]) -> Acquired {
        let engine = AcquisitionEngine::new(
            &self.provider,
            RevisionRepository::new(self.pool.clone()),
            Duration::from_secs(15),
        );
        let mut successful_channels = 0_usize;
        let mut deferred_channels = 0_usize;
        for (channel_id, username) in subscriptions {
            let outcome = engine
                .execute(&AcquisitionRequest {
                    run_id: run.run_id,
                    channel_id: *channel_id,
                    username,
                    window_start: &run.window_start,
                    window_end: &run.window_end,
                    max_pages: 20,
                    page_size: 100,
                })
                .await;
            match outcome {
                Ok(_) => successful_channels += 1,
                Err(AcquisitionError::Deferred) => deferred_channels += 1,
                Err(AcquisitionError::Unavailable) => {}
            }
        }
        if deferred_channels > 0 {
            Acquired::Deferred
        } else if !subscriptions.is_empty() && successful_channels == 0 {
            Acquired::Unavailable
        } else {
            Acquired::Ready
        }
    }

    /// Selects the latest body of every message in the window, numbering all its revisions.
    async fn select_sources(
        &self,
        run: &PendingRun,
    ) -> Result<Vec<ManifestSource>, RunExecutionError> {
        let rows: Vec<SourceRow> = sqlx::query_as(
            "select revision_id, channel_id, username, display_name, provider_message_id,
                    content_sha256,
                    to_char(published_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    canonical_link, body, revision_index
             from (
                 select distinct on (o.channel_id, o.provider_message_id)
                        o.revision_id, o.channel_id, c.username, c.display_name,
                        o.provider_message_id, o.content_sha256, o.published_at,
                        o.canonical_link, o.body, o.revision_index
                 from (
                     select r.*, row_number() over (
                                partition by r.channel_id, r.provider_message_id
                                order by r.observed_at, r.revision_id
                            ) as revision_index
                     from channel_digests.post_revisions r
                     where r.channel_id in (
                               select s.channel_id from channel_digests.subscriptions s
                               where s.owner_id = $1 and s.enabled
                           )
                       and r.published_at >= $2::timestamptz
                       and r.published_at < $3::timestamptz
                 ) o
                 join channel_digests.channels c on c.channel_id = o.channel_id
                 where o.body is not null
                 order by o.channel_id, o.provider_message_id, o.observed_at desc,
                          o.revision_id desc
             ) selected
             order by published_at, username, provider_message_id, revision_id limit 100",
        )
        .bind(run.owner_id)
        .bind(&run.window_start)
        .bind(&run.window_end)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| RunExecutionError)?;
        Ok(rows
            .into_iter()
            .map(
                |(
                    revision_id,
                    channel_id,
                    channel_username,
                    channel_display_name,
                    message_id,
                    content_sha256,
                    published_at,
                    canonical_link,
                    body,
                    revision_index,
                )| ManifestSource {
                    revision_id,
                    channel_id,
                    channel_username,
                    channel_display_name,
                    message_id,
                    content_sha256,
                    published_at,
                    canonical_link,
                    body,
                    revision_index: u32::try_from(revision_index).unwrap_or(0),
                },
            )
            .collect())
    }

    /// Completes an empty selection, fails an unbuildable one, or commits the manifest.
    async fn commit(
        &self,
        coordinator: &DigestCoordinator,
        run: &PendingRun,
        sources: &[ManifestSource],
    ) -> Result<(), RunExecutionError> {
        if sources.is_empty() {
            return coordinator
                .complete_empty_run(run.run_id, run.owner_id)
                .await
                .map_err(|_| RunExecutionError);
        }
        let built = ManifestBuilder::build(
            Uuid::now_v7(),
            run.owner_id,
            run.run_id,
            &run.window_start,
            &run.window_end,
            sources,
        );
        let Ok(manifest) = built else {
            return coordinator
                .fail_run(run.run_id, run.owner_id, RunFailure::ManifestInvalid)
                .await
                .map_err(|_| RunExecutionError);
        };
        let request = recap_request(run, &manifest)?;
        coordinator
            .commit_manifest(&manifest, &request)
            .await
            .map(|_outcome| ())
            .map_err(|_| RunExecutionError)
    }
}

/// Builds the body-free recap request for a committed manifest.
fn recap_request(
    run: &PendingRun,
    manifest: &CanonicalManifest,
) -> Result<Vec<u8>, RunExecutionError> {
    serde_json::to_vec(&serde_json::json!({
        "operation_id": run.operation_id,
        "owner": format!("user:{}", run.owner_id),
        "digest_run_id": run.run_id,
        "window": {"start_at": run.window_start, "end_at": run.window_end},
        "output_language": run.language,
        "source_count": manifest.source_count,
        "channel_count": manifest.channel_count,
        "manifest_ref": format!("channel-digest-manifest:{}", manifest.manifest_id),
        "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
        "analysis_family": "channel_digest_recap",
        "analysis_contract": "channel_digest_recap.v1"
    }))
    .map_err(|_| RunExecutionError)
}
