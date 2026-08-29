//! Durable provider-to-manifest run execution.

use std::time::Duration;

use uuid::Uuid;

use crate::{
    AcquisitionEngine, AcquisitionError, AcquisitionRequest, DigestCoordinator, ManifestBuilder,
    ManifestSource, PublicChannelProvider, RevisionRepository,
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
    #[expect(
        clippy::too_many_lines,
        reason = "the ordered durable acquisition, selection, manifest, and outbox state machine is intentionally linear"
    )]
    pub async fn execute_one(&self) -> Result<bool, RunExecutionError> {
        let pending: Option<(Uuid, Uuid, String, String, String, String, Uuid)> = sqlx::query_as(
            "select r.run_id, r.owner_id,
                    to_char(r.window_start at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    to_char(r.window_end at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    r.output_language, r.state, o.operation_id
             from channel_digests.digest_runs r
             join channel_digests.outbox_messages o
               on o.semantic_key = r.idempotency_key
              and o.subject = 'platform.operation.reported.v1'
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
        let Some((run_id, owner_id, window_start, window_end, language, state, operation_id)) =
            pending
        else {
            return Ok(false);
        };
        if state == "accepted" {
            let changed = sqlx::query(
                "update channel_digests.digest_runs set state = 'acquiring', updated_at = now() where run_id = $1 and state = 'accepted'",
            )
            .bind(run_id)
            .execute(&self.pool)
            .await
            .map_err(|_| RunExecutionError)?;
            if changed.rows_affected() != 1 {
                return Ok(true);
            }
        }
        let subscriptions: Vec<(Uuid, String)> = sqlx::query_as(
            "select c.channel_id, c.username
             from channel_digests.subscriptions s
             join channel_digests.channels c using (channel_id)
             where s.owner_id = $1 and s.enabled
             order by c.username limit 20",
        )
        .bind(owner_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| RunExecutionError)?;
        let engine = AcquisitionEngine::new(
            &self.provider,
            RevisionRepository::new(self.pool.clone()),
            Duration::from_secs(15),
        );
        let mut successful_channels = 0_usize;
        let mut deferred_channels = 0_usize;
        for (channel_id, username) in &subscriptions {
            let outcome = engine
                .execute(&AcquisitionRequest {
                    run_id,
                    channel_id: *channel_id,
                    username,
                    window_start: &window_start,
                    window_end: &window_end,
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
            return Ok(true);
        }
        if !subscriptions.is_empty() && successful_channels == 0 {
            sqlx::query(
                "update channel_digests.digest_runs set state = 'failed', safe_failure_class = 'provider_unavailable', updated_at = now() where run_id = $1 and state = 'acquiring'",
            )
            .bind(run_id)
            .execute(&self.pool)
            .await
            .map_err(|_| RunExecutionError)?;
            return Ok(true);
        }
        let sources: Vec<(Uuid, String, i64, String, String, String, String)> = sqlx::query_as(
            "select revision_id, username, provider_message_id, content_sha256,
                    to_char(published_at at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'),
                    canonical_link, body
             from (
                 select distinct on (r.channel_id, r.provider_message_id)
                        r.revision_id, c.username, r.provider_message_id, r.content_sha256,
                        r.published_at, r.canonical_link, r.body, r.observed_at
                 from channel_digests.post_revisions r
                 join channel_digests.channels c using (channel_id)
                 join channel_digests.subscriptions s using (channel_id)
                 where s.owner_id = $1 and s.enabled and r.body is not null
                   and r.published_at >= $2::timestamptz and r.published_at < $3::timestamptz
                 order by r.channel_id, r.provider_message_id, r.observed_at desc, r.revision_id desc
             ) selected
             order by published_at, username, provider_message_id, revision_id limit 100",
        )
        .bind(owner_id)
        .bind(&window_start)
        .bind(&window_end)
        .fetch_all(&self.pool)
        .await
        .map_err(|_| RunExecutionError)?;
        let manifest = ManifestBuilder::build(
            run_id,
            &window_start,
            &window_end,
            sources
                .into_iter()
                .map(
                    |(
                        revision_id,
                        channel_username,
                        message_id,
                        content_sha256,
                        published_at,
                        canonical_link,
                        body,
                    )| ManifestSource {
                        revision_id,
                        channel_username,
                        message_id,
                        content_sha256,
                        published_at,
                        canonical_link,
                        body,
                    },
                )
                .collect(),
        )
        .map_err(|_| RunExecutionError)?;
        let manifest_id = Uuid::now_v7();
        let recap_request = (manifest.source_count > 0).then(|| {
            serde_json::to_vec(&serde_json::json!({
                "operation_id": operation_id,
                "owner": format!("user:{owner_id}"),
                "digest_run_id": run_id,
                "window": {"start_at": window_start, "end_at": window_end},
                "output_language": language,
                "source_count": manifest.source_count,
                "channel_count": manifest.channel_count,
                "manifest_ref": format!("channel-digest-manifest:{manifest_id}"),
                "manifest_digest": {"algorithm": "sha256", "hex": manifest.sha256},
                "analysis_family": "channel_digest_recap",
                "analysis_contract": "channel_digest_recap.v1"
            }))
            .map_err(|_| RunExecutionError)
        });
        let recap_request = match recap_request {
            Some(bytes) => Some(bytes?),
            None => None,
        };
        DigestCoordinator::new(self.pool.clone())
            .commit_manifest(manifest_id, owner_id, &manifest, recap_request.as_deref())
            .await
            .map_err(|_| RunExecutionError)?;
        Ok(true)
    }
}
