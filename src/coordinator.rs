//! Atomic manifest-to-Knowledge exchange coordination.

use uuid::Uuid;

use crate::{CanonicalManifest, IntakeOutcome};
use ratatoskr_channel_digest_contracts::{
    ChannelDigestRecapFailureCode, KnowledgeChannelDigestRecapCompleted,
    KnowledgeChannelDigestRecapFailed, KnowledgeChannelDigestRecapRequested,
};
use sha2::Digest as _;

/// Safe digest coordination failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum CoordinatorError {
    /// Typed request does not match durable run/manifest evidence.
    #[error("digest recap linkage is invalid")]
    Invalid,
    /// Atomic storage operation failed.
    #[error("digest coordination is unavailable")]
    Storage,
}

/// Durable coordinator for manifest and recap-request sequencing.
#[derive(Debug, Clone)]
pub struct DigestCoordinator {
    pool: sqlx::PgPool,
}

impl DigestCoordinator {
    /// Creates a coordinator over the owned pool.
    #[must_use]
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Commits one immutable manifest and exactly one body-free recap request.
    ///
    /// # Errors
    ///
    /// Returns a finite linkage or storage class.
    #[expect(
        clippy::too_many_lines,
        reason = "the atomic manifest, outbox, and state transition is intentionally visible"
    )]
    pub async fn commit_manifest(
        &self,
        manifest_id: Uuid,
        owner_id: Uuid,
        manifest: &CanonicalManifest,
        recap_request: Option<&[u8]>,
    ) -> Result<IntakeOutcome, CoordinatorError> {
        let canonical_json: serde_json::Value =
            serde_json::from_slice(&manifest.bytes).map_err(|_| CoordinatorError::Invalid)?;
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.digest_manifests (manifest_id, run_id, owner_id, sha256, source_count, channel_count, canonical_json) values ($1, $2, $3, $4, $5, $6, $7) on conflict (run_id) do nothing returning manifest_id",
        )
        .bind(manifest_id)
        .bind(manifest.run_id)
        .bind(owner_id)
        .bind(&manifest.sha256)
        .bind(i32::try_from(manifest.source_count).map_err(|_| CoordinatorError::Invalid)?)
        .bind(i32::try_from(manifest.channel_count).map_err(|_| CoordinatorError::Invalid)?)
        .bind(canonical_json)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if inserted.is_none() {
            let existing: Option<(Uuid, Uuid, String)> = sqlx::query_as(
                "select manifest_id, owner_id, sha256 from channel_digests.digest_manifests where run_id = $1",
            )
            .bind(manifest.run_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
            transaction
                .rollback()
                .await
                .map_err(|_| CoordinatorError::Storage)?;
            return if existing == Some((manifest_id, owner_id, manifest.sha256.clone())) {
                Ok(IntakeOutcome::Replayed)
            } else {
                Err(CoordinatorError::Invalid)
            };
        }
        if manifest.source_count == 0 {
            if recap_request.is_some() {
                return Err(CoordinatorError::Invalid);
            }
            let changed = sqlx::query(
                "update channel_digests.digest_runs set state = 'completed', updated_at = now() where run_id = $1 and owner_id = $2 and state in ('accepted', 'acquiring')",
            )
            .bind(manifest.run_id)
            .bind(owner_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
            if changed.rows_affected() != 1 {
                return Err(CoordinatorError::Invalid);
            }
        } else {
            let raw = recap_request.ok_or(CoordinatorError::Invalid)?;
            let request: KnowledgeChannelDigestRecapRequested =
                serde_json::from_slice(raw).map_err(|_| CoordinatorError::Invalid)?;
            request
                .validate_for_publish()
                .map_err(|_| CoordinatorError::Invalid)?;
            let value = serde_json::to_value(&request).map_err(|_| CoordinatorError::Invalid)?;
            let expected_owner = format!("user:{owner_id}");
            let expected_manifest = format!("channel-digest-manifest:{manifest_id}");
            if value.get("owner").and_then(serde_json::Value::as_str)
                != Some(expected_owner.as_str())
                || value
                    .get("digest_run_id")
                    .and_then(serde_json::Value::as_str)
                    != Some(manifest.run_id.to_string().as_str())
                || value
                    .get("manifest_ref")
                    .and_then(serde_json::Value::as_str)
                    != Some(expected_manifest.as_str())
                || value
                    .pointer("/manifest_digest/hex")
                    .and_then(serde_json::Value::as_str)
                    != Some(manifest.sha256.as_str())
                || value
                    .get("source_count")
                    .and_then(serde_json::Value::as_u64)
                    != u64::try_from(manifest.source_count).ok()
                || value
                    .get("channel_count")
                    .and_then(serde_json::Value::as_u64)
                    != u64::try_from(manifest.channel_count).ok()
            {
                return Err(CoordinatorError::Invalid);
            }
            sqlx::query(
                "insert into channel_digests.outbox_messages (outbox_id, subject, semantic_key, owner_id, operation_id, payload) values ($1, 'knowledge.channel_digest_recap.requested.v1', $2, $3, $4, $5)",
            )
            .bind(Uuid::now_v7())
            .bind(manifest.run_id.to_string())
            .bind(owner_id)
            .bind(request.operation_id.0)
            .bind(value)
            .execute(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
            let changed = sqlx::query(
                "update channel_digests.digest_runs set state = 'waiting_recap', updated_at = now() where run_id = $1 and owner_id = $2 and state in ('accepted', 'acquiring')",
            )
            .bind(manifest.run_id)
            .bind(owner_id)
            .execute(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
            if changed.rows_affected() != 1 {
                return Err(CoordinatorError::Invalid);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }

    /// Settles one typed Knowledge completion against immutable manifest evidence.
    ///
    /// # Errors
    ///
    /// Returns a finite linkage or storage class.
    #[expect(
        clippy::too_many_lines,
        reason = "the atomic fact verification, inbox, result, and transition is intentionally visible"
    )]
    pub async fn settle_completion(
        &self,
        message_id: Uuid,
        completion: &[u8],
    ) -> Result<IntakeOutcome, CoordinatorError> {
        let fact: KnowledgeChannelDigestRecapCompleted =
            serde_json::from_slice(completion).map_err(|_| CoordinatorError::Invalid)?;
        fact.validate_for_publish()
            .map_err(|_| CoordinatorError::Invalid)?;
        let run_id = fact.digest_run_id.as_uuid();
        let owner_id = fact.owner.user_id().0;
        let result_id = fact.digest_result_id.as_uuid();
        let analysis_id = fact
            .analysis_ref
            .as_str()
            .strip_prefix("analysis:")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(CoordinatorError::Invalid)?;
        let result_digest_hex = fact.result_digest.hex.as_str();
        let outcome = if fact.coverage.omitted_count == 0 {
            "completed"
        } else {
            "partial"
        };
        let citation_count = i32::from(fact.coverage.included_count);
        let existing: Option<(Uuid, Uuid, String, Uuid, String, String, i32)> = sqlx::query_as(
            "select d.run_id, d.owner_id, m.sha256, d.recap_id, d.result_digest_hex, d.outcome, d.citation_count from channel_digests.digest_results d join channel_digests.digest_manifests m using (manifest_id) where d.result_id = $1",
        )
        .bind(result_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if let Some(existing) = existing {
            return if existing
                == (
                    run_id,
                    owner_id,
                    fact.manifest_digest.hex.as_str().to_owned(),
                    analysis_id,
                    result_digest_hex.to_owned(),
                    outcome.to_owned(),
                    citation_count,
                ) {
                Ok(IntakeOutcome::Replayed)
            } else {
                Err(CoordinatorError::Invalid)
            };
        }
        let durable: Option<(String, String, i32, i32)> = sqlx::query_as(
            "select r.state, m.sha256, m.source_count, m.channel_count from channel_digests.digest_runs r join channel_digests.digest_manifests m using (run_id) where r.run_id = $1 and r.owner_id = $2 and m.owner_id = $2",
        )
        .bind(run_id)
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        let Some((state, manifest_sha, source_count, channel_count)) = durable else {
            return Err(CoordinatorError::Invalid);
        };
        if state != "waiting_recap"
            || fact.manifest_digest.hex.as_str() != manifest_sha
            || i32::from(fact.coverage.selected_count.get()) != source_count
            || i32::from(fact.coverage.channel_count.get()) > channel_count
        {
            return Err(CoordinatorError::Invalid);
        }
        let semantic_key = result_id.to_string();
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'knowledge.channel_digest_recap.completed.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(&semantic_key)
        .bind(format!("{:x}", sha2::Sha256::digest(completion)))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| CoordinatorError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        let result = sqlx::query(
            "insert into channel_digests.digest_results (result_id, run_id, manifest_id, owner_id, outcome, recap_id, result_digest_hex, citation_count) select $1, r.run_id, m.manifest_id, r.owner_id, $2, $3, $4, $5 from channel_digests.digest_runs r join channel_digests.digest_manifests m using (run_id) where r.run_id = $6 and r.owner_id = $7",
        )
        .bind(result_id)
        .bind(outcome)
        .bind(analysis_id)
        .bind(result_digest_hex)
        .bind(citation_count)
        .bind(run_id)
        .bind(owner_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if result.rows_affected() != 1 {
            return Err(CoordinatorError::Invalid);
        }
        let changed = sqlx::query(
            "update channel_digests.digest_runs set state = $1, updated_at = now() where run_id = $2 and owner_id = $3 and state = 'waiting_recap'",
        )
        .bind(outcome)
        .bind(run_id)
        .bind(owner_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if changed.rows_affected() != 1 {
            return Err(CoordinatorError::Invalid);
        }
        sqlx::query(
            "update channel_digests.inbox_messages set state = 'completed', completed_at = now() where message_id = $1",
        )
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }

    /// Settles one typed Knowledge failure against immutable manifest evidence.
    ///
    /// # Errors
    ///
    /// Returns a finite linkage or storage class.
    pub async fn settle_failure(
        &self,
        message_id: Uuid,
        failure: &[u8],
    ) -> Result<IntakeOutcome, CoordinatorError> {
        let fact: KnowledgeChannelDigestRecapFailed =
            serde_json::from_slice(failure).map_err(|_| CoordinatorError::Invalid)?;
        fact.validate_for_publish()
            .map_err(|_| CoordinatorError::Invalid)?;
        let run_id = fact.digest_run_id.as_uuid();
        let owner_id = fact.owner.user_id().0;
        let failure_class = match fact.failure_code {
            ChannelDigestRecapFailureCode::ManifestUnavailable => "manifest_unavailable",
            ChannelDigestRecapFailureCode::ManifestIntegrity => "manifest_integrity",
            ChannelDigestRecapFailureCode::UnsupportedLanguage => "unsupported_language",
            ChannelDigestRecapFailureCode::ContextBudget => "context_budget",
            ChannelDigestRecapFailureCode::ProviderUnavailable => "provider_unavailable",
            ChannelDigestRecapFailureCode::ProviderTimeout => "provider_timeout",
            ChannelDigestRecapFailureCode::InvalidOutput => "invalid_output",
            ChannelDigestRecapFailureCode::CostBudget => "cost_budget",
            ChannelDigestRecapFailureCode::Cancelled => "cancelled",
        };
        let durable: Option<(String, Uuid, String)> = sqlx::query_as(
            "select r.state, m.manifest_id, m.sha256
             from channel_digests.digest_runs r
             join channel_digests.digest_manifests m using (run_id)
             where r.run_id = $1 and r.owner_id = $2 and m.owner_id = $2",
        )
        .bind(run_id)
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        let Some((state, manifest_id, manifest_sha)) = durable else {
            return Err(CoordinatorError::Invalid);
        };
        if state == "failed" {
            return Ok(IntakeOutcome::Replayed);
        }
        if state != "waiting_recap" || fact.manifest_digest.hex.as_str() != manifest_sha {
            return Err(CoordinatorError::Invalid);
        }
        let semantic_key = run_id.to_string();
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'knowledge.channel_digest_recap.failed.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(&semantic_key)
        .bind(format!("{:x}", sha2::Sha256::digest(failure)))
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| CoordinatorError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        sqlx::query(
            "insert into channel_digests.digest_results (result_id, run_id, manifest_id, owner_id, outcome, safe_failure_class) values ($1, $2, $3, $4, 'failed', $5)",
        )
        .bind(Uuid::now_v7())
        .bind(run_id)
        .bind(manifest_id)
        .bind(owner_id)
        .bind(failure_class)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        let changed = sqlx::query(
            "update channel_digests.digest_runs set state = 'failed', safe_failure_class = $1, updated_at = now() where run_id = $2 and owner_id = $3 and state = 'waiting_recap'",
        )
        .bind(failure_class)
        .bind(run_id)
        .bind(owner_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if changed.rows_affected() != 1 {
            return Err(CoordinatorError::Invalid);
        }
        sqlx::query(
            "update channel_digests.inbox_messages set state = 'completed', completed_at = now() where message_id = $1",
        )
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }

    /// Accepts and fans one authoritative schedule occurrence out to active owners.
    ///
    /// # Errors
    ///
    /// Returns a safe storage class when fan-out cannot be made durable.
    pub async fn accept_occurrence(
        &self,
        message_id: Uuid,
        payload: &[u8],
        occurrence_key: &str,
        prior_at: &str,
        due_at: &str,
    ) -> Result<IntakeOutcome, CoordinatorError> {
        if !occurrence_key.starts_with("schedule-occurrence:") {
            return Err(CoordinatorError::Invalid);
        }
        let payload_sha256 = format!("{:x}", sha2::Sha256::digest(payload));
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        let inserted: Option<(Uuid,)> = sqlx::query_as(
            "insert into channel_digests.inbox_messages (message_id, subject, semantic_key, payload_sha256, state) values ($1, 'channel_digest.schedule.occurrence_requested.v1', $2, $3, 'processing') on conflict do nothing returning message_id",
        )
        .bind(message_id)
        .bind(occurrence_key)
        .bind(payload_sha256)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        if inserted.is_none() {
            transaction
                .rollback()
                .await
                .map_err(|_| CoordinatorError::Storage)?;
            return Ok(IntakeOutcome::Replayed);
        }
        let owners: Vec<(Uuid, String)> = sqlx::query_as(
            "select owner_id, min(first_activated_at)::text from channel_digests.subscriptions where enabled and first_activated_at < $1::timestamptz group by owner_id order by owner_id",
        )
        .bind(due_at)
        .fetch_all(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        for (owner_id, activation_at) in owners {
            let window: (String, String) = sqlx::query_as(
                "select start_at::text, end_at::text from channel_digests.normalized_window(true, $1::timestamptz, $2::timestamptz, $3::timestamptz)",
            )
            .bind(&activation_at)
            .bind(prior_at)
            .bind(due_at)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
            sqlx::query(
                "insert into channel_digests.digest_runs (run_id, owner_id, trigger, idempotency_key, window_start, window_end, state) values ($1, $2, 'scheduled', $3, $4::timestamptz, $5::timestamptz, 'accepted') on conflict (owner_id, trigger, idempotency_key, window_start, window_end) do nothing",
            )
            .bind(Uuid::now_v7())
            .bind(owner_id)
            .bind(occurrence_key)
            .bind(&window.0)
            .bind(&window.1)
            .execute(&mut *transaction)
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        }
        sqlx::query(
            "update channel_digests.inbox_messages set state = 'completed', completed_at = now() where message_id = $1",
        )
        .bind(message_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| CoordinatorError::Storage)?;
        transaction
            .commit()
            .await
            .map_err(|_| CoordinatorError::Storage)?;
        Ok(IntakeOutcome::Applied)
    }
}
