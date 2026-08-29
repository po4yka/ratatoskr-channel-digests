//! Bounded read-through boundary for Knowledge-owned recap results.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::Config;

/// One Knowledge-owned recap projection after linkage verification.
#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeResultProjection {
    /// Knowledge analysis identity returned by the result reader.
    pub analysis_id: Uuid,
    /// Canonical lowercase SHA-256 accepted from Knowledge.
    pub result_digest_hex: String,
    /// Closed recap object owned and validated by Knowledge.
    pub recap: Value,
}

/// Finite safe failure classes for a Knowledge result read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum KnowledgeResultReadError {
    /// Transport or upstream availability did not permit a result read.
    #[error("Knowledge result reader is unavailable")]
    Unavailable,
    /// The upstream response did not match the immutable expected linkage.
    #[error("Knowledge result projection is invalid")]
    Invalid,
}

/// Reusable API-role client for the dedicated Knowledge result-reader route.
#[derive(Clone)]
pub struct KnowledgeResultReader {
    client: reqwest::Client,
    base_url: Arc<str>,
    service_secret: Arc<str>,
    max_response_bytes: usize,
}

impl fmt::Debug for KnowledgeResultReader {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KnowledgeResultReader")
            .field("base_url", &"[loopback]")
            .field("service_secret", &"[redacted]")
            .field("max_response_bytes", &self.max_response_bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct KnowledgeEnvelope {
    analysis_id: Uuid,
    result_digest: ResultDigest,
    recap: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultDigest {
    algorithm: String,
    hex: String,
}

impl KnowledgeResultReader {
    /// Builds one reader from strict API-role configuration.
    ///
    /// # Errors
    ///
    /// Returns a safe invalid class when API reader authority is absent.
    pub fn from_config(config: &Config) -> Result<Self, KnowledgeResultReadError> {
        let policy = config
            .knowledge_result_reader
            .as_ref()
            .ok_or(KnowledgeResultReadError::Invalid)?;
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(policy.connect_timeout_ms))
            .timeout(Duration::from_millis(policy.request_timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .pool_max_idle_per_host(2)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()
            .map_err(|_| KnowledgeResultReadError::Invalid)?;
        Ok(Self {
            client,
            base_url: Arc::from(policy.base_url.as_str()),
            service_secret: Arc::from(policy.service_secret()),
            max_response_bytes: policy.max_response_bytes,
        })
    }

    /// Reads one result and verifies its exact analysis and digest linkage.
    ///
    /// # Errors
    ///
    /// Returns a finite unavailable or invalid class without upstream diagnostics.
    pub async fn read(
        &self,
        analysis_id: Uuid,
        expected_digest_hex: &str,
    ) -> Result<KnowledgeResultProjection, KnowledgeResultReadError> {
        if !is_sha256_hex(expected_digest_hex) {
            return Err(KnowledgeResultReadError::Invalid);
        }
        let response = self
            .client
            .get(format!(
                "{}/internal/channel-digest-results/{analysis_id}",
                self.base_url
            ))
            .bearer_auth(self.service_secret.as_ref())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| KnowledgeResultReadError::Unavailable)?;
        if response.status().is_server_error() {
            return Err(KnowledgeResultReadError::Unavailable);
        }
        if response.status() != reqwest::StatusCode::OK {
            return Err(KnowledgeResultReadError::Invalid);
        }
        let content_type_is_json = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
        if !content_type_is_json {
            return Err(KnowledgeResultReadError::Invalid);
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(KnowledgeResultReadError::Invalid);
        }
        let mut bytes = Vec::with_capacity(
            response
                .content_length()
                .and_then(|length| usize::try_from(length).ok())
                .unwrap_or_default()
                .min(self.max_response_bytes),
        );
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| KnowledgeResultReadError::Unavailable)?;
            if bytes.len().saturating_add(chunk.len()) > self.max_response_bytes {
                return Err(KnowledgeResultReadError::Invalid);
            }
            bytes.extend_from_slice(&chunk);
        }
        let envelope: KnowledgeEnvelope =
            serde_json::from_slice(&bytes).map_err(|_| KnowledgeResultReadError::Invalid)?;
        if envelope.analysis_id != analysis_id
            || envelope.result_digest.algorithm != "sha256"
            || !is_sha256_hex(&envelope.result_digest.hex)
            || envelope.result_digest.hex != expected_digest_hex
            || !envelope.recap.is_object()
        {
            return Err(KnowledgeResultReadError::Invalid);
        }
        Ok(KnowledgeResultProjection {
            analysis_id,
            result_digest_hex: envelope.result_digest.hex,
            recap: envelope.recap,
        })
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
