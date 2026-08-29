//! Strict role-scoped process configuration.

use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Executable role selected before configuration is decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Owner-authorized loopback domain API.
    Api,
    /// Provider and event-driven execution worker.
    Worker,
}

/// A value that is available only to an explicit transport boundary.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct Secret(String);

impl Secret {
    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[redacted]")
    }
}

/// Database connection and finite-pool configuration.
#[derive(Debug, Clone)]
pub struct DatabaseConfig {
    pub(crate) url: Secret,
    /// Maximum `PostgreSQL` connections owned by this process.
    pub max_connections: u32,
    /// Pool acquisition timeout.
    pub acquire_timeout_ms: u64,
}

impl DatabaseConfig {
    /// Exposes the database URL only to process composition.
    #[must_use]
    pub fn url(&self) -> &str {
        self.url.expose()
    }
}

/// One bound listener.
#[derive(Debug, Clone, Copy)]
pub struct ListenerConfig {
    /// Exact loopback socket address.
    pub listen_address: SocketAddr,
}

/// Shared finite input and execution limits.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum decoded request body.
    pub request_bytes: usize,
    /// Maximum list page size.
    pub page_size: usize,
    /// Maximum selected source revisions.
    pub source_count: usize,
    /// Maximum active/source channels.
    pub channel_count: usize,
    /// Maximum normalized bytes per source revision.
    pub source_bytes: usize,
    /// Maximum attempts for retryable execution.
    pub retry_attempts: u32,
    /// Graceful shutdown bound.
    pub shutdown_timeout_ms: u64,
}

/// Worker-only `MTProto` identity and session paths.
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    /// Telegram application identifier.
    pub api_id: i32,
    api_hash: Secret,
    /// Encrypted session artifact path.
    pub session_file: PathBuf,
    /// Separate session decryption key path.
    pub session_key_file: PathBuf,
    /// Maximum concurrent provider calls.
    pub max_concurrency: usize,
}

impl ProviderConfig {
    /// Exposes the Telegram application hash only to the provider adapter.
    #[must_use]
    pub fn api_hash(&self) -> &str {
        self.api_hash.expose()
    }
}

/// Worker bus connection policy.
#[derive(Debug, Clone)]
pub struct BusConfig {
    /// Fixed NATS endpoint.
    pub endpoint: String,
}

/// API-only authority and finite policy for reading completed recaps from Knowledge.
#[derive(Debug, Clone)]
pub struct KnowledgeResultReaderConfig {
    /// Exact loopback HTTP origin of the Knowledge admin listener.
    pub base_url: String,
    service_secret: Secret,
    /// Maximum time allowed to establish one connection.
    pub connect_timeout_ms: u64,
    /// End-to-end deadline for one result read.
    pub request_timeout_ms: u64,
    /// Maximum accepted response bytes.
    pub max_response_bytes: usize,
}

impl KnowledgeResultReaderConfig {
    /// Exposes the dedicated credential only to the Knowledge HTTP boundary.
    #[must_use]
    pub fn service_secret(&self) -> &str {
        self.service_secret.expose()
    }
}

/// Strict effective configuration with role-inexpressible provider authority.
#[derive(Debug, Clone)]
pub struct Config {
    /// Selected executable role.
    pub role: Role,
    /// Database policy.
    pub database: DatabaseConfig,
    /// API listener (unused by worker but kept stable for structural inspection).
    pub api: ListenerConfig,
    /// Role-specific operator listener.
    pub operator: ListenerConfig,
    /// Shared finite limits.
    pub limits: Limits,
    /// Present only for the worker role.
    pub provider: Option<ProviderConfig>,
    /// Present only for the worker role.
    pub bus: Option<BusConfig>,
    /// Present only for the API role.
    pub knowledge_result_reader: Option<KnowledgeResultReaderConfig>,
    service_secret: Secret,
}

/// Safe configuration failure that never includes the rejected value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid configuration for {key}: {reason}")]
pub struct ConfigError {
    key: String,
    reason: &'static str,
}

impl ConfigError {
    fn new(key: &str, reason: &'static str) -> Self {
        Self {
            key: key.to_owned(),
            reason,
        }
    }
}

impl Config {
    /// Loads the selected role from the real process environment.
    ///
    /// # Errors
    ///
    /// Returns a safe strict-key or finite-value error.
    pub fn load(role: Role) -> Result<Self, ConfigError> {
        Self::from_environment(role, std::env::vars())
    }

    /// Decodes a deterministic environment fixture.
    ///
    /// # Errors
    ///
    /// Returns a safe error for unknown keys, invalid values, missing authority, or role leakage.
    pub fn from_environment<I, K, V>(role: Role, entries: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut builder = Builder::new(role);
        for (key, value) in entries {
            let key = key.as_ref();
            if key.starts_with("RATATOSKR__") {
                builder.apply(key, value.as_ref())?;
            }
        }
        builder.finish()
    }

    /// Exposes the fixed service credential only to the HTTP authorization boundary.
    #[must_use]
    pub fn service_secret(&self) -> &str {
        self.service_secret.expose()
    }
}

struct Builder {
    role: Role,
    database_url: Option<Secret>,
    service_secret: Option<Secret>,
    provider_api_id: Option<i32>,
    provider_api_hash: Option<Secret>,
    session_file: Option<PathBuf>,
    session_key_file: Option<PathBuf>,
    bus_endpoint: Option<String>,
    knowledge_base_url: Option<String>,
    knowledge_result_reader_service_secret: Option<Secret>,
    knowledge_connect_timeout_ms: Option<u64>,
    knowledge_request_timeout_ms: Option<u64>,
    knowledge_max_response_bytes: Option<usize>,
    api_listen_address: SocketAddr,
    operator_listen_address: SocketAddr,
    limits: Limits,
}

impl Builder {
    fn new(role: Role) -> Self {
        Self {
            role,
            database_url: None,
            service_secret: None,
            provider_api_id: None,
            provider_api_hash: None,
            session_file: None,
            session_key_file: None,
            bus_endpoint: None,
            knowledge_base_url: None,
            knowledge_result_reader_service_secret: None,
            knowledge_connect_timeout_ms: None,
            knowledge_request_timeout_ms: None,
            knowledge_max_response_bytes: None,
            api_listen_address: SocketAddr::from(([127, 0, 0, 1], 8098)),
            operator_listen_address: SocketAddr::from((
                [127, 0, 0, 1],
                if role == Role::Api { 9469 } else { 9470 },
            )),
            limits: Limits {
                request_bytes: 262_144,
                page_size: 100,
                source_count: 100,
                channel_count: 20,
                source_bytes: 16_384,
                retry_attempts: 3,
                shutdown_timeout_ms: 120_000,
            },
        }
    }

    fn apply(&mut self, key: &str, value: &str) -> Result<(), ConfigError> {
        match key {
            "RATATOSKR__DATABASE__URL" => self.database_url = Some(nonempty_secret(key, value)?),
            "RATATOSKR__AUTH__SERVICE_SECRET" => {
                self.service_secret = Some(nonempty_secret(key, value)?);
            }
            "RATATOSKR__PROVIDER__API_ID" if self.role == Role::Worker => {
                self.provider_api_id = Some(parse_positive(key, value)?);
            }
            "RATATOSKR__PROVIDER__API_HASH" if self.role == Role::Worker => {
                self.provider_api_hash = Some(nonempty_secret(key, value)?);
            }
            "RATATOSKR__PROVIDER__SESSION_FILE" if self.role == Role::Worker => {
                self.session_file = Some(absolute_path(key, value)?);
            }
            "RATATOSKR__PROVIDER__SESSION_KEY_FILE" if self.role == Role::Worker => {
                self.session_key_file = Some(absolute_path(key, value)?);
            }
            "RATATOSKR__BUS__ENDPOINT" if self.role == Role::Worker => {
                self.bus_endpoint = Some(nonempty(key, value)?.to_owned());
            }
            "RATATOSKR__KNOWLEDGE__BASE_URL" if self.role == Role::Api => {
                self.knowledge_base_url = Some(loopback_http_base_url(key, value)?);
            }
            "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET" if self.role == Role::Api => {
                self.knowledge_result_reader_service_secret =
                    Some(bounded_secret(key, value, 4_096)?);
            }
            "RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS" if self.role == Role::Api => {
                self.knowledge_connect_timeout_ms = Some(parse_range(key, value, &1, &10_000)?);
            }
            "RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS" if self.role == Role::Api => {
                self.knowledge_request_timeout_ms = Some(parse_range(key, value, &1, &30_000)?);
            }
            "RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES" if self.role == Role::Api => {
                self.knowledge_max_response_bytes = Some(parse_range(key, value, &1, &65_536)?);
            }
            "RATATOSKR__API__LISTEN_ADDRESS" if self.role == Role::Api => {
                self.api_listen_address = parse_loopback_address(key, value)?;
            }
            "RATATOSKR__OPERATOR__LISTEN_ADDRESS" => {
                self.operator_listen_address = parse_loopback_address(key, value)?;
            }
            "RATATOSKR__LIMITS__REQUEST_BYTES" => {
                self.limits.request_bytes = parse_range(key, value, &1, &1_048_576)?;
            }
            "RATATOSKR__LIMITS__PAGE_SIZE" => {
                self.limits.page_size = parse_range(key, value, &1, &100)?;
            }
            "RATATOSKR__LIMITS__SOURCE_COUNT" => {
                self.limits.source_count = parse_range(key, value, &1, &100)?;
            }
            "RATATOSKR__LIMITS__CHANNEL_COUNT" => {
                self.limits.channel_count = parse_range(key, value, &1, &20)?;
            }
            "RATATOSKR__LIMITS__SOURCE_BYTES" => {
                self.limits.source_bytes = parse_range(key, value, &1, &16_384)?;
            }
            "RATATOSKR__LIMITS__RETRY_ATTEMPTS" => {
                self.limits.retry_attempts = parse_range(key, value, &1, &10)?;
            }
            "RATATOSKR__LIMITS__SHUTDOWN_TIMEOUT_MS" => {
                self.limits.shutdown_timeout_ms = parse_range(key, value, &1, &130_000)?;
            }
            _ => return Err(ConfigError::new(key, "is not recognized for this role")),
        }
        Ok(())
    }

    fn finish(self) -> Result<Config, ConfigError> {
        let database_url = required(self.database_url, "RATATOSKR__DATABASE__URL")?;
        let service_secret = required(self.service_secret, "RATATOSKR__AUTH__SERVICE_SECRET")?;
        let (provider, bus, knowledge_result_reader) = match self.role {
            Role::Api => {
                let connect_timeout_ms = required(
                    self.knowledge_connect_timeout_ms,
                    "RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS",
                )?;
                let request_timeout_ms = required(
                    self.knowledge_request_timeout_ms,
                    "RATATOSKR__KNOWLEDGE__REQUEST_TIMEOUT_MS",
                )?;
                if connect_timeout_ms > request_timeout_ms {
                    return Err(ConfigError::new(
                        "RATATOSKR__KNOWLEDGE__CONNECT_TIMEOUT_MS",
                        "must not exceed the request timeout",
                    ));
                }
                (
                    None,
                    None,
                    Some(KnowledgeResultReaderConfig {
                        base_url: required(
                            self.knowledge_base_url,
                            "RATATOSKR__KNOWLEDGE__BASE_URL",
                        )?,
                        service_secret: required(
                            self.knowledge_result_reader_service_secret,
                            "RATATOSKR__KNOWLEDGE__RESULT_READER_SERVICE_SECRET",
                        )?,
                        connect_timeout_ms,
                        request_timeout_ms,
                        max_response_bytes: required(
                            self.knowledge_max_response_bytes,
                            "RATATOSKR__KNOWLEDGE__MAX_RESPONSE_BYTES",
                        )?,
                    }),
                )
            }
            Role::Worker => (
                Some(ProviderConfig {
                    api_id: required(self.provider_api_id, "RATATOSKR__PROVIDER__API_ID")?,
                    api_hash: required(self.provider_api_hash, "RATATOSKR__PROVIDER__API_HASH")?,
                    session_file: required(self.session_file, "RATATOSKR__PROVIDER__SESSION_FILE")?,
                    session_key_file: required(
                        self.session_key_file,
                        "RATATOSKR__PROVIDER__SESSION_KEY_FILE",
                    )?,
                    max_concurrency: 2,
                }),
                Some(BusConfig {
                    endpoint: required(self.bus_endpoint, "RATATOSKR__BUS__ENDPOINT")?,
                }),
                None,
            ),
        };
        Ok(Config {
            role: self.role,
            database: DatabaseConfig {
                url: database_url,
                max_connections: 8,
                acquire_timeout_ms: 5_000,
            },
            api: ListenerConfig {
                listen_address: self.api_listen_address,
            },
            operator: ListenerConfig {
                listen_address: self.operator_listen_address,
            },
            limits: self.limits,
            provider,
            bus,
            knowledge_result_reader,
            service_secret,
        })
    }
}

fn nonempty_secret(key: &str, value: &str) -> Result<Secret, ConfigError> {
    Ok(Secret::new(nonempty(key, value)?.to_owned()))
}

fn bounded_secret(key: &str, value: &str, maximum_bytes: usize) -> Result<Secret, ConfigError> {
    let value = nonempty(key, value)?;
    if value.len() <= maximum_bytes {
        Ok(Secret::new(value.to_owned()))
    } else {
        Err(ConfigError::new(key, "exceeds the finite byte limit"))
    }
}

fn nonempty<'a>(key: &str, value: &'a str) -> Result<&'a str, ConfigError> {
    if value.is_empty() {
        Err(ConfigError::new(key, "must not be empty"))
    } else {
        Ok(value)
    }
}

fn absolute_path(key: &str, value: &str) -> Result<PathBuf, ConfigError> {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(ConfigError::new(key, "must be an absolute path"))
    }
}

fn parse_loopback_address(key: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| ConfigError::new(key, "must be a loopback socket address"))?;
    if address.ip().is_loopback() && address.port() != 0 {
        Ok(address)
    } else {
        Err(ConfigError::new(key, "must be a loopback socket address"))
    }
}

fn loopback_http_base_url(key: &str, value: &str) -> Result<String, ConfigError> {
    let uri = value
        .parse::<axum::http::Uri>()
        .map_err(|_| ConfigError::new(key, "must be a loopback HTTP origin"))?;
    let authority = uri
        .authority()
        .ok_or_else(|| ConfigError::new(key, "must be a loopback HTTP origin"))?;
    let address = authority
        .as_str()
        .parse::<SocketAddr>()
        .map_err(|_| ConfigError::new(key, "must be a loopback HTTP origin"))?;
    let path_is_origin = uri.path_and_query().is_none_or(|path| path.as_str() == "/");
    if uri.scheme_str() == Some("http")
        && address.ip().is_loopback()
        && address.port() != 0
        && path_is_origin
    {
        Ok(format!("http://{authority}"))
    } else {
        Err(ConfigError::new(key, "must be a loopback HTTP origin"))
    }
}

fn parse_positive<T>(key: &str, value: &str) -> Result<T, ConfigError>
where
    T: std::str::FromStr + Default + PartialOrd,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| ConfigError::new(key, "must be a positive integer"))?;
    if parsed > T::default() {
        Ok(parsed)
    } else {
        Err(ConfigError::new(key, "must be a positive integer"))
    }
}

fn parse_range<T>(key: &str, value: &str, minimum: &T, maximum: &T) -> Result<T, ConfigError>
where
    T: std::str::FromStr + PartialOrd,
{
    let parsed = value
        .parse::<T>()
        .map_err(|_| ConfigError::new(key, "is outside the finite range"))?;
    if &parsed >= minimum && &parsed <= maximum {
        Ok(parsed)
    } else {
        Err(ConfigError::new(key, "is outside the finite range"))
    }
}

fn required<T>(value: Option<T>, key: &str) -> Result<T, ConfigError> {
    value.ok_or_else(|| ConfigError::new(key, "is required"))
}
