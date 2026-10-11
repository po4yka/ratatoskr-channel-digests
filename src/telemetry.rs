//! The process log subscriber both binaries install (XR-021 CONTRACTS.md R3-10 rule 5).
//!
//! Without a subscriber every `tracing` event is dropped, so the warnings the operations guide
//! promises never reached an operator. Events carry safe classes and counts only: no post body,
//! channel name, token or session byte is ever a field.

use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, fmt};

use crate::{Config, LogFormat};

/// Safe subscriber installation failure.
#[derive(Debug, thiserror::Error)]
#[error("the log subscriber is unavailable")]
pub struct TelemetryError;

/// Installs the global subscriber from the validated telemetry settings, writing to standard output.
///
/// Call it once, first thing after the configuration loads.
///
/// # Errors
///
/// Returns a safe class when the filter does not parse (normally unreachable, because the
/// configuration loader validated it) or when a subscriber is already installed.
pub fn install_subscriber(config: &Config) -> Result<(), TelemetryError> {
    let filter = EnvFilter::try_new(&config.telemetry.log_filter).map_err(|_| TelemetryError)?;
    let layers = tracing_subscriber::registry().with(filter);
    let installed = match config.telemetry.log_format {
        LogFormat::Json => layers
            .with(
                fmt::layer()
                    .json()
                    .with_current_span(true)
                    .with_span_list(false)
                    .with_writer(std::io::stdout),
            )
            .try_init(),
        LogFormat::Pretty => layers
            .with(fmt::layer().pretty().with_writer(std::io::stdout))
            .try_init(),
    };
    installed.map_err(|_| TelemetryError)
}
