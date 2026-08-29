#![forbid(unsafe_code)]

//! Loopback owner-authorized channel-digest API process.

use ratatoskr_channel_digests::{Config, Role, run_api};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(Role::Api)?;
    if std::env::args().nth(1).as_deref() == Some("check-config") {
        return Ok(());
    }
    run_api(config).await?;
    Ok(())
}
