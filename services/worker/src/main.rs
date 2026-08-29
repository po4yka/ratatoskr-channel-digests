#![forbid(unsafe_code)]

//! Event-driven channel acquisition and digest execution worker.

use ratatoskr_channel_digests::{Config, Role, SessionMaterial, run_worker};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load(Role::Worker)?;
    let provider = config
        .provider
        .as_ref()
        .ok_or("worker provider configuration is absent")?;
    let session = SessionMaterial::load(provider)?;
    if std::env::args().nth(1).as_deref() == Some("check-config") {
        return Ok(());
    }
    run_worker(config, session).await?;
    Ok(())
}
