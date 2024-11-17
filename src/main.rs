use types::agents::{agent_main, LoadableFromSettings, Settings};
use worker::Worker;

pub mod db;
pub mod primary;
pub mod settings;
pub mod types;
pub mod worker;

// Empty settings for now
impl AsRef<Settings> for Settings {
    fn as_ref(&self) -> &Settings {
        self
    }
}

impl LoadableFromSettings for Settings {
    fn load() -> anyhow::Result<Self> {
        Ok(Settings {})
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    agent_main::<Worker>().await?;

    Ok(())
}
