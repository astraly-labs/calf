use types::agents::{agent_main, LoadableFromSettings, Settings};
use worker::Worker;

pub mod db;
pub mod primary;
pub mod settings;
pub mod types;
pub mod utils;
pub mod worker;

#[macro_export]
macro_rules! safe_send {
    ($sender:expr, $message:expr, $error_msg:expr) => {
        if let Err(err) = $sender.send($message).await {
            tracing::error!(
                "{}: Failed to send message. Reason: {}",
                $error_msg,
                err
            );
        }
    };
}

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
    let subscriber = tracing_subscriber::fmt()
        .compact()
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    agent_main::<Worker>().await?;

    Ok(())
}
