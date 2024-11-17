#[derive(Debug)]
pub struct Settings {}

/// Settings of an agent defined from configuration
pub trait LoadableFromSettings: AsRef<Settings> + Sized {
    /// Create a new instance of these settings by reading the configs and env
    /// vars.
    fn load() -> anyhow::Result<Self>;
}

/// A fundamental agent which does not make any assumptions about the tools
/// which are used.
#[async_trait::async_trait]
pub trait BaseAgent: Send + Sync + std::fmt::Debug {
    /// The agent's name
    const AGENT_NAME: &'static str;

    /// The settings object for this agent
    type Settings: LoadableFromSettings;

    /// Instantiate the agent from the standard settings object
    async fn from_settings(settings: Self::Settings) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Start running this agent.
    #[allow(clippy::async_yields_async)]
    async fn run(self);
}

/// Call this from `main` to fully initialize and run the agent for its entire
/// lifecycle. This assumes only a single agent is being run. This will
/// initialize the metrics server and tracing as well.
#[allow(unexpected_cfgs)] // TODO: `rustc` 1.80.1 clippy issue
pub async fn agent_main<A: BaseAgent>() -> anyhow::Result<()> {
    let settings = A::Settings::load()?;

    let agent = A::from_settings(settings).await?;

    // This await will only end if a panic happens. We won't crash, but instead gracefully shut down
    agent.run().await;
    tracing::info!(agent = A::AGENT_NAME, "Shutting down agent...");
    Ok(())
}
