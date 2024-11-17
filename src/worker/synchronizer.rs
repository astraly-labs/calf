use futures_util::future::try_join_all;
use tokio::task::JoinHandle;
use tracing::Instrument;

pub(crate) struct Synchronizer {
    
}

impl Synchronizer {
    #[must_use]
    pub async fn spawn() -> JoinHandle<()> {
        tokio::spawn(async move {
            let _ = Self {}
                .run()
                .await
                .instrument(tracing::info_span!("synchronizer"));
        })
    }

    pub async fn run(&mut self) {
        let Self {} = self;

        let tasks = vec![tokio::spawn(synchronizer_task())];

        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in Synchronizer: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn synchronizer_task() {
    todo!()
}
