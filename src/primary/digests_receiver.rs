use std::sync::Arc;

use tokio::{
    sync::{broadcast, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{Digest, ReceivedObject},
    utils::CircularBuffer,
};

pub(crate) struct DigestReceiver {
    pub digest_rx: broadcast::Receiver<ReceivedObject<Digest>>,
    pub buffer: Arc<Mutex<CircularBuffer<Digest>>>,
    pub db: Arc<Db>,
}

impl DigestReceiver {
    pub fn spawn(
        digests_rx: broadcast::Receiver<ReceivedObject<Digest>>,
        buffer: Arc<Mutex<CircularBuffer<Digest>>>,
        db: Arc<Db>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        digest_rx: digests_rx,
                        buffer,
                        db,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("digest receviver finished");
                        }
                        Err(e) => {
                            tracing::error!("digest receiver finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("VoteAggregator cancelled");
                }
            };
        })
    }
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let digest = self.digest_rx.recv().await?;
            self.db.insert(
                db::Column::Digests,
                &hex::encode(digest.object),
                &digest.object,
            )?;
            self.buffer.lock().await.push(digest.object);
        }
    }
}
