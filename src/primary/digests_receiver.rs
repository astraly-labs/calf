use std::sync::Arc;

use proc_macros::Spawn;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{Digest, ReceivedObject},
    utils::CircularBuffer,
};

#[derive(Spawn)]
pub(crate) struct DigestReceiver {
    pub digest_rx: broadcast::Receiver<ReceivedObject<Digest>>,
    pub buffer: Arc<Mutex<CircularBuffer<Digest>>>,
    pub db: Arc<Db>,
}

impl DigestReceiver {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let digest = self.digest_rx.recv().await?;
            tracing::info!(
                "digest {}: \"I am in the digest receiver\"",
                hex::encode(&digest.object)
            );
            self.db.insert(
                db::Column::Digests,
                &hex::encode(digest.object),
                &digest.object,
            )?;
            self.buffer.lock().await.push(digest.object);
        }
    }
}
