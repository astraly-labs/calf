use std::sync::Arc;

use proc_macros::Spawn;
use tokio::sync::{broadcast, Mutex};
use tokio_util::sync::CancellationToken;

use crate::{
    db::{self, Db},
    types::{batch::BatchId, network::ReceivedObject, traits::AsHex, Digest},
    utils::CircularBuffer,
};

#[derive(Spawn)]
pub(crate) struct DigestReceiver {
    pub digest_rx: broadcast::Receiver<ReceivedObject<BatchId>>,
    pub buffer: Arc<Mutex<CircularBuffer<BatchId>>>,
    pub db: Arc<Db>,
}

impl DigestReceiver {
    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            let digest = self.digest_rx.recv().await?;
            self.db.insert(
                db::Column::Digests,
                &digest.object.0.as_hex_string(),
                &digest.object,
            )?;
            self.buffer.lock().await.push(digest.object);
        }
    }
}
