use tokio::{sync::mpsc, task::JoinHandle};

use crate::{safe_send, types::{NetworkRequest, ReceivedBatch, RequestPayload}};

pub struct Batchacknowledger {
    batches_rx: mpsc::Receiver<ReceivedBatch>,
    resquests_tx: mpsc::Sender<NetworkRequest>,
}

impl Batchacknowledger {
    #[must_use]
    pub fn spawn(
        batches_rx: mpsc::Receiver<ReceivedBatch>,
        resquests_tx: mpsc::Sender<NetworkRequest>,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {Self {batches_rx, resquests_tx}.run().await})
    }
    
    pub async fn run(mut self) {
        while let Some(batch) = self.batches_rx.recv().await {
            tracing::info!("Received batch from {}", batch.sender);
            let digest = blake3::hash(&bincode::serialize(&batch.batch).expect("batch type that implements Serialize and has been deserialized from binary data can't be serialized : could it really append ?"));
            safe_send!(self.resquests_tx, NetworkRequest::SendTo(
                batch.sender,
                RequestPayload::Acknoledgment(digest.as_bytes().to_vec()),
            ), "failed to send acknoledgment");
        }
    }
}
