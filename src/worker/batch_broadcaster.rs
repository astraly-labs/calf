use futures_util::future::try_join_all;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::Instrument;

use crate::types::{NetworkRequest, TxBatch};

pub(crate) struct BatchBroadcaster {
    batches_rx: Receiver<TxBatch>,
    network_tx: Sender<NetworkRequest>,
}

impl BatchBroadcaster {
    pub fn new(batches_rx: Receiver<TxBatch>, network_tx: Sender<NetworkRequest>) -> Self {
        Self {
            batches_rx,
            network_tx,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        tokio::spawn(
            self.run()
                .instrument(tracing::info_span!("batch_broadcaster")),
        )
    }

    pub async fn run(self) {
        let Self {
            batches_rx,
            network_tx,
        } = self;

        let tasks = vec![tokio::spawn(broadcast_task(batches_rx, network_tx))];

        if let Err(e) = try_join_all(tasks).await {
            tracing::error!("Error in BatchBroadcaster: {:?}", e);
        }
    }
}

#[tracing::instrument(skip_all)]
async fn broadcast_task(mut rx: Receiver<TxBatch>, network_tx: Sender<NetworkRequest>) {
    while let Some(batch) = rx.recv().await {
        tracing::info!("Broadcasting batch: {:?}", batch);
        let encoded_batch = bincode::serialize(&batch).unwrap();
        network_tx
            .send(NetworkRequest::Broadcast(encoded_batch))
            .await
            .expect("Failed to broadcast batch");
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use crate::types::Transaction;

    use super::*;
    use rstest::*;
    use tokio::sync::mpsc;

    const CHANNEL_CAPACITY: usize = 1000;

    type BatchBroadcasterFixture = (Receiver<NetworkRequest>, Sender<TxBatch>, JoinHandle<()>);

    #[fixture]
    fn launch_batch_broadcaster() -> BatchBroadcasterFixture {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (batches_tx, batches_rx) = mpsc::channel(CHANNEL_CAPACITY);

        let batch_maker = BatchBroadcaster::new(batches_rx, tx);

        let handle = batch_maker.spawn();

        (rx, batches_tx, handle)
    }

    #[rstest]
    #[tokio::test]
    async fn test_broadcast_task(launch_batch_broadcaster: BatchBroadcasterFixture) {
        let (mut rx, tx, _) = launch_batch_broadcaster;

        let batch = vec![Transaction::new(vec![1; 100])];

        tx.send(batch.clone()).await.unwrap();

        let network_request = rx.recv().await.unwrap();

        match network_request {
            NetworkRequest::Broadcast(encoded_batch) => {
                let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
                assert_eq!(decoded_batch, batch);
            }
            _ => panic!("Expected NetworkRequest::Broadcast"),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_broadcast_task_multiple_batches(
        launch_batch_broadcaster: BatchBroadcasterFixture,
    ) {
        let (mut rx, tx, _) = launch_batch_broadcaster;

        let batch1 = vec![Transaction::new(vec![1; 100])];
        let batch2 = vec![Transaction::new(vec![2; 100])];

        tx.send(batch1.clone()).await.unwrap();
        tx.send(batch2.clone()).await.unwrap();

        let network_request1 = rx.recv().await.unwrap();
        let network_request2 = rx.recv().await.unwrap();

        match network_request1 {
            NetworkRequest::Broadcast(encoded_batch) => {
                let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
                assert_eq!(decoded_batch, batch1);
            }
            _ => panic!("Expected NetworkRequest::Broadcast"),
        }

        match network_request2 {
            NetworkRequest::Broadcast(encoded_batch) => {
                let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
                assert_eq!(decoded_batch, batch2);
            }
            _ => panic!("Expected NetworkRequest::Broadcast"),
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_broadcast_task_no_batches(launch_batch_broadcaster: BatchBroadcasterFixture) {
        let (mut rx, _, _) = launch_batch_broadcaster;

        let receive_timeout = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;

        assert!(receive_timeout.is_err());
    }
}
