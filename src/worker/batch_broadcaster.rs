use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::types::{NetworkRequest, RequestPayload, TxBatch};

pub(crate) struct BatchBroadcaster {
    batches_rx: broadcast::Receiver<TxBatch>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl BatchBroadcaster {
    #[must_use]
    pub fn spawn(
        batches_rx: broadcast::Receiver<TxBatch>,
        network_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        batches_rx,
                        network_tx,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("BatchBroadcaster finnished");
                        }
                        Err(e) => {
                            tracing::error!("BatchBroadcaster finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("BatchBroadcaster cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        while let Ok(batch) = self.batches_rx.recv().await {
            tracing::info!("Broadcasting batch: {:?}", batch);
            self.network_tx
                .send(NetworkRequest::Broadcast(RequestPayload::Batch(batch)))
                .await?;
        }
        Ok(())
    }
}

// #[cfg(test)]
// mod test {
//     use std::time::Duration;

//     use crate::types::Transaction;

//     use super::*;
//     use rstest::*;
//     use tokio::sync::mpsc;

//     const CHANNEL_CAPACITY: usize = 1000;

//     type BatchBroadcasterFixture = (
//         mpsc::Receiver<NetworkRequest>,
//         tokio::sync::broadcast::Sender<TxBatch>,
//         JoinHandle<()>,
//     );

//     #[fixture]
//     fn launch_batch_broadcaster() -> BatchBroadcasterFixture {
//         let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
//         let (batches_tx, batches_rx) = tokio::sync::broadcast::channel(CHANNEL_CAPACITY);

//         let batch_maker = BatchBroadcaster::new(batches_rx, tx);

//         let handle = batch_maker.spawn();

//         (rx, batches_tx, handle)
//     }

//     #[rstest]
//     #[tokio::test]
//     async fn test_broadcast_task(launch_batch_broadcaster: BatchBroadcasterFixture) {
//         let (mut rx, tx, _) = launch_batch_broadcaster;

//         let batch = vec![Transaction::new(vec![1; 100])];

//         tx.send(batch.clone()).unwrap();

//         let network_request = rx.recv().await.unwrap();

//         match network_request {
//             NetworkRequest::Broadcast(batch) => {
//                 //let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
//                 assert_eq!(decoded_batch, batch);
//             }
//             _ => panic!("Expected NetworkRequest::Broadcast"),
//         }
//     }

//     #[rstest]
//     #[tokio::test]
//     async fn test_broadcast_task_multiple_batches(
//         launch_batch_broadcaster: BatchBroadcasterFixture,
//     ) {
//         let (mut rx, tx, _) = launch_batch_broadcaster;

//         let batch1 = vec![Transaction::new(vec![1; 100])];
//         let batch2 = vec![Transaction::new(vec![2; 100])];

//         tx.send(batch1.clone()).unwrap();
//         tx.send(batch2.clone()).unwrap();

//         let network_request1 = rx.recv().await.unwrap();
//         let network_request2 = rx.recv().await.unwrap();

//         match network_request1 {
//             NetworkRequest::Broadcast(encoded_batch) => {
//                 let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
//                 assert_eq!(decoded_batch, batch1);
//             }
//             _ => panic!("Expected NetworkRequest::Broadcast"),
//         }

//         match network_request2 {
//             NetworkRequest::Broadcast(encoded_batch) => {
//                 let decoded_batch: TxBatch = bincode::deserialize(&encoded_batch).unwrap();
//                 assert_eq!(decoded_batch, batch2);
//             }
//             _ => panic!("Expected NetworkRequest::Broadcast"),
//         }
//     }

//     #[rstest]
//     #[tokio::test]
//     async fn test_broadcast_task_no_batches(launch_batch_broadcaster: BatchBroadcasterFixture) {
//         let (mut rx, _, _) = launch_batch_broadcaster;

//         let receive_timeout = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;

//         assert!(receive_timeout.is_err());
//     }
// }
