use std::sync::Arc;

use futures_util::future::try_join_all;
use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tracing::Instrument;

use crate::{
    db::Db,
    types::{
        BatchAcknowledgement, NetworkRequest, ReceivedAcknoledgement, RequestPayload, TxBatch,
    },
};
// ARBITRAIRE !!!
const BATCH_QUORUM_TIMEOUT: u128 = 100; //ms
pub(crate) struct BatchBroadcaster {
    batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl BatchBroadcaster {
    pub fn new(
        batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
        network_tx: mpsc::Sender<NetworkRequest>,
    ) -> Self {
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
async fn broadcast_task(
    mut rx: tokio::sync::broadcast::Receiver<TxBatch>,
    network_tx: mpsc::Sender<NetworkRequest>,
) {
    while let Ok(batch) = rx.recv().await {
        tracing::info!("Broadcasting batch: {:?}", batch);
        network_tx
            .send(NetworkRequest::Broadcast(RequestPayload::Batch(batch)))
            .await
            .expect("Failed to broadcast batch");
    }
}

struct WaitingBatch {
    ack_number: u32,
    batch: TxBatch,
    digest: blake3::Hash,
    timestamp: tokio::time::Instant,
}

impl WaitingBatch {
    fn new(batch: TxBatch) -> Self {
        let digest = blake3::hash(&bincode::serialize(&batch).expect("batch hash failed"));
        Self {
            ack_number: 0,
            batch,
            digest,
            timestamp: tokio::time::Instant::now(),
        }
    }
}

#[tracing::instrument(skip_all)]
pub async fn quorum_waiter_task(
    mut batches_rx: tokio::sync::broadcast::Receiver<TxBatch>,
    mut acknolwedgements_rx: tokio::sync::mpsc::Receiver<ReceivedAcknoledgement>,
    quorum_threshold: u32,
    digest_tx: tokio::sync::mpsc::Sender<blake3::Hash>,
    db: Arc<Db>,
) -> Result<(), tokio::task::JoinError> {
    let mut batches = vec![];
    loop {
        tokio::select! {
            Ok(batch) = batches_rx.recv() => {
                let waiting_batch = WaitingBatch::new(batch);
                if batches.iter().any(|elm: &WaitingBatch| {
                    elm.digest.as_bytes() == waiting_batch.digest.as_bytes()
                }) {
                    tracing::warn!("Received duplicate batch");
                }
                else {
                    batches.push(waiting_batch);
                    tracing::info!("Received new batch");
                }
                let now = tokio::time::Instant::now();
                //perfectible ? rayon ? plein de timers dans le select ?
                for i in 0..batches.len() {
                    if now.duration_since(batches[i].timestamp).as_millis() > BATCH_QUORUM_TIMEOUT {
                        tracing::warn!("Batch timed out: {:?}", batches[i].digest);
                        batches.remove(i);
                    }
                }
            },
            Some(ack) = acknolwedgements_rx.recv() => {
                let ack = ack.acknoledgement;
                match batches.iter().position(|b| b.digest.as_bytes() == ack.as_slice()) {
                    Some(batch_index) => {
                        let batch = &mut batches[batch_index];
                        batch.ack_number += 1;
                        if batch.ack_number >= quorum_threshold {
                            tracing::info!("Batch is now confirmed: {:?}", batch.digest);
                            digest_tx.send(batch.digest).await.expect("Failed to send digest");
                            match db.insert(crate::db::Column::Batches, &batch.digest.to_string(), &batch.batch) {
                                Ok(_) => {
                                    tracing::info!("Batch inserted in DB");
                                },
                                Err(e) => {
                                    tracing::error!("Failed to insert batch in DB: {:?}", e);
                                }
                            }
                            batches.remove(batch_index);
                        }
                    },
                    None => {
                        tracing::warn!("Received ack for unknown batch");
                    }
                };
            }
        }
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
