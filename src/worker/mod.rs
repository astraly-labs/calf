use blake3::{self, Hash};
use std::time::Duration;
use tokio::task::JoinSet;

use crate::{
    config::{NetworkInfos, WorkerConfig, WorkerInfo},
    types::{services::Service, Transaction, TxBatch},
};

#[derive(Debug)]
pub struct Worker {
    pub config: WorkerConfig,
    pub current_batch: Vec<Transaction>,
    pub other_workers: Vec<WorkerInfo>,
}

#[derive(thiserror::Error, Debug)]
pub enum WorkerIntialisationError {
    #[error("No peers in configuration")]
    NoPeers,
}

impl Worker {
    // établis la connexion avec ses pairs (les workers de même id d'autres validateurs) et le primary pour lequel il travaille (TODO)
    // récupère des txs ->
    // les agrège en un batch ->
    // l'envoie à ses pairs ->
    // le stocke dans la db ->
    // attend le quorum (un nombre suffisant de pairs qui confirment la reception du batch) (TODO) ->
    // calcule envoie le digest du batch au primary (TODO)
    // reçoit des batches d'autres workers et confirme leur reception (renvoie leur hash signé)
    pub fn new(
        config: WorkerConfig,
        network: NetworkInfos,
        starting_batch: Option<TxBatch>,
    ) -> Result<Self, WorkerIntialisationError> {
        let peers: Vec<WorkerInfo> = network
            .validators
            .iter()
            .filter(|v| v.pubkey != config.validator_pubkey)
            .flat_map(|v| v.workers.iter())
            .cloned()
            .collect();
        if peers.is_empty() {
            return Err(WorkerIntialisationError::NoPeers);
        }
        Ok(Self {
            config,
            current_batch: starting_batch.unwrap_or_default(),
            other_workers: peers,
        })
    }

    pub fn send_digest(_hash: Hash) {
        todo!()
    }

    pub fn broadcast_batch(_batch: TxBatch) {
        todo!()
    }
}

#[async_trait::async_trait]
impl Service for Worker {
    async fn start(&mut self, _join_set: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
        let (_transactions_tx, transactions_rx) = tokio::sync::mpsc::channel(20);
        tracing::info!("Worker {:?} Created", self.config.id);
        let (batches_tx, mut _batches_rx) = tokio::sync::mpsc::channel(20);
        tracing::info!("Spawning batch generator");

        let timeout = self.config.timeout;
        let batches_size = self.config.batch_size;

        tracing::info!("Worker {} | {} started", self.config.id, self.config.pubkey);

        let mut batch_maker = BatchMaker {
            current_batch: vec![],
            current_batch_size: 0,
            batches_tx,
            transactions_rx,
            timeout,
            max_batch_size: batches_size,
        };
        batch_maker.start(_join_set).await?;

        Ok(())
    }
}

#[derive(Debug)]
struct BatchMaker {
    current_batch: Vec<Transaction>,
    current_batch_size: usize,
    batches_tx: tokio::sync::mpsc::Sender<TxBatch>,
    transactions_rx: tokio::sync::mpsc::Receiver<Transaction>,
    timeout: u64,
    max_batch_size: usize,
}

#[async_trait::async_trait]
impl Service for BatchMaker {
    async fn start(&mut self, join_set: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
        let batches_tx = self.batches_tx.clone();
        let transactions_rx =
            std::mem::replace(&mut self.transactions_rx, tokio::sync::mpsc::channel(1).1); // Replace with dummy receiver
        let timeout = self.timeout;
        let max_batch_size = self.max_batch_size;

        join_set.spawn(async move {
            let mut service = BatchMaker {
                current_batch: Vec::new(),
                current_batch_size: 0,
                batches_tx,
                transactions_rx,
                timeout,
                max_batch_size,
            };
            service.run_forever().await?;
            Ok(())
        });
        Ok(())
    }
}

impl BatchMaker {
    async fn run_forever(&mut self) -> anyhow::Result<()> {
        loop {
            // Create a new timer at the start of each iteration
            let timer = tokio::time::sleep(Duration::from_millis(self.timeout));
            tokio::pin!(timer);
            loop {
                tokio::select! {
                    Some(tx) = self.transactions_rx.recv() => {
                        let serialized_tx = match bincode::serialize(&tx) {
                            Ok(serialized) => serialized,
                            Err(e) => {
                                tracing::error!("Failed to serialize transaction: {}", e);
                                continue;
                            }
                        };

                        let tx_size = serialized_tx.len();
                        self.current_batch.push(tx);
                        self.current_batch_size += tx_size;

                        if self.current_batch_size >= self.max_batch_size {
                            tracing::info!("batch size reached: worker sending batch of size {}", self.current_batch_size);
                            self.send_batch().await?;
                            break; // Break to create a new timer
                        }
                    }
                    _ = &mut timer => {
                        if !self.current_batch.is_empty() {
                            tracing::info!("batch timeout reached: worker sending batch of size {}", self.current_batch_size);
                            self.send_batch().await?;
                        }
                        break; // Break to create a new timer
                    }
                }
            }
        }
    }

    async fn send_batch(&mut self) -> anyhow::Result<()> {
        let batch = std::mem::take(&mut self.current_batch);
        self.current_batch_size = 0;

        self.batches_tx.send(batch).await.map_err(|e| {
            tracing::error!("channel error: failed to send batch: {}", e);
            anyhow::anyhow!("Failed to send batch: {}", e)
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use tokio::time;

    const MAX_BATCH_SIZE: usize = 200; // Size in bytes
    const TIMEOUT: u64 = 100; // 100ms
    const CHANNEL_CAPACITY: usize = 20;

    // Helper to create a transaction of a specific size
    fn create_test_tx(size: usize) -> Transaction {
        Transaction::new(vec![1u8; size])
    }

    type BatchMakerFixture = (
        tokio::sync::mpsc::Sender<Transaction>,
        tokio::sync::mpsc::Receiver<TxBatch>,
        JoinSet<anyhow::Result<()>>,
    );

    #[fixture]
    async fn launch_batch_maker() -> BatchMakerFixture {
        let (tx, rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let (batches_tx, batches_rx) = tokio::sync::mpsc::channel(CHANNEL_CAPACITY);
        let mut join_set = JoinSet::new();

        let mut batch_maker = BatchMaker {
            current_batch: vec![],
            current_batch_size: 0,
            batches_tx,
            transactions_rx: rx,
            timeout: TIMEOUT,
            max_batch_size: MAX_BATCH_SIZE,
        };

        batch_maker.start(&mut join_set).await.unwrap();

        (tx, batches_rx, join_set)
    }

    /// Test that the batch maker tasks does not send any batch if no transactions are received
    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_no_txs(#[future] launch_batch_maker: BatchMakerFixture) {
        let (_tx, mut batches_rx, _join_set) = launch_batch_maker.await;

        // Advance time past the timeout
        time::sleep(Duration::from_millis(TIMEOUT + 10)).await;

        // Try to receive a batch with a small timeout
        let receive_timeout =
            tokio::time::timeout(Duration::from_millis(10), batches_rx.recv()).await;

        // Verify no batch was received
        assert!(receive_timeout.is_err() || receive_timeout.unwrap().is_none());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_timeout_trigger(#[future] launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _join_set) = launch_batch_maker.await;

        // Send one small transaction (not enough to trigger size-based batch)
        let test_tx = create_test_tx(50); // 50 bytes
        tx.send(test_tx).await.unwrap();

        // Advance time past the timeout
        time::sleep(Duration::from_millis(TIMEOUT + 10)).await;

        // Should receive a batch with one transaction
        let batch = time::timeout(Duration::from_millis(10), batches_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(batch.len(), 1);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_size_trigger(#[future] launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _join_set) = launch_batch_maker.await;

        // Send transactions that will exceed MAX_BATCH_SIZE
        let tx_size = MAX_BATCH_SIZE / 2 + 1; // Two transactions will exceed batch size

        // Send first transaction
        tx.send(create_test_tx(tx_size)).await.unwrap();

        // Small delay to ensure ordering
        time::sleep(Duration::from_millis(1)).await;

        // Send second transaction - this should trigger the batch
        tx.send(create_test_tx(tx_size)).await.unwrap();

        // Should receive a batch without needing to advance time much
        let batch = tokio::time::timeout(Duration::from_millis(10), batches_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(batch.len(), 2);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_mixed_triggers(#[future] launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _join_set) = launch_batch_maker.await;

        // First batch: timeout trigger
        tx.send(create_test_tx(50)).await.unwrap();
        time::sleep(Duration::from_millis(TIMEOUT + 10)).await;

        let first_batch = tokio::time::timeout(Duration::from_millis(10), batches_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_batch.len(), 1);

        // Second batch: size trigger
        let tx_size = MAX_BATCH_SIZE / 2 + 1;
        tx.send(create_test_tx(tx_size)).await.unwrap();
        tx.send(create_test_tx(tx_size)).await.unwrap();

        let second_batch = tokio::time::timeout(Duration::from_millis(10), batches_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_batch.len(), 2);
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_rapid_transactions(#[future] launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _join_set) = launch_batch_maker.await;

        // Send many small transactions rapidly
        let small_tx_size = 10;
        let num_txs = 10;

        for _ in 0..num_txs {
            tx.send(create_test_tx(small_tx_size)).await.unwrap();
        }

        // Advance time to ensure processing
        time::advance(Duration::from_millis(10)).await;

        // Should still be accumulating since size not reached
        let timeout_result =
            tokio::time::timeout(Duration::from_millis(5), batches_rx.recv()).await;
        assert!(
            timeout_result.is_err(),
            "No batch should be sent before timeout"
        );

        // Advance to timeout
        time::sleep(Duration::from_millis(TIMEOUT)).await;

        // Now should receive all transactions in one batch
        let batch = tokio::time::timeout(Duration::from_millis(10), batches_rx.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(batch.len(), num_txs);
    }
}
