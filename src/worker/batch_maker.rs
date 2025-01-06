use proc_macros::Spawn;
use std::time::Duration;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

use crate::types::batch::Batch;
use crate::types::traits::{AsHex, Hash};
use crate::types::transaction::Transaction;

#[derive(Spawn)]
pub(crate) struct BatchMaker {
    batches_tx: tokio::sync::broadcast::Sender<Batch<Transaction>>,
    transactions_rx: Receiver<Transaction>,
    timeout: u64,
    max_batch_size: usize,
}

impl BatchMaker {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut current_batch: Vec<Transaction> = vec![];
        let mut current_batch_size = 0;
        let timer = tokio::time::sleep(Duration::from_millis(self.timeout));
        tokio::pin!(timer);
        loop {
            let sender = self.batches_tx.clone();
            tokio::select! {
                Some(tx) = self.transactions_rx.recv() => {
                    tracing::info!("received transaction: {}", tx.digest().as_hex_string());
                    let serialized_tx = match bincode::serialize(&tx) {
                        Ok(serialized) => serialized,
                        Err(e) => {
                            tracing::error!("Failed to serialize transaction: {}", e);
                            continue;
                        }
                    };

                    let tx_size = serialized_tx.len();
                    current_batch.push(tx);
                    current_batch_size += tx_size;

                    if current_batch_size >= self.max_batch_size {
                        tracing::info!("batch size reached: worker sending batch of size {}", current_batch_size);
                        send_batch(sender, Batch::new(std::mem::take(&mut current_batch))).await?;
                        current_batch_size = 0;
                        timer.as_mut().reset(tokio::time::Instant::now() + tokio::time::Duration::from_millis(self.timeout));
                    }
                },
                _ = &mut timer => {
                    if !current_batch.is_empty() {
                        tracing::info!("batch timeout reached: worker sending batch of size {}", current_batch_size);
                        send_batch(sender, Batch::new(std::mem::take(&mut current_batch))).await?;

                    }
                    tracing::info!("batch timeout reached... doing nothing");
                    timer.as_mut().reset(tokio::time::Instant::now() + tokio::time::Duration::from_millis(self.timeout));
                }
            }
        }
    }
}

async fn send_batch(
    batches_tx: tokio::sync::broadcast::Sender<Batch<Transaction>>,
    batch: Batch<Transaction>,
) -> anyhow::Result<()> {
    batches_tx.send(batch).map_err(|e| {
        tracing::error!("channel error: failed to send batch: {}", e);
        anyhow::anyhow!("Failed to send batch: {}", e)
    })?;

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use rstest::*;
    use tokio::{sync::mpsc, task::JoinHandle, time};

    const MAX_BATCH_SIZE: usize = 1000; // Size in bytes
    const TIMEOUT: u64 = 100; // 100ms
    const CHANNEL_CAPACITY: usize = 1000;

    // Helper to create a transaction of a specific size
    fn create_test_tx(size: usize) -> Transaction {
        Transaction::new(vec![1u8; size])
    }

    type BatchMakerFixture = (
        mpsc::Sender<Transaction>,
        tokio::sync::broadcast::Receiver<Batch<Transaction>>,
        JoinHandle<()>,
    );

    #[fixture]
    fn launch_batch_maker() -> BatchMakerFixture {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (batches_tx, batches_rx) = tokio::sync::broadcast::channel(CHANNEL_CAPACITY);
        let handle = BatchMaker::spawn(
            CancellationToken::new(),
            batches_tx,
            rx,
            TIMEOUT,
            MAX_BATCH_SIZE,
        );

        (tx, batches_rx, handle)
    }

    /// Test that the batch maker tasks does not send any batch if no transactions are received
    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_no_txs(launch_batch_maker: BatchMakerFixture) {
        let (_tx, mut batches_rx, _) = launch_batch_maker;

        // Advance time past the timeout
        time::sleep(Duration::from_millis(TIMEOUT + 10)).await;

        // Try to receive a batch with a small timeout
        let receive_timeout =
            tokio::time::timeout(Duration::from_millis(10), batches_rx.recv()).await;

        // Verify no batch was received
        assert!(receive_timeout.is_err());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_batch_maker_timeout_trigger(launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _) = launch_batch_maker;

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
    async fn test_batch_maker_size_trigger(launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _) = launch_batch_maker;

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
    async fn test_batch_maker_mixed_triggers(launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _) = launch_batch_maker;

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
    async fn test_batch_maker_rapid_transactions(launch_batch_maker: BatchMakerFixture) {
        let (tx, mut batches_rx, _) = launch_batch_maker;

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
