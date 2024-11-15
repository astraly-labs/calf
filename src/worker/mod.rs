use blake3::{self, Hash};
use std::time::Duration;

use crate::{
    config::{NetworkInfos, WorkerConfig, WorkerInfo},
    types::{Transaction, TxBatch},
};

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
            config: config,
            current_batch: starting_batch.unwrap_or(vec![]),
            other_workers: peers,
        })
    }
    pub async fn spawn(&self) -> Result<(), anyhow::Error> {
        let (_transactions_tx, transactions_rx) = tokio::sync::mpsc::channel(20);
        tracing::info!("Worker {:?} Created", self.config.id);
        let (batches_tx, mut _batches_rx) = tokio::sync::mpsc::channel(20);
        tracing::info!("Spawning batch generator");

        let timeout = self.config.timeout;
        let batches_size = self.config.batch_size;

        tracing::info!("Worker {} | {} started", self.config.id, self.config.pubkey);
        let handle = tokio::spawn(async move {
            batch_maker_task(timeout, batches_size, batches_tx, transactions_rx).await
        });

        let _ = handle.await?;
        Ok(())
    }

    pub fn send_digest(_hash: Hash) {
        todo!()
    }

    pub fn broadcast_batch(_batch: TxBatch) {
        todo!()
    }
}

// TODO: gérer les erreurs de serialisation
async fn batch_maker_task(
    timeout: u64, /* in ms */
    batches_size: usize,
    batches_tx: tokio::sync::mpsc::Sender<TxBatch>,
    mut transactions_rx: tokio::sync::mpsc::Receiver<Transaction>,
) -> Result<(), anyhow::Error> {
    let mut current_batch: Vec<Transaction> = vec![];
    let mut batch_size = 0;
    let timer = tokio::time::sleep(Duration::from_millis(timeout));
    tokio::pin!(timer); // magie
    loop {
        tokio::select! {
            Some(transaction) = transactions_rx.recv() => {
                let serialized_tx = match bincode::serialize(&transaction) {
                    Ok(serialized) => serialized,
                    Err(e) => {
                        tracing::error!("Failed to serialize transaction: {}", e);
                        // skipping this transactions if it can't be serialized: invalid
                        continue;
                    }
                };
                tracing::debug!("Worker received a transaction: {}", blake3::hash(&serialized_tx).to_hex());
                let tx_size = bincode::serialize(&transaction).unwrap().len();
                current_batch.push(transaction);
                batch_size += tx_size;
                if batch_size >= batches_size {
                    tracing::info!("batch size reached: worker sending batch of size {}", batch_size);
                    batches_tx.send(std::mem::take(&mut current_batch)).await.unwrap();
                    batch_size = 0;
                    timer.as_mut().reset(tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout));
                }
            },
            () = &mut timer => {
                if !current_batch.is_empty() {
                    tracing::info!("batch timeout reached: worker sending batch of size {}", batch_size);
                    match batches_tx.send(std::mem::take(&mut current_batch)).await {
                        Ok(_) => (),
                        Err(e) => {
                            tracing::error!("channel error: failed to send batch: {}", e);
                            return Err(e.into());
                        }
                    }
                    timer.as_mut().reset(tokio::time::Instant::now() + tokio::time::Duration::from_millis(timeout));
                }
            }
        }
    }
}

#[cfg(test)]
mod test {

    #[test]
    fn test_digest() {
        todo!()
    }

    #[test]
    fn test_batch() {
        todo!()
    }

    #[test]
    fn test_send_batch() {
        todo!()
    }

    #[test]
    fn test_broadcast() {
        todo!()
    }

    #[test]
    fn test_batch_maker() {
        todo!()
    }
}
