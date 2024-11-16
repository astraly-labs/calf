pub mod batch_maker;
pub mod batch_broadcaster;

use async_channel::{bounded, Receiver};
use batch_broadcaster::BatchBroadcaster;
use batch_maker::BatchMaker;
use blake3::{self, Hash};
use tokio::task::JoinSet;

use crate::{
    config::{NetworkInfos, WorkerConfig, WorkerInfo},
    db::Db,
    types::{
        services::{Service, ServiceGroup},
        Transaction, TxBatch,
    },
};

#[derive(Debug)]
pub struct Worker {
    pub config: WorkerConfig,
    pub current_batch: Vec<Transaction>,
    pub other_workers: Vec<WorkerInfo>,
    pub db: Db,
    pub transactions_rx: Receiver<Transaction>,
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
        db: Db,
        transactions_rx: Receiver<Transaction>,
    ) -> Result<Self, WorkerIntialisationError> {
        let peers: Vec<WorkerInfo> = network
            .validators
            .iter()
            .filter(|v| v.pubkey != config.validator_pubkey)
            .flat_map(|v| v.workers.iter())
            .cloned()
            .collect();
        // if peers.is_empty() {
        //     return Err(WorkerIntialisationError::NoPeers);
        // }

        Ok(Self {
            config,
            current_batch: vec![],
            other_workers: peers,
            db,
            transactions_rx,
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
        tracing::info!("Worker {:?} Created", self.config.id);
        let (batches_tx, batches_rx) = bounded(20);

        let timeout = self.config.timeout;
        let batches_size = self.config.batch_size;

        tracing::info!("Worker {} | {} started", self.config.id, self.config.pubkey);

        tracing::info!("Spawning batch maker");

        let batch_maker = BatchMaker::new(
            batches_tx,
            self.transactions_rx.clone(),
            timeout,
            batches_size,
        );

        let batch_sender = BatchBroadcaster::new(batches_rx);

        ServiceGroup::default()
            .with(batch_maker)
            .with(batch_sender)
            .start_and_drive_to_end()
            .await?;

        Ok(())
    }
}

