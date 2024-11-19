use tokio::{
    sync::{broadcast, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::types::{BlockHeader, NetworkRequest, RequestPayload};

pub(crate) struct HeaderProcessor {
    header_rx: broadcast::Receiver<BlockHeader>,
    network_tx: mpsc::Sender<NetworkRequest>,
}

impl HeaderProcessor {
    #[must_use]
    pub fn spawn(
        header_rx: broadcast::Receiver<BlockHeader>,
        network_tx: mpsc::Sender<NetworkRequest>,
        cancellation_token: CancellationToken,
    ) -> JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(
                    Self {
                        header_rx,
                        network_tx,
                    }
                    .run(),
                )
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => {
                            tracing::info!("HeaderProcessor finnished");
                        }
                        Err(e) => {
                            tracing::error!("HeaderProcessor finished with Error: {:?}", e);
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {
                    tracing::info!("HeaderProcessor cancelled");
                }
            };
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                Ok(header) = self.header_rx.recv() => {
                    tracing::info!("🔨 Processing header...");

                    // Checks
                    // 1. is the header built by an authority

                    // 2. is the header the first one they see for this round



                    // Then
                    // 1. Send back a vote for it
                    // 2. Store it in db

                }
            }
        }
    }
}
