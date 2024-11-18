use std::time::Duration;

use crate::{safe_send, types::Transaction};
use crossterm::event::{Event, EventStream, KeyCode};
use futures_timer::Delay;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub struct TransactionEventListener {
    tx: mpsc::Sender<Transaction>,
}

impl TransactionEventListener {
    #[must_use]
    pub fn spawn(
        tx: mpsc::Sender<Transaction>,
        cancellation_token: CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let res = cancellation_token
                .run_until_cancelled(Self { tx }.run())
                .await;

            match res {
                Some(res) => {
                    match res {
                        Ok(_) => tracing::info!("TransactionEventListener terminated successfully"),
                        Err(e) => {
                            tracing::error!(
                                "TransactionEventListener terminated with error: {:?}",
                                e
                            );
                        }
                    };
                    cancellation_token.cancel();
                }
                None => {}
            };
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let transaction = Transaction { data: vec![1; 100] };

        let mut reader = EventStream::new();

        loop {
            let delay = futures::FutureExt::fuse(Delay::new(Duration::from_millis(1_000)));
            let event = futures::FutureExt::fuse(futures::StreamExt::next(&mut reader));

            tokio::select! {
                _ = delay => { },
                maybe_event = event => {
                    match maybe_event {
                        Some(Ok(event)) => {
                            if event == Event::Key(KeyCode::Char('t').into()) {
                                safe_send!(self.tx, transaction.clone(), "failed to send transaction");
                                tracing::info!("transaction sent");
                            }

                            if event == Event::Key(KeyCode::Esc.into()) {
                                break;
                            }
                        }
                        Some(Err(e)) => tracing::error!("Transaction Sender Error: {:?}\r", e),
                        None => break,
                    }
                }
            };
        }
        Ok(())
    }
}
