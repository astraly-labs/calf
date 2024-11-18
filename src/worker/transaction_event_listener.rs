use std::time::Duration;

use crossterm::event::{Event, EventStream, KeyCode};
use futures_timer::Delay;
use tokio::sync::mpsc;
use crate::types::Transaction;

pub struct TransactionEventListener {
    tx: mpsc::Sender<Transaction>,
}

impl TransactionEventListener {
    #[must_use]
    pub fn spawn(tx: mpsc::Sender<Transaction>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            Self { tx }.run().await;
        })
    }

    pub async fn run(self) {
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
                            self.tx.send(transaction.clone()).await.unwrap();
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
    }
}
