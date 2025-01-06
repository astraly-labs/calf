#[cfg(test)]
pub mod fixtures {
    use std::sync::Arc;

    use tokio::sync::{broadcast, Mutex};
    use tokio_util::sync::CancellationToken;

    use crate::{
        db::{Column, Db},
        primary::DigestReceiver,
        settings::parser::{Committee, FileLoader},
        types::{
            batch::{Batch, BatchId},
            network::{ObjectSource, ReceivedObject},
            traits::{AsHex, Hash, Random},
            transaction::Transaction,
        },
        utils::CircularBuffer,
    };

    pub const CHANNEL_CAPACITY: usize = 1000;
    pub const BUFFER_CAPACITY: usize = 10;
    pub const RANDOM_BATCH_SIZE: usize = 10;
    pub const COMMITTEE_PATH: &str = "committee.json";
    pub const GENESIS_SEED: [u8; 32] = [0; 32];

    pub type DigestReceiverFixture = (
        broadcast::Sender<ReceivedObject<(BatchId, ObjectSource)>>,
        Arc<Mutex<CircularBuffer<BatchId>>>,
        Arc<Db>,
        CancellationToken,
    );

    pub fn load_committee(path: &str) -> Committee {
        Committee::load_from_file(path).unwrap()
    }

    pub fn launch_digest_receiver(db_path: &str) -> DigestReceiverFixture {
        let (digest_tx, digest_rx) = broadcast::channel(CHANNEL_CAPACITY);
        let buffer = Arc::new(Mutex::new(CircularBuffer::new(BUFFER_CAPACITY)));
        let db = Arc::new(Db::new(db_path.into()).unwrap());
        let buffer_clone = buffer.clone();
        let db_clone = db.clone();
        let token = CancellationToken::new();
        let token_clone = token.clone();
        let _ = tokio::spawn(async move {
            DigestReceiver::spawn(token_clone, digest_rx, buffer_clone, db_clone)
                .await
                .unwrap();
        });
        (digest_tx, buffer, db, token)
    }

    pub fn random_digests(count: usize) -> Vec<BatchId> {
        (0..count)
            .map(|_| {
                Batch::<Transaction>::random(RANDOM_BATCH_SIZE)
                    .digest()
                    .into()
            })
            .collect()
    }

    pub fn check_storage_for_digests(db: &Db, digests: &[BatchId]) {
        for digest in digests {
            let stored_digest: BatchId = db
                .get(Column::Digests, &digest.0.as_hex_string())
                .unwrap()
                .unwrap();
            assert_eq!(stored_digest, *digest);
        }
    }
}
