use crate::{
    settings::parser::Committee,
    types::{certificate::Certificate, dag::Dag, ReceivedObject, Round},
};
use proc_macros::Spawn;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Spawn)]
pub(crate) struct DagProcessor {
    peers_certificates_rx: broadcast::Receiver<ReceivedObject<Certificate>>,
    certificates_rx: mpsc::Receiver<Certificate>,
    rounds_tx: watch::Sender<(Round, Vec<Certificate>)>,
    committee: Committee,
}

impl DagProcessor {
    /// TODO: test, verify cerificates
    pub async fn run(mut self) -> Result<(), anyhow::Error> {
        let genesis = Certificate::genesis();
        let mut dag = Dag::new(genesis.clone())?;
        let mut my_previous_certificate = genesis.clone();
        let mut my_current_certificate = genesis;
        let mut round = 1;
        self.rounds_tx.send((1, vec![Certificate::genesis()]))?;
        loop {
            tokio::select! {
                Some(certificate) = self.certificates_rx.recv() => {
                    my_current_certificate = certificate.clone();
                    match dag.insert_certificate(certificate) {
                        Ok(()) => {
                            tracing::info!("certificate inserted in the DAG");
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate: {:?}", error);
                        }
                    }
                }
                Ok(certificate) = self.peers_certificates_rx.recv() => {
                    match dag.insert_certificate(certificate.object) {
                        Ok(()) => {
                            tracing::info!("certificate from {} inserted in the DAG", certificate.sender);
                        },
                        Err(error) => {
                            tracing::warn!("error inserting certificate from: {}, {:?}", certificate.sender, error);
                        }
                    }
                }
                else => break,
            }
            let connections = dag.count_all(|certificate| {
                certificate.round == my_previous_certificate.round + 1
                    && certificate.parents().contains(&my_previous_certificate)
            });

            if connections >= self.committee.quorum_threshold() as usize {
                self.rounds_tx.send((
                    round + 1,
                    dag.get_all(|certificate| certificate.round == round)
                        .into_iter()
                        .cloned()
                        .collect(),
                ))?;
                round += 1;
                my_previous_certificate = my_current_certificate.clone();
            }
        }
        Ok(())
    }
}
